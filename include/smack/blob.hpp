#ifndef __SMACK_BLOB_HPP
#define __SMACK_BLOB_HPP

#include <sys/types.h>
#include <sys/stat.h>

#include <unistd.h>

#include <set>
#include <algorithm>

#include <boost/version.hpp>

#include <boost/thread/condition.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/copy.hpp>

#include <smack/base.hpp>

namespace ioremap { namespace smack {

typedef std::map<key, std::string, keycomp> cache_t;

/* index offset within global index file, not within chunk */
typedef std::map<key, size_t, keycomp> rcache_t;

namespace bio = boost::iostreams;

#if BOOST_VERSION < 104400
#define file_desriptor_close_handle	false
#else
#define file_desriptor_close_handle	bio::never_close_handle
#endif

#define smack_time_diff(s, e) ((e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec))
#define smack_rcache_mult	10000

struct chunk_ctl {
	uint64_t		index_offset;		/* index offset in index file for given chunk */
	uint64_t		data_offset;		/* data offset in data file for given chunk */
	uint64_t		data_size;		/* size of (compressed) data on disk */
	int			num;			/* number of records in the chunk */
	int			bloom_size;		/* bloom size in bytes */
} __attribute__ ((packed));

class chunk : public bloom {
	public:
		chunk(int bloom_size = 128) : bloom(bloom_size)
		{
			memset(&m_ctl, 0, sizeof(struct chunk_ctl));
			m_ctl.bloom_size = bloom_size;
		}	

		chunk(struct chunk_ctl &ctl, std::vector<char> &data) :
		bloom(data)
		{
			memcpy(&m_ctl, &ctl, sizeof(struct chunk_ctl));
			m_ctl.bloom_size = data.size();
		}

		chunk(const chunk &ch) : bloom(ch.data()) {
			m_start = ch.m_start;
			m_end = ch.m_end;
			m_ctl = ch.m_ctl;

			std::copy(ch.m_rcache.begin(), ch.m_rcache.end(), std::inserter(m_rcache, m_rcache.end()));
		}

		struct chunk_ctl *ctl(void) {
			return &m_ctl;
		}

		key &start(void) {
			return m_start;
		}

		key &end(void) {
			return m_end;
		}

		/* this must be (and it is) single-threaded operation */
		void rcache_add(const key &key, size_t offset) {
			std::pair<rcache_t::iterator, bool> p = m_rcache.insert(std::make_pair(key, offset));

			if (!p.second)
				p.first->second = offset;
		}

		bool rcache_find(const key &key, size_t &idx_start, size_t &idx_end) {
			if (m_rcache.size() == 0) {
				if (key > m_end)
					return false;

				idx_start = m_ctl.index_offset;
				idx_end = m_ctl.index_offset + m_ctl.num * sizeof(struct index);
				return true;
			}

			rcache_t::iterator it = m_rcache.upper_bound(key);
			if (it == m_rcache.begin()) {
				if (key < m_start)
					return false;

				idx_start = m_ctl.index_offset;
				idx_end = it->second;
				return true;
			}

			if (it == m_rcache.end()) {
				if (key > m_end)
					return false;

				idx_start = m_rcache.rbegin()->second;
				idx_end = m_ctl.index_offset + m_ctl.num * sizeof(struct index);
				return true;
			}

			idx_end = it->second;
			--it;
			idx_start = it->second;
			return true;
		}

	private:
		struct chunk_ctl m_ctl;
		key m_start, m_end;
		rcache_t m_rcache;
};

class blob_store {
	public:
		blob_store(const std::string &path, int bloom_size) :
		m_path_base(path),
		m_data(path + ".data"),
		m_index(path + ".index"),
		m_chunk(path + ".chunk"),
		m_bloom_size(bloom_size)
		{
			log(SMACK_LOG_NOTICE, "blob-store: %s, bloom-size: %d\n", path.c_str(), bloom_size);
		}

		/* returns index offset of the new chunk written */
		chunk store_chunk(cache_t &cache, size_t num, size_t max_cache_size) {
			chunk ch(m_bloom_size);

			bio::file_descriptor_sink dst_idx(m_index.get(), file_desriptor_close_handle);
#if BOOST_VERSION < 104400
			bio::file_descriptor_sink dst_data(dup(m_data.get()), file_desriptor_close_handle);
#else
			bio::file_descriptor_sink dst_data(m_data.get(), file_desriptor_close_handle);
#endif

			try {
				m_index.set_size(bio::seek<bio::file_descriptor_sink>(dst_idx, 0, std::ios_base::end));
				m_data.set_size(bio::seek<bio::file_descriptor_sink>(dst_data, 0, std::ios_base::end));
			} catch (const std::exception &e) {
				log(SMACK_LOG_ERROR, "%s: store-chunk: start: %s, end: %s, "
						"index-fd: %d, data-fd: %d, num: %zd, cache-size: %zd: %s\n",
						m_path_base.c_str(), ch.start().str(), ch.end().str(),
						m_index.get(), m_data.get(), num, cache.size(),	e.what());
				throw;
			}

			size_t data_offset = 0;
			size_t index_offset = m_index.size();

			ch.ctl()->index_offset = index_offset;
			ch.ctl()->data_offset = m_data.size();

			ch.start().set(cache.begin()->first.idx());
			ch.end().set(cache.rbegin()->first.idx());

			{
				bio::filtering_streambuf<bio::output> out;
				out.push(bio::zlib_compressor());
				out.push(dst_data);

				out.set_auto_close(false);

				size_t count = 0;
				cache_t::iterator it;
				int step = cache.size();
				if (max_cache_size)
					step = std::min<size_t>(cache.size(), num) / max_cache_size + 1;

				int st = 0;
				for (it = cache.begin(); it != cache.end(); ++it) {
					struct index idx = *it->first.idx();
					idx.data_offset = data_offset;
					idx.data_size = it->second.size();

					bio::write<bio::file_descriptor_sink>(dst_idx, (char *)&idx, sizeof(struct index));
					bio::write<bio::filtering_streambuf<bio::output> >(out, it->second.data(), it->second.size());

					ch.add((char *)idx.id, SMACK_KEY_SIZE);

					if (++st == step) {
						key k(&idx);
						ch.rcache_add(k, index_offset);
						st = 0;
					}

					index_offset += sizeof(struct index);
					data_offset += it->second.size();

					if (++count == num) {
						ch.end().set(it->first.idx());
						++it;
						break;
					}
				}

				cache.erase(cache.begin(), it);
				out.strict_sync();

				ch.ctl()->num = count;
			}

			/*
			 * Looks like streambuf deletes all components we pushed, including 'sink' which
			 * is dst_data in our case
			 */
			bio::file_descriptor_sink dst_data_tmp(m_data.get(), file_desriptor_close_handle);
			size_t data_size = bio::seek<bio::file_descriptor_sink>(dst_data_tmp, 0, std::ios_base::end);

			/*
			 * streambuf out must be destroyed, since it looks like it writes data
			 * to 'sink' somewhere around its destructor.
			 * strict_sync() _does_not_ ensures that
			 */
			ch.ctl()->data_size = data_size - ch.ctl()->data_offset;

			store_chunk_meta(ch);

			log(SMACK_LOG_NOTICE, "%s: store-chunk: start: %s, end: %s, index-fd: %d, index-offset: %zd, num: %d, "
					"data-fd: %d, data-offset: %zd, uncompressed-data-size: %zd, compressed-data-size: %zd\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(), m_index.get(), ch.ctl()->index_offset,
					ch.ctl()->num, m_data.get(), ch.ctl()->data_offset, data_offset, ch.ctl()->data_size);

			return ch;
		}

		void write_raw(chunk &ch, int src_index_fd, int src_data_fd) {
			bio::file_descriptor_source src_idx(src_index_fd, file_desriptor_close_handle);
			bio::file_descriptor_source src_data(src_data_fd, file_desriptor_close_handle);

			bio::file_descriptor_sink dst_idx(m_index.get(), file_desriptor_close_handle);
			bio::file_descriptor_sink dst_data(m_data.get(), file_desriptor_close_handle);

			size_t data_offset = bio::seek<bio::file_descriptor_sink>(dst_data, 0, std::ios_base::end);
			size_t index_offset = bio::seek<bio::file_descriptor_sink>(dst_idx, 0, std::ios_base::end);

			bio::copy<bio::file_descriptor_source, bio::file_descriptor_sink>(src_data, dst_data, ch.ctl()->data_size);
			bio::copy<bio::file_descriptor_source, bio::file_descriptor_sink>(src_idx, dst_idx, ch.ctl()->num * sizeof(struct index));

			store_chunk_meta(ch);

			log(SMACK_LOG_NOTICE, "%s: write-raw: start: %s, end: %s, data-offset: %zd, data-size: %zd, "
					"index-offset: %zd, index-size: %zd, num: %d\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(),
					data_offset, ch.ctl()->data_size, index_offset, ch.ctl()->num * sizeof(struct index), ch.ctl()->num);
		}

		void copy_chunk(chunk &ch, blob_store &dst) {
			dst.write_raw(ch, m_index.get(), m_data.get());
		}

		void read_chunk(chunk &ch, cache_t &cache) {
			bio::file_descriptor_source src_idx(m_index.get(), file_desriptor_close_handle);
			bio::seek<bio::file_descriptor_source>(src_idx, ch.ctl()->index_offset, std::ios_base::beg);

#if BOOST_VERSION < 104400
			bio::file_descriptor_source src_data(dup(m_data.get()), file_desriptor_close_handle);
#else
			bio::file_descriptor_source src_data(m_data.get(), file_desriptor_close_handle);
#endif
			bio::seek<bio::file_descriptor_source>(src_data, ch.ctl()->data_offset, std::ios_base::beg);

			bio::filtering_streambuf<bio::input> in;
			in.push(bio::zlib_decompressor());
			in.push(src_data);
			in.set_auto_close(false);

			struct timeval start, end;
			gettimeofday(&start, NULL);

			struct index idx;
			for (int i = 0; i < ch.ctl()->num; ++i) {
				bio::read<bio::file_descriptor_source>(src_idx, (char *)&idx, sizeof(struct index));

				std::string str;
				str.resize(idx.data_size);

				bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)str.data(), str.size());
				cache.insert(std::make_pair(key(&idx), str));
			}
			gettimeofday(&end, NULL);

			long read_time = smack_time_diff(start, end);

			log(SMACK_LOG_NOTICE, "%s: read-chunk: start: %s, end: %s, num: %d, read-time: %ld usecs\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(), ch.ctl()->num, read_time);
		}

		void read_index(std::map<key, chunk, keycomp> &chunks, std::vector<chunk> &chunks_unsorted, size_t max_rcache_size) {
			try {
				read_chunks(chunks, chunks_unsorted, max_rcache_size);
			} catch (const std::runtime_error &e) {
				log(SMACK_LOG_ERROR, "%s: read chunks failed: %s\n", m_path_base.c_str(), e.what());
				throw;
			}
		}

		bool chunk_read(key &key, chunk &ch, std::string &ret) {
			struct timeval start, lookup_time, seek_time, decompress_time;

			gettimeofday(&start, NULL);
			if (!ch.check((char *)key.id(), SMACK_KEY_SIZE)) {
				log(SMACK_LOG_DSA, "%s: %s: chunk start: %s, end: %s: bloom-check failed\n",
						m_path_base.c_str(), key.str(), ch.start().str(), ch.end().str());
				return false;
			}

			size_t index_offset, next_index_offset;
			bool found = ch.rcache_find(key, index_offset, next_index_offset);
			if (!found) {
				log(SMACK_LOG_DSA, "%s: %s: chunk start: %s, end: %s: rcache lookup failed\n",
						m_path_base.c_str(), key.str(), ch.start().str(), ch.end().str());
				return false;
			}

			struct index_lookup l;
			memset(&l, 0, sizeof(struct index_lookup));

			l.index_offset[0] = index_offset;
			l.index_offset[1] = next_index_offset;

			found = m_index.lookup(key, l);
			if (!found || !l.exact) {
				log(SMACK_LOG_DSA, "%s: %s: chunk start: %s, end: %s: bsearch lookup failed in: %zd-%zd\n",
						m_path_base.c_str(), key.str(), ch.start().str(), ch.end().str(),
						index_offset, next_index_offset);
				return false;
			}

			gettimeofday(&lookup_time, NULL);

			long lookup_diff = smack_time_diff(start, lookup_time);

			log(SMACK_LOG_NOTICE, "%s: %s: chunk start: %s, end: %s: chunk-read: req-index-offset: %zd-%zd, "
					"index-offset: %zd, data-offset: %zd, chunk-start-offset: %zd, data-size: %zd, num: %d, "
					"lookup-time: %ld usecs\n",
					m_path_base.c_str(), key.str(), ch.start().str(), ch.end().str(), index_offset, next_index_offset,
					l.index_offset[0], l.data_offset, ch.ctl()->data_offset, l.data_size, ch.ctl()->num,
					lookup_diff);

#if BOOST_VERSION < 104400
			bio::file_descriptor_source src_data(dup(m_data.get()), file_desriptor_close_handle);
#else
			bio::file_descriptor_source src_data(m_data.get(), file_desriptor_close_handle);
#endif

			/* seeking to the start of the chunk */
			size_t pos = bio::seek<bio::file_descriptor_source>(src_data, ch.ctl()->data_offset, std::ios_base::beg);
			if (pos != ch.ctl()->data_offset) {
				std::ostringstream str;
				str << m_path_base << ": " << key.str() << ": read: could not seek to: " <<
					l.data_offset << ", seeked to: " << pos;
				throw std::out_of_range(str.str());
			}

			gettimeofday(&seek_time, NULL);

			bio::filtering_streambuf<bio::input> in;
			in.push(bio::zlib_decompressor());
			in.push(src_data);
			in.set_auto_close(false);

#if 1
			std::ostringstream str;
			bio::copy(in, str, l.data_size + l.data_offset);
			ret = str.str().substr(l.data_offset, l.data_size);
#else
			m_data.read((char *)ret.data(), (ch.ctl()->data_offset + l.data_offset) % m_data.size(), l.data_size);
#endif
			gettimeofday(&decompress_time, NULL);

			long seek_diff = smack_time_diff(lookup_time, seek_time);
			long decompress_diff = smack_time_diff(seek_time, decompress_time);

			log(SMACK_LOG_INFO, "%s: %s: chunk start: %s, end: %s: chunk-read: req-index-offset: %zd-%zd, "
					"index-offset: %zd, data-offset: %zd, chunk-start-offset: %zd, data-size: %zd, num: %d, "
					"lookup-time: %ld, seek-time: %ld, decompress-time: %ld usecs\n",
					m_path_base.c_str(), key.str(), ch.start().str(), ch.end().str(), index_offset, next_index_offset,
					l.index_offset[0], l.data_offset, ch.ctl()->data_offset, l.data_size, ch.ctl()->num,
					lookup_diff, seek_diff, decompress_diff);

			return true;
		}

		void truncate() {
			m_data.truncate(0);
			m_index.truncate(0);
			m_chunk.truncate(0);
		}

		/* returns current number of records and data size on disk */
		void size(size_t &num, size_t &data_size) {
			bio::file_descriptor_sink idx(m_index.get(), file_desriptor_close_handle);
			bio::file_descriptor_sink data(m_data.get(), file_desriptor_close_handle);

			num = bio::seek<bio::file_descriptor_sink>(idx, 0, std::ios_base::end) / sizeof(struct index);
			data_size = bio::seek<bio::file_descriptor_sink>(data, 0, std::ios_base::end);
		}

	private:
		std::string m_path_base;
		mmap m_data;
		file_index m_index;
		mmap m_chunk;
		int m_bloom_size;

		void store_chunk_meta(chunk &ch) {
			m_chunk.write((char *)ch.ctl(), m_chunk.size(), sizeof(struct chunk_ctl));
			m_chunk.write((char *)ch.data().data(), m_chunk.size(), ch.data().size());
		}

		void read_chunks(std::map<key, chunk, keycomp> &chunks, std::vector<chunk> &chunks_unsorted, size_t max_rcache_size) {
			size_t offset = 0;
			while (offset < m_chunk.size()) {
				struct chunk_ctl ctl;

				m_chunk.read((char *)&ctl, offset, sizeof(struct chunk_ctl));

				std::vector<char> data(ctl.bloom_size);
				m_chunk.read(data.data(), offset + sizeof(struct chunk_ctl), data.size());

				chunk ch(ctl, data);

				struct index tmp;

				int step = ctl.num;
				if (max_rcache_size)
					step = ctl.num / max_rcache_size + 1;

				m_index.read((char *)&tmp, ctl.index_offset, sizeof(struct index));
				ch.start().set(&tmp);

				for (size_t off = ctl.index_offset + step * sizeof(struct index);
						off < ctl.index_offset + ctl.num * sizeof(struct index);
						off += step * sizeof(struct index)) {

					m_index.read((char *)&tmp, off, sizeof(struct index));
					key k(&tmp);
					ch.rcache_add(k, off);
				}

				m_index.read((char *)&tmp, ctl.index_offset + (ctl.num - 1) * sizeof(struct index), sizeof(struct index));
				ch.end().set(&tmp);

				log(SMACK_LOG_NOTICE, "%s: read_chunks: %zd: index-offset: %zd, data-offset: %zd, "
						"compressed-size: %zd, num: %d, bloom-size: %d, start: %s, end: %s\n",
						m_path_base.c_str(), chunks.size(), ctl.index_offset, ctl.data_offset, ctl.data_size,
						ctl.num, ctl.bloom_size, ch.start().str(), ch.end().str());

				if ((chunks.size() == 0) || (ch.start() >= chunks.rbegin()->second.end()))
					chunks.insert(std::make_pair(ch.start(), ch));
				else
					chunks_unsorted.push_back(ch);

				offset += sizeof(struct chunk_ctl) + ctl.bloom_size;
			}
		}
};

template <class filter_t>
class blob {
	public:
		blob(const std::string &path, int bloom_size, size_t max_cache_size) :
		m_path(path),
		m_cache_size(max_cache_size),
		m_bloom_size(bloom_size),
		m_chunk_idx(0)
		{
			time_t mtime = 0;
			ssize_t size = 0;
			int idx = -1;

			for (int i = 0; i < 2; ++i) {
				struct stat st;
				int err;

				std::string prefix = path + "." + boost::lexical_cast<std::string>(i);

				err = stat((prefix + ".data").c_str(), &st);
				if (err == 0) {
					log(SMACK_LOG_NOTICE, "%s: old-idx: %d, old-mtime: %ld, old-size: %zd, mtime: %ld, size: %zd\n",
							prefix.c_str(), idx, mtime, size, st.st_mtime, st.st_size);
					if (st.st_mtime > mtime) {
						mtime = st.st_mtime;
						size = st.st_size;
						idx = i;
					} else if (st.st_mtime == mtime) {
						if (st.st_size > size) {
							idx = i;
							mtime = st.st_mtime;
							size = st.st_size;
						}
					}
				}

				m_files.push_back(boost::shared_ptr<blob_store>(new blob_store(prefix, m_bloom_size)));
			}

			if (idx != -1) {
				m_chunk_idx = idx;
				m_files[idx]->read_index(m_chunks, m_chunks_unsorted, m_cache_size * sizeof(key) / smack_rcache_mult);
				log(SMACK_LOG_INFO, "%s: read-index: idx: %d, sorted: %zd, unsorted: %zd\n",
						m_path.c_str(), idx, m_chunks.size(), m_chunks_unsorted.size());

				if (m_chunks_unsorted.size()) {
					cache_t cache;
					chunks_resort(cache);
				}
			}

			if (m_chunks.size()) {
				m_start = m_chunks.begin()->second.start();
			}
		}

		bool write(const key &key, const char *data, size_t size) {
			boost::lock_guard<boost::mutex> guard(m_write_lock);

			m_remove_cache.erase(key);

			std::pair<typename cache_t::iterator, bool> ret =
				m_wcache.insert(std::make_pair(key, std::string(data, size)));
			if (!ret.second)
				ret.first->second = std::string(data, size);

			return m_wcache.size() >= m_cache_size;
		}

		std::string read(key &key) {
			boost::mutex::scoped_lock guard(m_write_lock);

			/*
			 * First, check remove cache
			 * Write operation updates it first, so if something is here,
			 * then we removed object and did not write anything above
			 */
			if (m_remove_cache.find(key) != m_remove_cache.end()) {
				std::ostringstream str;
				str << key.str() << ": blob::read::in-removed-cache";
				throw std::out_of_range(str.str());
			}

			/*
			 * Second, check write cache
			 * If something is found, return it from cache
			 */
			cache_t::iterator it = m_wcache.find(key);
			if (it != m_wcache.end()) {
				struct index *idx = (struct index *)key.idx();
				idx->data_offset = 0;
				idx->data_size = it->second.size();
				return it->second;
			}

			/*
			 * that's a tricky place
			 * we lock m_disk_lock to prevent modification of disk indexes
			 * while doing lookup, but we have to lock it under m_wcache_lock
			 * to prevent race where m_wcache can be switched with temporal map,
			 * but not yet written to disk index
			 */
			boost::mutex::scoped_lock disk_guard(m_disk_lock);
			guard.unlock();

			std::string ret;
			bool found = false;

			if (m_chunks.size()) {
				std::map<class key, chunk, keycomp>::iterator it = m_chunks.upper_bound(key);
				if (it == m_chunks.begin()) {
					found = current_bstore()->chunk_read(key, it->second, ret);
				} else {
					--it;

					found = current_bstore()->chunk_read(key, it->second, ret);
					if (!found && (key > it->second.end())) {
						++it;

						if (it != m_chunks.end())
							found = current_bstore()->chunk_read(key, it->second, ret);
					}
				}

				if (found)
					return ret;
			}

			if (!found) {
				for (std::vector<chunk>::reverse_iterator it = m_chunks_unsorted.rbegin(); it != m_chunks_unsorted.rend(); ++it) {
					log(SMACK_LOG_NOTICE, "%s: read key: unsorted chunk: start: %s, end: %s\n",
							key.str(), it->start().str(), it->end().str());
					if (key < it->start())
						continue;
					if (key > it->end())
						continue;

					found = current_bstore()->chunk_read(key, *it, ret);
					if (found)
						return ret;
				}
			}

			std::ostringstream str;
			str << key.str() << ": read: no data";
			throw std::out_of_range(str.str());
		}

		bool remove(const key &key) {
			boost::mutex::scoped_lock guard(m_write_lock);
			m_remove_cache.insert(key);
			m_wcache.erase(key);
			return m_remove_cache.size() > m_cache_size;
		}

		std::string lookup(key &) {
			return std::string();
		}

		key &start() {
			return m_start;
		}

		bool write_cache() {
			boost::mutex::scoped_lock write_guard(m_write_lock);

			cache_t tmp;
			m_wcache.swap(tmp);
			write_guard.unlock();

			boost::mutex::scoped_lock disk_guard(m_disk_lock);

			if ((m_chunks_unsorted.size() > 50) || m_split_dst) {
				chunks_resort(tmp);

				if (m_split_dst) {
					write_guard.lock();
					/*
					 * check if someone added data into wcache while we processed data on disk
					 * write cache lock is still being held
					 */
					cache_t::iterator wcache_split_it = m_wcache.lower_bound(m_split_dst->start());
					for (cache_t::iterator it = wcache_split_it; it != m_wcache.end(); ++it)
						m_split_dst->write(it->first, it->second.data(), it->second.size());

					m_wcache.erase(wcache_split_it, m_wcache.end());

					m_split_dst.reset();
				}
			} else if (tmp.size()) {
				write_cache_to_chunks(tmp, false);
			}

			return m_wcache.size() >= m_cache_size;
		}

		/* returns current number of records and data size on disk */
		void size(size_t &num, size_t &data_size, bool &have_split) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);

			have_split = false;
			if (m_split_dst)
				have_split = true;

			current_bstore()->size(num, data_size);
			log(SMACK_LOG_NOTICE, "%s: size-check: num: %zd, data-size: %zd, have-split: %d\n",
					m_start.str(), num, data_size, have_split);
		}

		void set_split_dst(boost::shared_ptr<blob<filter_t> > dst) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);
			if (m_split_dst)
				return;

			m_split_dst = dst;
			m_split_dst->start().set(m_last_average_key.idx());
		}

	private:
		key m_start;
		boost::mutex m_write_lock;
		boost::mutex m_disk_lock;
		boost::condition m_cond;
		cache_t m_wcache;
		std::set<key, keycomp> m_remove_cache;
		std::string m_path;
		size_t m_cache_size;
		size_t m_bloom_size;
		int m_chunk_idx;
		boost::shared_ptr<blob<filter_t> > m_split_dst;

		std::vector<boost::shared_ptr<blob_store> > m_files;
		std::map<key, chunk, keycomp> m_chunks;
		std::vector<chunk> m_chunks_unsorted;

		key m_last_average_key;

		boost::shared_ptr<blob_store> current_bstore(void) {
			return m_files[m_chunk_idx];
		}

		void write_chunk(cache_t &cache, size_t num, bool sorted) {
			int average = cache.size() / 2;
			for (cache_t::iterator it = cache.begin(); it != cache.end(); ++it) {
				if (--average == 0) {
					m_last_average_key = it->first;
					break;
				}
			}

			chunk ch = current_bstore()->store_chunk(cache, num, m_cache_size * sizeof(key) / smack_rcache_mult);
			if (sorted) {
				m_chunks.insert(std::make_pair(ch.start(), ch));
			} else {
				m_chunks_unsorted.push_back(ch);
			}
		}

		void write_cache_to_chunks(cache_t &cache, bool sorted) {
			while (cache.size()) {
				size_t size = m_cache_size;
				if (cache.size() < m_cache_size * 1.5)
					size = cache.size();

				write_chunk(cache, size, sorted);
			}
		}

		void chunks_resort(cache_t &cache) {
			for (std::vector<chunk>::reverse_iterator it = m_chunks_unsorted.rbegin(); it != m_chunks_unsorted.rend(); ++it) {
				current_bstore()->read_chunk(*it, cache);
			}
			m_chunks_unsorted.erase(m_chunks_unsorted.begin(), m_chunks_unsorted.end());

			/* always resort all chunks */
			if (m_split_dst || true) {
				for (std::map<key, chunk, keycomp>::iterator it = m_chunks.begin(); it != m_chunks.end(); ++it) {
					current_bstore()->read_chunk(it->second, cache);
				}

				m_chunks.erase(m_chunks.begin(), m_chunks.end());
			}

			boost::shared_ptr<blob_store> src = current_bstore();

			//int prev_idx = m_chunk_idx;
			/* update data file index */
			if (++m_chunk_idx >= (int)m_files.size())
				m_chunk_idx = 0;
			/* truncate new data files */
			current_bstore()->truncate();
#if 0
			log(SMACK_LOG_NOTICE, "%s: resort: idx: %d -> %d, copy-chunks: %zd, resort-keys: %zd\n",
					m_path.c_str(), prev_idx, m_chunk_idx, m_chunks.size(), cache.size());

			for (std::map<key, chunk, keycomp>::iterator it = m_chunks.begin(); it != m_chunks.end(); ++it) {
				src->copy_chunk(it->second, *current_bstore());
			}
#endif
			/* split cache if m_split_dst is set, this will cut part of the cache which is >= than m_split_dst->start() */
			if (m_split_dst)
				split(m_split_dst->start(), cache);

			write_cache_to_chunks(cache, true);

			size_t idx_num, data_size;
			current_bstore()->size(idx_num, data_size);
			log(SMACK_LOG_NOTICE, "%s: %s: chunks resorted: idx: %d, chunks: %zd, num: %zd, data-size: %zd, split: %s\n",
					m_path.c_str(), m_start.str(), m_chunk_idx, m_chunks.size(),
					idx_num, data_size, m_split_dst ? m_split_dst->start().str() : "none");
		}

		void split(const key &key, cache_t &cache) {
			size_t orig_size = cache.size();

			cache_t::iterator split_it = cache.lower_bound(key);
			for (cache_t::iterator it = split_it; it != cache.end(); ++it)
				m_split_dst->write(it->first, it->second.data(), it->second.size());

			cache.erase(split_it, cache.end());

			log(SMACK_LOG_NOTICE, "%s: split to new blob: %zd entries, old blob: %zd entries\n",
					key.str(), orig_size - cache.size(), cache.size());
		}

};

}}

#endif /* __SMACK_BLOB_HPP */
