#ifndef __SMACK_BLOB_HPP
#define __SMACK_BLOB_HPP

#include <sys/types.h>
#include <sys/stat.h>

#include <unistd.h>

#include <set>
#include <algorithm>

#include <boost/version.hpp>

#include <boost/filesystem.hpp>

#include <boost/thread/condition.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/stream.hpp>

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
	unsigned char		start[SMACK_KEY_SIZE];	/* ID of the first key */
	unsigned char		end[SMACK_KEY_SIZE];	/* ID of the last key */
	
	uint64_t		data_offset;		/* data offset in data file for given chunk */
	uint64_t		compressed_data_size;		/* size of (compressed) data on disk */
	uint64_t		uncompressed_data_size;		/* size of uncompressed data stored into this compressed chunk on disk */
	int			num;			/* number of records in the chunk */
	int			bloom_size;		/* bloom size in bytes */
} __attribute__ ((packed));

#define SMACK_DISK_FORMAT_VERSION		1
#define SMACK_DISK_FORMAT_MAGIC			"SmAcK BaCkEnD"

struct chunk_header {
	char			magic[16];
	uint64_t		timestamp;
	int			version;
	int			pad[3];
};

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
			m_start = key(ctl.start, sizeof(ctl.start));
			m_end = key(ctl.end, sizeof(ctl.end));
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

		const key &start(void) const {
			return m_start;
		}

		const key &end(void) const {
			return m_end;
		}

		void set_bounds(const struct index *start, const struct index *end) {
			m_start.set(start);
			m_end.set(end);

			memcpy(m_ctl.start, start->id, SMACK_KEY_SIZE);
			memcpy(m_ctl.end, end->id, SMACK_KEY_SIZE);
		}

		/* this must be (and it is) single-threaded operation */
		void rcache_add(const key &key, size_t offset) {
			std::pair<rcache_t::iterator, bool> p = m_rcache.insert(std::make_pair(key, offset));

			if (!p.second)
				p.first->second = offset;
		}

		bool rcache_find(const key &key, size_t &data_offset) {
			if (m_rcache.size() == 0) {
				if (key > m_end)
					return false;

				data_offset = m_ctl.uncompressed_data_size;
				return true;
			}

			rcache_t::iterator it = m_rcache.upper_bound(key);
			if (it == m_rcache.begin()) {
				if (key < m_start)
					return false;

				data_offset = it->second;
				return true;
			}

			if (it == m_rcache.end()) {
				if (key > m_end)
					return false;

				data_offset = m_ctl.uncompressed_data_size;
				return true;
			}

			data_offset = it->second;
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
		m_bloom_size(bloom_size)
		{
			log(SMACK_LOG_NOTICE, "blob-store: %s, bloom-size: %d\n", path.c_str(), bloom_size);
		}

		/* returns offset of the new chunk written */
		template <class fout_t>
		chunk store_chunk(fout_t &out_processor, cache_t &cache, size_t num, size_t max_cache_size) {
			chunk ch(m_bloom_size);

			size_t data_offset = 0;
			bio::file_sink dst_data(m_path_base + ".data", std::ios::app);

			ch.ctl()->data_offset = bio::seek<bio::file_sink>(dst_data, 0, std::ios_base::end);

			const struct index *end_idx = cache.rbegin()->first.idx();
			{
				bio::filtering_streambuf<bio::output> out;
				out.push(out_processor);
				out.push(dst_data);

				size_t count = 0;
				cache_t::iterator it;
				int step = cache.size();
				if (max_cache_size)
					step = std::min<size_t>(cache.size(), num) / max_cache_size + 1;

				int st = 0;
				for (it = cache.begin(); it != cache.end(); ++it) {
					struct index *idx = (struct index *)it->first.idx();
					idx->data_size = it->second.size();

					std::string tmp;
					tmp.reserve(sizeof(struct index) + it->second.size());
					tmp.assign((char *)idx, sizeof(struct index));
					tmp += it->second;

					bio::write<bio::filtering_streambuf<bio::output> >(out, tmp.data(), tmp.size());

					ch.add((char *)idx->id, SMACK_KEY_SIZE);

					if (++st == step) {
						key k(idx);
						ch.rcache_add(k, data_offset);
						st = 0;
					}

					data_offset += it->second.size() + sizeof(struct index);

					log(SMACK_LOG_DEBUG, "%s: %s: stored %zd/%zd ts: %zu, data-size: %d\n",
							m_path_base.c_str(), key(idx).str(), count, num, idx->ts, idx->data_size);

					if (++count == num) {
						end_idx = idx;
						++it;
						break;
					}
				}
#if 1
				/*
				 * XXX XXX XXX XXX XXX
				 *
				 * This weird junk is needed because bzip2 somehow does not always flush buffers
				 * back to disk, and the last record becomes corrupted (partially written).
				 * 
				 * This is strange, since if we put read_chunk() right at the end, it will always
				 * correctly read all records, but with time something breaks.
				 *
				 * And I do not yet know why.
				 *
				 * zlib works perfectly good as well as large scale bzip2 tests on Ubuntu Lucid
				 * (hundreds of millions of records)
				 */
				std::string tmp;
				tmp.resize(128);
				bio::write<bio::filtering_streambuf<bio::output> >(out, tmp.data(), tmp.size());
#endif

				ch.set_bounds(cache.begin()->first.idx(), end_idx);
				cache.erase(cache.begin(), it);
				out.strict_sync();

				ch.ctl()->num = count;
			}

			/*
			 * XXX
			 * seek() can return -1
			 *
			 * It does not hurt system, but it is really weird, how boost::iostreams flush data to underlying sinks
			 */
			size_t data_size = bio::seek<bio::file_sink>(dst_data, 0, std::ios_base::end);

			ch.ctl()->compressed_data_size = data_size - ch.ctl()->data_offset;
			ch.ctl()->uncompressed_data_size = data_offset;

			store_chunk_meta(ch);

			log(SMACK_LOG_NOTICE, "%s: store-chunk: start: %s, end: %s, num: %d, file-size: %zd, chunk-data-offset: %zd, "
					"uncompressed-data-size: %zd, compressed-data-size: %zd, errno: %d\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(), ch.ctl()->num,
					data_size, ch.ctl()->data_offset,
					ch.ctl()->uncompressed_data_size, ch.ctl()->compressed_data_size, errno);

			return ch;
		}

		template <class fin_t>
		void read_chunk(fin_t &input_processor, chunk &ch, cache_t &cache) {
			bio::file_source src_data(m_path_base + ".data");
			bio::seek<bio::file_source>(src_data, ch.ctl()->data_offset, std::ios_base::beg);

			bio::filtering_streambuf<bio::input> in;
			in.push(input_processor);
			in.push(src_data);

			struct timeval start, end;
			gettimeofday(&start, NULL);

			log(SMACK_LOG_NOTICE, "%s: read-chunk: start: %s, end: %s, num: %d, compressed-size: %zd, uncompressed-size: %zd\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(),
					ch.ctl()->num, ch.ctl()->compressed_data_size, ch.ctl()->uncompressed_data_size);

			struct index idx;

			size_t offset = 0;
			try {
				for (int i = 0; i < ch.ctl()->num; ++i) {
					bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)&idx, sizeof(struct index));
					std::string tmp;
					tmp.resize(idx.data_size);
					bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)tmp.data(), idx.data_size);

					cache.insert(std::make_pair(key(&idx), tmp));

					offset += sizeof(struct index) + idx.data_size;
				}
			} catch (const bio::bzip2_error &e) {
				log(SMACK_LOG_ERROR, "%s: %s: bzip error: %s: %d\n", m_path_base.c_str(), key(&idx).str(), e.what(), e.error());
				throw;
			}
			gettimeofday(&end, NULL);

			long read_time = smack_time_diff(start, end);

			log(SMACK_LOG_NOTICE, "%s: read-chunk: start: %s, end: %s, num: %d, read-time: %ld usecs\n",
					m_path_base.c_str(), ch.start().str(), ch.end().str(), ch.ctl()->num, read_time);
		}

		template <class fin_t>
		void read_index(fin_t &in, std::map<key, chunk, keycomp> &chunks, std::vector<chunk> &chunks_unsorted, size_t max_rcache_size) {
			try {
				read_chunks<fin_t>(in, chunks, chunks_unsorted, max_rcache_size);
			} catch (const std::runtime_error &e) {
				log(SMACK_LOG_ERROR, "%s: read chunks failed: %s\n", m_path_base.c_str(), e.what());
				throw;
			}
		}

		template <class fin_t>
		bool chunk_read(fin_t &input_processor, key &read_key, chunk &ch, std::string &ret) {
			struct timeval start, seek_time, decompress_time;

			gettimeofday(&start, NULL);

			if (!ch.check((char *)read_key.id(), SMACK_KEY_SIZE)) {
				log(SMACK_LOG_DEBUG, "%s: %s: chunk start: %s, end: %s: bloom-check failed\n",
						m_path_base.c_str(), read_key.str(), ch.start().str(), ch.end().str());
				return false;
			}

			size_t data_offset;
			bool found = ch.rcache_find(read_key, data_offset);
			if (!found) {
				log(SMACK_LOG_DEBUG, "%s: %s: chunk start: %s, end: %s: rcache lookup failed\n",
						m_path_base.c_str(), read_key.str(), ch.start().str(), ch.end().str());
				return false;
			}

			log(SMACK_LOG_NOTICE, "%s: %s: start: %s, end: %s, rcache returned offset: %zd, "
					"compressed-size: %zd, uncompressed-size: %zd\n",
					m_path_base.c_str(), read_key.str(), ch.start().str(), ch.end().str(), data_offset,
					ch.ctl()->compressed_data_size, ch.ctl()->uncompressed_data_size);

			bio::file_source src_data(m_path_base + ".data");

			/* seeking to the start of the chunk */
			size_t pos = bio::seek<bio::file_source>(src_data, ch.ctl()->data_offset, std::ios_base::beg);
			if (pos != ch.ctl()->data_offset) {
				std::ostringstream str;
				str << m_path_base << ": " << read_key.str() << ": read: could not seek to: " <<
					ch.ctl()->data_offset << ", seeked to: " << pos;
				throw std::out_of_range(str.str());
			}

			gettimeofday(&seek_time, NULL);

			bio::filtering_streambuf<bio::input> in;
			in.push(input_processor);
			in.push(src_data);
			in.set_auto_close(false);

			struct index idx;

			ret.clear();

			size_t offset = 0;
			while (offset <= data_offset) {
				bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)&idx, sizeof(struct index));

				std::string tmp;
				tmp.resize(idx.data_size);
				bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)tmp.data(), idx.data_size);

				key tmp_key(&idx);
				if (read_key < tmp_key)
					return false;

				if (read_key == tmp_key) {
					ret.swap(tmp);
					break;
				}

				offset += sizeof(struct index) + idx.data_size;
			}

			gettimeofday(&decompress_time, NULL);

			long seek_diff = smack_time_diff(start, seek_time);
			long decompress_diff = smack_time_diff(seek_time, decompress_time);

			log(SMACK_LOG_NOTICE, "%s: %s: chunk start: %s, end: %s: chunk-read: data-offset: %zd, chunk-start-offset: %zd, "
					"num: %d, seek-time: %ld, decompress-time: %ld usecs, return-size: %zd\n",
					m_path_base.c_str(), read_key.str(), ch.start().str(), ch.end().str(),
					data_offset, ch.ctl()->data_offset, ch.ctl()->num,
					seek_diff, decompress_diff, ret.size());

			return ret.size() > 0;
		}

		void forget() {
			forget_path(m_path_base + ".data");
			forget_path(m_path_base + ".chunk");
		}

		void truncate() {
			forget();

			boost::filesystem::remove(m_path_base + ".data");
			boost::filesystem::remove(m_path_base + ".chunk");
		}

		/* returns data size on disk and number of elements */
		void size(size_t &data_size) {
			data_size = 0;
			try {
				data_size = boost::filesystem::file_size(m_path_base + ".data");
			} catch (...) {
			}
		}

	private:
		std::string m_path_base;
		int m_bloom_size;

		void forget_path(const std::string &path) {
			int fd;

			fd = open(path.c_str(), O_RDONLY);
			if (fd >= 0) {
				posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
				close(fd);
			}
		}

		void store_chunk_meta(chunk &ch) {
			bio::file_sink chunk(m_path_base + ".chunk", std::ios::app);
			size_t data_size = bio::seek<bio::file_sink>(chunk, 0, std::ios::end);
			if (!data_size) {
				struct chunk_header h;
				memset(&h, 0, sizeof(struct chunk_header));

				snprintf(h.magic, sizeof(h.magic), SMACK_DISK_FORMAT_MAGIC);
				h.version = SMACK_DISK_FORMAT_VERSION;
				h.timestamp = time(NULL);

				bio::write<bio::file_sink>(chunk, (char *)&h, sizeof(struct chunk_header));
			}

			bio::write<bio::file_sink>(chunk, (char *)ch.ctl(), sizeof(struct chunk_ctl));
			bio::write<bio::file_sink>(chunk, ch.data().data(), ch.data().size());
		}

		template <class fin_t>
		void read_chunks(fin_t &input_processor,
				 std::map<key, chunk, keycomp> &chunks,
				 std::vector<chunk> &chunks_unsorted,
				 size_t max_rcache_size) {
			size_t offset = 0;
			bio::file_source ch_src(m_path_base + ".chunk");
			size_t chunk_size = bio::seek<bio::file_source>(ch_src, 0, std::ios::end);
			bio::seek<bio::file_source>(ch_src, 0, std::ios::beg);

			check_chunk_header(ch_src);

			while (offset < chunk_size) {
				struct chunk_ctl ctl;

				bio::read<bio::file_source>(ch_src, (char *)&ctl, sizeof(struct chunk_ctl));

				std::vector<char> data(ctl.bloom_size);
				bio::read<bio::file_source>(ch_src, data.data(), data.size());

				chunk ch(ctl, data);

				int step = ctl.num;
				if (max_rcache_size)
					step = ctl.num / max_rcache_size + 1;

				bio::file_source src_data(m_path_base + ".data");

				/* seeking to the start of the chunk */
				size_t pos = bio::seek<bio::file_source>(src_data, ctl.data_offset, std::ios_base::beg);
				if (pos != ch.ctl()->data_offset) {
					std::ostringstream str;
					str << m_path_base << ": read_chunks: could not seek to: " <<
						ctl.data_offset << ", seeked to: " << pos;
					throw std::out_of_range(str.str());
				}

				bio::filtering_streambuf<bio::input> in;
				in.push(input_processor);
				in.push(src_data);
				in.set_auto_close(false);

				struct index idx;

				if (step < ctl.num) {
					int st = 0;
					size_t off = 0;
					for (int i = 0; i < ch.ctl()->num; ++i) {
						bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)&idx, sizeof(struct index));

						log(SMACK_LOG_DEBUG, "%s: %s: ts: %zd, data-size: %d, flags: %x\n",
								m_path_base.c_str(), key(&idx).str(), idx.ts, idx.data_size, idx.flags);

						std::string tmp;
						tmp.resize(idx.data_size);
						bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)tmp.data(), idx.data_size);

						if (++st == step) {
							ch.rcache_add(key(&idx), off);
							st = 0;
						}

						off += sizeof(struct index) + idx.data_size;
					}
				}

				log(SMACK_LOG_NOTICE, "%s: read_chunks: %zd: data-offset: %zd, "
						"compressed-size: %zd, uncompressed-size: %zd, "
						"num: %d, bloom-size: %d, start: %s, end: %s\n",
						m_path_base.c_str(), chunks.size(), ctl.data_offset,
						ctl.compressed_data_size, ctl.uncompressed_data_size,
						ctl.num, ctl.bloom_size, ch.start().str(), ch.end().str());

				if ((chunks.size() == 0) || (ch.start() >= chunks.rbegin()->second.end()))
					chunks.insert(std::make_pair(ch.start(), ch));
				else
					chunks_unsorted.push_back(ch);

				offset += sizeof(struct chunk_ctl) + ctl.bloom_size;
			}
		}

		void check_chunk_header(bio::file_source &ch_src) {
			struct chunk_header h;
			bio::read<bio::file_source>(ch_src, (char *)&h, sizeof(struct chunk_header));
			if (memcmp(h.magic, SMACK_DISK_FORMAT_MAGIC, sizeof(SMACK_DISK_FORMAT_MAGIC))) {
				log(SMACK_LOG_ERROR, "%s: smack disk format magic mismatch\n", m_path_base.c_str());
				throw std::runtime_error("smack disk format magic mismatch");
			}
			if (h.version != SMACK_DISK_FORMAT_VERSION) {
				log(SMACK_LOG_ERROR, "%s: smack disk format version mismatch: stored: %d, current: %d, please convert\n",
						m_path_base.c_str(), h.version, SMACK_DISK_FORMAT_VERSION);
				throw std::runtime_error("smack disk format version mismatch");
			}
		}
};

template <class fout_t, class fin_t>
class blob {
	public:
		blob(const std::string &path, int bloom_size, size_t max_cache_size) :
		m_path(path),
		m_cache_size(max_cache_size),
		m_bloom_size(bloom_size),
		m_chunk_idx(0),
		m_want_rcache(false),
		m_want_resort(false)
		{
			time_t mtime = 0;
			ssize_t size = 0;
			int idx = -1;
			int num = 2;

			for (int i = 0; i < num; ++i) {
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
				fin_t in;
				m_files[idx]->read_index<fin_t>(in, m_chunks, m_chunks_unsorted, 0);

				log(SMACK_LOG_INFO, "%s: read-index: idx: %d, sorted: %zd, unsorted: %zd, num: %zd\n",
						m_path.c_str(), idx, m_chunks.size(), m_chunks_unsorted.size(), this->num());
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
			 * we remove object and do not write anything above
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
					fin_t in;
					found = current_bstore()->chunk_read(in, key, it->second, ret);
				} else {
					--it;

					fin_t in;
					found = current_bstore()->chunk_read(in, key, it->second, ret);
					if (!found && (key > it->second.end())) {
						++it;

						if (it != m_chunks.end()) {
							fin_t in;
							found = current_bstore()->chunk_read(in, key, it->second, ret);
						}
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

					fin_t in;
					found = current_bstore()->chunk_read(in, key, *it, ret);
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

			if ((m_chunks_unsorted.size() > 50) || m_split_dst || m_want_resort) {
				m_want_resort = false;
				m_want_rcache = false;

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
			} else {
				if (m_want_rcache) {
					fin_t in;

					m_chunks.clear();
					m_chunks_unsorted.clear();
					current_bstore()->read_index(in, m_chunks, m_chunks_unsorted,
							m_cache_size * sizeof(key) / smack_rcache_mult);
					m_want_rcache = false;
				}
				
				if (tmp.size())
					write_cache_to_chunks(tmp, false);
			}

			return m_wcache.size() >= m_cache_size;
		}

		/* returns current number of records and data size on disk */
		void disk_stat(size_t &num, size_t &data_size, bool &have_split) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);

			have_split = false;
			if (m_split_dst)
				have_split = true;

			num = this->num() + m_wcache.size();
			current_bstore()->size(data_size);
		}

		void set_split_dst(boost::shared_ptr<blob<fout_t, fin_t> > dst) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);
			if (m_split_dst)
				return;

			m_split_dst = dst;
			m_split_dst->start().set(m_last_average_key.idx());
		}

		size_t have_unsorted_chunks() {
			return m_chunks_unsorted.size();
		}

		void set_want_rcache(bool want_rcache) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);

			m_want_rcache = want_rcache;
		}

		void set_want_resort(bool want_resort) {
			boost::mutex::scoped_lock disk_guard(m_disk_lock);

			m_want_resort = want_resort;
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
		boost::shared_ptr<blob<fout_t, fin_t> > m_split_dst;

		std::vector<boost::shared_ptr<blob_store> > m_files;
		std::map<key, chunk, keycomp> m_chunks;
		std::vector<chunk> m_chunks_unsorted;

		key m_last_average_key;
		bool m_want_rcache, m_want_resort;

		size_t num() {
			size_t num = 0;
			for (std::map<key, chunk, keycomp>::iterator it = m_chunks.begin(); it != m_chunks.end(); ++it)
				num += it->second.ctl()->num;

			for (std::vector<chunk>::iterator it = m_chunks_unsorted.begin(); it != m_chunks_unsorted.end(); ++it)
				num += it->ctl()->num;

			return num;
		}

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

			fout_t out;
			chunk ch = current_bstore()->store_chunk(out, cache, num, m_cache_size * sizeof(key) / smack_rcache_mult);
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
				fin_t in;
				current_bstore()->read_chunk(in, *it, cache);
			}
			m_chunks_unsorted.erase(m_chunks_unsorted.begin(), m_chunks_unsorted.end());

			/* always resort all chunks and try to drop old copy from page cache */
			for (std::map<key, chunk, keycomp>::iterator it = m_chunks.begin(); it != m_chunks.end(); ++it) {
				fin_t in;
				current_bstore()->read_chunk(in, it->second, cache);
			}

			m_chunks.erase(m_chunks.begin(), m_chunks.end());
			current_bstore()->forget();

			boost::shared_ptr<blob_store> src = current_bstore();

			if (++m_chunk_idx >= (int)m_files.size())
				m_chunk_idx = 0;

			/* truncate new data files */
			current_bstore()->truncate();

			/* split cache if m_split_dst is set, this will cut part of the cache which is >= than m_split_dst->start() */
			if (m_split_dst)
				split(m_split_dst->start(), cache);

			write_cache_to_chunks(cache, true);

			size_t data_size;
			current_bstore()->size(data_size);
			log(SMACK_LOG_NOTICE, "%s: %s: chunks resorted: idx: %d, chunks: %zd, data-size: %zd, split: %s\n",
					m_path.c_str(), m_start.str(), m_chunk_idx, m_chunks.size(),
					data_size, m_split_dst ? m_split_dst->start().str() : "none");
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
