#ifndef __SMACK_BLOB_HPP
#define __SMACK_BLOB_HPP

#include <sys/types.h>
#include <sys/stat.h>

#include <unistd.h>

#include <set>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include <smack/base.hpp>

namespace ioremap { namespace smack {

typedef std::map<key, std::string, keycomp> cache_t;

/* index offset within global index file, not within chunk */
typedef std::map<key, size_t, keycomp> rcache_t;

namespace bio = boost::iostreams;

struct chunk_ctl {
	uint64_t		index_offset;
	uint64_t		data_offset;
	int			num;
	int			pad;
} __attribute__ ((packed));

class blob_store {
	public:
		blob_store(const std::string &path) :
		m_data(path + ".data"),
		m_index(path + ".index"),
		m_chunk(path + ".chunk")
		{
		}

		/* returns index offset of the new chunk written */
		struct chunk_ctl store_chunk(cache_t &cache, size_t num, rcache_t &rcache) {
			struct chunk_ctl ctl;
			memset(&ctl, 0, sizeof(struct chunk_ctl));

			bio::file_descriptor_sink dst_idx(m_index.get(), bio::never_close_handle);
			ctl.index_offset = bio::seek<bio::file_descriptor_sink>(dst_idx, 0, std::ios_base::end);

			bio::file_descriptor_sink dst_data(m_data.get(), bio::never_close_handle);
			ctl.data_offset = bio::seek<bio::file_descriptor_sink>(dst_data, 0, std::ios_base::end);

			size_t data_offset = 0;
			size_t index_offset = ctl.index_offset;

			bio::filtering_streambuf<bio::output> out;
			out.push(bio::zlib_compressor());
			out.push(dst_data);

			int step_count = 0;
			int step = 100;
			size_t count = 0;
			cache_t::iterator it;
			for (it = cache.begin(); it != cache.end(); ++it) {
				bio::write<bio::filtering_streambuf<bio::output> >(out, it->second.data(), it->second.size());

				struct index idx = *it->first.idx();
				idx.data_offset = data_offset;
				idx.data_size = it->second.size();
				bio::write<bio::file_descriptor_sink>(dst_idx, (char *)&idx, sizeof(struct index));

				if (++count == num)
					break;

				if (--step_count <= 0) {
					step_count = step;
					rcache.insert(std::make_pair(it->first, index_offset));

					log(SMACK_LOG_NOTICE, "%s: rcache-stored: index-offset: %zu\n", it->first.str(), index_offset);
				}

				index_offset += sizeof(struct index);
				data_offset += it->second.size();
			}

			ctl.num = count;

			m_chunk.write((char *)&ctl, m_chunk.size(), sizeof(struct chunk_ctl));

			m_index.set_size(bio::seek<bio::file_descriptor_sink>(dst_idx, 0, std::ios_base::end));
			m_data.set_size(bio::seek<bio::file_descriptor_sink>(dst_data, 0, std::ios_base::end));

			log(SMACK_LOG_INFO, "stored-chunk: index-offset: %zd, num: %zd, data-offset: %zd, uncompressed-data-size: %zd\n",
					ctl.index_offset, num, ctl.data_offset, data_offset - ctl.data_offset);

			return ctl;
		}

		void read_whole(cache_t &cache) {
			bio::file_descriptor_source src_idx(m_index.get(), bio::never_close_handle);

			bio::file_descriptor_source src_data(m_data.get(), bio::never_close_handle);
			bio::filtering_streambuf<bio::input> in;
			in.push(bio::zlib_decompressor());
			in.push(src_data);

			struct index idx;
			while (true) {
				if (bio::read<bio::file_descriptor_source>(src_idx, (char *)&idx, sizeof(struct index)) != sizeof(struct index))
					break;

				std::string str;
				str.resize(idx.data_size);

				if ((size_t)bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)str.data(), str.size()) != str.size())
					break;

				cache.insert(std::make_pair(key(&idx), str));
			}
		}

		void read_index(rcache_t &cache, size_t max_cache_size) {
			size_t index_size = m_index.size() / sizeof(struct index);
			int step = index_size / max_cache_size + 1;
			size_t offset = 0;

			bio::file_descriptor_source src_idx(m_index.get(), bio::never_close_handle);

			struct index idx;
			int count = 0;
			while (true) {
				if (bio::read<bio::file_descriptor_source>(src_idx, (char *)&idx, sizeof(struct index)) != sizeof(struct index))
					break;

				if (--count <= 0) {
					cache.insert(std::make_pair(key(&idx), offset));
					count = step;
				}

				offset += sizeof(struct index);
			}
		}

		std::string chunk_read(key &key, size_t index_offset, size_t next_index_offset, struct chunk_ctl &ctl) {
			struct index_lookup l;
			memset(&l, 0, sizeof(struct index_lookup));

			if (!next_index_offset)
				next_index_offset = m_index.size();

			l.index_offset[0] = index_offset;
			l.index_offset[1] = next_index_offset;

			bool found = m_index.lookup(key, l);
			if (!found || !l.exact) {
				std::ostringstream str;
				str << key.str() << ": read: was not found in index-offset: [" << index_offset << ", " << next_index_offset << ")";
				throw std::out_of_range(str.str());
			}

			log(SMACK_LOG_NOTICE, "%s: chunk-read: req-index-offset: %zd-%zd, "
					"index-offset: %zd, data-offset: %zd, chunk-start-offset: %zd, data-size: %zd\n",
					key.str(), index_offset, next_index_offset,
					l.index_offset[0], l.data_offset, ctl.data_offset, l.data_size);

			bio::file_descriptor_source src_data(m_data.get(), bio::never_close_handle);

			/* seeking to the start of the chunk */
			size_t pos = bio::seek<bio::file_descriptor_source>(src_data, ctl.data_offset, std::ios_base::beg);
			if (pos != ctl.data_offset) {
				std::ostringstream str;
				str << key.str() << ": read: could not seek to: " << l.data_offset << ", seeked to: " << pos;
				throw std::out_of_range(str.str());
			}

			bio::filtering_streambuf<bio::input> in;
			in.push(bio::zlib_decompressor());
			in.push(src_data);

			std::string ret;
			ret.resize(l.data_size + l.data_offset);

			if ((size_t)bio::read<bio::filtering_streambuf<bio::input> >(in, (char *)ret.data(), ret.size()) != ret.size()) {
				std::ostringstream str;
				str << key.str() << ": read: could not read: " << l.data_size << " bytes";
				throw std::out_of_range(str.str());
			}

			return ret.substr(l.data_offset, l.data_size);
		}

		void truncate() {
			m_data.truncate(0);
			m_index.truncate(0);
			m_chunk.truncate(0);
		}

	private:
		mmap m_data;
		file_index m_index;
		mmap m_chunk;
};

template <class filter_t>
class blob {
	public:
		blob(const std::string &path, int bloom_size, size_t max_cache_size) :
		m_path(path),
		m_cache_size(max_cache_size),
		m_bloom_size(bloom_size),
		m_chunk_idx(0),
		m_chunks_unsorted(0)
		{
			time_t mtime = 0;
			int idx = -1;

			for (int i = 0; i < 2; ++i) {
				struct stat st;
				int err;

				std::ostringstream str;
				str << path << "/smack." << i;

				err = stat(str.str().c_str(), &st);
				if (err == 0) {
					if (st.st_mtime > mtime) {
						mtime = st.st_mtime;
						idx = i;
					}
				}

				m_files.push_back(boost::shared_ptr<blob_store>(new blob_store(str.str())));
			}

			if (idx != -1)
				m_files[idx]->read_index(m_rcache, m_cache_size);
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
			for (std::vector<struct chunk_ctl>::iterator it = m_chunk_pos.begin(); it != m_chunk_pos.end(); ++it) {
				try {
					ret = m_files[m_chunk_idx]->chunk_read(key,
						it->index_offset, it->index_offset + it->num * sizeof(struct index), *it);
					break;
				} catch (const std::out_of_range &e) {
					continue;
				}
			}

			if (ret.size() == 0) {
				std::ostringstream str;
				str << key.str() << ": read: no data";
				throw std::out_of_range(str.str());
			}

			return ret;
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
			boost::mutex::scoped_lock guard(m_write_lock);

			cache_t tmp;
			m_wcache.swap(tmp);
			guard.unlock();

			boost::mutex::scoped_lock disk_guard(m_disk_lock);
			if (tmp.size())
				write_chunk(tmp, tmp.size(), m_rcache);

			if (m_chunks_unsorted > 5)
				chunks_resort();

			return m_wcache.size() >= m_cache_size;
		}

	private:
		key m_start;
		boost::mutex m_write_lock;
		boost::mutex m_disk_lock;
		boost::condition m_cond;
		cache_t m_wcache;
		std::set<key, keycomp> m_remove_cache;
		rcache_t m_rcache;
		std::string m_path;
		size_t m_cache_size;
		size_t m_bloom_size;
		size_t m_chunk_idx;

		std::vector<boost::shared_ptr<blob_store> > m_files;
		std::vector<struct chunk_ctl> m_chunk_pos;
		int m_chunks_unsorted;



		void write_chunk(cache_t &cache, size_t num, rcache_t &rcache) {
			struct chunk_ctl ctl = m_files[m_chunk_idx]->store_chunk(cache, num, rcache);

			m_chunk_pos.push_back(ctl);
			++m_chunks_unsorted;
		}

		void chunks_resort() {
			cache_t cache;
			rcache_t rcache;

			m_files[m_chunk_idx]->read_whole(cache);

			if (++m_chunk_idx >= m_files.size())
				m_chunk_idx = 0;

			m_files[m_chunk_idx]->truncate();

			while (cache.size()) {
				write_chunk(cache, m_cache_size, rcache);
			}

			m_chunks_unsorted = 0;
			m_rcache.swap(rcache);

			log(SMACK_LOG_NOTICE, "chunks resorted: idx: %zd, chunks: %zd\n", m_chunk_idx, m_chunk_pos.size());
		}

		std::string read_from_rcache(key &key, size_t offset, size_t next_offset) {
			std::vector<struct chunk_ctl>::iterator it;
			for (it = m_chunk_pos.begin(); it != m_chunk_pos.end(); ++it) {
				if (it->index_offset > offset) {
					--it;
					break;
				}
			}

			if (it == m_chunk_pos.end()) {
				log(SMACK_LOG_ERROR, "%s: rcache-read: index-offset: [%zd, %zd), no-chunk\n",
						key.str(), offset, next_offset);
				throw std::out_of_range("rcache-read: no-chunk");
			}

			log(SMACK_LOG_NOTICE, "%s: rcache-read: index-offset: [%zd, %zd), chunk: index-offset: %zd, data-offset: %zd, num: %d\n",
					key.str(), offset, next_offset, it->index_offset, it->data_offset, it->num);

			std::string ret;

			ret = m_files[m_chunk_idx]->chunk_read(key, offset, next_offset, *it);
			return ret;
		}
};

}}

#endif /* __SMACK_BLOB_HPP */
