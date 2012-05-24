#ifndef __SMACK_BLOB_HPP
#define __SMACK_BLOB_HPP

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include <smack/base.hpp>

namespace ioremap { namespace smack {

typedef std::map<key, std::string, keycomp> cache_t;

namespace bio = boost::iostreams;

struct chunk_ctl {
	struct index		start, end;
	int			num;
	int			pad;
	uint64_t		size;
} __attribute__ ((packed));


template <class filter_t>
class chunk {
	public:
		chunk(int bloom_size, cache_t &cache, size_t num) {
			bloom b(bloom_size);

			m_idx.reserve(cache.size());
			m_data.reserve(cache.size() * 100);

			size_t count = 0;
			cache_t::iterator it;
			for (it = cache.begin(); it != cache.end(); ++it) {
				b.add(it->second.data(), it->second.size());

				m_idx.push_back(*(it->first.idx()));
				m_data.insert(m_data.end(), it->second.data(), it->second.data() + it->second.size());
				if (++count == num)
					break;
			}

			m_data = m_filter.filter_out(m_data);

			m_ctl.start = *(cache.begin()->first.idx());
			m_ctl.end = *(cache.rbegin()->first.idx());
			m_ctl.size = m_data.size();
			m_ctl.num = count;

			cache.erase(cache.begin(), it);
		}

		std::vector<struct index> &index() {
			return m_idx;
		}

		std::vector<char> &data() {
			return m_data;
		}

		struct chunk_ctl &ctl() {
			return m_ctl;
		}

	private:
		std::vector<struct index> m_idx;
		std::vector<char> m_data;
		filter_t m_filter;
		struct chunk_ctl m_ctl;
};

template <class filter_t>
class blob_store {
	public:
		blob_store(const std::string &path) :
		m_data(path + ".data"),
		m_index(path + ".index"),
		m_chunk(path + ".chunk")
		{
		}

		void write(chunk<filter_t> &c) {
			m_chunk.write((char *)&c.ctl(), m_chunk.size(), sizeof(struct chunk_ctl));
			m_data.write((char *)c.data().data(), m_data.size(), c.data().size());
			m_index.write((char *)c.index().data(), m_index.size(), c.index().size() * sizeof(struct index));
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

		void truncate() {
			m_data.truncate(0);
			m_index.truncate(0);
			m_chunk.truncate(0);
		}

	private:
		mmap m_data;
		mmap m_index;
		mmap m_chunk;
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
			for (int i = 0; i < 2; ++i) {
				std::ostringstream str;
				str << path << "/smack." << i;

				m_files.push_back(boost::shared_ptr<blob_store<filter_t> >(new blob_store<filter_t>(str.str())));
			}
		}

		bool write(const key &k, const char *data, size_t size) {
			boost::lock_guard<boost::mutex> guard(m_write_lock);

			std::pair<typename cache_t::iterator, bool> ret =
				m_wcache.insert(std::make_pair(k, std::string(data, size)));
			if (!ret.second)
				ret.first->second = std::string(data, size);

			return m_wcache.size() >= m_cache_size;
		}

		std::string read(key &) {
			return std::string();
		}

		bool remove(const key &) {
			return false;
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

			write_chunk(tmp, tmp.size());

			if (m_chunks.size() > 5)
				chunks_resort();

			return m_wcache.size() >= m_cache_size;
		}

	private:
		key m_start;
		boost::mutex m_write_lock;
		cache_t m_wcache;
		std::string m_path;
		size_t m_cache_size;
		size_t m_bloom_size;
		size_t m_chunk_idx;

		std::vector<boost::shared_ptr<blob_store<filter_t> > > m_files;
		std::map<key, chunk_ctl> m_chunks;

		void write_chunk(cache_t &cache, size_t num) {
			chunk<filter_t> c(m_bloom_size, cache, num);
			m_files[m_chunk_idx]->write(c);

			m_chunks.insert(std::make_pair(key(&c.ctl().start), c.ctl()));
		}

		void chunks_resort() {
			cache_t cache;

			m_files[m_chunk_idx]->read_whole(cache);

			if (++m_chunk_idx >= m_files.size())
				m_chunk_idx = 0;

			m_files[m_chunk_idx]->truncate();
			m_chunks.erase(m_chunks.begin(), m_chunks.end());

			while (cache.size()) {
				write_chunk(cache, m_cache_size);
			}
		}
};

}}

#endif /* __SMACK_BLOB_HPP */
