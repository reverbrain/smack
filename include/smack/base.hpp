#ifndef __SMACK_BASE_HPP
#define __SMACK_BASE_HPP

#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <string>

#include <boost/thread/mutex.hpp>
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>

#include <smack/smack.h>

namespace ioremap {
namespace smack {

class key {
	public:
		key();
		key(const std::string &name);
		key(const key &k);
		key(const unsigned char *id, int size);
		key(const struct index *);

		char *str(int len = 16) const;
		~key();

		bool operator >(const key &k) const;
		bool operator <(const key &k) const;
		bool operator ==(const key &k) const;
		bool operator >=(const key &k) const;
		bool operator <=(const key &k) const;

		const unsigned char *id() const;
		const struct index *idx(void) const;

		void set(const struct index *);

	private:
		struct index idx_;
		mutable char raw_str[2 * SMACK_KEY_SIZE + 1];

		int cmp(const key &k) const;
};

struct keycomp {
	bool operator() (const key& lhs, const key& rhs) const {
		return lhs < rhs;
	}
};


#define SMACK_LOG_NOTICE		(1<<0)
#define SMACK_LOG_INFO			(1<<1)
#define SMACK_LOG_UNUSED		(1<<2)
#define SMACK_LOG_ERROR			(1<<3)
#define SMACK_LOG_DSA			(1<<4)
#define SMACK_LOG_DATA			(1<<5)

#define SMACK_LOG_CHECK  __attribute__ ((format(printf, 3, 4)))

class logger {
	public:
		int log_mask_;

		static logger *instance(void);
		void init(const std::string &path, int log_mask, bool flush = true);

		void do_log(const int mask, const char *format, ...) SMACK_LOG_CHECK;

	private:
		FILE *log_;
		bool flush_;
		boost::mutex lock_;

		logger(void);
		logger(const logger &);
		~logger(void);

		logger & operator= (logger const &);

		static void destroy(void);

		static logger *logger_;
};

#define log(mask, msg...) \
	do { \
		if (logger::instance()->log_mask_ & (mask)) \
			logger::instance()->do_log((mask), ##msg); \
	} while (0)

class file {
	public:
		file(const std::string &path);
		file(int fd_);

		virtual ~file();

		void write(const char *data, size_t offset, size_t size);
		void read(char *data, size_t offset, size_t size);

		size_t size() const;
		void set_size(size_t size);

		void truncate(ssize_t size);

		int get() const;

	protected:
		int fd;
		size_t size_;
};

class mmap : public file {
	public:
		mmap(const std::string &path);
		mmap(int fd);

		virtual ~mmap();

	protected:
		char *data_;
		size_t mapped_size;
		boost::mutex lock;

		void do_mmap();
		void remap();

		void check_and_remap(size_t offset) {
			if (offset > mapped_size) {
				remap();
				if (offset > mapped_size) {
					std::ostringstream str;
					str << "[] is out of bands, mapped-size: " << mapped_size <<
						", total-size: " << size() << ", offset: " << offset;
					throw std::range_error(str.str());
				}
			}
		}
};

struct index_lookup {
	size_t		index_offset[2];
	size_t		data_offset, data_size;
	bool		exact;
};

class file_index : public file {
	public:
		file_index(const std::string &path) : file(path) {}

		bool lookup(const key &k, struct index_lookup &l) {
			struct index idx;
			int i = -1, high, low, res = -1;

			low = l.index_offset[0] / sizeof(struct index) - 1;
			high = l.index_offset[1] / sizeof(struct index);

			l.exact = false;

			while (high - low > 1) {
				i = low + (high - low)/2;

				read((char *)&idx, i * sizeof(struct index), sizeof(struct index));

				int cmp = memcmp(idx.id, k.id(), SMACK_KEY_SIZE);

				log(SMACK_LOG_DSA, "%s: lookup: %d/%zd: cmp: %s, offset: %llu, size: %llu, cmp: %d\n",
						k.str(), i, l.index_offset[1] / sizeof(struct index),
						key(idx.id, SMACK_KEY_SIZE).str(),
						(unsigned long long)idx.data_offset, (unsigned long long)idx.data_size, cmp);

				if (cmp < 0) {
					low = i;
				} else if (cmp > 0) {
					high = i;
					res = i;

					l.data_offset = idx.data_offset;
					l.data_size = idx.data_size;
				} else {
					l.exact = true;
					res = i;

					l.data_offset = idx.data_offset;
					l.data_size = idx.data_size;
					break;
				}
			}

			if (res < 0)
				return false;

			l.index_offset[0] = res * sizeof(struct index);
			return true;
		}
};

class mmap_index : public mmap {
	public:
		mmap_index(const std::string &path) : mmap(path) {}

		bool lookup(const key &k, struct index_lookup &l) {
			boost::mutex::scoped_lock guard(lock);

			check_and_remap(size());

			int i = -1, high, low, res = -1;

			low = l.index_offset[0] / sizeof(struct index) - 1;
			high = l.index_offset[1] / sizeof(struct index);

			l.exact = false;

			while (high - low > 1) {
				i = low + (high - low)/2;

				struct index *idx = (struct index *)(data_ + i * sizeof(struct index));

				int cmp = memcmp(idx->id, k.id(), SMACK_KEY_SIZE);

				log(SMACK_LOG_DSA, "%s: lookup: %d/%zd: cmp: %s, offset: %llu, size: %llu, cmp: %d\n",
						k.str(), i, size() / sizeof(struct index),
						key(idx->id, SMACK_KEY_SIZE).str(),
						(unsigned long long)idx->data_offset, (unsigned long long)idx->data_size, cmp);

				if (cmp < 0) {
					low = i;
				} else if (cmp > 0) {
					high = i;
					res = i;

					l.data_offset = idx->data_offset;
					l.data_size = idx->data_size;
				} else {
					l.exact = true;
					res = i;

					l.data_offset = idx->data_offset;
					l.data_size = idx->data_size;
					break;
				}
			}

			if (res < 0)
				return false;

			l.index_offset[0] = res * sizeof(struct index);
			return true;
		}
};


typedef unsigned int (* bloom_hash_t)(const char *data, int size);

class bloom {
	public:
		bloom(int bloom_size = 128);
		bloom(std::vector<char> &data);
		virtual ~bloom();

		void add(const char *data, int size);
		bool check(const char *data, int size);

		std::vector<char> &data();
		std::string str(void);

	private:
		std::vector<bloom_hash_t> m_hashes;
		std::vector<char> m_data;

		void add_hashes(void);
};

} /* namespace smack */
} /* namespace ioremap */

#endif /* __SMACK_BASE_HPP */
