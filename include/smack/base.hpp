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
		key &operator =(const key &k);

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

enum smack_log_level {
	SMACK_LOG_DATA = 0,
	SMACK_LOG_ERROR,
	SMACK_LOG_INFO,
	SMACK_LOG_NOTICE,
	SMACK_LOG_DEBUG,
};

#define SMACK_LOG_CHECK  __attribute__ ((format(printf, 3, 4)))

class logger {
	public:
		int m_log_level;

		static logger *instance(void);
		void init(const std::string &path, int log_level, bool flush = true);

		void do_log(const int mask, const char *format, ...) SMACK_LOG_CHECK;

	private:
		FILE *m_log;
		bool m_flush;
		boost::mutex m_lock;

		logger(void);
		logger(const logger &);
		~logger(void);

		logger & operator= (logger const &);

		static void destroy(void);

		static logger *m_logger;
};

#define log(level, msg...) \
	do { \
		if (logger::instance()->m_log_level >= (level)) \
			logger::instance()->do_log((level), ##msg); \
	} while (0)

typedef unsigned int (* bloom_hash_t)(const char *data, int size);

class bloom {
	public:
		bloom(const int bloom_size = 128);
		bloom(const std::vector<char> &data);
		virtual ~bloom();

		void add(const char *data, int size);
		bool check(const char *data, int size);

		const std::vector<char> &data() const;
		std::string str(void);

	private:
		std::vector<bloom_hash_t> m_hashes;
		std::vector<char> m_data;

		void add_hashes(void);
};

} /* namespace smack */
} /* namespace ioremap */

#endif /* __SMACK_BASE_HPP */
