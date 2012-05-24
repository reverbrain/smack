#ifndef __SMACK_ZLIB_HPP
#define __SMACK_ZLIB_HPP

#include <stdio.h>
#include <string.h>
#include <zlib.h>

#include <smack/base.hpp>

namespace ioremap {
namespace smack {

#define SMACK_ZLIB_CHUNK_SIZE		(256 * 1024)
#define windowBits			15
#define GZIP_ENCODING			16
/* maximum window size and wrap into gzip header - allows zcat and friends to understand files */
#define SMACK_ZLIB_WINDOW_BITS		(windowBits | GZIP_ENCODING)

struct zlib_compress {
	z_stream stream;

	zlib_compress() {
		memset(&stream, 0, sizeof(stream));

		int err;
		err = deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, SMACK_ZLIB_WINDOW_BITS, 8, Z_DEFAULT_STRATEGY);
		if (err != Z_OK) {
			std::ostringstream str;
			str << "zlib: deflate initialization failed: " << err;
			throw std::runtime_error(str.str());
		}
	}

	~zlib_compress() {
		deflateEnd(&stream);
	}

	size_t compress(void *out, size_t out_size, bool flush) {
		stream.next_out = (Bytef *)out;
		stream.avail_out = out_size;

		::deflate(&stream, flush ? Z_FINISH : Z_NO_FLUSH);

		return out_size - stream.avail_out;
	}
};

struct zlib_decompress {
	z_stream stream;

	zlib_decompress() {
		memset(&stream, 0, sizeof(stream));

		int err;
		err = inflateInit2(&stream, SMACK_ZLIB_WINDOW_BITS);
		if (err != Z_OK) {
			std::ostringstream str;
			str << "zlib: inflate initialization failed: " << err;
			throw std::runtime_error(str.str());
		}
	}

	~zlib_decompress() {
		inflateEnd(&stream);
	}

	int decompress(void *next_out, size_t next_size) {
		stream.next_out = (Bytef *)next_out;
		stream.avail_out = next_size;

		int err = ::inflate(&stream, Z_SYNC_FLUSH);
		switch (err) {
			case Z_NEED_DICT:
				err = Z_DATA_ERROR;
			case Z_VERSION_ERROR:
			case Z_STREAM_ERROR:
			case Z_DATA_ERROR:
			case Z_MEM_ERROR:
				std::ostringstream str;
				str << "zlib: inflate failed: size: " << next_size << ": " << err;
				throw std::runtime_error(str.str());
		}

		return err;
	}
};

typedef boost::shared_array<char> bsa_t;

struct zlib {
	std::vector<char> filter_out(std::vector<char> &data) {
		std::vector<char> vec;

		bsa_t out(new char[SMACK_ZLIB_CHUNK_SIZE]);

		zlib_compress zs;
		zs.stream.avail_in = data.size();
		zs.stream.next_in = (Bytef *)data.data();

		do {
			size_t have = zs.compress((void *)out.get(), SMACK_ZLIB_CHUNK_SIZE, true);
			if (have)
				vec.insert(vec.end(), out.get(), out.get() + have);
			log(SMACK_LOG_DSA, "zlib: %s: data-size: %zu, processed-size: %zu, have: %zu\n",
					__func__, data.size(), vec.size(), have);
		} while (zs.stream.avail_out == 0);

		return vec;
	}

	std::vector<char> filter_in(std::vector<char> &data) {
		std::vector<char> vec;
		bsa_t out(new char[SMACK_ZLIB_CHUNK_SIZE]);

		zlib_decompress zs;
		zs.stream.next_in = (Bytef *)data.data();
		zs.stream.avail_in = data.size();

		int err;
		do {
			err = zs.decompress((void *)out.get(), SMACK_ZLIB_CHUNK_SIZE);
			size_t have = SMACK_ZLIB_CHUNK_SIZE - zs.stream.avail_out;

			if (have)
				vec.insert(vec.end(), out.get(), out.get() + have);

			log(SMACK_LOG_DSA, "zlib: %s: data-size: %zu, processed-size: %zu, have: %zu\n",
					__func__, data.size(), vec.size(), have);
		} while ((zs.stream.avail_out == 0) && (err != Z_STREAM_END));

		return vec;
	}
};

} /* namespace smack */
} /* namespace ioremap*/

#endif /* __SMACK_ZLIB_HPP */
