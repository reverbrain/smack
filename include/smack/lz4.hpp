#ifndef __LZ4_HPP
#define __LZ4_HPP

#include <algorithm>
#include <vector>

#include <iosfwd>                       // streamsize
#include <boost/iostreams/concepts.hpp> // multichar_input_filter
#include <boost/iostreams/operations.hpp>
#include <boost/iostreams/char_traits.hpp>

#include <smack/lz4.h>
#include <smack/lz4hc.h>
#include <smack/base.hpp>

namespace bio = boost::iostreams;

namespace ioremap { namespace smack { namespace lz4 {

enum state {
	s_start = 0,
	s_done,
	s_have_data,
};

enum compression_type {
	lz4_fast = 1,
	lz4_high,
};

/* lz4 chunk is at most 32-bits long */
struct header {
	int32_t		compressed_size;
	int32_t		uncompressed_size;
};

class decompressor : public bio::multichar_input_filter {
	public:
		explicit decompressor(size_t chunk_size = 1024 * 1024) :
		s_state(s_start),
		m_chunk(chunk_size),
		m_dec_offset(0)
		{
		}

		template<typename Source>
		std::streamsize read(Source& src, char *s, std::streamsize n) {
			std::streamsize total = 0;
			std::streamsize tmp;

			while (total < n) {
				if (s_state == s_have_data) {
					tmp = copy(s + total, n);
					n -= tmp;
					total += tmp;

					if (n == 0)
						break;
				}

				struct header header;
				std::streamsize h = bio::read(src, (char *)&header, sizeof(struct header));
				if (h < 0) {
					if (!total)
						total = -1;
					break;
				}

				m_chunk.resize(header.compressed_size);
				std::streamsize have = bio::read(src, (char *)m_chunk.data(), header.compressed_size);
				if (have == -1) {
					if (!total)
						total = -1;
					break;
				}

				m_dec.resize(header.uncompressed_size);
				int consumed = LZ4_uncompress(m_chunk.data(), (char *)m_dec.data(), header.uncompressed_size);
				if (consumed < 0)
					return -1;

				if (consumed > header.compressed_size) {
					log(SMACK_LOG_ERROR, "lz4: decompression header: compressed: %d, "
							"uncompressed: %d, consumed: %d\n",
							header.compressed_size, header.uncompressed_size, consumed);
					throw std::runtime_error("lz4: decompression header mismatch");
				}

				log(SMACK_LOG_DSA, "lz4: decompress: read: %zd, consumed: %d -> %d\n", have, consumed, header.uncompressed_size);

				m_dec_offset = 0;
				s_state = s_have_data;

				tmp = copy(s + total, n);
				n -= tmp;
				total += tmp;
			}

			return total;
		}

		template<typename Source>
		void close(Source &) {
			s_state = s_start;
		}

	private:
		int (* m_uncompress_function)(const char* source, char* dest, int osize);
		state s_state;
		std::vector<char> m_chunk;
		std::vector<char> m_dec;
		std::streamsize m_dec_offset;

		std::streamsize copy(char *s, std::streamsize have_space) {
			std::streamsize sz = std::min<std::streamsize>(have_space, m_dec.size() - m_dec_offset);

			memcpy(s, m_dec.data() + m_dec_offset, sz);
			m_dec_offset += sz;

			if (m_dec_offset == (std::streamsize)m_dec.size()) {
				s_state = s_start;
				m_dec_offset = 0;
			}

			return sz;
		}
};

class compressor : public bio::multichar_output_filter {
	public:
		explicit compressor(compression_type type, size_t chunk_size = 1024 * 1024) :
		m_compress_function(NULL),
		s_state(s_start),
		m_chunk(chunk_size),
		m_chunk_size(0),
		m_compr_offset(0)
		{
			if (type == lz4_high)
				m_compress_function = LZ4_compressHC;
			else
				m_compress_function = LZ4_compress;
		}

		template<typename Sink>
		std::streamsize write(Sink& dst, const char* s, std::streamsize n) {
			std::streamsize consumed = 0;
			std::streamsize tmp;

			while (consumed < n) {
				if (s_state == s_start) {
					if (m_chunk_size + n < (std::streamsize)m_chunk.size()) {
						memcpy((char *)m_chunk.data() + m_chunk_size, s, n);
						m_chunk_size += n;
						consumed += n;
					} else {
						compress(dst);
					}
				}

				if (s_state == s_have_data) {
					tmp = copy<Sink>(dst);
					if (tmp < 0) {
						if (consumed)
							return consumed;

						return -1;
					}
				}
			}

			return consumed;
		}

		template<typename Sink>
		void close(Sink &dst) {
			if (s_state == s_have_data)
				copy<Sink>(dst);

			if ((s_state == s_start) && (m_chunk_size > 0)) {
				compress(dst);
				copy<Sink>(dst);
			}

			s_state = s_start;
		}

	private:
		int (* m_compress_function)(const char* source, char* dest, int isize);
		state s_state;
		std::vector<char> m_chunk;
		std::streamsize m_chunk_size;
		std::string m_compr;
		std::streamsize m_compr_offset;

		template<typename Sink>
		void compress(Sink &dst) {
			m_compr.resize(LZ4_compressBound(m_chunk_size));
			int compressed = m_compress_function(m_chunk.data(), (char *)m_compr.data(), m_chunk_size);
			m_compr.resize(compressed);

			log(SMACK_LOG_DSA, "lz4: compress: %zd -> %zd\n", m_chunk_size, m_compr.size());

			struct header header;

			header.compressed_size = m_compr.size();
			header.uncompressed_size = m_chunk_size;

			bio::write(dst, (char *)&header, sizeof(struct header));

			m_compr_offset = 0;
			s_state = s_have_data;
			m_chunk_size = 0;
		}

		template<typename Sink>
		std::streamsize copy(Sink &dst) {
			std::streamsize written = bio::write(dst, m_compr.data() + m_compr_offset, m_compr.size() - m_compr_offset);
			if (written < 0)
				return written;

			m_compr_offset += written;
			if (m_compr_offset == (std::streamsize)m_compr.size()) {
				s_state = s_start;
				m_compr_offset = 0;
			}

			return written;
		}
};

class high_compressor : public compressor {
	public:
		high_compressor(size_t chunk_size = 1024 * 1024) : compressor(lz4_high, chunk_size) {};
};

class fast_compressor : public compressor {
	public:
		fast_compressor(size_t chunk_size = 1024 * 1024) : compressor(lz4_fast, chunk_size) {};
};

}}}

#endif /* __LZ4_HPP */
