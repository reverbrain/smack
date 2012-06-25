#include <unistd.h>

#include <fstream>
#include <iostream>

#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <smack/blob.hpp>
#include <smack/snappy.hpp>

using namespace ioremap::smack;

template <class fin_t>
class chunk_reader {
	public:
		chunk_reader(const std::string &path, bool show_data, const key &key, const int klen) :
		m_path(path), m_st(path, 128), m_show_data(show_data) {
			fin_t in;
			m_st.read_index(in, m_chunks, m_chunks_unsorted, 0);

			find(key, klen);
		}

	private:
		std::string m_path;
		blob_store m_st;
		bool m_show_data;
		std::map<key, chunk, keycomp> m_chunks;
		std::vector<chunk> m_chunks_unsorted;

		void find(const key &key, const int klen) {
			for (std::vector<chunk>::iterator it = m_chunks_unsorted.begin(); it != m_chunks_unsorted.end(); ++it) {
				find_in_chunk(*it, key, klen);
			}

			if (!m_chunks.size())
				return;

			if (klen != 0) {
				std::map<class key, chunk, keycomp>::iterator it = m_chunks.upper_bound(key);
				if (it == m_chunks.end()) {
					find_in_chunk(m_chunks.rbegin()->second, key, klen);
				} else if (it == m_chunks.begin()) {
					return;
				} else {
					--it;
					find_in_chunk(it->second, key, klen);
				}
			} else {
				for (std::map<class key, chunk, keycomp>::iterator it = m_chunks.begin(); it != m_chunks.end(); ++it) {
					find_in_chunk(it->second, key, klen);
				}
			}
		}

		void find_in_chunk(chunk &ch, const key &key, const int klen) {
			cache_t cache;
			fin_t in;
			m_st.read_chunk(in, ch, cache);
			bool found = false;

			size_t offset = 0;
			for (cache_t::iterator it = cache.begin(); it != cache.end(); ++it) {
				const struct index *idx = it->first.idx();

				if (!klen || !memcmp(key.idx()->id, idx->id, klen)) {
					if (!found) {
						log(SMACK_LOG_INFO, "chunk: %s: start: %s, end: %s, num: %d, data-start: %zd, "
								"compressed-data-size: %zd, uncompressed-data-size: %zd\n",
								m_path.c_str(),
								ch.start().str(), ch.end().str(), ch.ctl()->num, ch.ctl()->data_offset,
								ch.ctl()->compressed_data_size, ch.ctl()->uncompressed_data_size);
						found = true;
					}

					log(SMACK_LOG_INFO, "%s: ts: %zd, data-offset: %zd/%zd, data-size: %d, data: %s\n",
						it->first.str(), idx->ts,
						offset, offset + ch.ctl()->data_offset,	idx->data_size,
						m_show_data ? it->second.c_str() : "none");
				}

				offset += idx->data_size + sizeof(struct index);
			}
		}
};

static int parse_numeric_id(char *value, unsigned char *id)
{
	unsigned char ch[5];
	unsigned int i, len = strlen(value);

	memset(id, 0, SMACK_KEY_SIZE);

	if (len/2 > SMACK_KEY_SIZE)
		len = SMACK_KEY_SIZE * 2;

	ch[0] = '0';
	ch[1] = 'x';
	ch[4] = '\0';
	for (i=0; i<len / 2; i++) {
		ch[2] = value[2*i + 0];
		ch[3] = value[2*i + 1];

		id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
	}

	if (len & 1) {
		ch[2] = value[2*i + 0];
		ch[3] = '0';

		id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
	}

	return 0;
}


static void chunk_reader_usage(const char *p)
{
	std::cerr << "Usage: " << p << " <options>\n" <<
		" -p path            - smack path prefix, like /tmp/smack/test/smack.13.0\n" <<
		" -k key             - key id, like aabbccdd..., which incodes 64-byte ID, rest of the key will be set to 0\n" <<
		"                        if not present, all keys will be shown\n" <<
		" -n name            - key to be found is set to sha512(name)\n"
		" -d                 - show data if present\n" <<
		" -m                 - log mask\n"
		" -h                 - this help\n" <<
		std::endl;
}

int main(int argc, char *argv[])
{
	std::string path;
	key k;
	int klen = 0;
	bool show_data = false;
	int log_mask = SMACK_LOG_INFO | SMACK_LOG_ERROR;
	int ch;

	while ((ch = getopt(argc, argv, "p:k:n:dhm:")) != -1) {
		switch (ch) {
			case 'p':
				path.assign(optarg);
				break;
			case 'k':
				klen = strlen(optarg) / 2;
				unsigned char id[SMACK_KEY_SIZE];

				parse_numeric_id(optarg, id);
				k = key(id, klen);
				break;
			case 'n':
				k = key(std::string(optarg));
				klen = SMACK_KEY_SIZE;
				break;
			case 'd':
				show_data = true;
				break;
			case 'm':
				log_mask = atoi(optarg);
				break;
			case 'h':
			default:
				chunk_reader_usage(argv[0]);
				return -1;

		}
	}

	if (path.size() == 0) {
		std::cerr << "You have to provide smack prefix path\n";
		chunk_reader_usage(argv[0]);
		return -1;
	}

	logger::instance()->init("/dev/stdout", log_mask);
	//chunk_reader<boost::iostreams::zlib_decompressor> chr(path, show_data, k, klen);
	chunk_reader<boost::iostreams::bzip2_decompressor> chr(path, show_data, k, klen);
	//chunk_reader<ioremap::smack::snappy::snappy_decompressor> chr(path, show_data, k, klen);
}
