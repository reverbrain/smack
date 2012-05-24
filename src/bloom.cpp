#include <smack/base.hpp>

using namespace ioremap::smack;

static unsigned int h1(const char *data, int size)
{
	unsigned int h = 0;
	const unsigned int *ptr = (const unsigned int *)data;

	for (unsigned i = 0; i < size / sizeof(int); ++i) {
		h += *ptr;
		ptr++;
	}

	return h;
}

#ifdef SMACK_WANT_FNV
/*
 * FNV-hash
 */
static unsigned int h2(const char *data, int size)
{
	unsigned int h = 2166136261;

	for (int i = 0; i < size; ++i) {
		h ^= data[i];
		h *= 16777619;
	}

	return h;
}
#endif

bloom::bloom(int bits) : m_bits(bits)
{
	add_hashes();
	m_data.resize(bits / 8);
}

bloom::~bloom()
{
}

void bloom::add(const char *data, int size)
{
	unsigned int h, byte, bit;

	for (std::vector<bloom_hash_t>::iterator it = m_hashes.begin(); it < m_hashes.end(); ++it) {
		h = (*it)(data, size) % m_bits;
		byte = h / 8;
		bit = h % 8;

		m_data[byte] |= 1 << bit;
	}
}

bool bloom::check(const char *data, int size)
{
	unsigned int h, byte, bit;

	for (std::vector<bloom_hash_t>::iterator it = m_hashes.begin(); it < m_hashes.end(); ++it) {
		h = (*it)(data, size) % m_bits;
		byte = h / 8;
		bit = h % 8;

		if (!(m_data[byte] & (1 << bit)))
			return false;
	}

	return true;
}

void bloom::add_hashes(void)
{
	m_hashes.push_back(h1);
#ifdef SMACK_WANT_FNV
	m_hashes.push_back(h2);
#endif
}

std::vector<char> &bloom::get()
{
	return m_data;
}
