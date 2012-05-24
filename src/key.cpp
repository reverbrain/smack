#include "crypto/sha512.h"

#include <smack/base.hpp>

using namespace ioremap::smack;

key::key()
{
	memset(&idx_, 0, sizeof(struct index));
}

key::key(const key &k)
{
	idx_ = k.idx_;
}

key::key(const std::string &name)
{
	idx_.data_offset = 0;
	idx_.data_size = 0;

	sha512_buffer((const char *)name.data(), name.size(), (void *)idx_.id);
}

key::key(const unsigned char *id, int size)
{
	idx_.data_offset = 0;
	idx_.data_size = 0;

	if (size > SMACK_KEY_SIZE)
		size = SMACK_KEY_SIZE;

	memcpy(idx_.id, id, size);
	if (size < SMACK_KEY_SIZE) {
		memset(idx_.id + size, 0, SMACK_KEY_SIZE - size);
	}
}

key::key(const struct index *idx)
{
	if (idx)
		idx_ = *idx;
}

key::~key()
{
}

char *key::str(int len) const
{
	if (len > SMACK_KEY_SIZE)
		len = SMACK_KEY_SIZE;

	/* Yes, this is kind of non-constant behaviour */
	for (int i = 0; i < len; ++i)
		sprintf((char *)&raw_str[2*i], "%02x", idx_.id[i]);
	((char *)raw_str)[2 * len + 1] = '\0';
	return (char *)raw_str;
}

bool key::operator >(const key &k) const
{
	return cmp(k) > 0;
}

bool key::operator <(const key &k) const
{
	return cmp(k) < 0;
}

bool key::operator ==(const key &k) const
{
	return cmp(k) == 0;
}

bool key::operator >=(const key &k) const
{
	return cmp(k) >= 0;
}

bool key::operator <=(const key &k) const
{
	return cmp(k) <= 0;
}

const unsigned char *key::id(void) const
{
	return idx_.id;
}

const struct index *key::idx(void) const
{
	return &idx_;
}

int key::cmp(const key &k) const
{
	return (const int)memcmp(idx_.id, k.id(), SMACK_KEY_SIZE);
}

void key::set(const struct index *idx)
{
	memcpy(&idx_, idx, sizeof(struct index));
}
