#include <sys/mman.h>

#include <smack/base.hpp>

using namespace ioremap::smack;

mmap::mmap(const std::string &path) : file(path), data_(NULL), mapped_size(0)
{
	do_mmap();
}

mmap::mmap(int fd) : file(fd), data_(NULL), mapped_size(0)
{
	do_mmap();
}

mmap::~mmap()
{
	if (data_ && mapped_size)
		munmap(data_, mapped_size);
}

bsa_t mmap::data(size_t offset, size_t sz)
{
	boost::mutex::scoped_lock guard(lock);

	check_and_remap(offset + sz);

	bsa_t a(new char[sz]);
	memcpy(a.get(), data_ + offset, sz);
	return a;
}

void mmap::do_mmap()
{
	mapped_size = size();

	if (mapped_size) {
		data_ = (char *)::mmap(NULL, mapped_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
		if (data_ == MAP_FAILED) {
			int err = -errno;
			std::ostringstream str;
			str << "mmap failed: size: " << mapped_size << ", err: " << err;
			throw std::runtime_error(str.str());
		}
	}
}

void mmap::remap()
{
	size_t nsize = size();

	if (!nsize) {
		munmap(data_, mapped_size);
		mapped_size = 0;
		data_ = NULL;
		return;
	}

	if (nsize != mapped_size) {
		if (data_) {
			char *ndata = (char *)mremap(data_, mapped_size, nsize, MREMAP_MAYMOVE);
			if (ndata == MAP_FAILED) {
				int err = -errno;
				std::ostringstream str;
				str << "remap failed: mapped-size: " << mapped_size << ", new-size: " << nsize << ", err: " << err;
				throw std::runtime_error(str.str());
			}

			data_ = ndata;
			mapped_size = nsize;
		} else {
			do_mmap();
		}
	}
}
