#include <smack/base.hpp>

using namespace ioremap::smack;

file::file(const std::string &path) : fd(-1)
{
	fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
	if (fd < 0) {
		int err;
		err = -errno;
		std::ostringstream str;
		str << "could not open file '" << path << "': " << err;
		throw std::runtime_error(str.str());
	}

	struct stat st;
	fstat(fd, &st);

	size_ = st.st_size;
}

file::file(int fd_) : fd(-1)
{
	fd = dup(fd_);
	if (fd < 0) {
		int err;
		err = -errno;
		std::ostringstream str;
		str << "could not dup file: " << err;
		throw std::runtime_error(str.str());
	}

	struct stat st;
	fstat(fd, &st);

	size_ = st.st_size;
}

file::~file()
{
	close(fd);
}

void file::write(const char *data, size_t offset, size_t size)
{
	ssize_t err;

	while (size > 0) {
		err = pwrite(fd, data, size, offset);
		if (err <= 0) {
			if (err == 0)
				err = -EPIPE;
			else
				err = -errno;

			std::ostringstream str;
			str << "write failed: offset: " << offset << ", size: " << size << ": " << err;
			throw std::runtime_error(str.str());
		}

		offset += err;
		data += err;
		size -= err;
	}

	if (offset > size_)
		size_ = offset;

	log(SMACK_LOG_DSA, "write: offset: %zu, size: %zu, file-size: %zu\n", offset, size, size_);
}

void file::read(char *data, size_t offset, size_t size)
{
	ssize_t err;

	log(SMACK_LOG_DSA, "data read: fd: %d, offset: %zu, size: %zu\n", fd, offset, size);

	while (size > 0) {
		err = pread(fd, data, size, offset);
		if (err <= 0) {
			if (err == 0)
				err = -EPIPE;
			else
				err = -errno;

			std::ostringstream str;
			str << "read failed: offset: " << offset << ", size: " << size << ", total-size: " << size_ << ": " << err;
			throw std::runtime_error(str.str());
		}

		offset += err;
		data += err;
		size -= err;
	}
}

size_t file::size() const
{
	return size_;
}

void file::truncate(ssize_t size)
{
	if ((size < 0) || (size > 1024 * 1024 * 1024)) {
		std::ostringstream str;
		str << "could not truncate file to " << size << " bytes, current size: " << this->size();
		throw std::runtime_error(str.str());
	}

	ssize_t err = ftruncate(fd, size);
	if (err < 0) {
		err = -errno;

		std::ostringstream str;
		str << "could not truncate file to " << size << " bytes: " << err;
		throw std::runtime_error(str.str());
	}

	size_ = size;
	lseek(fd, size, SEEK_SET);
}

int file::get() const
{
	return fd;
}
