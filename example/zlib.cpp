#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <fstream>
#include <iostream>
#include <sstream>

#include <boost/version.hpp>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include <smack/base.hpp>

namespace bio = boost::iostreams;

#if BOOST_VERSION < 104400
#define file_desriptor_close_handle	false
#else
#define file_desriptor_close_handle	bio::never_close_handle
#endif

using namespace ioremap::smack;

static size_t test_write(int fd, const std::string &test)
{
	bio::file_descriptor_sink dst(fd, file_desriptor_close_handle);

	bio::filtering_streambuf<bio::output> out;
	out.push(bio::zlib_compressor());
	out.push(dst);

	bio::write<bio::filtering_streambuf<bio::output> >(out, test.data(), test.size());
	out.strict_sync();

	size_t offset = bio::seek<bio::file_descriptor_sink>(dst, 0, std::ios_base::end);

	log(SMACK_LOG_INFO, "written: size: %zd, fd-offset: %zd\n", test.size(), offset);
	return offset;
}

static std::string test_read(int fd, size_t pos)
{
	bio::file_descriptor_source src(fd, file_desriptor_close_handle);

	bio::seek<bio::file_descriptor_source>(src, pos, std::ios_base::beg);
	bio::filtering_streambuf<bio::input> in;
	in.push(bio::zlib_decompressor());
	in.push(src);

	std::ostringstream str;
	bio::copy(in, str);

	log(SMACK_LOG_INFO, "read: offset: %zd, data: %s\n", pos, str.str().c_str());
	return str.str();
}

int main(int argc, char *argv[])
{
	int fd;
	char *path = (char *)"/tmp/zlib-test";

	if (argc > 1)
		path = argv[1];

	logger::instance()->init("/dev/stdout", 15);

	fd = open(path, O_RDWR | O_TRUNC | O_CREAT, 0644);
	if (fd < 0)
		return fd;

	std::string test = "test1: ";
	for (int i = 0; i < 10; ++i)
		test += "01234566789";
	size_t offset = test_write(fd, test);

	test = "test2: ";
	for (int i = 0; i < 10; ++i)
		test += "01234566789";
	test_write(fd, test);

	test_read(fd, 0);
	test_read(fd, offset);
	return 0;
}
