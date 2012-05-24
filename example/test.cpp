#include <boost/lexical_cast.hpp>

#include <smack/smack.hpp>

using namespace ioremap::smack;

static void rewrite_test()
{
	smack<zlib> s("/tmp/smack/test", 100, 1, 1, 1);

	std::string data = "01234567890";
	std::string key_str = "test key";

	for (int i = 0; i < 1; ++i) {
		s.write(key(key_str), data.data(), data.size());

		std::string nk = "qwe" + boost::lexical_cast<std::string>(i);
		s.write(key(nk), data.data(), data.size());
	}

	key key(key_str);
	std::string ret = s.read(key);
	log(SMACK_LOG_INFO, "%s: size: %zd, data: '%s'\n", key_str.c_str(), ret.size(), ret.c_str());
	exit(0);
}

int main(int argc, char *argv[])
{
	logger::instance()->init("/dev/stdout", 0xff);
	std::string path("/tmp/smack/test");
	long diff;

	rewrite_test();

	if (argc > 1)
		path.assign(argv[1]);

	log(SMACK_LOG_INFO, "starting test in %s\n", path.c_str());

	size_t bloom_size = 1024 * 1024;
	size_t max_cache_size = 1000;
	int max_blob_num = 5000;
	int cache_thread_num = 16;
	smack<zlib> s(path, bloom_size, max_cache_size, max_blob_num, cache_thread_num);

	std::string data = "we;lkqrjw34npvqt789340cmq23p490crtm qwpe90xwp oqu;evoeiruqvwoeiruqvbpoeiqnpqvriuevqiouei uropqwie qropeiru qwopeir";
	std::string key_base = "qweqeqwe-";

	long num = 100000000;
	struct timeval start, end;

#if 0
	io_test<file>("/tmp/smack/smack", num);
	io_test<mmap>("/tmp/smack/smack", num);
	io_test<bloom>("/tmp/smack/smack", num);
	exit(0);
#endif
	//logger::instance()->init("/dev/stdout", 0xff);

#if 0
	log(SMACK_LOG_INFO, "starting write test\n");
	gettimeofday(&start, NULL);
	for (long i = 0; i < num; ++i) {
		std::ostringstream str;
		str << key_base << i;
		key key(str.str());

		log(SMACK_LOG_DATA, "%s: write key: %s\n", key.str(), str.str().c_str());
		std::string d = data + str.str() + "\n";
		s.write(key, d.data(), d.size());

		if (i && (i % 100000 == 0)) {
			gettimeofday(&end, NULL);
			long diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
			log(SMACK_LOG_INFO, "write: num: %ld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
					i, diff / 1000000., i * 1000000 / diff, diff / i);
		}
	}
	gettimeofday(&end, NULL);

	diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
	log(SMACK_LOG_INFO, "write: num: %ld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
			num, diff / 1000000., num * 1000000 / diff, diff / num);
#endif

#if 0
	log(SMACK_LOG_INFO, "starting remove test\n");
	for (long i = 0; i < num; i += num / 10000 + 1) {
		std::ostringstream str;
		str << key_base << i;
		key key(str.str());
		s.remove(key);
	}

	s.sync();
	logger::instance()->init("/dev/stdout", 10);
#endif

	log(SMACK_LOG_INFO, "starting read test\n");
	gettimeofday(&start, NULL);
	for (long i = 0; i < num; ++i) {
		std::ostringstream str;
		str << key_base << i;
		key key(str.str());

		log(SMACK_LOG_DATA, "%s: read key: %s\n", key.str(), str.str().c_str());
		try {
			std::string d = s.read(key);
			std::string want = data + str.str() + "\n";

			if (d != want) {
				std::ostringstream err;

				log(SMACK_LOG_ERROR, "%s: invalid read: key: %s, data-size: %zd, read: '%s', want: '%s'\n",
						key.str(), str.str().c_str(), d.size(), d.c_str(), want.c_str());
				err << key.str() << ": invalid read: key: " << str.str();
				throw std::runtime_error(err.str());
			}

		} catch (const std::exception &e) {
			log(SMACK_LOG_ERROR, "%s: could not read key '%s': %s\n", key.str(), str.str().c_str(), e.what());
			continue;
		}

		if (i && (i % 10000 == 0)) {
			gettimeofday(&end, NULL);
			long diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
			log(SMACK_LOG_INFO, "read: num: %ld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
					i, diff / 1000000., i * 1000000 / diff, diff / i);
		}
	}
	gettimeofday(&end, NULL);

	diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
	log(SMACK_LOG_INFO, "read: num: %ld, total-time: %ld usecs, ops: %ld, operation-time: %ld usecs\n",
			num, diff, num * 1000000 / diff, diff / num);
}
