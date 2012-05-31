#include <boost/lexical_cast.hpp>

#include <smack/smack.hpp>

using namespace ioremap::smack;

int main(int argc, char *argv[])
{
	logger::instance()->init("/dev/stdout", 10);
	std::string path("/tmp/smack/test");
	long diff;

	//rewrite_test();

	if (argc > 1)
		path.assign(argv[1]);

	log(SMACK_LOG_INFO, "starting test in %s\n", path.c_str());

	size_t bloom_size = 1024;
	size_t max_cache_size = 1000;
	int max_blob_num = 100;
	int cache_thread_num = 4;
	smack<file> s(path, bloom_size, max_cache_size, max_blob_num, cache_thread_num);

	std::string data = "we;lkqrjw34npvqt789340cmq23p490crtm qwpe90xwp oqu;evoeiruqvwoeiruqvbpoeiqnpqvriuevqiouei uropqwie qropeiru qwopeir";
	std::string key_base = "qweqeqwe-";

	long num = 1000000, i;
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
	for (i = 0; i < num; ++i) {
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

	if (i) {
		diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
		log(SMACK_LOG_INFO, "write: num: %ld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
				i, diff / 1000000., i * 1000000 / diff, diff / i);
	}

	s.sync();
#endif

#if 0
	log(SMACK_LOG_INFO, "starting remove test\n");
	for (i = 0; i < num; i += num / 10000 + 1) {
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
	for (i = 0; i < num; ++i) {
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
			break;
		}

		if (i && (i % 10000 == 0)) {
			gettimeofday(&end, NULL);
			long diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
			log(SMACK_LOG_INFO, "read: num: %ld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
					i, diff / 1000000., i * 1000000 / diff, diff / i);
		}
	}
	gettimeofday(&end, NULL);

	if (i) {
		diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
		log(SMACK_LOG_INFO, "read: num: %ld, total-time: %ld usecs, ops: %ld, operation-time: %ld usecs\n",
				i, diff, i * 1000000 / diff, diff / i);
	}

	return 0;
}
