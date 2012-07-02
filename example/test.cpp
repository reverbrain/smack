#include <boost/lexical_cast.hpp>

#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

#include <smack/snappy.hpp>
#include <smack/smack.hpp>

using namespace ioremap::smack;
namespace bio = boost::iostreams;

int main(int argc, char *argv[])
{
	std::string path("/tmp/smack/test");
	long diff;

	//rewrite_test();

	if (argc < 2) {
		std::cerr << "Usage: " << argv[0] << " compression <path>" << std::endl;
		return -1;
	}
	if (argc > 2)
		path.assign(argv[2]);

	struct smack_init_ctl ictl;
	struct smack_ctl *sctl;

	memset(&ictl, 0, sizeof(struct smack_init_ctl));
	ictl.path = (char *)path.c_str();
	ictl.log = (char *)"/dev/stdout";
	ictl.log_mask = 10;
	ictl.flush = 1;
	ictl.bloom_size  = 1024;
	ictl.max_cache_size = 1000;
	ictl.max_blob_num = 100;
	ictl.cache_thread_num = 4;
	ictl.type = argv[1];

	int err;
	sctl = smack_init(&ictl, &err);
	if (!sctl)
		return err;

	std::string data = "we;lkqrjw34npvqt789340cmq23p490crtm qwpe90xwp oqu;evoeiruqvwoeiruqvbpoeiqnpqvriuevqiouei uropqwie qropeiru qwopeir";
	std::string key_base = "qweqeqwe-";

	long num = 1000000, i;
	struct timeval start, end;

#if 1
	log(SMACK_LOG_INFO, "starting write test\n");
	gettimeofday(&start, NULL);
	for (i = 0; i < num; ++i) {
		std::ostringstream str;
		str << key_base << i;
		key key(str.str());

		struct index idx = *key.idx();

		log(SMACK_LOG_DATA, "%s: write key: %s\n", key.str(), str.str().c_str());
		std::string d = data + str.str() + "\n";
		idx.data_size = d.size();
		smack_write(sctl, &idx, d.data());

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
		log(SMACK_LOG_INFO, "write: num: %ld/%lld, total-time: %.3f secs, ops: %ld, operation-time: %ld usecs\n",
				i, smack_total_num(sctl), diff / 1000000., i * 1000000 / diff, diff / i);
	}

	smack_sync(sctl);
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

	//logger::instance()->init("/dev/stdout", 15);

	log(SMACK_LOG_INFO, "starting read test\n");
	gettimeofday(&start, NULL);
	for (i = 0; i < num; ++i) {
		std::ostringstream str;
		str << key_base << i;
		key key(str.str());

		log(SMACK_LOG_DATA, "%s: read key: %s\n", key.str(), str.str().c_str());
		char *rdata = NULL;
		try {
			int err = smack_read(sctl, (struct index *)key.idx(), &rdata);
			if (err < 0)
				throw std::runtime_error("no data");

			int len = key.idx()->data_size;
			std::string want = data + str.str() + "\n";

			if (((int)want.size() != len) || memcmp(want.data(), rdata, want.size())) {
				std::ostringstream err;

				log(SMACK_LOG_ERROR, "%s: invalid read: key: %s, data-size: %d, read: '%.*s', want: '%s'\n",
						key.str(), str.str().c_str(), len, len, rdata, want.c_str());
				err << key.str() << ": invalid read: key: " << str.str();
				throw std::runtime_error(err.str());
			}
			free(rdata);
		} catch (const std::exception &e) {
			free(rdata);
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
		log(SMACK_LOG_INFO, "read: num: %ld/%lld, total-time: %ld usecs, ops: %ld, operation-time: %ld usecs\n",
				i, smack_total_num(sctl), diff, i * 1000000 / diff, diff / i);
	}

	return 0;
}
