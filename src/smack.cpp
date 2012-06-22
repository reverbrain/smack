#include <boost/iostreams/filter/zlib.hpp>

#include <smack/smack.hpp>
#include <smack/smack.h>

using namespace ioremap::smack;
namespace bio = boost::iostreams;

struct smack_ctl {
	smack_zlib		*sm;
};

struct smack_ctl *smack_init(struct smack_init_ctl *ictl, int *errp)
{
	struct smack_ctl *ctl;
	int err;

	ctl = (struct smack_ctl *)malloc(sizeof(struct smack_ctl));
	if (!ctl) {
		err = -ENOMEM;
		goto err_out_exit;
	}

	memset(ctl, 0, sizeof(struct smack_ctl));

	if (ictl->log)
		logger::instance()->init(ictl->log, ictl->log_mask);
	try {
		switch (ictl->type) {
			case SMACK_STORAGE_ZLIB:
				ctl->sm = new smack_zlib(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			default:
				err = -ENOTSUP;
				goto err_out_free;
		}
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "could not initialize smack\n");
		err = -EINVAL;
		goto err_out_free;
	}

	log(SMACK_LOG_INFO, "smack initialized\n");
	return ctl;

err_out_free:
	free(ctl);
err_out_exit:
	*errp = err;
	return NULL;
}

void smack_cleanup(struct smack_ctl *ctl)
{
	if (ctl->sm)
		delete ctl->sm;

	free(ctl);
}

int smack_read(struct smack_ctl *ctl, struct index *idx, char **datap)
{
	char *data;
	std::string ret;

	key k(idx);
	try {
		ret = ctl->sm->read(k);

		data = (char *)malloc(ret.size());
		if (!data)
			return -ENOMEM;

		memcpy(data, ret.data(), ret.size());
		idx->data_size = ret.size();
		*datap = data;

		return 0;
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "%s: could not read data: %s: %s\n", k.str(), e.what(), strerror(errno));
		return -EINVAL;
	}
}

int smack_write(struct smack_ctl *ctl, struct index *idx, const char *data)
{
	key k(idx);
	try {
		ctl->sm->write(k, data, idx->data_size);
		return 0;
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "%s: could not write data: %s: %s\n", k.str(), e.what(), strerror(errno));
		return -EINVAL;
	}
}

int smack_remove(struct smack_ctl *ctl, struct index *idx)
{
	try {
		key k(idx);

		ctl->sm->remove(k);
	} catch (...) {
		return -EINVAL;
	}
	return 0;
}

int smack_lookup(struct smack_ctl *ctl, struct index *idx, char **pathp)
{
	try {
		key k(idx);
		std::string path;

		path = ctl->sm->lookup(k);

		path += ".data";
		char *p = (char *)malloc(path.size() + 1);
		if (!p)
			return -ENOMEM;

		idx->data_size = k.idx()->data_size;

		sprintf(p, "%s", (char *)path.data());
		*pathp = p;

		return path.size();
	} catch (...) {
		return -EINVAL;
	}
}
