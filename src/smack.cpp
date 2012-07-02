#include <boost/iostreams/filter/zlib.hpp>

#include <smack/smack.hpp>
#include <smack/smack.h>

using namespace ioremap::smack;

enum smack_storage_type {
	SMACK_STORAGE_ZLIB_DEFAULT = 0,
	SMACK_STORAGE_ZLIB_BEST_COMPRESSION,
	SMACK_STORAGE_BZIP2,
	SMACK_STORAGE_SNAPPY,
	SMACK_STORAGE_LZ4_FAST,
	SMACK_STORAGE_LZ4_HIGH,
};

struct smack_ctl {
	union {
		smack_zlib_default	*smzd;
		smack_zlib_best		*smzb;
		smack_bzip2		*smb;
		smack_snappy		*sms;
		smack_lz4_fast		*smlf;
		smack_lz4_high		*smlh;
	} sm;

	smack_storage_type type;
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

	if (!ictl->type) {
		ctl->type = SMACK_STORAGE_ZLIB_DEFAULT;
	} else if (!strcmp(ictl->type, "zlib")) {
		ctl->type = SMACK_STORAGE_ZLIB_DEFAULT;
	} else if (!strcmp(ictl->type, "zlib_best")) {
		ctl->type = SMACK_STORAGE_ZLIB_BEST_COMPRESSION;
	} else if (!strcmp(ictl->type, "bzip2")) {
		ctl->type = SMACK_STORAGE_BZIP2;
	} else if (!strcmp(ictl->type, "snappy")) {
		ctl->type = SMACK_STORAGE_SNAPPY;
	} else if (!strcmp(ictl->type, "lz4_fast")) {
		ctl->type = SMACK_STORAGE_LZ4_FAST;
	} else if (!strcmp(ictl->type, "lz4_high")) {
		ctl->type = SMACK_STORAGE_LZ4_HIGH;
	} else {
		err = -ENOTSUP;
		goto err_out_free;
	}

	if (ictl->log)
		logger::instance()->init(ictl->log, ictl->log_mask);
	try {
		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				ctl->sm.smzd = new smack_zlib_default(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				ctl->sm.smzb = new smack_zlib_best(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			case SMACK_STORAGE_BZIP2:
				ctl->sm.smb = new smack_bzip2(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			case SMACK_STORAGE_SNAPPY:
				ctl->sm.sms = new smack_snappy(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			case SMACK_STORAGE_LZ4_FAST:
				ctl->sm.smlf = new smack_lz4_fast(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				ctl->sm.smlh = new smack_lz4_high(ictl->path,
						ictl->bloom_size, ictl->max_cache_size,
						ictl->max_blob_num, ictl->cache_thread_num);
				break;
		}
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "could not initialize smack\n");
		err = -EINVAL;
		goto err_out_free;
	}

	*errp = 0;
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
	switch (ctl->type) {
		case SMACK_STORAGE_ZLIB_DEFAULT:
			if (ctl->sm.smzd)
				delete ctl->sm.smzd;
			break;
		case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
			if (ctl->sm.smzb)
				delete ctl->sm.smzb;
			break;
		case SMACK_STORAGE_BZIP2:
			if (ctl->sm.smb)
				delete ctl->sm.smb;
			break;
		case SMACK_STORAGE_SNAPPY:
			if (ctl->sm.sms)
				delete ctl->sm.sms;
			break;
		case SMACK_STORAGE_LZ4_FAST:
			if (ctl->sm.smlf)
				delete ctl->sm.smlf;
			break;
		case SMACK_STORAGE_LZ4_HIGH:
			if (ctl->sm.smlh)
				delete ctl->sm.smlh;
			break;
	}

	free(ctl);
}

int smack_read(struct smack_ctl *ctl, struct index *idx, char **datap)
{
	char *data;
	std::string ret;

	key k(idx);
	try {
		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				ret = ctl->sm.smzd->read(k);
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				ret = ctl->sm.smzb->read(k);
				break;
			case SMACK_STORAGE_BZIP2:
				ret = ctl->sm.smb->read(k);
				break;
			case SMACK_STORAGE_SNAPPY:
				ret = ctl->sm.sms->read(k);
				break;
			case SMACK_STORAGE_LZ4_FAST:
				ret = ctl->sm.smlf->read(k);
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				ret = ctl->sm.smlh->read(k);
				break;
		}

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
		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				ctl->sm.smzd->write(k, data, idx->data_size);
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				ctl->sm.smzb->write(k, data, idx->data_size);
				break;
			case SMACK_STORAGE_BZIP2:
				ctl->sm.smb->write(k, data, idx->data_size);
				break;
			case SMACK_STORAGE_SNAPPY:
				ctl->sm.sms->write(k, data, idx->data_size);
				break;
			case SMACK_STORAGE_LZ4_FAST:
				ctl->sm.smlf->write(k, data, idx->data_size);
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				ctl->sm.smlh->write(k, data, idx->data_size);
				break;
		}
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

		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				ctl->sm.smzd->remove(k);
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				ctl->sm.smzb->remove(k);
				break;
			case SMACK_STORAGE_BZIP2:
				ctl->sm.smb->remove(k);
				break;
			case SMACK_STORAGE_SNAPPY:
				ctl->sm.sms->remove(k);
				break;
			case SMACK_STORAGE_LZ4_FAST:
				ctl->sm.smlf->remove(k);
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				ctl->sm.smlh->remove(k);
				break;
		}
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "%s: could not remove data: %s: %s\n", key(idx).str(), e.what(), strerror(errno));
		return -EINVAL;
	}
	return 0;
}

int smack_lookup(struct smack_ctl *ctl, struct index *idx, char **pathp)
{
	try {
		key k(idx);
		std::string path;

		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				path = ctl->sm.smzd->lookup(k);
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				path = ctl->sm.smzb->lookup(k);
				break;
			case SMACK_STORAGE_BZIP2:
				path = ctl->sm.smb->lookup(k);
				break;
			case SMACK_STORAGE_SNAPPY:
				path = ctl->sm.sms->lookup(k);
				break;
			case SMACK_STORAGE_LZ4_FAST:
				path = ctl->sm.smlf->lookup(k);
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				path = ctl->sm.smlh->lookup(k);
				break;
		}

		path += ".data";
		char *p = (char *)malloc(path.size() + 1);
		if (!p)
			return -ENOMEM;

		idx->data_size = k.idx()->data_size;

		sprintf(p, "%s", (char *)path.data());
		*pathp = p;

		return path.size();
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "%s: could not remove data: %s: %s\n", key(idx).str(), e.what(), strerror(errno));
		return -EINVAL;
	}
}

long long smack_total_num(struct smack_ctl *ctl)
{
	try {
		long long num;
		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				num = ctl->sm.smzd->total_num();
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				num = ctl->sm.smzb->total_num();
				break;
			case SMACK_STORAGE_BZIP2:
				num = ctl->sm.smb->total_num();
				break;
			case SMACK_STORAGE_SNAPPY:
				num = ctl->sm.sms->total_num();
				break;
			case SMACK_STORAGE_LZ4_FAST:
				num = ctl->sm.smlf->total_num();
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				num = ctl->sm.smlh->total_num();
				break;
		}

		return num;
	} catch (...) {
		return -EINVAL;
	}
}

void smack_sync(struct smack_ctl *ctl)
{
	try {
		switch (ctl->type) {
			case SMACK_STORAGE_ZLIB_DEFAULT:
				ctl->sm.smzd->sync();
				break;
			case SMACK_STORAGE_ZLIB_BEST_COMPRESSION:
				ctl->sm.smzb->sync();
				break;
			case SMACK_STORAGE_BZIP2:
				ctl->sm.smb->sync();
				break;
			case SMACK_STORAGE_SNAPPY:
				ctl->sm.sms->sync();
				break;
			case SMACK_STORAGE_LZ4_FAST:
				ctl->sm.smlf->sync();
				break;
			case SMACK_STORAGE_LZ4_HIGH:
				ctl->sm.smlh->sync();
				break;
		}
	} catch (const std::exception &e) {
		log(SMACK_LOG_ERROR, "Could not sync: %s\n", e.what());
	}
}

void smack_log_update(struct smack_ctl *, char *log, uint32_t mask)
{
	logger::instance()->init(log, mask);
}
