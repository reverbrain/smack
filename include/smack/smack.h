#ifndef __SMACK_H
#define __SMACK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

#include <stdint.h>
#include <stdio.h>

#define SMACK_KEY_SIZE		64

struct index {
	unsigned char		id[SMACK_KEY_SIZE];
	uint64_t		ts;
	uint64_t		flags;
	uint64_t		data_offset; /* offset within chunk */
	uint64_t		data_size;
};

enum smack_storage_type {
	SMACK_STORAGE_FILE = 0,
	SMACK_STORAGE_MMAP,
	SMACK_STORAGE_ZLIB,
};

struct smack_ctl;

struct smack_init_ctl {
	char			*path;
	char			*log;
	int			log_mask;
	int			flush;

	int			bloom_size;
	int			max_cache_size;
	int			max_blob_num;
	int			cache_thread_num;
	enum			smack_storage_type type;
};

struct smack_ctl *smack_init(struct smack_init_ctl *ictl, int *errp);
void smack_cleanup(struct smack_ctl *ctl);

int smack_read(struct smack_ctl *ctl, struct index *idx, char **datap);
int smack_write(struct smack_ctl *ctl, struct index *idx, const char *data);
int smack_remove(struct smack_ctl *ctl, struct index *idx);
int smack_lookup(struct smack_ctl *ctl, struct index *idx, char **pathp);

#ifdef __cplusplus
}
#endif

#endif /* __SMACK_H */
