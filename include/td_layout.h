#ifndef TD_LAYOUT_H
#define TD_LAYOUT_H

#include <pthread.h>

#include "td_config.h"

typedef struct {
    uint64_t guard_epoch;
    uint64_t visible_epoch;
    uint64_t key_hash;
    uint64_t tie_breaker;
    uint32_t flags;
    uint32_t value_len;
    unsigned char iv[16];
    unsigned char mac[32];
    unsigned char ciphertext[TD_MAX_VALUE_SIZE];
} td_slot_t;

typedef struct {
    uint64_t magic;
    uint64_t version;
    uint64_t node_id;
    uint64_t prime_slot_count;
    uint64_t cache_slot_count;
    uint64_t backup_slot_count;
    uint64_t max_value_size;
    uint64_t cache_usage;
    uint64_t eviction_cursor;
    uint64_t cache_mode;
    uint64_t region_size;
} td_region_header_t;

typedef struct {
    void *base;
    size_t mapped_bytes;
    int fd;
    int anonymous_mapping;
    char backing_path[TD_PATH_BYTES];
    td_region_header_t *header;
    pthread_mutex_t lock;
} td_local_region_t;

size_t td_region_required_bytes(const td_config_t *cfg);
int td_region_open(td_local_region_t *region, const td_config_t *cfg, char *err, size_t err_len);
void td_region_close(td_local_region_t *region);

size_t td_region_kind_slot_count(const td_region_header_t *header, td_region_kind_t kind);
size_t td_region_kind_base_offset(const td_region_header_t *header, td_region_kind_t kind);
size_t td_region_slot_offset(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash);
size_t td_region_slot_index(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash);
size_t td_region_slot_offset_for_index(const td_region_header_t *header, td_region_kind_t kind, size_t slot_index);
td_slot_t *td_region_slot_ptr(td_local_region_t *region, td_region_kind_t kind, size_t slot_index);

int td_region_read_bytes(td_local_region_t *region, size_t offset, void *buf, size_t len);
int td_region_write_bytes(td_local_region_t *region, size_t offset, const void *buf, size_t len);
int td_region_cas64(td_local_region_t *region, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value);

size_t td_region_count_cache_usage(td_local_region_t *region);
void td_region_evict_if_needed(td_local_region_t *region, size_t threshold_pct);

#endif
