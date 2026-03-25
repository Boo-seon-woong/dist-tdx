#ifndef TD_LAYOUT_H
#define TD_LAYOUT_H

#include <pthread.h>

#include "td_common.h"
#include "td_tdx.h"

typedef struct td_config td_config_t;

typedef struct {
    uint64_t guard_epoch;
    uint64_t visible_epoch;
    uint64_t key_hash;
    uint64_t tie_breaker;
    uint32_t flags;
    uint32_t value_len;
    unsigned char iv[16];
    unsigned char mac[32];
} td_slot_shared_header_t;

typedef struct {
    td_slot_shared_header_t header;
    /* RDMA-visible encrypted payload. */
    unsigned char ciphertext[TD_MAX_VALUE_SIZE];
} td_slot_t;

typedef struct {
    uint64_t prime_slot_count;
    uint64_t cache_slot_count;
    uint64_t backup_slot_count;
    uint64_t max_value_size;
    uint64_t region_size;
} td_region_header_t;

typedef enum {
    TD_SLOT_PRIVATE_EMPTY = 0,
    TD_SLOT_PRIVATE_LIVE = 1,
    TD_SLOT_PRIVATE_TOMBSTONE = 2,
} td_slot_private_state_t;

typedef struct {
    /* RDMA-visible slot storage lives here. */
    void *base;
    size_t mapped_bytes;
    int fd;
    int anonymous_mapping;
    char backing_path[TD_PATH_BYTES];
    td_tdx_runtime_t tdx;
} td_shared_region_t;

typedef struct {
    /* MN-only control state stays private. */
    td_region_header_t header;
    pthread_mutex_t lock;
    size_t cache_usage;
    size_t eviction_cursor;
    uint8_t *prime_slot_state;
    uint8_t *cache_slot_state;
    uint8_t *backup_slot_state;
} td_private_region_t;

typedef struct {
    td_shared_region_t shared;
    td_private_region_t private_state;
} td_local_region_t;

size_t td_region_required_bytes(const td_config_t *cfg);
int td_region_open(td_local_region_t *region, const td_config_t *cfg, char *err, size_t err_len);
void td_region_close(td_local_region_t *region);
const td_region_header_t *td_region_header_view(const td_local_region_t *region);
void *td_region_shared_base(td_local_region_t *region);
size_t td_region_shared_bytes(const td_local_region_t *region);
const char *td_region_backing_path(const td_local_region_t *region);
const td_tdx_runtime_t *td_region_tdx_runtime(const td_local_region_t *region);
size_t td_region_private_bytes(const td_local_region_t *region);

size_t td_region_kind_slot_count(const td_region_header_t *header, td_region_kind_t kind);
size_t td_region_kind_base_offset(const td_region_header_t *header, td_region_kind_t kind);
size_t td_region_slot_offset(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash);
size_t td_region_slot_index(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash);
size_t td_region_slot_offset_for_index(const td_region_header_t *header, td_region_kind_t kind, size_t slot_index);
td_slot_t *td_region_slot_ptr(td_local_region_t *region, td_region_kind_t kind, size_t slot_index);
size_t td_slot_commit_offset(void);
size_t td_slot_commit_length(void);
void *td_slot_commit_ptr(td_slot_t *slot);
const void *td_slot_commit_const_ptr(const td_slot_t *slot);

int td_region_read_bytes(td_local_region_t *region, size_t offset, void *buf, size_t len);
int td_region_write_bytes(td_local_region_t *region, size_t offset, const void *buf, size_t len);
int td_region_cas64(td_local_region_t *region, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value);

size_t td_region_count_cache_usage(td_local_region_t *region);
void td_region_evict_if_needed(td_local_region_t *region, size_t threshold_pct);

#endif
