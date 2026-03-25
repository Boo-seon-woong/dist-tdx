#ifndef TD_COMMON_H
#define TD_COMMON_H

#define _POSIX_C_SOURCE 200809L

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#define TD_PROJECT_MAGIC 0x5444444953544b56ULL
#define TD_WIRE_MAGIC 0x54445752u

#define TD_MAX_ENDPOINTS 8
#define TD_MAX_VALUE_SIZE 256
#define TD_KEY_MATERIAL_BYTES 32
#define TD_PATH_BYTES 256
#define TD_HOST_BYTES 64
#define TD_KEY_BYTES 128
#define TD_CMD_BYTES 512
#define TD_RECV_QUEUE_DEPTH 32

#define TD_SLOT_FLAG_VALID 0x1u
#define TD_SLOT_FLAG_TOMBSTONE 0x2u

typedef enum {
    TD_MODE_CN = 0,
    TD_MODE_MN = 1,
} td_mode_t;

typedef enum {
    TD_TRANSPORT_TCP = 0,
    TD_TRANSPORT_RDMA = 1,
} td_transport_t;

typedef enum {
    TD_RDMA_BOOTSTRAP_TCP = 0,
    TD_RDMA_BOOTSTRAP_OOB_FILE = 1,
    TD_RDMA_BOOTSTRAP_VSOCK = 2,
} td_rdma_bootstrap_t;

typedef enum {
    TD_CACHE_OFF = 0,
    TD_CACHE_ON = 1,
} td_cache_mode_t;

typedef enum {
    TD_TDX_OFF = 0,
    TD_TDX_ON = 1,
} td_tdx_mode_t;

typedef enum {
    TD_REGION_PRIME = 0,
    TD_REGION_CACHE = 1,
    TD_REGION_BACKUP = 2,
} td_region_kind_t;

typedef struct {
    char host[TD_HOST_BYTES];
    int port;
    int node_id;
} td_endpoint_t;

uint64_t td_hash64_bytes(const void *data, size_t len);
uint64_t td_hash64_string(const char *text);
uint64_t td_now_ns(void);
char *td_trim(char *text);
int td_hex_to_bytes(const char *hex, unsigned char *out, size_t out_len);
void td_format_error(char *buf, size_t buf_len, const char *fmt, ...);
int td_parse_host_port(const char *input, td_endpoint_t *endpoint);

#endif
