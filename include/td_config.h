#ifndef TD_CONFIG_H
#define TD_CONFIG_H

#include "td_common.h"

typedef enum {
    TD_RDMA_CONTROL_SEND = 0,
    /* Deprecated compatibility value; normalized to SEND during config load. */
    TD_RDMA_CONTROL_WRITE_IMM = 1,
} td_rdma_control_mode_t;

typedef enum {
    TD_RDMA_DATA_DIRECT = 0,
    TD_RDMA_DATA_RPC = 1,
} td_rdma_data_mode_t;

typedef struct td_config {
    td_mode_t mode;
    td_transport_t transport;
    int replication;
    td_tdx_mode_t tdx;
    td_cache_mode_t cache;
    size_t mn_memory_size;
    char encryption_key_hex[(TD_KEY_MATERIAL_BYTES * 2) + 1];
    char rdma_device[TD_HOST_BYTES];
    td_rdma_bootstrap_t rdma_bootstrap;
    td_rdma_control_mode_t rdma_control_mode;
    td_rdma_data_mode_t rdma_data_mode;
    int rdma_skip_hello;
    int rdma_gid_index;
    int rdma_port_num;
    size_t rdma_region_segment_bytes;
    char rdma_oob_dir[TD_PATH_BYTES];
    size_t rdma_oob_poll_ms;
    size_t rdma_oob_timeout_ms;
    char listen_host[TD_HOST_BYTES];
    int listen_port;
    int node_id;
    char memory_file[TD_PATH_BYTES];
    size_t prime_slots;
    size_t cache_slots;
    size_t backup_slots;
    int prime_slots_explicit;
    int cache_slots_explicit;
    int backup_slots_explicit;
    size_t max_value_size;
    size_t eviction_threshold_pct;
    size_t recv_queue_depth;
    td_endpoint_t mn_endpoints[TD_MAX_ENDPOINTS];
    size_t mn_count;
} td_config_t;

void td_config_init_defaults(td_config_t *cfg);
int td_config_load(const char *path, td_config_t *cfg, char *err, size_t err_len);

#endif
