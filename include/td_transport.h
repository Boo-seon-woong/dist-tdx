#ifndef TD_TRANSPORT_H
#define TD_TRANSPORT_H

#include <signal.h>

#include "td_config.h"
#include "td_crypto.h"

typedef enum {
    TD_WIRE_HELLO = 1,
    TD_WIRE_READ = 2,
    TD_WIRE_WRITE = 3,
    TD_WIRE_CAS = 4,
    TD_WIRE_EVICT = 5,
    TD_WIRE_ACK = 6,
    TD_WIRE_CLOSE = 7,
} td_wire_op_t;

#define TD_WIRE_FLAG_PROFILE 0x1u
#define TD_WIRE_FLAG_DIRECT_REGION 0x2u

typedef struct {
    uint32_t magic;
    uint16_t op;
    uint16_t status;
    uint64_t offset;
    uint64_t length;
    uint64_t compare;
    uint64_t swap;
    uint64_t remote_addr;
    uint32_t rkey;
    uint32_t flags;
    uint32_t reserved;
    td_region_header_t header;
    uint64_t profile_total_ns;
    uint64_t profile_stage1_ns;
    uint64_t profile_stage2_ns;
    uint64_t profile_stage3_ns;
} td_wire_msg_t;

struct td_session;

typedef struct {
    uint64_t read_send_ns;
    uint64_t read_wait_ns;
    uint64_t read_copy_ns;
    uint64_t write_copy_ns;
    uint64_t write_send_ns;
    uint64_t write_wait_ns;
    uint64_t cas_send_ns;
    uint64_t cas_wait_ns;
    uint64_t control_send_ns;
    uint64_t control_wait_ns;
    uint64_t tcp_read_request_header_send_ns;
    uint64_t tcp_read_response_header_wait_ns;
    uint64_t tcp_read_response_payload_wait_ns;
    uint64_t tcp_write_request_header_send_ns;
    uint64_t tcp_write_request_payload_send_ns;
    uint64_t tcp_write_response_header_wait_ns;
    uint64_t tcp_cas_request_header_send_ns;
    uint64_t tcp_cas_response_header_wait_ns;
    uint64_t tcp_control_request_header_send_ns;
    uint64_t tcp_control_response_header_wait_ns;
    uint64_t tcp_server_read_total_ns;
    uint64_t tcp_server_read_alloc_ns;
    uint64_t tcp_server_read_region_ns;
    uint64_t tcp_server_write_total_ns;
    uint64_t tcp_server_write_recv_ns;
    uint64_t tcp_server_write_region_ns;
    uint64_t tcp_server_cas_total_ns;
    uint64_t tcp_server_cas_region_ns;
    uint64_t tcp_server_control_total_ns;
    uint64_t tcp_server_control_exec_ns;
    uint64_t tcp_read_wire_estimate_ns;
    uint64_t tcp_write_wire_estimate_ns;
    uint64_t tcp_cas_wire_estimate_ns;
    uint64_t tcp_control_wire_estimate_ns;
    uint64_t rdma_read_post_send_ns;
    uint64_t rdma_read_poll_cq_ns;
    uint64_t rdma_read_backoff_ns;
    uint64_t rdma_write_post_send_ns;
    uint64_t rdma_write_poll_cq_ns;
    uint64_t rdma_write_backoff_ns;
    uint64_t rdma_cas_request_post_send_ns;
    uint64_t rdma_cas_request_send_wait_ns;
    uint64_t rdma_cas_request_send_poll_cq_ns;
    uint64_t rdma_cas_request_send_backoff_ns;
    uint64_t rdma_cas_response_wait_ns;
    uint64_t rdma_cas_response_poll_cq_ns;
    uint64_t rdma_cas_response_backoff_ns;
    uint64_t rdma_control_request_post_send_ns;
    uint64_t rdma_control_request_send_wait_ns;
    uint64_t rdma_control_request_send_poll_cq_ns;
    uint64_t rdma_control_request_send_backoff_ns;
    uint64_t rdma_control_response_wait_ns;
    uint64_t rdma_control_response_poll_cq_ns;
    uint64_t rdma_control_response_backoff_ns;
    uint64_t rdma_response_copy_ns;
    uint64_t rdma_server_read_total_ns;
    uint64_t rdma_server_read_region_ns;
    uint64_t rdma_server_write_total_ns;
    uint64_t rdma_server_write_region_ns;
    uint64_t rdma_server_cas_total_ns;
    uint64_t rdma_server_cas_region_ns;
    uint64_t rdma_server_control_total_ns;
    uint64_t rdma_server_control_exec_ns;
    size_t rdma_read_empty_polls;
    size_t rdma_read_backoff_count;
    size_t rdma_write_empty_polls;
    size_t rdma_write_backoff_count;
    size_t rdma_cas_send_empty_polls;
    size_t rdma_cas_send_backoff_count;
    size_t rdma_cas_response_empty_polls;
    size_t rdma_cas_response_backoff_count;
    size_t rdma_control_send_empty_polls;
    size_t rdma_control_send_backoff_count;
    size_t rdma_control_response_empty_polls;
    size_t rdma_control_response_backoff_count;
} td_transport_profile_t;

typedef int (*td_read_fn)(struct td_session *session, size_t offset, void *buf, size_t len, char *err, size_t err_len);
typedef int (*td_write_fn)(struct td_session *session, size_t offset, const void *buf, size_t len, char *err, size_t err_len);
typedef int (*td_cas_fn)(struct td_session *session, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value, char *err, size_t err_len);
typedef int (*td_ctrl_fn)(struct td_session *session, td_wire_op_t op, char *err, size_t err_len);
typedef void (*td_close_fn)(struct td_session *session);

typedef struct td_session {
    td_transport_t transport;
    td_endpoint_t endpoint;
    uint64_t remote_addr;
    uint32_t rkey;
    td_region_header_t header;
    size_t region_size;
    void *impl;
    td_transport_profile_t *transport_profile;
    td_read_fn read_region;
    td_write_fn write_region;
    td_cas_fn cas64;
    td_ctrl_fn control;
    td_close_fn close;
} td_session_t;

int td_session_connect(td_session_t *session, const td_config_t *cfg, const td_endpoint_t *endpoint, char *err, size_t err_len);
void td_session_close(td_session_t *session);

int td_tcp_server_run(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len);
int td_rdma_server_run(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len);

#endif
