#include "td_transport.h"

#include <dirent.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <linux/vm_sockets.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum {
    TD_RDMA_BOOTSTRAP_MAGIC = 0x54444253u,
    TD_RDMA_BOOTSTRAP_VERSION = 1,
    TD_RDMA_OOB_MAGIC = 0x54444f42u,
    TD_RDMA_OOB_VERSION = 1,
    TD_RDMA_OOB_REQUEST = 1,
    TD_RDMA_OOB_RESPONSE = 2,
};

typedef struct {
    uint16_t lid;
    uint8_t mtu;
    uint8_t has_gid;
    uint8_t gid[16];
    uint8_t reserved0;
    uint16_t reserved1;
    uint32_t qp_num;
    uint32_t psn;
} td_rdma_conn_info_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    td_rdma_conn_info_t conn;
} td_rdma_bootstrap_msg_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    int32_t target_node_id;
    int32_t source_node_id;
    uint64_t session_id;
    td_rdma_conn_info_t conn;
} td_rdma_oob_record_t;

typedef struct {
    struct ibv_context *verbs;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    struct ibv_mr *op_mr;
    td_wire_msg_t *send_msg;
    td_wire_msg_t *recv_msg;
    unsigned char *op_buf;
    size_t op_buf_len;
    td_tdx_runtime_t tdx;
    int port_num;
    int gid_index;
    uint16_t lid;
    enum ibv_mtu active_mtu;
    union ibv_gid gid;
    int has_gid;
    uint32_t psn;
} td_rdma_impl_t;

typedef struct {
    td_rdma_impl_t impl;
    td_local_region_t *region;
    size_t eviction_threshold_pct;
    volatile sig_atomic_t *stop_flag;
    struct ibv_mr *region_mr;
} td_rdma_server_conn_t;

typedef struct {
    uint64_t *poll_cq_ns;
    uint64_t *backoff_ns;
    size_t *empty_polls;
    size_t *backoff_count;
} td_rdma_wait_profile_t;

static uint64_t td_rdma_profile_begin(td_session_t *session) {
    return session->transport_profile != NULL ? td_now_ns() : 0;
}

static void td_rdma_profile_end(td_session_t *session, uint64_t start_ns, uint64_t *field) {
    if (session->transport_profile != NULL && field != NULL && start_ns != 0) {
        *field += td_now_ns() - start_ns;
    }
}

static void td_rdma_tune_socket(int fd) {
    int one = 1;

    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef TCP_QUICKACK
    (void)setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
#endif
}

static int td_send_all(int fd, const void *buf, size_t len) {
    const unsigned char *cursor = (const unsigned char *)buf;

    while (len > 0) {
        ssize_t written = send(fd, cursor, len, 0);

        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        cursor += (size_t)written;
        len -= (size_t)written;
    }
    return 0;
}

static int td_recv_all(int fd, void *buf, size_t len) {
    unsigned char *cursor = (unsigned char *)buf;

    while (len > 0) {
        ssize_t read_bytes = recv(fd, cursor, len, 0);

        if (read_bytes == 0) {
            return -1;
        }
        if (read_bytes < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        cursor += (size_t)read_bytes;
        len -= (size_t)read_bytes;
    }
    return 0;
}

static void td_rdma_sleep_ms(size_t ms) {
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000ULL);
    nanosleep(&ts, NULL);
}

static uint64_t td_rdma_make_session_id(int target_node_id) {
    static uint64_t seq = 0;

    seq += 1;
    return td_now_ns() ^ ((uint64_t)(uint32_t)getpid() << 16) ^ (uint64_t)(uint32_t)(target_node_id + 1) ^ seq;
}

static int td_rdma_format_oob_path(const char *dir, const char *kind, int target_node_id, uint64_t session_id, char *path, size_t path_len) {
    int written;

    written = snprintf(path, path_len, "%s/rdma-%s.mn%d.sid%016llx.bin",
        dir,
        kind,
        target_node_id,
        (unsigned long long)session_id);
    return written > 0 && (size_t)written < path_len ? 0 : -1;
}

static int td_rdma_parse_oob_request_name(const char *name, int *target_node_id, uint64_t *session_id) {
    static const char prefix[] = "rdma-req.mn";
    const char *cursor = name + sizeof(prefix) - 1;
    char *end = NULL;
    unsigned long parsed_target;
    unsigned long long parsed_session;

    if (strncmp(name, prefix, sizeof(prefix) - 1) != 0) {
        return -1;
    }
    errno = 0;
    parsed_target = strtoul(cursor, &end, 10);
    if (errno != 0 || end == cursor || strncmp(end, ".sid", 4) != 0) {
        return -1;
    }
    cursor = end + 4;
    errno = 0;
    parsed_session = strtoull(cursor, &end, 16);
    if (errno != 0 || end == cursor || strcmp(end, ".bin") != 0) {
        return -1;
    }
    if (target_node_id != NULL) {
        *target_node_id = (int)parsed_target;
    }
    if (session_id != NULL) {
        *session_id = (uint64_t)parsed_session;
    }
    return 0;
}

static int td_rdma_write_binary_file(const char *path, const void *data, size_t data_len, char *err, size_t err_len) {
    char temp_path[TD_PATH_BYTES * 2];
    FILE *fp = NULL;

    if (snprintf(temp_path, sizeof(temp_path), "%s.tmp.%ld", path, (long)getpid()) >= (int)sizeof(temp_path)) {
        td_format_error(err, err_len, "rdma oob temp path too long");
        return -1;
    }

    fp = fopen(temp_path, "wb");
    if (fp == NULL) {
        td_format_error(err, err_len, "cannot open rdma oob temp file %s; rdma_oob_dir must be a filesystem path shared by both CN and MN", temp_path);
        return -1;
    }
    if (fwrite(data, 1, data_len, fp) != data_len || fflush(fp) != 0) {
        fclose(fp);
        unlink(temp_path);
        td_format_error(err, err_len, "cannot write rdma oob temp file %s", temp_path);
        return -1;
    }
    if (fclose(fp) != 0) {
        unlink(temp_path);
        td_format_error(err, err_len, "cannot close rdma oob temp file %s", temp_path);
        return -1;
    }
    if (rename(temp_path, path) != 0) {
        unlink(temp_path);
        td_format_error(err, err_len, "cannot publish rdma oob file %s", path);
        return -1;
    }
    return 0;
}

static int td_rdma_read_binary_file(const char *path, void *data, size_t data_len, int missing_ok, char *err, size_t err_len) {
    FILE *fp = NULL;

    fp = fopen(path, "rb");
    if (fp == NULL) {
        if (missing_ok && errno == ENOENT) {
            return 1;
        }
        td_format_error(err, err_len, "cannot open rdma oob file %s", path);
        return -1;
    }
    if (fread(data, 1, data_len, fp) != data_len) {
        fclose(fp);
        td_format_error(err, err_len, "cannot read rdma oob file %s", path);
        return -1;
    }
    if (fgetc(fp) != EOF) {
        fclose(fp);
        td_format_error(err, err_len, "rdma oob file %s has unexpected trailing bytes", path);
        return -1;
    }
    fclose(fp);
    return 0;
}

static int td_rdma_validate_oob_record(const td_rdma_oob_record_t *record, uint16_t expected_kind, int target_node_id, uint64_t session_id, char *err, size_t err_len) {
    if (record->magic != TD_RDMA_OOB_MAGIC) {
        td_format_error(err, err_len, "rdma oob magic mismatch");
        return -1;
    }
    if (record->version != TD_RDMA_OOB_VERSION) {
        td_format_error(err, err_len, "rdma oob version mismatch");
        return -1;
    }
    if (record->kind != expected_kind) {
        td_format_error(err, err_len, "rdma oob kind mismatch");
        return -1;
    }
    if (record->target_node_id != target_node_id) {
        td_format_error(err, err_len, "rdma oob target node mismatch");
        return -1;
    }
    if (record->session_id != session_id) {
        td_format_error(err, err_len, "rdma oob session mismatch");
        return -1;
    }
    return 0;
}

static int td_rdma_gid_is_zero(const union ibv_gid *gid) {
    const unsigned char *bytes = (const unsigned char *)gid;
    size_t idx;

    if (gid == NULL) {
        return 1;
    }
    for (idx = 0; idx < sizeof(*gid); ++idx) {
        if (bytes[idx] != 0) {
            return 0;
        }
    }
    return 1;
}

static int td_rdma_read_int_file(const char *path, int *value) {
    FILE *fp;
    char line[64];
    char *end = NULL;
    long parsed;

    if (value == NULL) {
        return -1;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        return -1;
    }
    if (fgets(line, sizeof(line), fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    errno = 0;
    parsed = strtol(line, &end, 0);
    if (errno != 0 || end == line) {
        return -1;
    }
    *value = (int)parsed;
    return 0;
}

static int td_rdma_pick_first_dirent(const char *path, char *name, size_t name_len) {
    DIR *dir;
    struct dirent *entry;

    dir = opendir(path);
    if (dir == NULL) {
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        snprintf(name, name_len, "%s", entry->d_name);
        closedir(dir);
        return 0;
    }

    closedir(dir);
    return -1;
}

static int td_rdma_parse_vsock_cid(const char *text, unsigned int *cid, char *err, size_t err_len) {
    char *end = NULL;
    unsigned long long parsed;

    if (text == NULL || text[0] == '\0') {
        td_format_error(err, err_len, "vsock bootstrap requires a non-empty CID");
        return -1;
    }
    if (strcmp(text, "host") == 0) {
        *cid = VMADDR_CID_HOST;
        return 0;
    }

    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed > UINT32_MAX) {
        td_format_error(err, err_len, "invalid vsock CID %s", text);
        return -1;
    }

    *cid = (unsigned int)parsed;
    return 0;
}

static int td_rdma_resolve_device_name(const char *configured, char *verbs_name, size_t verbs_name_len, int *port_num, char *err, size_t err_len) {
    struct ibv_device **devices = NULL;
    int device_count = 0;
    int idx;

    devices = ibv_get_device_list(&device_count);
    if (devices == NULL || device_count <= 0) {
        td_format_error(err, err_len, "ibv_get_device_list failed");
        return -1;
    }

    if (configured == NULL || configured[0] == '\0') {
        snprintf(verbs_name, verbs_name_len, "%s", ibv_get_device_name(devices[0]));
        ibv_free_device_list(devices);
        return 0;
    }

    for (idx = 0; idx < device_count; ++idx) {
        if (strcmp(configured, ibv_get_device_name(devices[idx])) == 0) {
            snprintf(verbs_name, verbs_name_len, "%s", configured);
            ibv_free_device_list(devices);
            return 0;
        }
    }

    {
        char path[TD_PATH_BYTES];
        int inferred_port = 0;

        snprintf(path, sizeof(path), "/sys/class/net/%s/device/infiniband", configured);
        if (td_rdma_pick_first_dirent(path, verbs_name, verbs_name_len) != 0) {
            ibv_free_device_list(devices);
            td_format_error(err, err_len,
                "cannot map rdma_device %s to an IB verbs device; rdma_device must be the local host/guest device name seen by this process",
                configured);
            return -1;
        }

        snprintf(path, sizeof(path), "/sys/class/net/%s/dev_port", configured);
        if (td_rdma_read_int_file(path, &inferred_port) == 0 && port_num != NULL && *port_num <= 0) {
            *port_num = inferred_port + 1;
        }
    }

    ibv_free_device_list(devices);
    return 0;
}

static int td_rdma_open_device(td_rdma_impl_t *impl, const td_config_t *cfg, char *err, size_t err_len) {
    struct ibv_device **devices = NULL;
    int device_count = 0;
    char verbs_name[TD_HOST_BYTES];
    int idx;
    int port_num = cfg->rdma_port_num;

    if (td_rdma_resolve_device_name(cfg->rdma_device, verbs_name, sizeof(verbs_name), &port_num, err, err_len) != 0) {
        return -1;
    }

    devices = ibv_get_device_list(&device_count);
    if (devices == NULL || device_count <= 0) {
        td_format_error(err, err_len, "ibv_get_device_list failed");
        return -1;
    }

    for (idx = 0; idx < device_count; ++idx) {
        if (strcmp(verbs_name, ibv_get_device_name(devices[idx])) == 0) {
            impl->verbs = ibv_open_device(devices[idx]);
            break;
        }
    }
    ibv_free_device_list(devices);

    if (impl->verbs == NULL) {
        td_format_error(err, err_len, "cannot open RDMA device %s", verbs_name);
        return -1;
    }

    impl->port_num = port_num > 0 ? port_num : 1;
    impl->gid_index = cfg->rdma_gid_index;
    return 0;
}

static int td_rdma_query_local_conn_info(td_rdma_impl_t *impl, td_rdma_conn_info_t *info, char *err, size_t err_len) {
    struct ibv_port_attr port_attr;

    memset(info, 0, sizeof(*info));
    if (ibv_query_port(impl->verbs, (uint8_t)impl->port_num, &port_attr) != 0) {
        td_format_error(err, err_len, "ibv_query_port failed on port %d", impl->port_num);
        return -1;
    }
    if (port_attr.state != IBV_PORT_ACTIVE) {
        td_format_error(err, err_len, "RDMA port %d is not active", impl->port_num);
        return -1;
    }

    impl->lid = port_attr.lid;
    impl->active_mtu = port_attr.active_mtu;
    memset(&impl->gid, 0, sizeof(impl->gid));
    impl->has_gid = 0;
    if (impl->gid_index >= 0 &&
        ibv_query_gid(impl->verbs, (uint8_t)impl->port_num, impl->gid_index, &impl->gid) == 0 &&
        !td_rdma_gid_is_zero(&impl->gid)) {
        impl->has_gid = 1;
    }

    info->lid = impl->lid;
    info->mtu = (uint8_t)impl->active_mtu;
    info->has_gid = (uint8_t)impl->has_gid;
    memcpy(info->gid, &impl->gid, sizeof(info->gid));
    info->qp_num = impl->qp->qp_num;
    info->psn = impl->psn;
    return 0;
}

static int td_rdma_qp_to_init(td_rdma_impl_t *impl, char *err, size_t err_len) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = (uint8_t)impl->port_num;
    attr.pkey_index = 0;
    attr.qp_access_flags = 0;

    if (ibv_modify_qp(impl->qp, &attr,
            IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
        td_format_error(err, err_len, "ibv_modify_qp INIT failed");
        return -1;
    }
    return 0;
}

static int td_rdma_qp_to_rtr(td_rdma_impl_t *impl, const td_rdma_conn_info_t *remote, char *err, size_t err_len) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = impl->active_mtu < (enum ibv_mtu)remote->mtu ? impl->active_mtu : (enum ibv_mtu)remote->mtu;
    attr.dest_qp_num = remote->qp_num;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = (uint8_t)impl->port_num;

    if (remote->lid == 0) {
        if (!remote->has_gid || impl->gid_index < 0) {
            td_format_error(err, err_len, "remote RDMA endpoint does not provide a routable LID or GID");
            return -1;
        }
        attr.ah_attr.is_global = 1;
        memcpy(&attr.ah_attr.grh.dgid, remote->gid, sizeof(attr.ah_attr.grh.dgid));
        attr.ah_attr.grh.sgid_index = (uint8_t)impl->gid_index;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.traffic_class = 0;
        attr.ah_attr.grh.flow_label = 0;
    }

    if (ibv_modify_qp(impl->qp, &attr,
            IBV_QP_STATE |
            IBV_QP_AV |
            IBV_QP_PATH_MTU |
            IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN |
            IBV_QP_MAX_DEST_RD_ATOMIC |
            IBV_QP_MIN_RNR_TIMER) != 0) {
        td_format_error(err, err_len, "ibv_modify_qp RTR failed");
        return -1;
    }
    return 0;
}

static int td_rdma_qp_to_rts(td_rdma_impl_t *impl, char *err, size_t err_len) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = impl->psn;
    attr.max_rd_atomic = 1;

    if (ibv_modify_qp(impl->qp, &attr,
            IBV_QP_STATE |
            IBV_QP_TIMEOUT |
            IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY |
            IBV_QP_SQ_PSN |
            IBV_QP_MAX_QP_RD_ATOMIC) != 0) {
        td_format_error(err, err_len, "ibv_modify_qp RTS failed");
        return -1;
    }
    return 0;
}

static int td_rdma_post_recv(td_rdma_impl_t *impl, char *err, size_t err_len) {
    struct ibv_sge sges[2];
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(sges, 0, sizeof(sges));
    sges[0].addr = (uintptr_t)impl->recv_msg;
    sges[0].length = sizeof(*impl->recv_msg);
    sges[0].lkey = impl->recv_mr->lkey;
    sges[1].addr = (uintptr_t)impl->op_buf;
    sges[1].length = impl->op_buf_len;
    sges[1].lkey = impl->op_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = sges;
    wr.num_sge = 2;

    if (ibv_post_recv(impl->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma post recv failed");
        return -1;
    }
    return 0;
}

static int td_rdma_poll_wc(td_rdma_impl_t *impl, enum ibv_wc_opcode expected, td_rdma_wait_profile_t *profile, struct ibv_wc *out_wc, char *err, size_t err_len) {
    struct ibv_wc wc;

    for (;;) {
        uint64_t poll_start_ns = profile != NULL && profile->poll_cq_ns != NULL ? td_now_ns() : 0;
        int n = ibv_poll_cq(impl->cq, 1, &wc);

        if (poll_start_ns != 0) {
            *profile->poll_cq_ns += td_now_ns() - poll_start_ns;
        }
        if (n < 0) {
            td_format_error(err, err_len, "rdma poll cq failed");
            return -1;
        }
        if (n == 0) {
            uint64_t backoff_start_ns = profile != NULL && profile->backoff_ns != NULL ? td_now_ns() : 0;

            if (profile != NULL && profile->empty_polls != NULL) {
                ++(*profile->empty_polls);
            }
            sched_yield();
            if (profile != NULL && profile->backoff_count != NULL) {
                ++(*profile->backoff_count);
            }
            if (backoff_start_ns != 0) {
                *profile->backoff_ns += td_now_ns() - backoff_start_ns;
            }
            continue;
        }
        if (wc.status != IBV_WC_SUCCESS) {
            td_format_error(err, err_len, "rdma completion failed status=%d (%s)", wc.status, ibv_wc_status_str(wc.status));
            return -1;
        }
        if (expected == (enum ibv_wc_opcode)-1 || wc.opcode == expected) {
            if (out_wc != NULL) {
                *out_wc = wc;
            }
            return 0;
        }
    }
}

static int td_rdma_register_shared_buffer_mr(td_rdma_impl_t *impl, void *base, size_t bytes, int access, struct ibv_mr **out_mr, char *err, size_t err_len) {
    struct ibv_mr *mr;

    if (td_tdx_accept_shared_memory(&impl->tdx, base, bytes, err, err_len) != 0) {
        return -1;
    }
    mr = ibv_reg_mr(impl->pd, base, bytes, access);
    if (mr == NULL) {
        (void)td_tdx_release_shared_memory(&impl->tdx, base, bytes, NULL, 0);
        td_format_error(err, err_len, "rdma mr registration failed");
        return -1;
    }
    *out_mr = mr;
    return 0;
}

static int td_rdma_register_server_region(td_rdma_server_conn_t *conn, char *err, size_t err_len) {
    fprintf(stderr, "[MN-DEBUG] registering shared region MR base=%p bytes=%zu\n",
        td_region_shared_base(conn->region),
        td_region_shared_bytes(conn->region));
    fflush(stderr);
    conn->region_mr = ibv_reg_mr(
        conn->impl.pd,
        td_region_shared_base(conn->region),
        td_region_shared_bytes(conn->region),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (conn->region_mr == NULL) {
        int saved_errno = errno;

        fprintf(stderr,
            "[MN-WARN] shared region MR registration failed: %s; falling back to SEND/RECV path for READ/WRITE\n",
            strerror(saved_errno));
        fflush(stderr);
        if (err != NULL && err_len > 0) {
            err[0] = '\0';
        }
        return 0;
    }
    fprintf(stderr, "[MN-DEBUG] shared region MR registered lkey=%u rkey=%u\n",
        conn->region_mr->lkey,
        conn->region_mr->rkey);
    fflush(stderr);
    return 0;
}

static void td_rdma_accumulate_server_profile(td_transport_profile_t *profile, td_wire_op_t op, const td_wire_msg_t *response) {
    if (profile == NULL || response == NULL) {
        return;
    }

    switch (op) {
        case TD_WIRE_READ:
            profile->rdma_server_read_total_ns += response->profile_total_ns;
            profile->rdma_server_read_region_ns += response->profile_stage1_ns;
            break;
        case TD_WIRE_WRITE:
            profile->rdma_server_write_total_ns += response->profile_total_ns;
            profile->rdma_server_write_region_ns += response->profile_stage1_ns;
            break;
        case TD_WIRE_CAS:
            profile->rdma_server_cas_total_ns += response->profile_total_ns;
            profile->rdma_server_cas_region_ns += response->profile_stage1_ns;
            break;
        case TD_WIRE_HELLO:
        case TD_WIRE_EVICT:
        case TD_WIRE_CLOSE:
            profile->rdma_server_control_total_ns += response->profile_total_ns;
            profile->rdma_server_control_exec_ns += response->profile_stage1_ns;
            break;
        default:
            break;
    }
}

static void td_rdma_destroy_impl(td_rdma_impl_t *impl) {
    if (impl->send_mr != NULL) {
        ibv_dereg_mr(impl->send_mr);
    }
    if (impl->recv_mr != NULL) {
        ibv_dereg_mr(impl->recv_mr);
    }
    if (impl->op_mr != NULL) {
        ibv_dereg_mr(impl->op_mr);
    }
    if (impl->qp != NULL) {
        ibv_destroy_qp(impl->qp);
    }
    if (impl->cq != NULL) {
        ibv_destroy_cq(impl->cq);
    }
    if (impl->pd != NULL) {
        ibv_dealloc_pd(impl->pd);
    }
    if (impl->verbs != NULL) {
        ibv_close_device(impl->verbs);
    }
    (void)td_tdx_release_shared_memory(&impl->tdx, impl->send_msg, sizeof(*impl->send_msg), NULL, 0);
    (void)td_tdx_release_shared_memory(&impl->tdx, impl->recv_msg, sizeof(*impl->recv_msg), NULL, 0);
    (void)td_tdx_release_shared_memory(&impl->tdx, impl->op_buf, impl->op_buf_len, NULL, 0);
    td_tdx_unmap_shared_memory(&impl->tdx, impl->send_msg, sizeof(*impl->send_msg));
    td_tdx_unmap_shared_memory(&impl->tdx, impl->recv_msg, sizeof(*impl->recv_msg));
    td_tdx_unmap_shared_memory(&impl->tdx, impl->op_buf, impl->op_buf_len);
    memset(impl, 0, sizeof(*impl));
}

static int td_rdma_setup_impl(td_rdma_impl_t *impl, const td_config_t *cfg, size_t op_buf_len, char *err, size_t err_len) {
    struct ibv_qp_init_attr qp_attr;

    if (td_tdx_runtime_init(&impl->tdx, cfg->mode, cfg->tdx, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_open_device(impl, cfg, err, err_len) != 0) {
        return -1;
    }

    impl->pd = ibv_alloc_pd(impl->verbs);
    impl->cq = ibv_create_cq(impl->verbs, 64, NULL, NULL, 0);
    if (impl->pd == NULL || impl->cq == NULL) {
        td_format_error(err, err_len, "rdma alloc pd/cq failed");
        return -1;
    }

    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = impl->cq;
    qp_attr.recv_cq = impl->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 64;
    qp_attr.cap.max_recv_wr = 32;
    qp_attr.cap.max_send_sge = 2;
    qp_attr.cap.max_recv_sge = 2;

    impl->qp = ibv_create_qp(impl->pd, &qp_attr);
    if (impl->qp == NULL) {
        td_format_error(err, err_len, "ibv_create_qp failed");
        return -1;
    }

    impl->op_buf_len = op_buf_len;
    if (td_tdx_map_shared_memory(&impl->tdx, sizeof(*impl->send_msg), (void **)&impl->send_msg, err, err_len) != 0 ||
        td_tdx_map_shared_memory(&impl->tdx, sizeof(*impl->recv_msg), (void **)&impl->recv_msg, err, err_len) != 0 ||
        td_tdx_map_shared_memory(&impl->tdx, impl->op_buf_len, (void **)&impl->op_buf, err, err_len) != 0) {
        return -1;
    }

    if (td_rdma_register_shared_buffer_mr(impl, impl->send_msg, sizeof(*impl->send_msg), IBV_ACCESS_LOCAL_WRITE, &impl->send_mr, err, err_len) != 0 ||
        td_rdma_register_shared_buffer_mr(impl, impl->recv_msg, sizeof(*impl->recv_msg), IBV_ACCESS_LOCAL_WRITE, &impl->recv_mr, err, err_len) != 0 ||
        td_rdma_register_shared_buffer_mr(impl, impl->op_buf, impl->op_buf_len, IBV_ACCESS_LOCAL_WRITE, &impl->op_mr, err, err_len) != 0) {
        return -1;
    }

    impl->psn = (uint32_t)((td_now_ns() ^ (uint64_t)getpid()) & 0x00ffffffu);
    if (td_rdma_qp_to_init(impl, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_validate_bootstrap(const td_rdma_bootstrap_msg_t *msg, char *err, size_t err_len) {
    if (msg->magic != TD_RDMA_BOOTSTRAP_MAGIC) {
        td_format_error(err, err_len, "rdma bootstrap magic mismatch");
        return -1;
    }
    if (msg->version != TD_RDMA_BOOTSTRAP_VERSION) {
        td_format_error(err, err_len, "rdma bootstrap version mismatch");
        return -1;
    }
    return 0;
}

static int td_rdma_wait_oob_response(const td_config_t *cfg, int target_node_id, uint64_t session_id, td_rdma_oob_record_t *response, char *err, size_t err_len) {
    char response_path[TD_PATH_BYTES];
    uint64_t deadline_ns;

    if (td_rdma_format_oob_path(cfg->rdma_oob_dir, "rsp", target_node_id, session_id, response_path, sizeof(response_path)) != 0) {
        td_format_error(err, err_len, "rdma oob response path too long");
        return -1;
    }

    deadline_ns = td_now_ns() + (cfg->rdma_oob_timeout_ms * 1000000ULL);
    while (td_now_ns() < deadline_ns) {
        int rc = td_rdma_read_binary_file(response_path, response, sizeof(*response), 1, err, err_len);

        if (rc == 0) {
            if (td_rdma_validate_oob_record(response, TD_RDMA_OOB_RESPONSE, target_node_id, session_id, err, err_len) != 0) {
                unlink(response_path);
                return -1;
            }
            unlink(response_path);
            return 0;
        }
        if (rc < 0) {
            return -1;
        }
        td_rdma_sleep_ms(cfg->rdma_oob_poll_ms);
    }

    td_format_error(err, err_len, "timed out waiting for rdma oob response for node %d", target_node_id);
    return -1;
}

static int td_rdma_exchange_client_bootstrap_oob_file(const td_config_t *cfg, const td_endpoint_t *endpoint, td_rdma_impl_t *impl, char *err, size_t err_len) {
    td_rdma_oob_record_t request;
    td_rdma_oob_record_t response;
    char request_path[TD_PATH_BYTES];
    uint64_t session_id = td_rdma_make_session_id(endpoint->node_id);

    memset(&request, 0, sizeof(request));
    request.magic = TD_RDMA_OOB_MAGIC;
    request.version = TD_RDMA_OOB_VERSION;
    request.kind = TD_RDMA_OOB_REQUEST;
    request.target_node_id = endpoint->node_id;
    request.source_node_id = cfg->node_id;
    request.session_id = session_id;
    if (td_rdma_query_local_conn_info(impl, &request.conn, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_format_oob_path(cfg->rdma_oob_dir, "req", endpoint->node_id, session_id, request_path, sizeof(request_path)) != 0) {
        td_format_error(err, err_len, "rdma oob request path too long");
        return -1;
    }
    if (td_rdma_write_binary_file(request_path, &request, sizeof(request), err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_wait_oob_response(cfg, endpoint->node_id, session_id, &response, err, err_len) != 0) {
        unlink(request_path);
        return -1;
    }
    unlink(request_path);
    if (td_rdma_qp_to_rtr(impl, &response.conn, err, err_len) != 0 ||
        td_rdma_qp_to_rts(impl, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_claim_oob_request(const td_config_t *cfg, td_rdma_oob_record_t *request, char *claimed_request_path, size_t claimed_request_path_len, char *response_path, size_t response_path_len, char *err, size_t err_len) {
    DIR *dir = NULL;
    struct dirent *entry;

    dir = opendir(cfg->rdma_oob_dir);
    if (dir == NULL) {
        td_format_error(err, err_len, "cannot open rdma oob dir %s; file bootstrap requires the same shared directory on both CN and MN", cfg->rdma_oob_dir);
        return -1;
    }

    while ((entry = readdir(dir)) != NULL) {
        char request_path[TD_PATH_BYTES];
        int target_node_id = -1;
        uint64_t session_id = 0;

        if (td_rdma_parse_oob_request_name(entry->d_name, &target_node_id, &session_id) != 0 || target_node_id != cfg->node_id) {
            continue;
        }
        if (td_rdma_format_oob_path(cfg->rdma_oob_dir, "req", target_node_id, session_id, request_path, sizeof(request_path)) != 0) {
            continue;
        }
        if (snprintf(claimed_request_path, claimed_request_path_len, "%s.claim.%ld", request_path, (long)getpid()) >= (int)claimed_request_path_len) {
            closedir(dir);
            td_format_error(err, err_len, "rdma oob claimed request path too long");
            return -1;
        }
        if (rename(request_path, claimed_request_path) != 0) {
            continue;
        }
        if (td_rdma_read_binary_file(claimed_request_path, request, sizeof(*request), 0, err, err_len) != 0 ||
            td_rdma_validate_oob_record(request, TD_RDMA_OOB_REQUEST, cfg->node_id, request->session_id, err, err_len) != 0) {
            unlink(claimed_request_path);
            continue;
        }
        if (td_rdma_format_oob_path(cfg->rdma_oob_dir, "rsp", request->target_node_id, request->session_id, response_path, response_path_len) != 0) {
            unlink(claimed_request_path);
            closedir(dir);
            td_format_error(err, err_len, "rdma oob response path too long");
            return -1;
        }
        closedir(dir);
        return 0;
    }

    closedir(dir);
    return 1;
}

static int td_rdma_wait_oob_request(const td_config_t *cfg, volatile sig_atomic_t *stop_flag, td_rdma_oob_record_t *request, char *claimed_request_path, size_t claimed_request_path_len, char *response_path, size_t response_path_len, char *err, size_t err_len) {
    for (;;) {
        int rc;

        if (*stop_flag) {
            return 1;
        }
        rc = td_rdma_claim_oob_request(cfg, request, claimed_request_path, claimed_request_path_len, response_path, response_path_len, err, err_len);
        if (rc == 0 || rc < 0) {
            return rc;
        }
        td_rdma_sleep_ms(cfg->rdma_oob_poll_ms);
    }
}

static int td_rdma_exchange_server_bootstrap_oob_file(const td_config_t *cfg, const td_rdma_oob_record_t *request, const char *response_path, td_rdma_impl_t *impl, char *err, size_t err_len) {
    td_rdma_oob_record_t response;

    memset(&response, 0, sizeof(response));
    response.magic = TD_RDMA_OOB_MAGIC;
    response.version = TD_RDMA_OOB_VERSION;
    response.kind = TD_RDMA_OOB_RESPONSE;
    response.target_node_id = request->target_node_id;
    response.source_node_id = cfg->node_id;
    response.session_id = request->session_id;
    if (td_rdma_query_local_conn_info(impl, &response.conn, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_qp_to_rtr(impl, &request->conn, err, err_len) != 0 ||
        td_rdma_qp_to_rts(impl, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_write_binary_file(response_path, &response, sizeof(response), err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_exchange_client_bootstrap(int fd, td_rdma_impl_t *impl, char *err, size_t err_len) {
    td_rdma_bootstrap_msg_t local_msg;
    td_rdma_bootstrap_msg_t remote_msg;

    memset(&local_msg, 0, sizeof(local_msg));
    local_msg.magic = TD_RDMA_BOOTSTRAP_MAGIC;
    local_msg.version = TD_RDMA_BOOTSTRAP_VERSION;
    if (td_rdma_query_local_conn_info(impl, &local_msg.conn, err, err_len) != 0) {
        return -1;
    }

    if (td_send_all(fd, &local_msg, sizeof(local_msg)) != 0 ||
        td_recv_all(fd, &remote_msg, sizeof(remote_msg)) != 0) {
        td_format_error(err, err_len, "rdma bootstrap exchange failed");
        return -1;
    }
    if (td_rdma_validate_bootstrap(&remote_msg, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_qp_to_rtr(impl, &remote_msg.conn, err, err_len) != 0 ||
        td_rdma_qp_to_rts(impl, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_exchange_server_bootstrap(int fd, td_rdma_impl_t *impl, char *err, size_t err_len) {
    td_rdma_bootstrap_msg_t local_msg;
    td_rdma_bootstrap_msg_t remote_msg;

    if (td_recv_all(fd, &remote_msg, sizeof(remote_msg)) != 0) {
        td_format_error(err, err_len, "rdma bootstrap receive failed");
        return -1;
    }
    if (td_rdma_validate_bootstrap(&remote_msg, err, err_len) != 0) {
        return -1;
    }

    memset(&local_msg, 0, sizeof(local_msg));
    local_msg.magic = TD_RDMA_BOOTSTRAP_MAGIC;
    local_msg.version = TD_RDMA_BOOTSTRAP_VERSION;
    if (td_rdma_query_local_conn_info(impl, &local_msg.conn, err, err_len) != 0) {
        return -1;
    }
    if (td_send_all(fd, &local_msg, sizeof(local_msg)) != 0) {
        td_format_error(err, err_len, "rdma bootstrap send failed");
        return -1;
    }
    if (td_rdma_qp_to_rtr(impl, &remote_msg.conn, err, err_len) != 0 ||
        td_rdma_qp_to_rts(impl, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_send_message(td_rdma_impl_t *impl, const td_wire_msg_t *msg, const void *payload, size_t payload_len, uint64_t *copy_ns, uint64_t *post_send_ns, uint64_t *send_wait_ns, td_rdma_wait_profile_t *wait_profile, char *err, size_t err_len) {
    struct ibv_sge sges[2];
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    uint64_t start_ns;

    if (payload_len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma payload length too large");
        return -1;
    }
    memcpy(impl->send_msg, msg, sizeof(*msg));
    if (payload_len > 0 && payload != NULL) {
        start_ns = copy_ns != NULL ? td_now_ns() : 0;
        if (payload != impl->op_buf) {
            memcpy(impl->op_buf, payload, payload_len);
        }
        if (copy_ns != NULL && start_ns != 0) {
            *copy_ns += td_now_ns() - start_ns;
        }
    }

    memset(sges, 0, sizeof(sges));
    sges[0].addr = (uintptr_t)impl->send_msg;
    sges[0].length = sizeof(*impl->send_msg);
    sges[0].lkey = impl->send_mr->lkey;
    if (payload_len > 0) {
        sges[1].addr = (uintptr_t)impl->op_buf;
        sges[1].length = payload_len;
        sges[1].lkey = impl->op_mr->lkey;
    }

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = sges;
    wr.num_sge = payload_len > 0 ? 2 : 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    start_ns = post_send_ns != NULL ? td_now_ns() : 0;
    if (ibv_post_send(impl->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma send message failed");
        return -1;
    }
    if (post_send_ns != NULL && start_ns != 0) {
        *post_send_ns += td_now_ns() - start_ns;
    }

    start_ns = send_wait_ns != NULL ? td_now_ns() : 0;
    if (td_rdma_poll_wc(impl, IBV_WC_SEND, wait_profile, NULL, err, err_len) != 0) {
        return -1;
    }
    if (send_wait_ns != NULL && start_ns != 0) {
        *send_wait_ns += td_now_ns() - start_ns;
    }
    return 0;
}

static int td_rdma_wait_message(td_rdma_impl_t *impl, td_wire_msg_t *response, void *payload, size_t payload_cap, size_t *payload_len_out, uint64_t *wait_ns, td_rdma_wait_profile_t *wait_profile, uint64_t *header_copy_ns, uint64_t *payload_copy_ns, char *err, size_t err_len) {
    struct ibv_wc wc;
    uint64_t start_ns = wait_ns != NULL ? td_now_ns() : 0;
    size_t payload_len = 0;

    if (td_rdma_poll_wc(impl, IBV_WC_RECV, wait_profile, &wc, err, err_len) != 0) {
        return -1;
    }
    if (wait_ns != NULL && start_ns != 0) {
        *wait_ns += td_now_ns() - start_ns;
    }
    if ((size_t)wc.byte_len < sizeof(*impl->recv_msg)) {
        td_format_error(err, err_len, "rdma recv message too short: %u", wc.byte_len);
        return -1;
    }
    payload_len = (size_t)wc.byte_len - sizeof(*impl->recv_msg);
    start_ns = header_copy_ns != NULL ? td_now_ns() : 0;
    memcpy(response, impl->recv_msg, sizeof(*response));
    if (header_copy_ns != NULL && start_ns != 0) {
        *header_copy_ns += td_now_ns() - start_ns;
    }
    if (response->length != payload_len) {
        td_format_error(err, err_len, "rdma recv payload length mismatch: header=%zu actual=%zu", (size_t)response->length, payload_len);
        return -1;
    }
    if (payload_len > payload_cap && payload != NULL) {
        td_format_error(err, err_len, "rdma recv payload too large: %zu > %zu", payload_len, payload_cap);
        return -1;
    }
    if (payload_len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma recv payload exceeds buffer: %zu > %zu", payload_len, impl->op_buf_len);
        return -1;
    }
    if (payload != NULL && payload_len > 0) {
        start_ns = payload_copy_ns != NULL ? td_now_ns() : 0;
        memcpy(payload, impl->op_buf, payload_len);
        if (payload_copy_ns != NULL && start_ns != 0) {
            *payload_copy_ns += td_now_ns() - start_ns;
        }
    }
    if (payload_len_out != NULL) {
        *payload_len_out = payload_len;
    }
    return 0;
}

static int td_rdma_client_read(td_session_t *session, size_t offset, void *buf, size_t len, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    uint64_t start_ns;

    if (len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma read length too large");
        return -1;
    }

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)impl->op_buf;
    sge.length = len;
    sge.lkey = impl->op_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = session->remote_addr + offset;
    wr.wr.rdma.rkey = session->rkey;

    start_ns = td_rdma_profile_begin(session);
    if (ibv_post_send(impl->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma read op failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->read_send_ns : NULL);
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->rdma_read_post_send_ns : NULL);

    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_poll_wc(
            impl,
            IBV_WC_RDMA_READ,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_read_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_read_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_read_empty_polls,
                .backoff_count = &session->transport_profile->rdma_read_backoff_count,
            } : NULL,
            NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma read op failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->read_wait_ns : NULL);

    start_ns = td_rdma_profile_begin(session);
    memcpy(buf, impl->op_buf, len);
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->read_copy_ns : NULL);
    return 0;
}

static int td_rdma_client_read_rpc(td_session_t *session, size_t offset, void *buf, size_t len, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    uint64_t start_ns;

    if (len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma read length too large");
        return -1;
    }

    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_READ;
    request.offset = offset;
    request.length = len;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_rdma_post_recv(impl, err, err_len) != 0) {
        return -1;
    }
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_message(
            impl,
            &request,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_post_send_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_send_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_request_send_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_request_send_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_send_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_send_backoff_count,
            } : NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma read rpc failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->read_send_ns : NULL);

    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_wait_message(
            impl,
            &response,
            buf,
            len,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->read_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->read_copy_ns : NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma read rpc failed");
        return -1;
    }
    if (response.status != 0 || response.length != len) {
        td_format_error(err, err_len, "rdma read rpc failed");
        return -1;
    }
    td_rdma_accumulate_server_profile(session->transport_profile, TD_WIRE_READ, &response);
    return 0;
}

static int td_rdma_client_write(td_session_t *session, size_t offset, const void *buf, size_t len, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    uint64_t start_ns;

    if (len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma write length too large");
        return -1;
    }

    start_ns = td_rdma_profile_begin(session);
    memcpy(impl->op_buf, buf, len);
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->write_copy_ns : NULL);

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)impl->op_buf;
    sge.length = len;
    sge.lkey = impl->op_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = session->remote_addr + offset;
    wr.wr.rdma.rkey = session->rkey;

    start_ns = td_rdma_profile_begin(session);
    if (ibv_post_send(impl->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma write op failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->write_send_ns : NULL);
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->rdma_write_post_send_ns : NULL);

    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_poll_wc(
            impl,
            IBV_WC_RDMA_WRITE,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_write_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_write_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_write_empty_polls,
                .backoff_count = &session->transport_profile->rdma_write_backoff_count,
            } : NULL,
            NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma write op failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->write_wait_ns : NULL);
    return 0;
}

static int td_rdma_client_write_rpc(td_session_t *session, size_t offset, const void *buf, size_t len, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    uint64_t start_ns;

    if (len > impl->op_buf_len) {
        td_format_error(err, err_len, "rdma write length too large");
        return -1;
    }

    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_WRITE;
    request.offset = offset;
    request.length = len;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_rdma_post_recv(impl, err, err_len) != 0) {
        return -1;
    }
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_message(
            impl,
            &request,
            buf,
            len,
            session->transport_profile != NULL ? &session->transport_profile->write_copy_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_post_send_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_send_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_request_send_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_request_send_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_send_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_send_backoff_count,
            } : NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma write rpc failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->write_send_ns : NULL);

    if (td_rdma_wait_message(
            impl,
            &response,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->write_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma write rpc failed");
        return -1;
    }
    if (response.status != 0) {
        td_format_error(err, err_len, "rdma write rpc failed");
        return -1;
    }
    td_rdma_accumulate_server_profile(session->transport_profile, TD_WIRE_WRITE, &response);
    return 0;
}

static int td_rdma_client_cas(td_session_t *session, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    uint64_t start_ns;

    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_CAS;
    request.offset = offset;
    request.compare = compare;
    request.swap = swap;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_rdma_post_recv(impl, err, err_len) != 0) {
        return -1;
    }
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_message(
            impl,
            &request,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_cas_request_post_send_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_cas_request_send_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_cas_request_send_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_cas_request_send_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_cas_send_empty_polls,
                .backoff_count = &session->transport_profile->rdma_cas_send_backoff_count,
            } : NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma cas control failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->cas_send_ns : NULL);
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_wait_message(
            impl,
            &response,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_cas_response_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_cas_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_cas_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_cas_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_cas_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma cas control failed");
        return -1;
    }
    if (response.status != 0) {
        td_format_error(err, err_len, "rdma cas control failed");
        return -1;
    }
    *old_value = response.compare;
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->cas_wait_ns : NULL);
    td_rdma_accumulate_server_profile(session->transport_profile, TD_WIRE_CAS, &response);
    return 0;
}

static int td_rdma_client_control(td_session_t *session, td_wire_op_t op, char *err, size_t err_len) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    uint64_t start_ns;

    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = (uint16_t)op;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_rdma_post_recv(impl, err, err_len) != 0) {
        return -1;
    }
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_message(
            impl,
            &request,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_post_send_ns : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_request_send_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_request_send_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_request_send_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_send_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_send_backoff_count,
            } : NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma control op %u failed", (unsigned int)op);
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->control_send_ns : NULL);
    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_wait_message(
            impl,
            &response,
            NULL,
            0,
            NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_response_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            NULL,
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma control op %u failed", (unsigned int)op);
        return -1;
    }
    if (response.status != 0) {
        td_format_error(err, err_len, "rdma control op %u failed", (unsigned int)op);
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->control_wait_ns : NULL);
    td_rdma_accumulate_server_profile(session->transport_profile, op, &response);
    return 0;
}

static void td_rdma_client_close(td_session_t *session) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;

    if (impl == NULL) {
        return;
    }
    (void)td_rdma_client_control(session, TD_WIRE_CLOSE, NULL, 0);
    td_rdma_destroy_impl(impl);
    free(impl);
    session->impl = NULL;
}

static int td_rdma_open_control_client(const td_endpoint_t *endpoint, char *err, size_t err_len) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *it;
    char port[16];
    int fd = -1;
    int last_errno = 0;

    /* Exchange RC QP attributes over a normal TCP socket so data plane RDMA stays off rdma_cm/IPoIB. */
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    snprintf(port, sizeof(port), "%d", endpoint->port);

    if (getaddrinfo(endpoint->host, port, &hints, &result) != 0) {
        td_format_error(err, err_len, "cannot resolve RDMA bootstrap endpoint %s:%d", endpoint->host, endpoint->port);
        return -1;
    }

    for (it = result; it != NULL; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
            td_rdma_tune_socket(fd);
            break;
        }
        last_errno = errno;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(result);

    if (fd < 0) {
        td_format_error(err, err_len, "cannot connect RDMA bootstrap endpoint %s:%d: %s",
            endpoint->host,
            endpoint->port,
            last_errno != 0 ? strerror(last_errno) : "unknown error");
        return -1;
    }
    return fd;
}

static int td_rdma_open_control_client_vsock(const td_endpoint_t *endpoint, char *err, size_t err_len) {
    struct sockaddr_vm addr;
    unsigned int cid = 0;
    int fd;

    if (td_rdma_parse_vsock_cid(endpoint->host, &cid, err, err_len) != 0) {
        return -1;
    }

    fd = socket(AF_VSOCK, SOCK_STREAM, 0);
    if (fd < 0) {
        td_format_error(err, err_len, "cannot create RDMA vsock bootstrap socket");
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.svm_family = AF_VSOCK;
    addr.svm_cid = cid;
    addr.svm_port = (unsigned int)endpoint->port;

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        int saved_errno = errno;

        close(fd);
        td_format_error(err, err_len, "cannot connect RDMA vsock bootstrap endpoint cid=%u port=%d: %s",
            cid,
            endpoint->port,
            strerror(saved_errno));
        return -1;
    }
    return fd;
}

static int td_rdma_client_connect(td_session_t *session, const td_config_t *cfg, const td_endpoint_t *endpoint, char *err, size_t err_len) {
    td_rdma_impl_t *impl = NULL;
    td_wire_msg_t hello;
    td_wire_msg_t response;
    int control_fd = -1;

    impl = (td_rdma_impl_t *)calloc(1, sizeof(*impl));
    if (impl == NULL) {
        td_format_error(err, err_len, "out of memory");
        return -1;
    }
    fprintf(stderr, "[DEBUG] connecting to %s:%d...\n", endpoint->host, endpoint->port);
    fflush(stderr);
    if (cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_TCP || cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK) {
        control_fd = cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK
            ? td_rdma_open_control_client_vsock(endpoint, err, err_len)
            : td_rdma_open_control_client(endpoint, err, err_len);
        if (control_fd < 0) {
            free(impl);
            return -1;
        }
    }
    fprintf(stderr, "[DEBUG] TCP connected, setting up QP...\n");
    fflush(stderr);
    if (td_rdma_setup_impl(impl, cfg, sizeof(td_slot_t), err, err_len) != 0) {
        fprintf(stderr, "[DEBUG] setup_impl failed: %s\n", err);
        fflush(stderr);
        if (control_fd >= 0) close(control_fd);
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    fprintf(stderr, "[DEBUG] QP setup done, exchanging bootstrap...\n");
    fflush(stderr);
    if (((cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_TCP || cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK)
            ? td_rdma_exchange_client_bootstrap(control_fd, impl, err, err_len)
            : td_rdma_exchange_client_bootstrap_oob_file(cfg, endpoint, impl, err, err_len)) != 0) {
        if (control_fd >= 0) {
            close(control_fd);
        }
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    if (control_fd >= 0) {
        close(control_fd);
    }

    fprintf(stderr, "[DEBUG] bootstrap done, local lid=%u qpn=%u psn=%u\n",
            impl->lid, impl->qp->qp_num, impl->psn);
    fflush(stderr);

    memset(&hello, 0, sizeof(hello));
    hello.magic = TD_WIRE_MAGIC;
    hello.op = TD_WIRE_HELLO;

    fprintf(stderr, "[DEBUG] sending HELLO via RDMA...\n");
    fflush(stderr);
    if (td_rdma_post_recv(impl, err, err_len) != 0 ||
        td_rdma_send_message(impl, &hello, NULL, 0, NULL, NULL, NULL, NULL, err, err_len) != 0) {
        fprintf(stderr, "[DEBUG] HELLO send FAILED: %s\n", err);
        fflush(stderr);
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    fprintf(stderr, "[DEBUG] HELLO send completed, waiting for response...\n");
    fflush(stderr);
    if (td_rdma_wait_message(impl, &response, NULL, 0, NULL, NULL, NULL, NULL, NULL, err, err_len) != 0) {
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    if (response.status != 0) {
        td_rdma_destroy_impl(impl);
        free(impl);
        td_format_error(err, err_len, "rdma hello failed with server status=%u", (unsigned int)response.status);
        return -1;
    }

    session->transport = TD_TRANSPORT_RDMA;
    session->endpoint = *endpoint;
    session->remote_addr = response.remote_addr;
    session->rkey = response.rkey;
    session->header = response.header;
    session->region_size = (size_t)response.header.region_size;
    session->impl = impl;
    if ((response.flags & TD_WIRE_FLAG_DIRECT_REGION) != 0 && response.remote_addr != 0 && response.rkey != 0) {
        session->read_region = td_rdma_client_read;
        session->write_region = td_rdma_client_write;
    } else {
        fprintf(stderr, "[DEBUG] server does not expose direct region MR; using SEND/RECV fallback for READ/WRITE\n");
        fflush(stderr);
        session->read_region = td_rdma_client_read_rpc;
        session->write_region = td_rdma_client_write_rpc;
    }
    session->cas64 = td_rdma_client_cas;
    session->control = td_rdma_client_control;
    session->close = td_rdma_client_close;
    return 0;
}

static void *td_rdma_server_conn_main(void *arg) {
    td_rdma_server_conn_t *conn = (td_rdma_server_conn_t *)arg;
    char err[256];
    int poll_count = 0;

    fprintf(stderr, "[MN-DEBUG] server thread started, qpn=%u lid=%u\n",
            conn->impl.qp->qp_num, conn->impl.lid);
    fflush(stderr);

    while (!(*conn->stop_flag)) {
        struct ibv_wc wc;
        int n = ibv_poll_cq(conn->impl.cq, 1, &wc);

        if (n < 0) {
            fprintf(stderr, "[MN-DEBUG] poll_cq returned error\n");
            fflush(stderr);
            break;
        }
        if (n == 0) {
            poll_count++;
            if (poll_count % 50000000 == 0) {
                fprintf(stderr, "[MN-DEBUG] still polling CQ, no completions yet (%d polls)\n", poll_count);
                fflush(stderr);
            }
            sched_yield();
            continue;
        }
        fprintf(stderr, "[MN-DEBUG] got CQ completion: opcode=%d status=%d byte_len=%u\n",
                wc.opcode, wc.status, wc.byte_len);
        fflush(stderr);
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "rdma server completion failed status=%d (%s)\n", wc.status, ibv_wc_status_str(wc.status));
            fflush(stderr);
            break;
        }
        if (wc.opcode == IBV_WC_SEND) {
            continue;
        }
        if (wc.opcode != IBV_WC_RECV) {
            continue;
        }

        {
            td_wire_msg_t request = *conn->impl.recv_msg;
            td_wire_msg_t response;
            size_t payload_len = 0;
            const void *response_payload = NULL;
            size_t response_payload_len = 0;
            int profile_enabled;
            uint64_t op_start;

            memset(&response, 0, sizeof(response));
            response.magic = TD_WIRE_MAGIC;
            response.op = TD_WIRE_ACK;
            response.flags = request.flags;
            profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
            op_start = profile_enabled ? td_now_ns() : 0;

            if ((size_t)wc.byte_len < sizeof(request)) {
                fprintf(stderr, "[MN-DEBUG] recv too short: %u < %zu\n", wc.byte_len, sizeof(request));
                fflush(stderr);
                response.status = 1;
                request.op = 0;
            } else {
                if (request.magic != TD_WIRE_MAGIC) {
                    fprintf(stderr, "[MN-DEBUG] magic mismatch: got 0x%08x expected 0x%08x, recv_msg@%p first 32 bytes:",
                            request.magic, TD_WIRE_MAGIC, (void*)conn->impl.recv_msg);
                    { unsigned char *p = (unsigned char*)conn->impl.recv_msg; int i;
                      for (i = 0; i < 32; i++) fprintf(stderr, " %02x", p[i]);
                    }
                    fprintf(stderr, "\n");
                    fflush(stderr);
                    break;
                }
                payload_len = (size_t)wc.byte_len - sizeof(request);
            }

            if (request.op == TD_WIRE_HELLO) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                response.header = *td_region_header_view(conn->region);
                if (conn->region_mr != NULL) {
                    response.flags |= TD_WIRE_FLAG_DIRECT_REGION;
                    response.remote_addr = (uint64_t)(uintptr_t)td_region_shared_base(conn->region);
                    response.rkey = conn->region_mr->rkey;
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_READ) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                if (request.length > conn->impl.op_buf_len || payload_len != 0 ||
                    td_region_read_bytes(conn->region, (size_t)request.offset, conn->impl.op_buf, (size_t)request.length) != 0) {
                    response.status = 1;
                    response.length = 0;
                } else {
                    response.length = request.length;
                    response_payload = conn->impl.op_buf;
                    response_payload_len = (size_t)request.length;
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_WRITE) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                if (request.length > conn->impl.op_buf_len || payload_len != (size_t)request.length ||
                    td_region_write_bytes(conn->region, (size_t)request.offset, conn->impl.op_buf, (size_t)request.length) != 0) {
                    response.status = 1;
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_CAS) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                if (payload_len != 0 ||
                    td_region_cas64(conn->region, (size_t)request.offset, request.compare, request.swap, &response.compare) != 0) {
                    response.status = 1;
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_EVICT) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                if (payload_len != 0) {
                    response.status = 1;
                } else {
                    td_region_evict_if_needed(conn->region, conn->eviction_threshold_pct);
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_CLOSE) {
                if (payload_len != 0) {
                    response.status = 1;
                }
                if (profile_enabled && op_start != 0) {
                    response.profile_total_ns = td_now_ns() - op_start;
                }
                if (td_rdma_send_message(&conn->impl, &response, NULL, 0, NULL, NULL, NULL, NULL, err, sizeof(err)) != 0) {
                    break;
                }
                break;
            } else {
                response.status = 1;
            }

            if (profile_enabled && op_start != 0) {
                response.profile_total_ns = td_now_ns() - op_start;
            }
            if (td_rdma_send_message(&conn->impl, &response, response_payload, response_payload_len, NULL, NULL, NULL, NULL, err, sizeof(err)) != 0) {
                break;
            }
            if (td_rdma_post_recv(&conn->impl, err, sizeof(err)) != 0) {
                break;
            }
        }
    }

    if (conn->region_mr != NULL) {
        ibv_dereg_mr(conn->region_mr);
    }
    td_rdma_destroy_impl(&conn->impl);
    free(conn);
    return NULL;
}

static int td_rdma_open_control_listener(const td_config_t *cfg, char *err, size_t err_len) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *it;
    char port[16];
    int listen_fd = -1;
    int one = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;
    snprintf(port, sizeof(port), "%d", cfg->listen_port);

    if (getaddrinfo(cfg->listen_host[0] != '\0' ? cfg->listen_host : NULL, port, &hints, &result) != 0) {
        td_format_error(err, err_len, "cannot resolve RDMA bootstrap listen endpoint %s:%d", cfg->listen_host, cfg->listen_port);
        return -1;
    }

    for (it = result; it != NULL; it = it->ai_next) {
        listen_fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (listen_fd < 0) {
            continue;
        }
        (void)setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (bind(listen_fd, it->ai_addr, it->ai_addrlen) == 0 && listen(listen_fd, 16) == 0) {
            break;
        }
        close(listen_fd);
        listen_fd = -1;
    }
    freeaddrinfo(result);

    if (listen_fd < 0) {
        td_format_error(err, err_len, "RDMA bootstrap bind/listen failed on %s:%d", cfg->listen_host, cfg->listen_port);
        return -1;
    }
    return listen_fd;
}

static int td_rdma_open_control_listener_vsock(const td_config_t *cfg, char *err, size_t err_len) {
    struct sockaddr_vm addr;
    int listen_fd;

    listen_fd = socket(AF_VSOCK, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        td_format_error(err, err_len, "cannot create RDMA vsock bootstrap listener");
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.svm_family = AF_VSOCK;
    addr.svm_cid = VMADDR_CID_ANY;
    addr.svm_port = (unsigned int)cfg->listen_port;

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        int saved_errno = errno;

        close(listen_fd);
        td_format_error(err, err_len, "RDMA vsock bootstrap bind failed on port %d: %s", cfg->listen_port, strerror(saved_errno));
        return -1;
    }
    if (listen(listen_fd, 16) != 0) {
        int saved_errno = errno;

        close(listen_fd);
        td_format_error(err, err_len, "RDMA vsock bootstrap listen failed on port %d: %s", cfg->listen_port, strerror(saved_errno));
        return -1;
    }

    return listen_fd;
}

static int td_rdma_server_run_oob_file(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len) {
    while (!(*stop_flag)) {
        td_rdma_oob_record_t request;
        char claimed_request_path[TD_PATH_BYTES * 2];
        char response_path[TD_PATH_BYTES];
        int wait_rc;

        wait_rc = td_rdma_wait_oob_request(cfg, stop_flag, &request, claimed_request_path, sizeof(claimed_request_path), response_path, sizeof(response_path), err, err_len);
        if (wait_rc > 0) {
            return 0;
        }
        if (wait_rc < 0) {
            return -1;
        }

        {
            td_rdma_server_conn_t *conn = (td_rdma_server_conn_t *)calloc(1, sizeof(*conn));
            pthread_t thread;
            char conn_err[256];

            if (conn == NULL) {
                unlink(claimed_request_path);
                td_format_error(err, err_len, "out of memory");
                return -1;
            }
            memset(conn_err, 0, sizeof(conn_err));
            conn->region = region;
            conn->eviction_threshold_pct = cfg->eviction_threshold_pct;
            conn->stop_flag = stop_flag;
            if (td_rdma_setup_impl(&conn->impl, cfg, sizeof(td_slot_t), conn_err, sizeof(conn_err)) == 0 &&
                td_rdma_register_server_region(conn, conn_err, sizeof(conn_err)) == 0 &&
                td_rdma_post_recv(&conn->impl, conn_err, sizeof(conn_err)) == 0 &&
                td_rdma_exchange_server_bootstrap_oob_file(cfg, &request, response_path, &conn->impl, conn_err, sizeof(conn_err)) == 0) {
            }
            unlink(claimed_request_path);
            if (conn_err[0] == '\0') {
                if (pthread_create(&thread, NULL, td_rdma_server_conn_main, conn) == 0) {
                    pthread_detach(thread);
                    conn = NULL;
                } else {
                    td_format_error(conn_err, sizeof(conn_err), "pthread_create failed");
                }
            }
            if (conn != NULL) {
                if (conn_err[0] != '\0') {
                    fprintf(stderr, "rdma connection setup failed: %s\n", conn_err);
                    fflush(stderr);
                }
                unlink(response_path);
                if (conn->region_mr != NULL) {
                    ibv_dereg_mr(conn->region_mr);
                }
                td_rdma_destroy_impl(&conn->impl);
                free(conn);
                return -1;
            }
        }
    }

    return 0;
}

int td_rdma_server_run(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len) {
    int listen_fd;

    if (cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_OOB_FILE) {
        return td_rdma_server_run_oob_file(cfg, region, stop_flag, err, err_len);
    }

    listen_fd = cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK
        ? td_rdma_open_control_listener_vsock(cfg, err, err_len)
        : td_rdma_open_control_listener(cfg, err, err_len);
    if (listen_fd < 0) {
        return -1;
    }

    while (!(*stop_flag)) {
        struct pollfd pfd;
        int ready;

        pfd.fd = listen_fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        ready = poll(&pfd, 1, 500);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            close(listen_fd);
            td_format_error(err, err_len, "RDMA bootstrap poll failed");
            return -1;
        }
        if (ready == 0) {
            continue;
        }
        if ((pfd.revents & POLLIN) != 0) {
            int client_fd = accept(listen_fd, NULL, NULL);

            if (client_fd >= 0) {
                td_rdma_server_conn_t *conn = (td_rdma_server_conn_t *)calloc(1, sizeof(*conn));
                pthread_t thread;
                char conn_err[256];

                if (conn == NULL) {
                    close(client_fd);
                    continue;
                }
                memset(conn_err, 0, sizeof(conn_err));
                td_rdma_tune_socket(client_fd);
                conn->region = region;
                conn->eviction_threshold_pct = cfg->eviction_threshold_pct;
                conn->stop_flag = stop_flag;
                if (td_rdma_setup_impl(&conn->impl, cfg, sizeof(td_slot_t), conn_err, sizeof(conn_err)) == 0 &&
                    td_rdma_register_server_region(conn, conn_err, sizeof(conn_err)) == 0 &&
                    td_rdma_post_recv(&conn->impl, conn_err, sizeof(conn_err)) == 0 &&
                    td_rdma_exchange_server_bootstrap(client_fd, &conn->impl, conn_err, sizeof(conn_err)) == 0) {
                    fprintf(stderr, "[MN-DEBUG] bootstrap OK, local lid=%u qpn=%u psn=%u recv_msg@%p send_msg@%p op_buf@%p\n",
                            conn->impl.lid, conn->impl.qp->qp_num, conn->impl.psn,
                            (void*)conn->impl.recv_msg, (void*)conn->impl.send_msg, (void*)conn->impl.op_buf);
                    fflush(stderr);
                }
                close(client_fd);
                if (conn_err[0] == '\0') {
                    if (pthread_create(&thread, NULL, td_rdma_server_conn_main, conn) == 0) {
                        pthread_detach(thread);
                        conn = NULL;
                    } else {
                        td_format_error(conn_err, sizeof(conn_err), "pthread_create failed");
                    }
                }
                if (conn != NULL) {
                    if (conn_err[0] != '\0') {
                        fprintf(stderr, "rdma connection setup failed: %s\n", conn_err);
                        fflush(stderr);
                    }
                    if (conn->region_mr != NULL) {
                        ibv_dereg_mr(conn->region_mr);
                    }
                    td_rdma_destroy_impl(&conn->impl);
                    free(conn);
                }
            }
        }
    }

    close(listen_fd);
    return 0;
}

int td_session_connect(td_session_t *session, const td_config_t *cfg, const td_endpoint_t *endpoint, char *err, size_t err_len) {
    memset(session, 0, sizeof(*session));
    if (cfg->transport == TD_TRANSPORT_TCP) {
        return td_tcp_client_connect(session, endpoint, err, err_len);
    }
    return td_rdma_client_connect(session, cfg, endpoint, err, err_len);
}

void td_session_close(td_session_t *session) {
    if (session->close != NULL) {
        session->close(session);
    }
    memset(session, 0, sizeof(*session));
}
