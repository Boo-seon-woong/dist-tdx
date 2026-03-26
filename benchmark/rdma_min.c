#include "td_common.h"
#include "td_tdx.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

enum {
    TD_RDMA_MIN_BOOTSTRAP_MAGIC = 0x4d524442u,
    TD_RDMA_MIN_BOOTSTRAP_VERSION = 1,
    TD_RDMA_MIN_MSG_MAGIC = 0x4d52444du,
    TD_RDMA_MIN_MAX_BYTES = 4096,
    TD_RDMA_MIN_WAIT_LOG_POLLS = 50000000ULL,
};

typedef enum {
    TD_RDMA_MIN_MODE_SERVER = 0,
    TD_RDMA_MIN_MODE_CLIENT = 1,
} td_rdma_min_mode_t;

typedef enum {
    TD_RDMA_MIN_OP_SEND = 0,
    TD_RDMA_MIN_OP_WRITE = 1,
    TD_RDMA_MIN_OP_WRITE_IMM = 2,
} td_rdma_min_op_t;

typedef struct {
    uint16_t lid;
    uint8_t mtu;
    uint8_t has_gid;
    uint8_t gid[16];
    uint8_t reserved0;
    uint16_t reserved1;
    uint32_t qp_num;
    uint32_t psn;
} td_rdma_min_conn_info_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    td_rdma_min_conn_info_t conn;
    uint64_t ctrl_addr;
    uint32_t ctrl_rkey;
    uint32_t reserved1;
    uint64_t data_addr;
    uint32_t data_rkey;
    uint32_t data_bytes;
} td_rdma_min_bootstrap_t;

typedef struct {
    uint32_t magic;
    uint32_t op;
    uint32_t bytes;
    uint32_t seq;
} td_rdma_min_msg_t;

typedef struct {
    td_rdma_min_mode_t mode;
    td_rdma_min_op_t op;
    char endpoint[TD_HOST_BYTES];
    int tcp_port;
    char rdma_device[TD_HOST_BYTES];
    int rdma_port_num;
    int gid_index;
    td_tdx_mode_t tdx;
    size_t bytes;
    uint32_t seq;
    unsigned int timeout_ms;
} td_rdma_min_options_t;

typedef struct {
    void *base;
    size_t bytes;
    int shared;
} td_rdma_min_buffer_t;

typedef struct {
    struct ibv_context *verbs;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    struct ibv_mr *ctrl_mr;
    struct ibv_mr *data_mr;
    td_rdma_min_buffer_t send_buf;
    td_rdma_min_buffer_t recv_buf;
    td_rdma_min_buffer_t ctrl_buf;
    td_rdma_min_buffer_t data_buf;
    td_tdx_runtime_t tdx;
    int use_shared;
    int port_num;
    int gid_index;
    uint16_t lid;
    enum ibv_mtu active_mtu;
    union ibv_gid gid;
    int has_gid;
    uint32_t psn;
} td_rdma_min_ctx_t;

static void td_rdma_min_usage(const char *argv0) {
    fprintf(stderr,
        "usage:\n"
        "  %s --mode server --listen <ip> --tcp-port <port> --rdma-device <dev> [--rdma-port N] [--gid-index N] [--tdx on|off] [--op send|write|write_imm] [--bytes N] [--timeout-ms N]\n"
        "  %s --mode client --connect <ip> --tcp-port <port> --rdma-device <dev> [--rdma-port N] [--gid-index N] [--op send|write|write_imm] [--bytes N] [--timeout-ms N]\n",
        argv0,
        argv0);
}

static const char *td_rdma_min_mode_name(td_rdma_min_mode_t mode) {
    return mode == TD_RDMA_MIN_MODE_SERVER ? "server" : "client";
}

static const char *td_rdma_min_op_name(td_rdma_min_op_t op) {
    switch (op) {
        case TD_RDMA_MIN_OP_SEND:
            return "send";
        case TD_RDMA_MIN_OP_WRITE:
            return "write";
        case TD_RDMA_MIN_OP_WRITE_IMM:
            return "write_imm";
    }
    return "unknown";
}

static const char *td_rdma_min_wc_opcode_name(enum ibv_wc_opcode opcode) {
    switch (opcode) {
        case IBV_WC_SEND:
            return "SEND";
        case IBV_WC_RDMA_WRITE:
            return "RDMA_WRITE";
        case IBV_WC_RECV:
            return "RECV";
        case IBV_WC_RECV_RDMA_WITH_IMM:
            return "RECV_RDMA_WITH_IMM";
        default:
            return "OTHER";
    }
}

static int td_rdma_min_parse_toggle(const char *text, td_tdx_mode_t *out) {
    if (strcmp(text, "on") == 0) {
        *out = TD_TDX_ON;
        return 0;
    }
    if (strcmp(text, "off") == 0) {
        *out = TD_TDX_OFF;
        return 0;
    }
    return -1;
}

static int td_rdma_min_parse_mode(const char *text, td_rdma_min_mode_t *out) {
    if (strcmp(text, "server") == 0) {
        *out = TD_RDMA_MIN_MODE_SERVER;
        return 0;
    }
    if (strcmp(text, "client") == 0) {
        *out = TD_RDMA_MIN_MODE_CLIENT;
        return 0;
    }
    return -1;
}

static int td_rdma_min_parse_op(const char *text, td_rdma_min_op_t *out) {
    if (strcmp(text, "send") == 0) {
        *out = TD_RDMA_MIN_OP_SEND;
        return 0;
    }
    if (strcmp(text, "write") == 0) {
        *out = TD_RDMA_MIN_OP_WRITE;
        return 0;
    }
    if (strcmp(text, "write_imm") == 0) {
        *out = TD_RDMA_MIN_OP_WRITE_IMM;
        return 0;
    }
    return -1;
}

static int td_rdma_min_parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    unsigned long value;

    errno = 0;
    value = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)value;
    return 0;
}

static int td_rdma_min_parse_size(const char *text, size_t *out) {
    char *end = NULL;
    unsigned long long value;

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }
    *out = (size_t)value;
    return 0;
}

static int td_rdma_min_parse_int(const char *text, int *out) {
    char *end = NULL;
    long value;

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }
    *out = (int)value;
    return 0;
}

static int td_rdma_min_parse_args(int argc, char **argv, td_rdma_min_options_t *opts) {
    int i;

    memset(opts, 0, sizeof(*opts));
    opts->mode = TD_RDMA_MIN_MODE_SERVER;
    opts->op = TD_RDMA_MIN_OP_WRITE_IMM;
    opts->rdma_port_num = 1;
    opts->gid_index = 0;
    opts->tdx = TD_TDX_OFF;
    opts->bytes = 32;
    opts->seq = 1;
    opts->timeout_ms = 15000;

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_mode(argv[++i], &opts->mode) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--listen") == 0 && i + 1 < argc) {
            snprintf(opts->endpoint, sizeof(opts->endpoint), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--connect") == 0 && i + 1 < argc) {
            snprintf(opts->endpoint, sizeof(opts->endpoint), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--tcp-port") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_int(argv[++i], &opts->tcp_port) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--rdma-device") == 0 && i + 1 < argc) {
            snprintf(opts->rdma_device, sizeof(opts->rdma_device), "%s", argv[++i]);
        } else if (strcmp(argv[i], "--rdma-port") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_int(argv[++i], &opts->rdma_port_num) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--gid-index") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_int(argv[++i], &opts->gid_index) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--tdx") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_toggle(argv[++i], &opts->tdx) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--op") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_op(argv[++i], &opts->op) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--bytes") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_size(argv[++i], &opts->bytes) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--seq") == 0 && i + 1 < argc) {
            if (td_rdma_min_parse_u32(argv[++i], &opts->seq) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            int parsed = 0;
            if (td_rdma_min_parse_int(argv[++i], &parsed) != 0 || parsed <= 0) {
                return -1;
            }
            opts->timeout_ms = (unsigned int)parsed;
        } else {
            return -1;
        }
    }

    if (opts->endpoint[0] == '\0' ||
        opts->tcp_port <= 0 ||
        opts->rdma_device[0] == '\0' ||
        opts->bytes == 0 ||
        opts->bytes > TD_RDMA_MIN_MAX_BYTES) {
        return -1;
    }
    if (opts->mode == TD_RDMA_MIN_MODE_CLIENT && opts->tdx != TD_TDX_OFF) {
        return -1;
    }
    return 0;
}

static void td_rdma_min_tune_socket(int fd) {
    int one = 1;

    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef TCP_QUICKACK
    (void)setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
#endif
}

static int td_rdma_min_send_all(int fd, const void *buf, size_t len) {
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

static int td_rdma_min_recv_all(int fd, void *buf, size_t len) {
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

static int td_rdma_min_listen(const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    struct sockaddr_in addr;
    int fd;
    int one = 1;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        td_format_error(err, err_len, "socket failed: %s", strerror(errno));
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
        close(fd);
        td_format_error(err, err_len, "setsockopt SO_REUSEADDR failed: %s", strerror(errno));
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)opts->tcp_port);
    if (inet_pton(AF_INET, opts->endpoint, &addr.sin_addr) != 1) {
        close(fd);
        td_format_error(err, err_len, "invalid listen IP %s", opts->endpoint);
        return -1;
    }
    if (bind(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        td_format_error(err, err_len, "bind failed: %s", strerror(errno));
        return -1;
    }
    if (listen(fd, 1) != 0) {
        close(fd);
        td_format_error(err, err_len, "listen failed: %s", strerror(errno));
        return -1;
    }
    return fd;
}

static int td_rdma_min_accept(int listen_fd, char *err, size_t err_len) {
    int fd = accept(listen_fd, NULL, NULL);

    if (fd < 0) {
        td_format_error(err, err_len, "accept failed: %s", strerror(errno));
        return -1;
    }
    td_rdma_min_tune_socket(fd);
    return fd;
}

static int td_rdma_min_connect(const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    struct sockaddr_in addr;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        td_format_error(err, err_len, "socket failed: %s", strerror(errno));
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)opts->tcp_port);
    if (inet_pton(AF_INET, opts->endpoint, &addr.sin_addr) != 1) {
        close(fd);
        td_format_error(err, err_len, "invalid connect IP %s", opts->endpoint);
        return -1;
    }
    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        td_format_error(err, err_len, "connect failed: %s", strerror(errno));
        return -1;
    }
    td_rdma_min_tune_socket(fd);
    return fd;
}

static int td_rdma_min_pick_first_dirent(const char *dir_path, char *name, size_t name_len) {
    DIR *dir = opendir(dir_path);
    struct dirent *entry;

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

static int td_rdma_min_read_int_file(const char *path, int *value) {
    FILE *fp = fopen(path, "r");

    if (fp == NULL) {
        return -1;
    }
    if (fscanf(fp, "%d", value) != 1) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return 0;
}

static int td_rdma_min_resolve_device_name(const char *configured, char *verbs_name, size_t verbs_name_len, int *port_num, char *err, size_t err_len) {
    struct ibv_device **devices = NULL;
    int device_count = 0;
    int idx;

    devices = ibv_get_device_list(&device_count);
    if (devices == NULL || device_count <= 0) {
        td_format_error(err, err_len, "ibv_get_device_list failed");
        return -1;
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
        if (td_rdma_min_pick_first_dirent(path, verbs_name, verbs_name_len) != 0) {
            ibv_free_device_list(devices);
            td_format_error(err, err_len, "cannot map rdma_device %s to an IB verbs device", configured);
            return -1;
        }
        snprintf(path, sizeof(path), "/sys/class/net/%s/dev_port", configured);
        if (port_num != NULL && *port_num <= 0 && td_rdma_min_read_int_file(path, &inferred_port) == 0) {
            *port_num = inferred_port + 1;
        }
    }

    ibv_free_device_list(devices);
    return 0;
}

static int td_rdma_min_open_device(td_rdma_min_ctx_t *ctx, const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    struct ibv_device **devices = NULL;
    char verbs_name[TD_HOST_BYTES];
    int device_count = 0;
    int idx;
    int port_num = opts->rdma_port_num;

    if (td_rdma_min_resolve_device_name(opts->rdma_device, verbs_name, sizeof(verbs_name), &port_num, err, err_len) != 0) {
        return -1;
    }
    devices = ibv_get_device_list(&device_count);
    if (devices == NULL || device_count <= 0) {
        td_format_error(err, err_len, "ibv_get_device_list failed");
        return -1;
    }
    for (idx = 0; idx < device_count; ++idx) {
        if (strcmp(verbs_name, ibv_get_device_name(devices[idx])) == 0) {
            ctx->verbs = ibv_open_device(devices[idx]);
            break;
        }
    }
    ibv_free_device_list(devices);
    if (ctx->verbs == NULL) {
        td_format_error(err, err_len, "cannot open RDMA device %s", verbs_name);
        return -1;
    }
    ctx->port_num = port_num > 0 ? port_num : 1;
    ctx->gid_index = opts->gid_index;
    return 0;
}

static int td_rdma_min_alloc_native(td_rdma_min_buffer_t *buf, size_t bytes, char *err, size_t err_len) {
    const size_t page_size = (size_t)sysconf(_SC_PAGESIZE);

    buf->base = NULL;
    buf->bytes = bytes;
    buf->shared = 0;
    if (posix_memalign(&buf->base, page_size, bytes) != 0) {
        td_format_error(err, err_len, "posix_memalign failed");
        return -1;
    }
    memset(buf->base, 0, bytes);
    return 0;
}

static int td_rdma_min_alloc_shared(td_rdma_min_ctx_t *ctx, td_rdma_min_buffer_t *buf, size_t bytes, char *err, size_t err_len) {
    buf->base = NULL;
    buf->bytes = bytes;
    buf->shared = 1;
    if (td_tdx_map_shared_memory(&ctx->tdx, bytes, &buf->base, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static void td_rdma_min_free_buffer(td_rdma_min_ctx_t *ctx, td_rdma_min_buffer_t *buf) {
    if (buf->base == NULL) {
        return;
    }
    if (buf->shared) {
        td_tdx_unmap_shared_memory(&ctx->tdx, buf->base, buf->bytes);
    } else {
        free(buf->base);
    }
    buf->base = NULL;
    buf->bytes = 0;
    buf->shared = 0;
}

static int td_rdma_min_register_mr(td_rdma_min_ctx_t *ctx, td_rdma_min_buffer_t *buf, int access, struct ibv_mr **out_mr, char *err, size_t err_len) {
    struct ibv_mr *mr;

    if (buf->shared) {
        fprintf(stderr, "[MIN-RDMA] converting shared buffer base=%p bytes=%zu access=0x%x\n", buf->base, buf->bytes, access);
        fflush(stderr);
        if (td_tdx_accept_shared_memory(&ctx->tdx, buf->base, buf->bytes, err, err_len) != 0) {
            return -1;
        }
    }
    mr = ibv_reg_mr(ctx->pd, buf->base, buf->bytes, access);
    if (mr == NULL) {
        if (buf->shared) {
            (void)td_tdx_release_shared_memory(&ctx->tdx, buf->base, buf->bytes, NULL, 0);
        }
        td_format_error(err, err_len, "ibv_reg_mr failed: %s", strerror(errno));
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] mr registered base=%p bytes=%zu lkey=%u rkey=%u access=0x%x\n",
        buf->base,
        buf->bytes,
        mr->lkey,
        mr->rkey,
        access);
    fflush(stderr);
    *out_mr = mr;
    return 0;
}

static void td_rdma_min_fill_pattern(void *buf, size_t bytes) {
    static const unsigned char pattern[] = "rdma-min";
    size_t idx;
    unsigned char *out = (unsigned char *)buf;

    for (idx = 0; idx < bytes; ++idx) {
        out[idx] = pattern[idx % (sizeof(pattern) - 1)];
    }
}

static void td_rdma_min_hexdump(const char *label, const void *buf, size_t bytes) {
    const unsigned char *data = (const unsigned char *)buf;
    size_t limit = bytes < 64 ? bytes : 64;
    size_t idx;

    fprintf(stderr, "%s (%zu bytes, showing %zu):", label, bytes, limit);
    for (idx = 0; idx < limit; ++idx) {
        if ((idx % 16) == 0) {
            fprintf(stderr, "\n  %04zx:", idx);
        }
        fprintf(stderr, " %02x", data[idx]);
    }
    if (limit == 0) {
        fprintf(stderr, " <empty>");
    }
    fprintf(stderr, "\n");
    fflush(stderr);
}

static int td_rdma_min_query_local_conn_info(td_rdma_min_ctx_t *ctx, td_rdma_min_conn_info_t *info, char *err, size_t err_len) {
    struct ibv_port_attr port_attr;
    union ibv_gid zero_gid;

    memset(info, 0, sizeof(*info));
    memset(&zero_gid, 0, sizeof(zero_gid));
    if (ibv_query_port(ctx->verbs, (uint8_t)ctx->port_num, &port_attr) != 0) {
        td_format_error(err, err_len, "ibv_query_port failed on port %d", ctx->port_num);
        return -1;
    }
    if (port_attr.state != IBV_PORT_ACTIVE) {
        td_format_error(err, err_len, "RDMA port %d is not active", ctx->port_num);
        return -1;
    }
    ctx->lid = port_attr.lid;
    ctx->active_mtu = port_attr.active_mtu;
    memset(&ctx->gid, 0, sizeof(ctx->gid));
    ctx->has_gid = 0;
    if (ctx->gid_index >= 0 &&
        ibv_query_gid(ctx->verbs, (uint8_t)ctx->port_num, ctx->gid_index, &ctx->gid) == 0 &&
        memcmp(&ctx->gid, &zero_gid, sizeof(ctx->gid)) != 0) {
        ctx->has_gid = 1;
    }
    info->lid = ctx->lid;
    info->mtu = (uint8_t)ctx->active_mtu;
    info->has_gid = (uint8_t)ctx->has_gid;
    memcpy(info->gid, &ctx->gid, sizeof(info->gid));
    info->qp_num = ctx->qp->qp_num;
    info->psn = ctx->psn;
    return 0;
}

static int td_rdma_min_qp_to_init(td_rdma_min_ctx_t *ctx, td_rdma_min_op_t op, char *err, size_t err_len) {
    struct ibv_qp_attr attr;
    int access_flags = 0;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = (uint8_t)ctx->port_num;
    attr.pkey_index = 0;
    if (op == TD_RDMA_MIN_OP_WRITE || op == TD_RDMA_MIN_OP_WRITE_IMM) {
        access_flags |= IBV_ACCESS_REMOTE_WRITE;
    }
    attr.qp_access_flags = access_flags;
    if (ibv_modify_qp(ctx->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
        td_format_error(err, err_len, "ibv_modify_qp INIT failed");
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] qp->INIT qpn=%u access=0x%x\n", ctx->qp->qp_num, access_flags);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_qp_to_rtr(td_rdma_min_ctx_t *ctx, const td_rdma_min_conn_info_t *remote, char *err, size_t err_len) {
    struct ibv_qp_attr attr;
    int use_global = remote->has_gid && ctx->gid_index >= 0;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = ctx->active_mtu < (enum ibv_mtu)remote->mtu ? ctx->active_mtu : (enum ibv_mtu)remote->mtu;
    attr.dest_qp_num = remote->qp_num;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = (uint8_t)ctx->port_num;
    if (use_global) {
        attr.ah_attr.is_global = 1;
        memcpy(&attr.ah_attr.grh.dgid, remote->gid, sizeof(attr.ah_attr.grh.dgid));
        attr.ah_attr.grh.sgid_index = (uint8_t)ctx->gid_index;
        attr.ah_attr.grh.hop_limit = 255;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
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
    fprintf(stderr,
        "[MIN-RDMA] qp->RTR qpn=%u remote_qpn=%u dlid=%u use_global=%d mtu=%d rq_psn=%u\n",
        ctx->qp->qp_num,
        remote->qp_num,
        remote->lid,
        use_global,
        attr.path_mtu,
        remote->psn);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_qp_to_rts(td_rdma_min_ctx_t *ctx, char *err, size_t err_len) {
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = ctx->psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
            IBV_QP_STATE |
            IBV_QP_TIMEOUT |
            IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY |
            IBV_QP_SQ_PSN |
            IBV_QP_MAX_QP_RD_ATOMIC) != 0) {
        td_format_error(err, err_len, "ibv_modify_qp RTS failed");
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] qp->RTS qpn=%u sq_psn=%u\n", ctx->qp->qp_num, ctx->psn);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_post_recv(td_rdma_min_ctx_t *ctx, size_t bytes, char *err, size_t err_len) {
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)ctx->recv_buf.base;
    sge.length = (uint32_t)bytes;
    sge.lkey = ctx->recv_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(ctx->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "ibv_post_recv failed");
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] post_recv qpn=%u recv_buf=%p bytes=%zu lkey=%u\n",
        ctx->qp->qp_num,
        ctx->recv_buf.base,
        bytes,
        ctx->recv_mr->lkey);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_poll_wc(td_rdma_min_ctx_t *ctx, enum ibv_wc_opcode expected, unsigned int timeout_ms, struct ibv_wc *out_wc, char *err, size_t err_len) {
    struct ibv_wc wc;
    uint64_t deadline_ns = td_now_ns() + ((uint64_t)timeout_ms * 1000000ULL);
    unsigned long long empty_polls = 0;

    for (;;) {
        int n = ibv_poll_cq(ctx->cq, 1, &wc);

        if (n < 0) {
            td_format_error(err, err_len, "ibv_poll_cq failed");
            return -1;
        }
        if (n == 0) {
            ++empty_polls;
            if ((empty_polls % TD_RDMA_MIN_WAIT_LOG_POLLS) == 0) {
                fprintf(stderr,
                    "[MIN-RDMA] waiting for CQ completion expected=%s polls=%llu qpn=%u\n",
                    expected == (enum ibv_wc_opcode)-1 ? "ANY" : td_rdma_min_wc_opcode_name(expected),
                    empty_polls,
                    ctx->qp != NULL ? ctx->qp->qp_num : 0);
                fflush(stderr);
            }
            if (td_now_ns() >= deadline_ns) {
                td_format_error(err, err_len, "timed out waiting for CQ completion (%s)",
                    expected == (enum ibv_wc_opcode)-1 ? "ANY" : td_rdma_min_wc_opcode_name(expected));
                return -1;
            }
            sched_yield();
            continue;
        }
        if (wc.status != IBV_WC_SUCCESS) {
            td_format_error(err, err_len, "completion failed status=%d (%s)", wc.status, ibv_wc_status_str(wc.status));
            return -1;
        }
        fprintf(stderr,
            "[MIN-RDMA] CQ completion opcode=%s(%d) byte_len=%u imm=0x%x wr_id=%llu qpn=%u\n",
            td_rdma_min_wc_opcode_name(wc.opcode),
            wc.opcode,
            wc.byte_len,
            wc.imm_data,
            (unsigned long long)wc.wr_id,
            ctx->qp != NULL ? ctx->qp->qp_num : 0);
        fflush(stderr);
        if (expected == (enum ibv_wc_opcode)-1 ||
            wc.opcode == expected ||
            (expected == IBV_WC_RECV && wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM)) {
            if (out_wc != NULL) {
                *out_wc = wc;
            }
            return 0;
        }
    }
}

static int td_rdma_min_init_ctx(td_rdma_min_ctx_t *ctx, const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    struct ibv_qp_init_attr qp_attr;
    td_mode_t runtime_mode = opts->mode == TD_RDMA_MIN_MODE_SERVER && opts->tdx == TD_TDX_ON ? TD_MODE_MN : TD_MODE_CN;
    int recv_access = IBV_ACCESS_LOCAL_WRITE;
    int ctrl_access = IBV_ACCESS_LOCAL_WRITE;
    int data_access = IBV_ACCESS_LOCAL_WRITE;

    memset(ctx, 0, sizeof(*ctx));
    ctx->use_shared = opts->mode == TD_RDMA_MIN_MODE_SERVER && opts->tdx == TD_TDX_ON;
    if (td_tdx_runtime_init(&ctx->tdx, runtime_mode, opts->tdx, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_min_open_device(ctx, opts, err, err_len) != 0) {
        return -1;
    }
    ctx->pd = ibv_alloc_pd(ctx->verbs);
    if (ctx->pd == NULL) {
        td_format_error(err, err_len, "ibv_alloc_pd failed");
        return -1;
    }
    ctx->cq = ibv_create_cq(ctx->verbs, 16, NULL, NULL, 0);
    if (ctx->cq == NULL) {
        td_format_error(err, err_len, "ibv_create_cq failed");
        return -1;
    }

    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = ctx->cq;
    qp_attr.recv_cq = ctx->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 8;
    qp_attr.cap.max_recv_wr = 8;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    ctx->qp = ibv_create_qp(ctx->pd, &qp_attr);
    if (ctx->qp == NULL) {
        td_format_error(err, err_len, "ibv_create_qp failed");
        return -1;
    }

    if (ctx->use_shared) {
        if (td_rdma_min_alloc_shared(ctx, &ctx->send_buf, sizeof(td_rdma_min_msg_t) + opts->bytes, err, err_len) != 0 ||
            td_rdma_min_alloc_shared(ctx, &ctx->recv_buf, sizeof(td_rdma_min_msg_t) + opts->bytes, err, err_len) != 0 ||
            td_rdma_min_alloc_shared(ctx, &ctx->ctrl_buf, sizeof(td_rdma_min_msg_t), err, err_len) != 0 ||
            td_rdma_min_alloc_shared(ctx, &ctx->data_buf, opts->bytes, err, err_len) != 0) {
            return -1;
        }
    } else {
        if (td_rdma_min_alloc_native(&ctx->send_buf, sizeof(td_rdma_min_msg_t) + opts->bytes, err, err_len) != 0 ||
            td_rdma_min_alloc_native(&ctx->recv_buf, sizeof(td_rdma_min_msg_t) + opts->bytes, err, err_len) != 0 ||
            td_rdma_min_alloc_native(&ctx->ctrl_buf, sizeof(td_rdma_min_msg_t), err, err_len) != 0 ||
            td_rdma_min_alloc_native(&ctx->data_buf, opts->bytes, err, err_len) != 0) {
            return -1;
        }
    }

    if (opts->op == TD_RDMA_MIN_OP_WRITE_IMM) {
        recv_access |= IBV_ACCESS_REMOTE_WRITE;
        ctrl_access |= IBV_ACCESS_REMOTE_WRITE;
        data_access |= IBV_ACCESS_REMOTE_WRITE;
    } else if (opts->op == TD_RDMA_MIN_OP_WRITE) {
        data_access |= IBV_ACCESS_REMOTE_WRITE;
    }

    if (td_rdma_min_register_mr(ctx, &ctx->send_buf, IBV_ACCESS_LOCAL_WRITE, &ctx->send_mr, err, err_len) != 0 ||
        td_rdma_min_register_mr(ctx, &ctx->recv_buf, recv_access, &ctx->recv_mr, err, err_len) != 0 ||
        td_rdma_min_register_mr(ctx, &ctx->ctrl_buf, ctrl_access, &ctx->ctrl_mr, err, err_len) != 0 ||
        td_rdma_min_register_mr(ctx, &ctx->data_buf, data_access, &ctx->data_mr, err, err_len) != 0) {
        return -1;
    }

    ctx->psn = (uint32_t)((td_now_ns() ^ (uint64_t)getpid()) & 0x00ffffffu);
    if (td_rdma_min_qp_to_init(ctx, opts->op, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

static int td_rdma_min_exchange_client_bootstrap(int fd, td_rdma_min_ctx_t *ctx, td_rdma_min_bootstrap_t *remote_out, char *err, size_t err_len) {
    td_rdma_min_bootstrap_t local;
    td_rdma_min_bootstrap_t remote;

    memset(&local, 0, sizeof(local));
    local.magic = TD_RDMA_MIN_BOOTSTRAP_MAGIC;
    local.version = TD_RDMA_MIN_BOOTSTRAP_VERSION;
    if (td_rdma_min_query_local_conn_info(ctx, &local.conn, err, err_len) != 0) {
        return -1;
    }
    local.ctrl_addr = (uint64_t)(uintptr_t)ctx->ctrl_buf.base;
    local.ctrl_rkey = ctx->ctrl_mr != NULL ? ctx->ctrl_mr->rkey : 0;
    local.data_addr = (uint64_t)(uintptr_t)ctx->data_buf.base;
    local.data_rkey = ctx->data_mr != NULL ? ctx->data_mr->rkey : 0;
    local.data_bytes = (uint32_t)ctx->data_buf.bytes;

    if (td_rdma_min_send_all(fd, &local, sizeof(local)) != 0 ||
        td_rdma_min_recv_all(fd, &remote, sizeof(remote)) != 0) {
        td_format_error(err, err_len, "bootstrap exchange failed");
        return -1;
    }
    if (remote.magic != TD_RDMA_MIN_BOOTSTRAP_MAGIC || remote.version != TD_RDMA_MIN_BOOTSTRAP_VERSION) {
        td_format_error(err, err_len, "invalid bootstrap reply");
        return -1;
    }
    if (remote.data_bytes < ctx->data_buf.bytes) {
        td_format_error(err, err_len, "remote data buffer too small: remote=%u local=%zu", remote.data_bytes, ctx->data_buf.bytes);
        return -1;
    }
    if (td_rdma_min_qp_to_rtr(ctx, &remote.conn, err, err_len) != 0 ||
        td_rdma_min_qp_to_rts(ctx, err, err_len) != 0) {
        return -1;
    }
    *remote_out = remote;
    return 0;
}

static int td_rdma_min_exchange_server_bootstrap(int fd, td_rdma_min_ctx_t *ctx, char *err, size_t err_len) {
    td_rdma_min_bootstrap_t local;
    td_rdma_min_bootstrap_t remote;

    if (td_rdma_min_recv_all(fd, &remote, sizeof(remote)) != 0) {
        td_format_error(err, err_len, "bootstrap receive failed");
        return -1;
    }
    if (remote.magic != TD_RDMA_MIN_BOOTSTRAP_MAGIC || remote.version != TD_RDMA_MIN_BOOTSTRAP_VERSION) {
        td_format_error(err, err_len, "invalid bootstrap request");
        return -1;
    }

    memset(&local, 0, sizeof(local));
    local.magic = TD_RDMA_MIN_BOOTSTRAP_MAGIC;
    local.version = TD_RDMA_MIN_BOOTSTRAP_VERSION;
    if (td_rdma_min_query_local_conn_info(ctx, &local.conn, err, err_len) != 0) {
        return -1;
    }
    local.ctrl_addr = (uint64_t)(uintptr_t)ctx->ctrl_buf.base;
    local.ctrl_rkey = ctx->ctrl_mr != NULL ? ctx->ctrl_mr->rkey : 0;
    local.data_addr = (uint64_t)(uintptr_t)ctx->data_buf.base;
    local.data_rkey = ctx->data_mr != NULL ? ctx->data_mr->rkey : 0;
    local.data_bytes = (uint32_t)ctx->data_buf.bytes;

    if (td_rdma_min_qp_to_rtr(ctx, &remote.conn, err, err_len) != 0 ||
        td_rdma_min_qp_to_rts(ctx, err, err_len) != 0) {
        return -1;
    }
    if (td_rdma_min_send_all(fd, &local, sizeof(local)) != 0) {
        td_format_error(err, err_len, "bootstrap send failed");
        return -1;
    }
    return 0;
}

static int td_rdma_min_post_send(td_rdma_min_ctx_t *ctx, size_t bytes, char *err, size_t err_len) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)ctx->send_buf.base;
    sge.length = (uint32_t)bytes;
    sge.lkey = ctx->send_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(ctx->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "ibv_post_send SEND failed");
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] post_send qpn=%u bytes=%zu\n", ctx->qp->qp_num, bytes);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_post_write(td_rdma_min_ctx_t *ctx, const td_rdma_min_bootstrap_t *remote, char *err, size_t err_len) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)ctx->data_buf.base;
    sge.length = (uint32_t)ctx->data_buf.bytes;
    sge.lkey = ctx->data_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote->data_addr;
    wr.wr.rdma.rkey = remote->data_rkey;

    if (ibv_post_send(ctx->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "ibv_post_send RDMA_WRITE failed");
        return -1;
    }
    fprintf(stderr, "[MIN-RDMA] post_write qpn=%u bytes=%zu remote=0x%llx/%u\n",
        ctx->qp->qp_num,
        ctx->data_buf.bytes,
        (unsigned long long)remote->data_addr,
        remote->data_rkey);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_post_write_imm(td_rdma_min_ctx_t *ctx, const td_rdma_min_bootstrap_t *remote, uint32_t imm_data, char *err, size_t err_len) {
    struct ibv_sge data_sge;
    struct ibv_sge ctrl_sge;
    struct ibv_send_wr data_wr;
    struct ibv_send_wr ctrl_wr;
    struct ibv_send_wr *first_wr = &ctrl_wr;
    struct ibv_send_wr *bad_wr = NULL;

    memset(&ctrl_sge, 0, sizeof(ctrl_sge));
    ctrl_sge.addr = (uintptr_t)ctx->ctrl_buf.base;
    ctrl_sge.length = sizeof(td_rdma_min_msg_t);
    ctrl_sge.lkey = ctx->ctrl_mr->lkey;

    memset(&ctrl_wr, 0, sizeof(ctrl_wr));
    ctrl_wr.sg_list = &ctrl_sge;
    ctrl_wr.num_sge = 1;
    ctrl_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    ctrl_wr.send_flags = IBV_SEND_SIGNALED;
    ctrl_wr.imm_data = htonl(imm_data);
    ctrl_wr.wr.rdma.remote_addr = remote->ctrl_addr;
    ctrl_wr.wr.rdma.rkey = remote->ctrl_rkey;

    if (ctx->data_buf.bytes > 0) {
        memset(&data_sge, 0, sizeof(data_sge));
        data_sge.addr = (uintptr_t)ctx->data_buf.base;
        data_sge.length = (uint32_t)ctx->data_buf.bytes;
        data_sge.lkey = ctx->data_mr->lkey;

        memset(&data_wr, 0, sizeof(data_wr));
        data_wr.sg_list = &data_sge;
        data_wr.num_sge = 1;
        data_wr.opcode = IBV_WR_RDMA_WRITE;
        data_wr.send_flags = 0;
        data_wr.wr.rdma.remote_addr = remote->data_addr;
        data_wr.wr.rdma.rkey = remote->data_rkey;
        data_wr.next = &ctrl_wr;
        first_wr = &data_wr;
    }

    if (ibv_post_send(ctx->qp, first_wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "ibv_post_send RDMA_WRITE_WITH_IMM failed");
        return -1;
    }
    fprintf(stderr,
        "[MIN-RDMA] post_write_imm qpn=%u ctrl=0x%llx/%u data=0x%llx/%u bytes=%zu imm=0x%x\n",
        ctx->qp->qp_num,
        (unsigned long long)remote->ctrl_addr,
        remote->ctrl_rkey,
        (unsigned long long)remote->data_addr,
        remote->data_rkey,
        ctx->data_buf.bytes,
        imm_data);
    fflush(stderr);
    return 0;
}

static int td_rdma_min_wait_for_remote_write(td_rdma_min_ctx_t *ctx, unsigned int timeout_ms, char *err, size_t err_len) {
    uint64_t deadline_ns = td_now_ns() + ((uint64_t)timeout_ms * 1000000ULL);

    while (td_now_ns() < deadline_ns) {
        const unsigned char *data = (const unsigned char *)ctx->data_buf.base;
        size_t idx;

        for (idx = 0; idx < ctx->data_buf.bytes; ++idx) {
            if (data[idx] != 0) {
                fprintf(stderr, "[MIN-RDMA] remote write detected at offset %zu\n", idx);
                fflush(stderr);
                return 0;
            }
        }
        sched_yield();
    }
    td_format_error(err, err_len, "timed out waiting for remote data buffer change");
    return -1;
}

static int td_rdma_min_server(const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    td_rdma_min_ctx_t ctx;
    struct ibv_wc wc;
    int listen_fd = -1;
    int fd = -1;
    int rc = -1;

    memset(&ctx, 0, sizeof(ctx));
    if (td_rdma_min_init_ctx(&ctx, opts, err, err_len) != 0) {
        goto done;
    }

    listen_fd = td_rdma_min_listen(opts, err, err_len);
    if (listen_fd < 0) {
        goto done;
    }
    fprintf(stderr,
        "[MIN-RDMA] %s waiting on %s:%d rdma_device=%s rdma_port=%d gid_index=%d tdx=%s op=%s bytes=%zu\n",
        td_rdma_min_mode_name(opts->mode),
        opts->endpoint,
        opts->tcp_port,
        opts->rdma_device,
        opts->rdma_port_num,
        opts->gid_index,
        opts->tdx == TD_TDX_ON ? "on" : "off",
        td_rdma_min_op_name(opts->op),
        opts->bytes);
    fflush(stderr);

    fd = td_rdma_min_accept(listen_fd, err, err_len);
    if (fd < 0) {
        goto done;
    }
    if (opts->op == TD_RDMA_MIN_OP_SEND) {
        if (td_rdma_min_post_recv(&ctx, sizeof(td_rdma_min_msg_t) + opts->bytes, err, err_len) != 0) {
            goto done;
        }
    } else if (opts->op == TD_RDMA_MIN_OP_WRITE_IMM) {
        if (td_rdma_min_post_recv(&ctx, sizeof(td_rdma_min_msg_t), err, err_len) != 0) {
            goto done;
        }
    }

    if (td_rdma_min_exchange_server_bootstrap(fd, &ctx, err, err_len) != 0) {
        goto done;
    }

    if (opts->op == TD_RDMA_MIN_OP_SEND) {
        td_rdma_min_msg_t *msg;

        if (td_rdma_min_poll_wc(&ctx, IBV_WC_RECV, opts->timeout_ms, &wc, err, err_len) != 0) {
            goto done;
        }
        msg = (td_rdma_min_msg_t *)ctx.recv_buf.base;
        fprintf(stderr, "[MIN-RDMA] server recv header magic=0x%x op=%u bytes=%u seq=%u\n",
            msg->magic,
            msg->op,
            msg->bytes,
            msg->seq);
        td_rdma_min_hexdump("[MIN-RDMA] server recv payload",
            (unsigned char *)ctx.recv_buf.base + sizeof(*msg),
            msg->bytes <= opts->bytes ? msg->bytes : opts->bytes);
    } else if (opts->op == TD_RDMA_MIN_OP_WRITE) {
        if (td_rdma_min_wait_for_remote_write(&ctx, opts->timeout_ms, err, err_len) != 0) {
            goto done;
        }
        td_rdma_min_hexdump("[MIN-RDMA] server data buffer", ctx.data_buf.base, ctx.data_buf.bytes);
    } else {
        td_rdma_min_msg_t *msg;

        if (td_rdma_min_poll_wc(&ctx, IBV_WC_RECV, opts->timeout_ms, &wc, err, err_len) != 0) {
            goto done;
        }
        msg = (td_rdma_min_msg_t *)ctx.ctrl_buf.base;
        fprintf(stderr, "[MIN-RDMA] server ctrl header magic=0x%x op=%u bytes=%u seq=%u imm=0x%x\n",
            msg->magic,
            msg->op,
            msg->bytes,
            msg->seq,
            ntohl(wc.imm_data));
        td_rdma_min_hexdump("[MIN-RDMA] server ctrl buffer", ctx.ctrl_buf.base, ctx.ctrl_buf.bytes);
        td_rdma_min_hexdump("[MIN-RDMA] server data buffer", ctx.data_buf.base, ctx.data_buf.bytes);
    }

    rc = 0;

done:
    if (fd >= 0) {
        close(fd);
    }
    if (listen_fd >= 0) {
        close(listen_fd);
    }
    if (ctx.send_mr != NULL) {
        ibv_dereg_mr(ctx.send_mr);
    }
    if (ctx.recv_mr != NULL) {
        ibv_dereg_mr(ctx.recv_mr);
    }
    if (ctx.ctrl_mr != NULL) {
        ibv_dereg_mr(ctx.ctrl_mr);
    }
    if (ctx.data_mr != NULL) {
        ibv_dereg_mr(ctx.data_mr);
    }
    if (ctx.send_buf.shared) {
        (void)td_tdx_release_shared_memory(&ctx.tdx, ctx.send_buf.base, ctx.send_buf.bytes, NULL, 0);
    }
    if (ctx.recv_buf.shared) {
        (void)td_tdx_release_shared_memory(&ctx.tdx, ctx.recv_buf.base, ctx.recv_buf.bytes, NULL, 0);
    }
    if (ctx.ctrl_buf.shared) {
        (void)td_tdx_release_shared_memory(&ctx.tdx, ctx.ctrl_buf.base, ctx.ctrl_buf.bytes, NULL, 0);
    }
    if (ctx.data_buf.shared) {
        (void)td_tdx_release_shared_memory(&ctx.tdx, ctx.data_buf.base, ctx.data_buf.bytes, NULL, 0);
    }
    td_rdma_min_free_buffer(&ctx, &ctx.send_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.recv_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.ctrl_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.data_buf);
    if (ctx.qp != NULL) {
        ibv_destroy_qp(ctx.qp);
    }
    if (ctx.cq != NULL) {
        ibv_destroy_cq(ctx.cq);
    }
    if (ctx.pd != NULL) {
        ibv_dealloc_pd(ctx.pd);
    }
    if (ctx.verbs != NULL) {
        ibv_close_device(ctx.verbs);
    }
    return rc;
}

static int td_rdma_min_client(const td_rdma_min_options_t *opts, char *err, size_t err_len) {
    td_rdma_min_ctx_t ctx;
    td_rdma_min_bootstrap_t remote;
    struct ibv_wc wc;
    int fd = -1;
    int rc = -1;

    memset(&ctx, 0, sizeof(ctx));
    memset(&remote, 0, sizeof(remote));
    if (td_rdma_min_init_ctx(&ctx, opts, err, err_len) != 0) {
        goto done;
    }
    fd = td_rdma_min_connect(opts, err, err_len);
    if (fd < 0) {
        goto done;
    }
    if (td_rdma_min_exchange_client_bootstrap(fd, &ctx, &remote, err, err_len) != 0) {
        goto done;
    }

    if (opts->op == TD_RDMA_MIN_OP_SEND) {
        td_rdma_min_msg_t *msg = (td_rdma_min_msg_t *)ctx.send_buf.base;

        memset(ctx.send_buf.base, 0, ctx.send_buf.bytes);
        msg->magic = TD_RDMA_MIN_MSG_MAGIC;
        msg->op = (uint32_t)opts->op;
        msg->bytes = (uint32_t)opts->bytes;
        msg->seq = opts->seq;
        td_rdma_min_fill_pattern((unsigned char *)ctx.send_buf.base + sizeof(*msg), opts->bytes);
        td_rdma_min_hexdump("[MIN-RDMA] client send payload",
            (unsigned char *)ctx.send_buf.base + sizeof(*msg),
            opts->bytes);
        if (td_rdma_min_post_send(&ctx, sizeof(*msg) + opts->bytes, err, err_len) != 0) {
            goto done;
        }
        if (td_rdma_min_poll_wc(&ctx, IBV_WC_SEND, opts->timeout_ms, &wc, err, err_len) != 0) {
            goto done;
        }
    } else if (opts->op == TD_RDMA_MIN_OP_WRITE) {
        memset(ctx.data_buf.base, 0, ctx.data_buf.bytes);
        td_rdma_min_fill_pattern(ctx.data_buf.base, ctx.data_buf.bytes);
        td_rdma_min_hexdump("[MIN-RDMA] client write payload", ctx.data_buf.base, ctx.data_buf.bytes);
        if (td_rdma_min_post_write(&ctx, &remote, err, err_len) != 0) {
            goto done;
        }
        if (td_rdma_min_poll_wc(&ctx, IBV_WC_RDMA_WRITE, opts->timeout_ms, &wc, err, err_len) != 0) {
            goto done;
        }
    } else {
        td_rdma_min_msg_t *msg = (td_rdma_min_msg_t *)ctx.ctrl_buf.base;

        memset(ctx.ctrl_buf.base, 0, ctx.ctrl_buf.bytes);
        memset(ctx.data_buf.base, 0, ctx.data_buf.bytes);
        msg->magic = TD_RDMA_MIN_MSG_MAGIC;
        msg->op = (uint32_t)opts->op;
        msg->bytes = (uint32_t)opts->bytes;
        msg->seq = opts->seq;
        td_rdma_min_fill_pattern(ctx.data_buf.base, ctx.data_buf.bytes);
        td_rdma_min_hexdump("[MIN-RDMA] client write_imm ctrl", ctx.ctrl_buf.base, ctx.ctrl_buf.bytes);
        td_rdma_min_hexdump("[MIN-RDMA] client write_imm payload", ctx.data_buf.base, ctx.data_buf.bytes);
        if (td_rdma_min_post_write_imm(&ctx, &remote, opts->seq, err, err_len) != 0) {
            goto done;
        }
        if (td_rdma_min_poll_wc(&ctx, IBV_WC_RDMA_WRITE, opts->timeout_ms, &wc, err, err_len) != 0) {
            goto done;
        }
    }

    rc = 0;

done:
    if (fd >= 0) {
        close(fd);
    }
    if (ctx.send_mr != NULL) {
        ibv_dereg_mr(ctx.send_mr);
    }
    if (ctx.recv_mr != NULL) {
        ibv_dereg_mr(ctx.recv_mr);
    }
    if (ctx.ctrl_mr != NULL) {
        ibv_dereg_mr(ctx.ctrl_mr);
    }
    if (ctx.data_mr != NULL) {
        ibv_dereg_mr(ctx.data_mr);
    }
    td_rdma_min_free_buffer(&ctx, &ctx.send_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.recv_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.ctrl_buf);
    td_rdma_min_free_buffer(&ctx, &ctx.data_buf);
    if (ctx.qp != NULL) {
        ibv_destroy_qp(ctx.qp);
    }
    if (ctx.cq != NULL) {
        ibv_destroy_cq(ctx.cq);
    }
    if (ctx.pd != NULL) {
        ibv_dealloc_pd(ctx.pd);
    }
    if (ctx.verbs != NULL) {
        ibv_close_device(ctx.verbs);
    }
    return rc;
}

int main(int argc, char **argv) {
    td_rdma_min_options_t opts;
    char err[256];

    if (td_rdma_min_parse_args(argc, argv, &opts) != 0) {
        td_rdma_min_usage(argv[0]);
        return 1;
    }

    fprintf(stderr,
        "[MIN-RDMA] start mode=%s endpoint=%s tcp_port=%d rdma_device=%s rdma_port=%d gid_index=%d tdx=%s op=%s bytes=%zu timeout_ms=%u\n",
        td_rdma_min_mode_name(opts.mode),
        opts.endpoint,
        opts.tcp_port,
        opts.rdma_device,
        opts.rdma_port_num,
        opts.gid_index,
        opts.tdx == TD_TDX_ON ? "on" : "off",
        td_rdma_min_op_name(opts.op),
        opts.bytes,
        opts.timeout_ms);
    fflush(stderr);

    err[0] = '\0';
    if (opts.mode == TD_RDMA_MIN_MODE_SERVER) {
        if (td_rdma_min_server(&opts, err, sizeof(err)) != 0) {
            fprintf(stderr, "rdma_min server failed: %s\n", err);
            return 1;
        }
    } else {
        if (td_rdma_min_client(&opts, err, sizeof(err)) != 0) {
            fprintf(stderr, "rdma_min client failed: %s\n", err);
            return 1;
        }
    }

    fprintf(stderr, "[MIN-RDMA] done\n");
    return 0;
}
