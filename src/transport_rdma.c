#include "td_transport.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    struct ibv_mr *op_mr;
    struct ibv_mr *cas_mr;
    td_wire_msg_t *send_msg;
    td_wire_msg_t *recv_msg;
    unsigned char *op_buf;
    uint64_t *cas_buf;
    size_t op_buf_len;
    td_tdx_runtime_t tdx;
} td_rdma_impl_t;

static uint64_t td_rdma_profile_begin(td_session_t *session) {
    return session->transport_profile != NULL ? td_now_ns() : 0;
}

static void td_rdma_profile_end(td_session_t *session, uint64_t start_ns, uint64_t *field) {
    if (session->transport_profile != NULL && field != NULL && start_ns != 0) {
        *field += td_now_ns() - start_ns;
    }
}

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

int td_tcp_client_connect(td_session_t *session, const td_endpoint_t *endpoint, char *err, size_t err_len);
static int td_rdma_wait_response(td_rdma_impl_t *impl, td_wire_msg_t *response, uint64_t *wait_ns, td_rdma_wait_profile_t *wait_profile, uint64_t *copy_ns, char *err, size_t err_len);

static int td_rdma_post_recv(td_rdma_impl_t *impl, char *err, size_t err_len) {
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)impl->recv_msg;
    sge.length = sizeof(*impl->recv_msg);
    sge.lkey = impl->recv_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(impl->id->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma post recv failed");
        return -1;
    }
    return 0;
}

static int td_rdma_poll_wc(td_rdma_impl_t *impl, enum ibv_wc_opcode expected, td_rdma_wait_profile_t *profile, char *err, size_t err_len) {
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
            td_format_error(err, err_len, "rdma completion failed status=%d", wc.status);
            return -1;
        }
        if (expected == (enum ibv_wc_opcode)-1 || wc.opcode == expected) {
            return 0;
        }
    }
}

static int td_rdma_wait_event(struct rdma_event_channel *ec, enum rdma_cm_event_type expected, struct rdma_cm_event **out_event, char *err, size_t err_len) {
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(ec, &event) != 0) {
        td_format_error(err, err_len, "rdma get cm event failed");
        return -1;
    }
    if (event->event != expected) {
        td_format_error(err, err_len, "unexpected rdma cm event %s(%d)", rdma_event_str(event->event), event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    *out_event = event;
    return 0;
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
    if (impl->cas_mr != NULL) {
        ibv_dereg_mr(impl->cas_mr);
    }
    if (impl->id != NULL && impl->id->qp != NULL) {
        rdma_destroy_qp(impl->id);
    }
    if (impl->cq != NULL) {
        ibv_destroy_cq(impl->cq);
    }
    if (impl->pd != NULL) {
        ibv_dealloc_pd(impl->pd);
    }
    if (impl->id != NULL) {
        rdma_destroy_id(impl->id);
    }
    if (impl->ec != NULL) {
        rdma_destroy_event_channel(impl->ec);
    }
    td_tdx_unmap_shared_memory(&impl->tdx, impl->send_msg, sizeof(*impl->send_msg));
    td_tdx_unmap_shared_memory(&impl->tdx, impl->recv_msg, sizeof(*impl->recv_msg));
    td_tdx_unmap_shared_memory(&impl->tdx, impl->op_buf, impl->op_buf_len);
    td_tdx_unmap_shared_memory(&impl->tdx, impl->cas_buf, sizeof(*impl->cas_buf));
    memset(impl, 0, sizeof(*impl));
}

static int td_rdma_setup_qp(td_rdma_impl_t *impl, size_t op_buf_len, char *err, size_t err_len) {
    struct ibv_qp_init_attr qp_attr;

    impl->pd = ibv_alloc_pd(impl->id->verbs);
    impl->cq = ibv_create_cq(impl->id->verbs, 64, NULL, NULL, 0);
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
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(impl->id, impl->pd, &qp_attr) != 0) {
        td_format_error(err, err_len, "rdma create qp failed");
        return -1;
    }

    impl->op_buf_len = op_buf_len;
    /* Keep all NIC-visible local buffers on pages that went through the shared-memory path. */
    if (td_tdx_map_shared_memory(&impl->tdx, sizeof(*impl->send_msg), (void **)&impl->send_msg, err, err_len) != 0 ||
        td_tdx_map_shared_memory(&impl->tdx, sizeof(*impl->recv_msg), (void **)&impl->recv_msg, err, err_len) != 0 ||
        td_tdx_map_shared_memory(&impl->tdx, impl->op_buf_len, (void **)&impl->op_buf, err, err_len) != 0 ||
        td_tdx_map_shared_memory(&impl->tdx, sizeof(*impl->cas_buf), (void **)&impl->cas_buf, err, err_len) != 0) {
        return -1;
    }

    impl->send_mr = ibv_reg_mr(impl->pd, impl->send_msg, sizeof(*impl->send_msg), IBV_ACCESS_LOCAL_WRITE);
    impl->recv_mr = ibv_reg_mr(impl->pd, impl->recv_msg, sizeof(*impl->recv_msg), IBV_ACCESS_LOCAL_WRITE);
    impl->op_mr = ibv_reg_mr(impl->pd, impl->op_buf, impl->op_buf_len, IBV_ACCESS_LOCAL_WRITE);
    impl->cas_mr = ibv_reg_mr(impl->pd, impl->cas_buf, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);
    if (impl->send_mr == NULL || impl->recv_mr == NULL || impl->op_mr == NULL || impl->cas_mr == NULL) {
        td_format_error(err, err_len, "rdma mr registration failed");
        return -1;
    }
    return 0;
}

static int td_rdma_send_control(td_rdma_impl_t *impl, const td_wire_msg_t *msg, uint64_t *post_send_ns, uint64_t *send_wait_ns, td_rdma_wait_profile_t *wait_profile, char *err, size_t err_len) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    uint64_t start_ns;

    memcpy(impl->send_msg, msg, sizeof(*msg));
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)impl->send_msg;
    sge.length = sizeof(*impl->send_msg);
    sge.lkey = impl->send_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    start_ns = post_send_ns != NULL ? td_now_ns() : 0;
    if (ibv_post_send(impl->id->qp, &wr, &bad_wr) != 0) {
        td_format_error(err, err_len, "rdma send control failed");
        return -1;
    }
    if (post_send_ns != NULL && start_ns != 0) {
        *post_send_ns += td_now_ns() - start_ns;
    }

    start_ns = send_wait_ns != NULL ? td_now_ns() : 0;
    if (td_rdma_poll_wc(impl, IBV_WC_SEND, wait_profile, err, err_len) != 0) {
        return -1;
    }
    if (send_wait_ns != NULL && start_ns != 0) {
        *send_wait_ns += td_now_ns() - start_ns;
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
    if (ibv_post_send(impl->id->qp, &wr, &bad_wr) != 0) {
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
    if (ibv_post_send(impl->id->qp, &wr, &bad_wr) != 0) {
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
            err,
            err_len) != 0) {
        td_format_error(err, err_len, "rdma write op failed");
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->write_wait_ns : NULL);
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

    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_control(
            impl,
            &request,
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
    if (td_rdma_wait_response(
            impl,
            &response,
            session->transport_profile != NULL ? &session->transport_profile->rdma_cas_response_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_cas_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_cas_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_cas_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_cas_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            err,
            err_len) != 0 ||
        response.status != 0) {
        td_format_error(err, err_len, "rdma cas control failed");
        return -1;
    }
    *old_value = response.compare;
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->cas_wait_ns : NULL);
    if (session->transport_profile != NULL) {
        session->transport_profile->rdma_server_cas_total_ns += response.profile_total_ns;
        session->transport_profile->rdma_server_cas_region_ns += response.profile_stage1_ns;
    }
    return 0;
}

static int td_rdma_wait_response(td_rdma_impl_t *impl, td_wire_msg_t *response, uint64_t *wait_ns, td_rdma_wait_profile_t *wait_profile, uint64_t *copy_ns, char *err, size_t err_len) {
    uint64_t start_ns = wait_ns != NULL ? td_now_ns() : 0;

    if (td_rdma_poll_wc(impl, IBV_WC_RECV, wait_profile, err, err_len) != 0) {
        return -1;
    }
    if (wait_ns != NULL && start_ns != 0) {
        *wait_ns += td_now_ns() - start_ns;
    }
    start_ns = copy_ns != NULL ? td_now_ns() : 0;
    memcpy(response, impl->recv_msg, sizeof(*response));
    if (copy_ns != NULL && start_ns != 0) {
        *copy_ns += td_now_ns() - start_ns;
    }
    if (td_rdma_post_recv(impl, err, err_len) != 0) {
        return -1;
    }
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

    start_ns = td_rdma_profile_begin(session);
    if (td_rdma_send_control(
            impl,
            &request,
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
    if (td_rdma_wait_response(
            impl,
            &response,
            session->transport_profile != NULL ? &session->transport_profile->rdma_control_response_wait_ns : NULL,
            session->transport_profile != NULL ? &(td_rdma_wait_profile_t){
                .poll_cq_ns = &session->transport_profile->rdma_control_response_poll_cq_ns,
                .backoff_ns = &session->transport_profile->rdma_control_response_backoff_ns,
                .empty_polls = &session->transport_profile->rdma_control_response_empty_polls,
                .backoff_count = &session->transport_profile->rdma_control_response_backoff_count,
            } : NULL,
            session->transport_profile != NULL ? &session->transport_profile->rdma_response_copy_ns : NULL,
            err,
            err_len) != 0 ||
        response.status != 0) {
        td_format_error(err, err_len, "rdma control op %u failed", (unsigned int)op);
        return -1;
    }
    td_rdma_profile_end(session, start_ns, session->transport_profile != NULL ? &session->transport_profile->control_wait_ns : NULL);
    if (session->transport_profile != NULL) {
        session->transport_profile->rdma_server_control_total_ns += response.profile_total_ns;
        session->transport_profile->rdma_server_control_exec_ns += response.profile_stage1_ns;
    }
    return 0;
}

static void td_rdma_client_close(td_session_t *session) {
    td_rdma_impl_t *impl = (td_rdma_impl_t *)session->impl;
    if (impl == NULL) {
        return;
    }
    (void)td_rdma_client_control(session, TD_WIRE_CLOSE, NULL, 0);
    if (impl->id != NULL) {
        rdma_disconnect(impl->id);
    }
    td_rdma_destroy_impl(impl);
    free(impl);
    session->impl = NULL;
}

static int td_rdma_client_connect(td_session_t *session, const td_config_t *cfg, const td_endpoint_t *endpoint, char *err, size_t err_len) {
    struct rdma_addrinfo hints;
    struct rdma_addrinfo *res = NULL;
    td_rdma_impl_t *impl = NULL;
    struct rdma_cm_event *event = NULL;
    struct rdma_conn_param conn_param;
    char port[16];
    td_wire_msg_t hello;
    td_wire_msg_t response;

    snprintf(port, sizeof(port), "%d", endpoint->port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;

    if (rdma_getaddrinfo((char *)endpoint->host, port, &hints, &res) != 0) {
        td_format_error(err, err_len, "rdma getaddrinfo failed for %s:%d", endpoint->host, endpoint->port);
        return -1;
    }

    impl = (td_rdma_impl_t *)calloc(1, sizeof(*impl));
    if (impl == NULL) {
        rdma_freeaddrinfo(res);
        td_format_error(err, err_len, "out of memory");
        return -1;
    }
    if (td_tdx_runtime_init(&impl->tdx, cfg->mode, cfg->tdx, err, err_len) != 0) {
        rdma_freeaddrinfo(res);
        free(impl);
        return -1;
    }

    impl->ec = rdma_create_event_channel();
    if (impl->ec == NULL || rdma_create_id(impl->ec, &impl->id, NULL, RDMA_PS_TCP) != 0) {
        rdma_freeaddrinfo(res);
        td_rdma_destroy_impl(impl);
        free(impl);
        td_format_error(err, err_len, "rdma create id failed");
        return -1;
    }

    if (rdma_resolve_addr(impl->id, NULL, res->ai_dst_addr, 2000) != 0 ||
        td_rdma_wait_event(impl->ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event, err, err_len) != 0) {
        rdma_freeaddrinfo(res);
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(impl->id, 2000) != 0 ||
        td_rdma_wait_event(impl->ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event, err, err_len) != 0) {
        rdma_freeaddrinfo(res);
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    rdma_ack_cm_event(event);
    rdma_freeaddrinfo(res);

    if (td_rdma_setup_qp(impl, sizeof(td_slot_t), err, err_len) != 0 ||
        td_rdma_post_recv(impl, err, err_len) != 0) {
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }

    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.initiator_depth = 1;
    conn_param.responder_resources = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    if (rdma_connect(impl->id, &conn_param) != 0 ||
        td_rdma_wait_event(impl->ec, RDMA_CM_EVENT_ESTABLISHED, &event, err, err_len) != 0) {
        td_rdma_destroy_impl(impl);
        free(impl);
        return -1;
    }
    rdma_ack_cm_event(event);

    memset(&hello, 0, sizeof(hello));
    hello.magic = TD_WIRE_MAGIC;
    hello.op = TD_WIRE_HELLO;

    if (td_rdma_send_control(impl, &hello, NULL, NULL, NULL, err, err_len) != 0 ||
        td_rdma_wait_response(impl, &response, NULL, NULL, NULL, err, err_len) != 0 ||
        response.status != 0) {
        td_rdma_destroy_impl(impl);
        free(impl);
        td_format_error(err, err_len, "rdma hello failed");
        return -1;
    }

    session->transport = TD_TRANSPORT_RDMA;
    session->endpoint = *endpoint;
    session->remote_addr = response.remote_addr;
    session->rkey = response.rkey;
    session->header = response.header;
    session->region_size = (size_t)response.header.region_size;
    session->impl = impl;
    session->read_region = td_rdma_client_read;
    session->write_region = td_rdma_client_write;
    session->cas64 = td_rdma_client_cas;
    session->control = td_rdma_client_control;
    session->close = td_rdma_client_close;
    return 0;
}

static void *td_rdma_server_conn_main(void *arg) {
    td_rdma_server_conn_t *conn = (td_rdma_server_conn_t *)arg;
    char err[256];

    while (!(*conn->stop_flag)) {
        struct ibv_wc wc;
        int n = ibv_poll_cq(conn->impl.cq, 1, &wc);
        if (n < 0) {
            break;
        }
        if (n == 0) {
            sched_yield();
            continue;
        }
        if (wc.status != IBV_WC_SUCCESS) {
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
            int profile_enabled;
            uint64_t op_start;

            if (td_rdma_post_recv(&conn->impl, err, sizeof(err)) != 0) {
                break;
            }

            memset(&response, 0, sizeof(response));
            response.magic = TD_WIRE_MAGIC;
            response.op = TD_WIRE_ACK;
            response.flags = request.flags;
            profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
            op_start = profile_enabled ? td_now_ns() : 0;

            if (request.op == TD_WIRE_HELLO) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                response.header = *td_region_header_view(conn->region);
                response.remote_addr = (uint64_t)(uintptr_t)td_region_shared_base(conn->region);
                response.rkey = conn->region_mr->rkey;
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_CAS) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                if (td_region_cas64(conn->region, (size_t)request.offset, request.compare, request.swap, &response.compare) != 0) {
                    response.status = 1;
                }
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_EVICT) {
                uint64_t stage_start = profile_enabled ? td_now_ns() : 0;
                td_region_evict_if_needed(conn->region, conn->eviction_threshold_pct);
                if (profile_enabled && stage_start != 0) {
                    response.profile_stage1_ns += td_now_ns() - stage_start;
                }
            } else if (request.op == TD_WIRE_CLOSE) {
                if (profile_enabled && op_start != 0) {
                    response.profile_total_ns = td_now_ns() - op_start;
                }
                if (td_rdma_send_control(&conn->impl, &response, NULL, NULL, NULL, err, sizeof(err)) != 0) {
                    break;
                }
                break;
            } else {
                response.status = 1;
            }

            if (profile_enabled && op_start != 0) {
                response.profile_total_ns = td_now_ns() - op_start;
            }
            if (td_rdma_send_control(&conn->impl, &response, NULL, NULL, NULL, err, sizeof(err)) != 0) {
                break;
            }
        }
    }

    if (conn->region_mr != NULL) {
        ibv_dereg_mr(conn->region_mr);
    }
    if (conn->impl.id != NULL) {
        rdma_disconnect(conn->impl.id);
    }
    td_rdma_destroy_impl(&conn->impl);
    free(conn);
    return NULL;
}

int td_rdma_server_run(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len) {
    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *listen_id = NULL;
    struct rdma_addrinfo hints;
    struct rdma_addrinfo *res = NULL;
    char port[16];

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port, sizeof(port), "%d", cfg->listen_port);

    if (rdma_getaddrinfo((char *)cfg->listen_host, port, &hints, &res) != 0) {
        td_format_error(err, err_len, "rdma getaddrinfo failed on %s:%d", cfg->listen_host, cfg->listen_port);
        return -1;
    }

    ec = rdma_create_event_channel();
    if (ec == NULL || rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP) != 0 ||
        rdma_bind_addr(listen_id, res->ai_src_addr) != 0 || rdma_listen(listen_id, 16) != 0) {
        rdma_freeaddrinfo(res);
        if (listen_id != NULL) {
            rdma_destroy_id(listen_id);
        }
        if (ec != NULL) {
            rdma_destroy_event_channel(ec);
        }
        td_format_error(err, err_len, "rdma listen setup failed");
        return -1;
    }
    rdma_freeaddrinfo(res);

    while (!(*stop_flag)) {
        struct pollfd pfd;
        int ready;

        pfd.fd = ec->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        ready = poll(&pfd, 1, 500);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            td_format_error(err, err_len, "rdma event poll failed");
            rdma_destroy_id(listen_id);
            rdma_destroy_event_channel(ec);
            return -1;
        }
        if (ready == 0) {
            continue;
        }
        if ((pfd.revents & POLLIN) != 0) {
            struct rdma_cm_event *event = NULL;
            if (rdma_get_cm_event(ec, &event) != 0) {
                continue;
            }
            if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
                td_rdma_server_conn_t *conn = (td_rdma_server_conn_t *)calloc(1, sizeof(*conn));
                pthread_t thread;
                if (conn != NULL) {
                    int accepted = 0;
                    err[0] = '\0';
                    conn->impl.id = event->id;
                    conn->impl.tdx = *td_region_tdx_runtime(region);
                    conn->region = region;
                    conn->eviction_threshold_pct = cfg->eviction_threshold_pct;
                    conn->stop_flag = stop_flag;
                    if (td_rdma_setup_qp(&conn->impl, sizeof(td_slot_t), err, err_len) == 0) {
                        conn->region_mr = ibv_reg_mr(conn->impl.pd, td_region_shared_base(region), td_region_shared_bytes(region),
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
                        if (conn->region_mr == NULL) {
                            td_format_error(err, err_len, "ibv_reg_mr failed: %s", strerror(errno));
                        }
                    }
                    if (conn->region_mr != NULL && td_rdma_post_recv(&conn->impl, err, err_len) == 0) {
                        struct rdma_conn_param conn_param;
                        memset(&conn_param, 0, sizeof(conn_param));
                        conn_param.initiator_depth = 1;
                        conn_param.responder_resources = 1;
                        conn_param.retry_count = 7;
                        conn_param.rnr_retry_count = 7;
                        if (rdma_accept(event->id, &conn_param) == 0) {
                            if (pthread_create(&thread, NULL, td_rdma_server_conn_main, conn) == 0) {
                                pthread_detach(thread);
                                accepted = 1;
                                conn = NULL;
                            } else {
                                td_format_error(err, err_len, "pthread_create failed");
                            }
                        } else {
                            td_format_error(err, err_len, "rdma_accept failed: %s", strerror(errno));
                        }
                    }
                    if (conn != NULL) {
                        if (err[0] == '\0') {
                            td_format_error(err, err_len, "rdma accept path failed before accept");
                        }
                        fprintf(stderr, "dist-td rdma reject on %s:%d: %s\n", cfg->listen_host, cfg->listen_port, err);
                        if (!accepted) {
                            (void)rdma_reject(event->id, NULL, 0);
                        }
                        if (conn->region_mr != NULL) {
                            ibv_dereg_mr(conn->region_mr);
                        }
                        td_rdma_destroy_impl(&conn->impl);
                        free(conn);
                    }
                } else {
                    fprintf(stderr, "dist-td rdma reject on %s:%d: out of memory\n", cfg->listen_host, cfg->listen_port);
                    (void)rdma_reject(event->id, NULL, 0);
                }
            }
            rdma_ack_cm_event(event);
        }
    }

    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);
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
