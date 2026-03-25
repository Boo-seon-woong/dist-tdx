#include "td_transport.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    int fd;
} td_tcp_impl_t;

typedef struct {
    int fd;
    td_local_region_t *region;
    size_t eviction_threshold_pct;
    volatile sig_atomic_t *stop_flag;
} td_tcp_conn_ctx_t;

typedef struct {
    uint64_t request_header_send_ns;
    uint64_t request_payload_send_ns;
    uint64_t response_header_wait_ns;
    uint64_t response_payload_wait_ns;
} td_tcp_exchange_profile_t;

static uint64_t td_tcp_measure_begin(int enabled) {
    return enabled ? td_now_ns() : 0;
}

static void td_tcp_measure_end(uint64_t start_ns, uint64_t *field) {
    if (field != NULL && start_ns != 0) {
        *field += td_now_ns() - start_ns;
    }
}

static void td_tcp_tune_socket(int fd) {
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

static int td_send_all_timed(int fd, const void *buf, size_t len, int enabled, uint64_t *latency_ns) {
    uint64_t start_ns = td_tcp_measure_begin(enabled);
    int rc = td_send_all(fd, buf, len);

    td_tcp_measure_end(start_ns, latency_ns);
    return rc;
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

static int td_recv_all_timed(int fd, void *buf, size_t len, int enabled, uint64_t *latency_ns) {
    uint64_t start_ns = td_tcp_measure_begin(enabled);
    int rc = td_recv_all(fd, buf, len);

    td_tcp_measure_end(start_ns, latency_ns);
    return rc;
}

static void td_tcp_accumulate_server_profile(td_transport_profile_t *profile, td_wire_op_t op, const td_wire_msg_t *response, const td_tcp_exchange_profile_t *exchange) {
    uint64_t wait_total = 0;
    uint64_t wire_estimate = 0;

    if (profile == NULL || exchange == NULL) {
        return;
    }

    wait_total = exchange->response_header_wait_ns + exchange->response_payload_wait_ns;
    if (wait_total > response->profile_total_ns) {
        wire_estimate = wait_total - response->profile_total_ns;
    }

    switch (op) {
        case TD_WIRE_READ:
            profile->tcp_server_read_total_ns += response->profile_total_ns;
            profile->tcp_server_read_alloc_ns += response->profile_stage1_ns;
            profile->tcp_server_read_region_ns += response->profile_stage2_ns;
            profile->tcp_read_wire_estimate_ns += wire_estimate;
            break;
        case TD_WIRE_WRITE:
            profile->tcp_server_write_total_ns += response->profile_total_ns;
            profile->tcp_server_write_recv_ns += response->profile_stage1_ns;
            profile->tcp_server_write_region_ns += response->profile_stage2_ns;
            profile->tcp_write_wire_estimate_ns += wire_estimate;
            break;
        case TD_WIRE_CAS:
            profile->tcp_server_cas_total_ns += response->profile_total_ns;
            profile->tcp_server_cas_region_ns += response->profile_stage1_ns;
            profile->tcp_cas_wire_estimate_ns += wire_estimate;
            break;
        case TD_WIRE_HELLO:
        case TD_WIRE_EVICT:
        case TD_WIRE_CLOSE:
            profile->tcp_server_control_total_ns += response->profile_total_ns;
            profile->tcp_server_control_exec_ns += response->profile_stage1_ns;
            profile->tcp_control_wire_estimate_ns += wire_estimate;
            break;
        default:
            break;
    }
}

static int td_tcp_exchange(td_session_t *session, int fd, const td_wire_msg_t *request, const void *payload, td_wire_msg_t *response, void *response_payload, td_tcp_exchange_profile_t *profile) {
    int profiling_enabled = session != NULL && session->transport_profile != NULL;

    if (td_send_all_timed(fd, request, sizeof(*request), profiling_enabled, profile != NULL ? &profile->request_header_send_ns : NULL) != 0) {
        return -1;
    }
    if (payload != NULL &&
        request->length > 0 &&
        td_send_all_timed(fd, payload, (size_t)request->length, profiling_enabled, profile != NULL ? &profile->request_payload_send_ns : NULL) != 0) {
        return -1;
    }

    if (td_recv_all_timed(fd, response, sizeof(*response), profiling_enabled, profile != NULL ? &profile->response_header_wait_ns : NULL) != 0) {
        return -1;
    }
    if (response_payload != NULL &&
        response->length > 0 &&
        td_recv_all_timed(fd, response_payload, (size_t)response->length, profiling_enabled, profile != NULL ? &profile->response_payload_wait_ns : NULL) != 0) {
        return -1;
    }
    return 0;
}

static int td_tcp_client_read(td_session_t *session, size_t offset, void *buf, size_t len, char *err, size_t err_len) {
    td_tcp_impl_t *impl = (td_tcp_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    td_tcp_exchange_profile_t exchange;

    memset(&exchange, 0, sizeof(exchange));
    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_READ;
    request.offset = offset;
    request.length = len;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_tcp_exchange(session, impl->fd, &request, NULL, &response, buf, &exchange) != 0 || response.status != 0) {
        td_format_error(err, err_len, "tcp read failed for node %d", session->endpoint.node_id);
        return -1;
    }
    if (session->transport_profile != NULL) {
        session->transport_profile->read_send_ns += exchange.request_header_send_ns + exchange.request_payload_send_ns;
        session->transport_profile->read_wait_ns += exchange.response_header_wait_ns + exchange.response_payload_wait_ns;
        session->transport_profile->tcp_read_request_header_send_ns += exchange.request_header_send_ns;
        session->transport_profile->tcp_read_response_header_wait_ns += exchange.response_header_wait_ns;
        session->transport_profile->tcp_read_response_payload_wait_ns += exchange.response_payload_wait_ns;
        td_tcp_accumulate_server_profile(session->transport_profile, TD_WIRE_READ, &response, &exchange);
    }
    return 0;
}

static int td_tcp_client_write(td_session_t *session, size_t offset, const void *buf, size_t len, char *err, size_t err_len) {
    td_tcp_impl_t *impl = (td_tcp_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    td_tcp_exchange_profile_t exchange;

    memset(&exchange, 0, sizeof(exchange));
    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_WRITE;
    request.offset = offset;
    request.length = len;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_tcp_exchange(session, impl->fd, &request, buf, &response, NULL, &exchange) != 0 || response.status != 0) {
        td_format_error(err, err_len, "tcp write failed for node %d", session->endpoint.node_id);
        return -1;
    }
    if (session->transport_profile != NULL) {
        session->transport_profile->write_send_ns += exchange.request_header_send_ns + exchange.request_payload_send_ns;
        session->transport_profile->write_wait_ns += exchange.response_header_wait_ns + exchange.response_payload_wait_ns;
        session->transport_profile->tcp_write_request_header_send_ns += exchange.request_header_send_ns;
        session->transport_profile->tcp_write_request_payload_send_ns += exchange.request_payload_send_ns;
        session->transport_profile->tcp_write_response_header_wait_ns += exchange.response_header_wait_ns;
        td_tcp_accumulate_server_profile(session->transport_profile, TD_WIRE_WRITE, &response, &exchange);
    }
    return 0;
}

static int td_tcp_client_cas(td_session_t *session, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value, char *err, size_t err_len) {
    td_tcp_impl_t *impl = (td_tcp_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    td_tcp_exchange_profile_t exchange;

    memset(&exchange, 0, sizeof(exchange));
    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = TD_WIRE_CAS;
    request.offset = offset;
    request.compare = compare;
    request.swap = swap;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_tcp_exchange(session, impl->fd, &request, NULL, &response, NULL, &exchange) != 0 || response.status != 0) {
        td_format_error(err, err_len, "tcp cas failed for node %d", session->endpoint.node_id);
        return -1;
    }
    *old_value = response.compare;
    if (session->transport_profile != NULL) {
        session->transport_profile->cas_send_ns += exchange.request_header_send_ns + exchange.request_payload_send_ns;
        session->transport_profile->cas_wait_ns += exchange.response_header_wait_ns + exchange.response_payload_wait_ns;
        session->transport_profile->tcp_cas_request_header_send_ns += exchange.request_header_send_ns;
        session->transport_profile->tcp_cas_response_header_wait_ns += exchange.response_header_wait_ns;
        td_tcp_accumulate_server_profile(session->transport_profile, TD_WIRE_CAS, &response, &exchange);
    }
    return 0;
}

static int td_tcp_client_control(td_session_t *session, td_wire_op_t op, char *err, size_t err_len) {
    td_tcp_impl_t *impl = (td_tcp_impl_t *)session->impl;
    td_wire_msg_t request;
    td_wire_msg_t response;
    td_tcp_exchange_profile_t exchange;

    memset(&exchange, 0, sizeof(exchange));
    memset(&request, 0, sizeof(request));
    request.magic = TD_WIRE_MAGIC;
    request.op = (uint16_t)op;
    request.flags = session->transport_profile != NULL ? TD_WIRE_FLAG_PROFILE : 0;

    if (td_tcp_exchange(session, impl->fd, &request, NULL, &response, NULL, &exchange) != 0 || response.status != 0) {
        td_format_error(err, err_len, "tcp control op %u failed", (unsigned int)op);
        return -1;
    }
    if (session->transport_profile != NULL) {
        session->transport_profile->control_send_ns += exchange.request_header_send_ns + exchange.request_payload_send_ns;
        session->transport_profile->control_wait_ns += exchange.response_header_wait_ns + exchange.response_payload_wait_ns;
        session->transport_profile->tcp_control_request_header_send_ns += exchange.request_header_send_ns;
        session->transport_profile->tcp_control_response_header_wait_ns += exchange.response_header_wait_ns;
        td_tcp_accumulate_server_profile(session->transport_profile, op, &response, &exchange);
    }
    return 0;
}

static void td_tcp_client_close(td_session_t *session) {
    td_tcp_impl_t *impl = (td_tcp_impl_t *)session->impl;
    if (impl == NULL) {
        return;
    }
    (void)td_tcp_client_control(session, TD_WIRE_CLOSE, NULL, 0);
    close(impl->fd);
    free(impl);
    session->impl = NULL;
}

int td_tcp_client_connect(td_session_t *session, const td_endpoint_t *endpoint, char *err, size_t err_len) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *it;
    char port[16];
    int fd = -1;
    td_tcp_impl_t *impl = NULL;
    td_wire_msg_t hello;
    td_wire_msg_t response;

    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    snprintf(port, sizeof(port), "%d", endpoint->port);

    if (getaddrinfo(endpoint->host, port, &hints, &result) != 0) {
        td_format_error(err, err_len, "cannot resolve %s:%d", endpoint->host, endpoint->port);
        return -1;
    }

    for (it = result; it != NULL; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
            td_tcp_tune_socket(fd);
            break;
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(result);

    if (fd < 0) {
        td_format_error(err, err_len, "cannot connect to %s:%d", endpoint->host, endpoint->port);
        return -1;
    }

    impl = (td_tcp_impl_t *)calloc(1, sizeof(*impl));
    if (impl == NULL) {
        close(fd);
        td_format_error(err, err_len, "out of memory");
        return -1;
    }
    impl->fd = fd;

    memset(&hello, 0, sizeof(hello));
    hello.magic = TD_WIRE_MAGIC;
    hello.op = TD_WIRE_HELLO;

    if (td_tcp_exchange(NULL, fd, &hello, NULL, &response, NULL, NULL) != 0 || response.status != 0) {
        close(fd);
        free(impl);
        td_format_error(err, err_len, "tcp hello failed with %s:%d", endpoint->host, endpoint->port);
        return -1;
    }

    session->transport = TD_TRANSPORT_TCP;
    session->endpoint = *endpoint;
    session->remote_addr = response.remote_addr;
    session->rkey = response.rkey;
    session->header = response.header;
    session->region_size = (size_t)response.header.region_size;
    session->impl = impl;
    session->read_region = td_tcp_client_read;
    session->write_region = td_tcp_client_write;
    session->cas64 = td_tcp_client_cas;
    session->control = td_tcp_client_control;
    session->close = td_tcp_client_close;
    return 0;
}

static void *td_tcp_connection_main(void *arg) {
    td_tcp_conn_ctx_t *ctx = (td_tcp_conn_ctx_t *)arg;

    while (!(*ctx->stop_flag)) {
        td_wire_msg_t request;
        td_wire_msg_t response;
        unsigned char *buffer = NULL;

        if (td_recv_all(ctx->fd, &request, sizeof(request)) != 0) {
            break;
        }
        if (request.magic != TD_WIRE_MAGIC) {
            break;
        }

        memset(&response, 0, sizeof(response));
        response.magic = TD_WIRE_MAGIC;
        response.op = TD_WIRE_ACK;
        response.flags = request.flags;

        switch (request.op) {
            case TD_WIRE_HELLO:
                if ((request.flags & TD_WIRE_FLAG_PROFILE) != 0) {
                    uint64_t op_start = td_now_ns();
                    response.header = *td_region_header_view(ctx->region);
                    response.profile_stage1_ns = td_now_ns() - op_start;
                    response.profile_total_ns = response.profile_stage1_ns;
                } else {
                    response.header = *td_region_header_view(ctx->region);
                }
                break;
            case TD_WIRE_READ:
                {
                    int profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
                    uint64_t op_start = td_tcp_measure_begin(profile_enabled);
                    uint64_t stage_start;

                    stage_start = td_tcp_measure_begin(profile_enabled);
                    buffer = (unsigned char *)malloc((size_t)request.length);
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage1_ns : NULL);
                    stage_start = td_tcp_measure_begin(profile_enabled);
                    if (buffer == NULL || td_region_read_bytes(ctx->region, (size_t)request.offset, buffer, (size_t)request.length) != 0) {
                        response.status = 1;
                        response.length = 0;
                    } else {
                        response.length = request.length;
                    }
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage2_ns : NULL);
                    if (profile_enabled && op_start != 0) {
                        response.profile_total_ns = td_now_ns() - op_start;
                    }
                }
                if (td_send_all(ctx->fd, &response, sizeof(response)) != 0) {
                    free(buffer);
                    goto done;
                }
                if (buffer != NULL && response.status == 0 && response.length > 0 && td_send_all(ctx->fd, buffer, (size_t)response.length) != 0) {
                    free(buffer);
                    goto done;
                }
                free(buffer);
                continue;
            case TD_WIRE_WRITE:
                {
                    int profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
                    uint64_t op_start = td_tcp_measure_begin(profile_enabled);
                    uint64_t stage_start = td_tcp_measure_begin(profile_enabled);

                    buffer = (unsigned char *)malloc((size_t)request.length);
                    if (buffer == NULL || td_recv_all(ctx->fd, buffer, (size_t)request.length) != 0) {
                        response.status = 1;
                    }
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage1_ns : NULL);
                    stage_start = td_tcp_measure_begin(profile_enabled);
                    if (response.status == 0 &&
                        td_region_write_bytes(ctx->region, (size_t)request.offset, buffer, (size_t)request.length) != 0) {
                        response.status = 1;
                    }
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage2_ns : NULL);
                    if (profile_enabled && op_start != 0) {
                        response.profile_total_ns = td_now_ns() - op_start;
                    }
                }
                free(buffer);
                break;
            case TD_WIRE_CAS:
                {
                    int profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
                    uint64_t op_start = td_tcp_measure_begin(profile_enabled);
                    uint64_t stage_start = td_tcp_measure_begin(profile_enabled);

                    if (td_region_cas64(ctx->region, (size_t)request.offset, request.compare, request.swap, &response.compare) != 0) {
                        response.status = 1;
                    }
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage1_ns : NULL);
                    if (profile_enabled && op_start != 0) {
                        response.profile_total_ns = td_now_ns() - op_start;
                    }
                }
                break;
            case TD_WIRE_EVICT:
                {
                    int profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
                    uint64_t op_start = td_tcp_measure_begin(profile_enabled);
                    uint64_t stage_start = td_tcp_measure_begin(profile_enabled);

                    td_region_evict_if_needed(ctx->region, ctx->eviction_threshold_pct);
                    td_tcp_measure_end(stage_start, profile_enabled ? &response.profile_stage1_ns : NULL);
                    if (profile_enabled && op_start != 0) {
                        response.profile_total_ns = td_now_ns() - op_start;
                    }
                }
                break;
            case TD_WIRE_CLOSE:
                {
                    int profile_enabled = (request.flags & TD_WIRE_FLAG_PROFILE) != 0;
                    uint64_t op_start = td_tcp_measure_begin(profile_enabled);

                    if (profile_enabled && op_start != 0) {
                        response.profile_total_ns = td_now_ns() - op_start;
                    }
                }
                if (td_send_all(ctx->fd, &response, sizeof(response)) != 0) {
                    goto done;
                }
                goto done;
            default:
                response.status = 1;
                break;
        }

        if (td_send_all(ctx->fd, &response, sizeof(response)) != 0) {
            break;
        }
    }

done:
    close(ctx->fd);
    free(ctx);
    return NULL;
}

int td_tcp_server_run(const td_config_t *cfg, td_local_region_t *region, volatile sig_atomic_t *stop_flag, char *err, size_t err_len) {
    struct sockaddr_in addr;
    int server_fd;
    int opt = 1;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        td_format_error(err, err_len, "tcp socket creation failed");
        return -1;
    }
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)cfg->listen_port);
    if (inet_pton(AF_INET, cfg->listen_host, &addr.sin_addr) != 1) {
        close(server_fd);
        td_format_error(err, err_len, "invalid listen_host %s", cfg->listen_host);
        return -1;
    }
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(server_fd, 16) != 0) {
        close(server_fd);
        td_format_error(err, err_len, "tcp bind/listen failed on %s:%d", cfg->listen_host, cfg->listen_port);
        return -1;
    }

    while (!(*stop_flag)) {
        fd_set readfds;
        struct timeval tv;
        int ready;

        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        ready = select(server_fd + 1, &readfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            close(server_fd);
            td_format_error(err, err_len, "tcp select failed");
            return -1;
        }
        if (ready == 0) {
            continue;
        }
        if (FD_ISSET(server_fd, &readfds)) {
            int client_fd = accept(server_fd, NULL, NULL);
            if (client_fd >= 0) {
                td_tcp_conn_ctx_t *ctx = (td_tcp_conn_ctx_t *)calloc(1, sizeof(*ctx));
                pthread_t thread;
                if (ctx == NULL) {
                    close(client_fd);
                    continue;
                }
                td_tcp_tune_socket(client_fd);
                ctx->fd = client_fd;
                ctx->region = region;
                ctx->eviction_threshold_pct = cfg->eviction_threshold_pct;
                ctx->stop_flag = stop_flag;
                if (pthread_create(&thread, NULL, td_tcp_connection_main, ctx) == 0) {
                    pthread_detach(thread);
                } else {
                    close(client_fd);
                    free(ctx);
                }
            }
        }
    }

    close(server_fd);
    return 0;
}
