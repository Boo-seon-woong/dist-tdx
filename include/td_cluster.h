#ifndef TD_CLUSTER_H
#define TD_CLUSTER_H

#include "td_transport.h"

typedef struct {
    td_config_t config;
    td_crypto_ctx_t crypto;
    td_session_t sessions[TD_MAX_ENDPOINTS];
    size_t session_count;
} td_cluster_t;

int td_cluster_init(td_cluster_t *cluster, const td_config_t *cfg, char *err, size_t err_len);
void td_cluster_close(td_cluster_t *cluster);
int td_cluster_execute(td_cluster_t *cluster, const char *line, FILE *out);
void td_cluster_print_status(td_cluster_t *cluster, FILE *out);
int td_cluster_read_kv(td_cluster_t *cluster, const char *key, unsigned char *value, size_t *value_len, int *found, char *err, size_t err_len);
int td_cluster_write_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len);
int td_cluster_update_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len);
int td_cluster_delete_kv(td_cluster_t *cluster, const char *key, int *rule_out, char *err, size_t err_len);

#endif
