#include "td_cluster.h"

#include <ctype.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    int success;
    uint64_t observed_epoch;
    uint64_t observed_tie;
} td_vote_t;

typedef struct {
    td_slot_t slot;
    size_t slot_index;
    int found;
    int tombstone;
    td_slot_t candidate_slot;
    size_t candidate_slot_index;
    int candidate_valid;
} td_slot_probe_t;

static int td_slot_is_empty(const td_slot_t *slot) {
    return slot->guard_epoch == 0 &&
           slot->visible_epoch == 0 &&
           slot->key_hash == 0 &&
           slot->flags == 0;
}

static int td_slot_present(const td_slot_t *slot, uint64_t key_hash) {
    return slot->guard_epoch == slot->visible_epoch &&
           slot->key_hash == key_hash &&
           (slot->flags & TD_SLOT_FLAG_VALID) != 0 &&
           (slot->flags & TD_SLOT_FLAG_TOMBSTONE) == 0;
}

static int td_fetch_slot_at(td_session_t *session, td_region_kind_t kind, size_t slot_index, td_slot_t *slot, char *err, size_t err_len) {
    size_t offset = td_region_slot_offset_for_index(&session->header, kind, slot_index);
    return session->read_region(session, offset, slot, sizeof(*slot), err, err_len);
}

static int td_commit_slot_at(td_session_t *session, td_region_kind_t kind, size_t slot_index, const td_slot_t *slot, uint64_t compare_epoch, uint64_t *observed_epoch, char *err, size_t err_len) {
    size_t slot_offset = td_region_slot_offset_for_index(&session->header, kind, slot_index);
    size_t body_offset = slot_offset + offsetof(td_slot_t, visible_epoch);
    size_t body_len = sizeof(td_slot_t) - offsetof(td_slot_t, visible_epoch);

    if (session->write_region(session, body_offset, &slot->visible_epoch, body_len, err, err_len) != 0) {
        return -1;
    }
    return session->cas64(session, slot_offset, compare_epoch, slot->visible_epoch, observed_epoch, err, err_len);
}

static int td_probe_slot(td_session_t *session, td_region_kind_t kind, uint64_t key_hash, td_slot_probe_t *probe, char *err, size_t err_len) {
    size_t count = td_region_kind_slot_count(&session->header, kind);
    size_t home = td_region_slot_index(&session->header, kind, key_hash);
    size_t idx;

    memset(probe, 0, sizeof(*probe));
    if (count == 0) {
        td_format_error(err, err_len, "region kind %d has no slots", kind);
        return -1;
    }

    for (idx = 0; idx < count; ++idx) {
        size_t slot_index = (home + idx) % count;
        td_slot_t slot;

        if (td_fetch_slot_at(session, kind, slot_index, &slot, err, err_len) != 0) {
            return -1;
        }

        if (slot.guard_epoch != slot.visible_epoch) {
            continue;
        }

        if ((slot.flags & TD_SLOT_FLAG_VALID) != 0 && slot.key_hash == key_hash) {
            probe->slot = slot;
            probe->slot_index = slot_index;
            probe->found = 1;
            probe->tombstone = (slot.flags & TD_SLOT_FLAG_TOMBSTONE) != 0;
            probe->candidate_slot = slot;
            probe->candidate_slot_index = slot_index;
            probe->candidate_valid = 1;
            return 0;
        }

        if ((slot.flags & TD_SLOT_FLAG_VALID) != 0 && (slot.flags & TD_SLOT_FLAG_TOMBSTONE) != 0) {
            if (!probe->candidate_valid) {
                probe->candidate_slot = slot;
                probe->candidate_slot_index = slot_index;
                probe->candidate_valid = 1;
            }
            continue;
        }

        if (td_slot_is_empty(&slot)) {
            if (!probe->candidate_valid) {
                probe->candidate_slot = slot;
                probe->candidate_slot_index = slot_index;
                probe->candidate_valid = 1;
            }
            return 0;
        }
    }

    if (!probe->candidate_valid) {
        td_format_error(err, err_len, "region kind %d is full for key hash %llu", kind, (unsigned long long)key_hash);
        return -1;
    }
    return 0;
}

static td_session_t *td_primary_session(td_cluster_t *cluster, uint64_t key_hash) {
    return &cluster->sessions[key_hash % cluster->session_count];
}

static td_session_t *td_replica_session(td_cluster_t *cluster, uint64_t key_hash, size_t ordinal) {
    size_t primary = key_hash % cluster->session_count;
    return &cluster->sessions[(primary + ordinal) % cluster->session_count];
}

static int td_wait_for_primary_change(td_cluster_t *cluster, uint64_t key_hash, uint64_t old_epoch) {
    td_session_t *primary = td_primary_session(cluster, key_hash);
    int attempts;
    char err[256];

    for (attempts = 0; attempts < 50; ++attempts) {
        td_slot_probe_t probe;
        if (td_probe_slot(primary, TD_REGION_PRIME, key_hash, &probe, err, sizeof(err)) == 0 &&
            (!probe.found || probe.slot.guard_epoch != old_epoch)) {
            return 0;
        }
        usleep(10000);
    }
    return -1;
}

static void td_refresh_cache_best_effort(td_cluster_t *cluster, const char *key, const td_slot_t *slot) {
    td_session_t *primary = td_primary_session(cluster, td_hash64_string(key));
    td_slot_probe_t probe;
    uint64_t observed = 0;
    char err[256];
    uint64_t key_hash = td_hash64_string(key);

    if (cluster->config.cache == TD_CACHE_OFF) {
        return;
    }
    if (td_probe_slot(primary, TD_REGION_CACHE, key_hash, &probe, err, sizeof(err)) != 0 || !probe.candidate_valid) {
        return;
    }
    (void)td_commit_slot_at(primary, TD_REGION_CACHE, probe.candidate_slot_index, slot, probe.candidate_slot.guard_epoch, &observed, err, sizeof(err));
}

static int td_cluster_read_value(td_cluster_t *cluster, const char *key, unsigned char *value, size_t *value_len, int *found, char *err, size_t err_len) {
    td_session_t *primary;
    uint64_t key_hash = td_hash64_string(key);
    td_slot_probe_t prime_probe;
    td_slot_probe_t cache_probe;

    *found = 0;
    primary = td_primary_session(cluster, key_hash);

    if (td_probe_slot(primary, TD_REGION_PRIME, key_hash, &prime_probe, err, err_len) != 0) {
        return -1;
    }

    if (cluster->config.cache == TD_CACHE_ON) {
        if (td_probe_slot(primary, TD_REGION_CACHE, key_hash, &cache_probe, err, err_len) == 0 &&
            cache_probe.found &&
            prime_probe.found &&
            cache_probe.slot.guard_epoch == prime_probe.slot.guard_epoch &&
            cache_probe.slot.visible_epoch == prime_probe.slot.visible_epoch &&
            td_slot_present(&cache_probe.slot, key_hash) &&
            td_crypto_decode_slot(&cluster->crypto, key, &cache_probe.slot, value, value_len) == 0) {
            *found = 1;
            return 0;
        }
    }

    if (!prime_probe.found || !td_slot_present(&prime_probe.slot, key_hash)) {
        return 0;
    }
    if (td_crypto_decode_slot(&cluster->crypto, key, &prime_probe.slot, value, value_len) != 0) {
        td_format_error(err, err_len, "mac verification failed for key %s", key);
        return -1;
    }
    td_refresh_cache_best_effort(cluster, key, &prime_probe.slot);
    *found = 1;
    return 0;
}

static int td_evaluate_votes(td_vote_t *votes, size_t backup_count, uint64_t my_tie) {
    size_t success = 0;
    size_t idx;
    uint64_t min_other = UINT64_MAX;

    for (idx = 0; idx < backup_count; ++idx) {
        if (votes[idx].success) {
            ++success;
        } else if (votes[idx].observed_tie != 0 && votes[idx].observed_tie < min_other) {
            min_other = votes[idx].observed_tie;
        }
    }

    if (backup_count == 0 || success == backup_count) {
        return 1;
    }
    if ((success * 2) > backup_count) {
        return 2;
    }
    if (my_tie < min_other) {
        return 3;
    }
    return 0;
}

static int td_cluster_write_value(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int update_only, int tombstone, char *err, size_t err_len) {
    uint64_t key_hash = td_hash64_string(key);
    td_session_t *primary = td_primary_session(cluster, key_hash);
    td_slot_probe_t primary_probe;
    td_slot_t proposal;
    td_vote_t votes[TD_MAX_ENDPOINTS];
    size_t replica_count = (size_t)cluster->config.replication;
    size_t backup_count;
    size_t idx;
    uint64_t current_epoch;
    int rule;
    uint64_t observed = 0;

    if (replica_count > cluster->session_count) {
        replica_count = cluster->session_count;
    }
    backup_count = replica_count > 0 ? replica_count - 1 : 0;
    memset(votes, 0, sizeof(votes));

    if (td_probe_slot(primary, TD_REGION_PRIME, key_hash, &primary_probe, err, err_len) != 0) {
        return -1;
    }
    current_epoch = primary_probe.candidate_slot.guard_epoch;
    if (update_only && (!primary_probe.found || primary_probe.tombstone || !td_slot_present(&primary_probe.slot, key_hash))) {
        td_format_error(err, err_len, "update failed: key %s not found", key);
        return -1;
    }

    if (td_crypto_make_slot(
            &cluster->crypto,
            key,
            value,
            value_len,
            tombstone ? (TD_SLOT_FLAG_VALID | TD_SLOT_FLAG_TOMBSTONE) : TD_SLOT_FLAG_VALID,
            current_epoch + 1,
            &proposal) != 0) {
        td_format_error(err, err_len, "cannot prepare encrypted slot");
        return -1;
    }

    for (idx = 0; idx < backup_count; ++idx) {
        td_session_t *backup = td_replica_session(cluster, key_hash, idx + 1);
        td_slot_probe_t probe;
        uint64_t prior_epoch = 0;
        if (td_probe_slot(backup, TD_REGION_BACKUP, key_hash, &probe, err, err_len) != 0 || !probe.candidate_valid) {
            return -1;
        }
        prior_epoch = probe.candidate_slot.guard_epoch;
        if (td_commit_slot_at(backup, TD_REGION_BACKUP, probe.candidate_slot_index, &proposal, prior_epoch, &observed, err, err_len) == 0 &&
            observed == prior_epoch) {
            votes[idx].success = 1;
        } else {
            votes[idx].observed_epoch = observed;
            votes[idx].observed_tie = probe.candidate_slot.tie_breaker;
        }
    }

    rule = td_evaluate_votes(votes, backup_count, proposal.tie_breaker);
    if (rule == 0) {
        (void)td_wait_for_primary_change(cluster, key_hash, current_epoch);
        td_format_error(err, err_len, "snapshot consensus lost for key %s", key);
        return -1;
    }

    if (td_commit_slot_at(primary, TD_REGION_PRIME, primary_probe.candidate_slot_index, &proposal, current_epoch, &observed, err, err_len) != 0 ||
        observed != current_epoch) {
        td_format_error(err, err_len, "primary CAS failed for key %s", key);
        return -1;
    }

    for (idx = 0; idx < backup_count; ++idx) {
        if (!votes[idx].success) {
            td_session_t *backup = td_replica_session(cluster, key_hash, idx + 1);
            td_slot_probe_t probe;
            if (td_probe_slot(backup, TD_REGION_BACKUP, key_hash, &probe, err, err_len) == 0 && probe.candidate_valid) {
                (void)td_commit_slot_at(backup, TD_REGION_BACKUP, probe.candidate_slot_index, &proposal, probe.candidate_slot.guard_epoch, &observed, err, err_len);
            }
        }
    }

    td_refresh_cache_best_effort(cluster, key, &proposal);
    return rule;
}

int td_cluster_init(td_cluster_t *cluster, const td_config_t *cfg, char *err, size_t err_len) {
    size_t idx;

    memset(cluster, 0, sizeof(*cluster));
    cluster->config = *cfg;
    if (td_crypto_init(&cluster->crypto, cfg->encryption_key_hex, err, err_len) != 0) {
        return -1;
    }
    cluster->session_count = cfg->mn_count;
    for (idx = 0; idx < cluster->session_count; ++idx) {
        if (td_session_connect(&cluster->sessions[idx], cfg, &cfg->mn_endpoints[idx], err, err_len) != 0) {
            td_cluster_close(cluster);
            return -1;
        }
    }
    return 0;
}

int td_cluster_read_kv(td_cluster_t *cluster, const char *key, unsigned char *value, size_t *value_len, int *found, char *err, size_t err_len) {
    return td_cluster_read_value(cluster, key, value, value_len, found, err, err_len);
}

int td_cluster_write_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, value, value_len, 0, 0, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_update_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, value, value_len, 1, 0, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_delete_kv(td_cluster_t *cluster, const char *key, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, (const unsigned char *)"", 0, 0, 1, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

void td_cluster_close(td_cluster_t *cluster) {
    size_t idx;
    for (idx = 0; idx < cluster->session_count; ++idx) {
        td_session_close(&cluster->sessions[idx]);
    }
    memset(cluster, 0, sizeof(*cluster));
}

void td_cluster_print_status(td_cluster_t *cluster, FILE *out) {
    size_t idx;

    fprintf(out, "transport=%s replication=%d cache=%s sessions=%zu\n",
        cluster->config.transport == TD_TRANSPORT_RDMA ? "rdma" : "tcp",
        cluster->config.replication,
        cluster->config.cache == TD_CACHE_ON ? "on" : "off",
        cluster->session_count);
    for (idx = 0; idx < cluster->session_count; ++idx) {
        td_session_t *session = &cluster->sessions[idx];
        fprintf(out, "mn[%zu] %s:%d node_id=%d prime=%llu cache=%llu backup=%llu bytes=%llu\n",
            idx,
            session->endpoint.host,
            session->endpoint.port,
            session->endpoint.node_id,
            (unsigned long long)session->header.prime_slot_count,
            (unsigned long long)session->header.cache_slot_count,
            (unsigned long long)session->header.backup_slot_count,
            (unsigned long long)session->header.region_size);
    }
}

static int td_split_command(char *line, char **cmd, char **arg1, char **arg2) {
    char *save = NULL;
    *cmd = strtok_r(line, " \t\r\n", &save);
    *arg1 = *cmd == NULL ? NULL : strtok_r(NULL, " \t\r\n", &save);
    *arg2 = *arg1 == NULL ? NULL : strtok_r(NULL, "\r\n", &save);
    if (*arg2 != NULL) {
        *arg2 = td_trim(*arg2);
    }
    return *cmd != NULL ? 0 : -1;
}

int td_cluster_execute(td_cluster_t *cluster, const char *line, FILE *out) {
    char scratch[TD_CMD_BYTES];
    char *cmd = NULL;
    char *arg1 = NULL;
    char *arg2 = NULL;
    char err[256];

    snprintf(scratch, sizeof(scratch), "%s", line);
    if (td_split_command(scratch, &cmd, &arg1, &arg2) != 0) {
        return 1;
    }

    if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        return 0;
    }
    if (strcmp(cmd, "help") == 0) {
        fprintf(out, "commands: read <key>, write <key> <value>, update <key> <value>, delete <key>, status, evict, quit\n");
        return 1;
    }
    if (strcmp(cmd, "status") == 0) {
        td_cluster_print_status(cluster, out);
        return 1;
    }
    if (strcmp(cmd, "evict") == 0) {
        size_t idx;
        for (idx = 0; idx < cluster->session_count; ++idx) {
            if (cluster->sessions[idx].control(&cluster->sessions[idx], TD_WIRE_EVICT, err, sizeof(err)) != 0) {
                fprintf(out, "error: %s\n", err);
                return 1;
            }
        }
        fprintf(out, "eviction requested\n");
        return 1;
    }
    if (arg1 == NULL) {
        fprintf(out, "error: missing key\n");
        return 1;
    }

    if (strcmp(cmd, "read") == 0) {
        unsigned char value[TD_MAX_VALUE_SIZE + 1];
        size_t value_len = 0;
        int found = 0;
        if (td_cluster_read_kv(cluster, arg1, value, &value_len, &found, err, sizeof(err)) != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        if (!found) {
            fprintf(out, "not_found %s\n", arg1);
            return 1;
        }
        value[value_len] = '\0';
        fprintf(out, "value %s %s\n", arg1, value);
        return 1;
    }

    if ((strcmp(cmd, "write") == 0 || strcmp(cmd, "update") == 0) && arg2 == NULL) {
        fprintf(out, "error: missing value\n");
        return 1;
    }

    if (strcmp(cmd, "write") == 0 || strcmp(cmd, "update") == 0) {
        uint64_t start = td_now_ns();
        int rule = 0;
        int rc = strcmp(cmd, "update") == 0
            ? td_cluster_update_kv(cluster, arg1, (const unsigned char *)arg2, strlen(arg2), &rule, err, sizeof(err))
            : td_cluster_write_kv(cluster, arg1, (const unsigned char *)arg2, strlen(arg2), &rule, err, sizeof(err));
        if (rc != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        fprintf(out, "ok %s rule=%d latency_us=%llu\n", arg1, rule, (unsigned long long)((td_now_ns() - start) / 1000ULL));
        return 1;
    }

    if (strcmp(cmd, "delete") == 0) {
        uint64_t start = td_now_ns();
        int rule = 0;
        if (td_cluster_delete_kv(cluster, arg1, &rule, err, sizeof(err)) != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        fprintf(out, "deleted %s rule=%d latency_us=%llu\n", arg1, rule, (unsigned long long)((td_now_ns() - start) / 1000ULL));
        return 1;
    }

    fprintf(out, "error: unknown command %s\n", cmd);
    return 1;
}
