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

typedef struct {
    uint64_t write_ns;
    uint64_t cas_ns;
} td_commit_timing_t;

typedef struct {
    uint64_t *total_ns;
    uint64_t *scan_ns;
    size_t *read_count;
    size_t *slots_examined;
    size_t *guard_mismatch_count;
    size_t *tombstone_count;
    size_t *empty_count;
} td_probe_profile_t;

static uint64_t td_profile_begin(td_latency_profile_t *profile) {
    return profile != NULL ? td_now_ns() : 0;
}

static void td_profile_end(td_latency_profile_t *profile, uint64_t start_ns, uint64_t *field) {
    if (profile != NULL && field != NULL && start_ns != 0) {
        *field += td_now_ns() - start_ns;
    }
}

static double td_ns_to_us(uint64_t ns) {
    return (double)ns / 1000.0;
}

static void td_cluster_attach_transport_profile(td_cluster_t *cluster, td_transport_profile_t *transport_profile) {
    size_t idx;

    for (idx = 0; idx < cluster->session_count; ++idx) {
        cluster->sessions[idx].transport_profile = transport_profile;
    }
}

static int td_slot_is_empty(const td_slot_t *slot) {
    return slot->header.guard_epoch == 0 &&
           slot->header.visible_epoch == 0 &&
           slot->header.key_hash == 0 &&
           slot->header.flags == 0;
}

static int td_slot_present(const td_slot_t *slot, uint64_t key_hash) {
    return slot->header.guard_epoch == slot->header.visible_epoch &&
           slot->header.key_hash == key_hash &&
           (slot->header.flags & TD_SLOT_FLAG_VALID) != 0 &&
           (slot->header.flags & TD_SLOT_FLAG_TOMBSTONE) == 0;
}

static int td_fetch_slot_at(td_session_t *session, td_region_kind_t kind, size_t slot_index, td_slot_t *slot, td_probe_profile_t *profile, char *err, size_t err_len) {
    size_t offset = td_region_slot_offset_for_index(&session->header, kind, slot_index);
    uint64_t start_ns = profile != NULL && profile->total_ns != NULL ? td_now_ns() : 0;
    int rc = session->read_region(session, offset, slot, sizeof(*slot), err, err_len);

    if (profile != NULL && profile->total_ns != NULL && start_ns != 0) {
        *profile->total_ns += td_now_ns() - start_ns;
    }
    if (profile != NULL && profile->read_count != NULL) {
        ++(*profile->read_count);
    }
    return rc;
}

static int td_commit_slot_at(td_session_t *session, td_region_kind_t kind, size_t slot_index, const td_slot_t *slot, uint64_t compare_epoch, uint64_t *observed_epoch, td_commit_timing_t *timing, char *err, size_t err_len) {
    size_t slot_offset = td_region_slot_offset_for_index(&session->header, kind, slot_index);
    size_t body_offset = slot_offset + td_slot_commit_offset();
    size_t body_len = td_slot_commit_length();
    uint64_t start_ns = timing != NULL ? td_now_ns() : 0;

    if (session->write_region(session, body_offset, td_slot_commit_const_ptr(slot), body_len, err, err_len) != 0) {
        return -1;
    }
    if (timing != NULL && start_ns != 0) {
        timing->write_ns += td_now_ns() - start_ns;
        start_ns = td_now_ns();
    }
    if (session->cas64(session, slot_offset, compare_epoch, slot->header.visible_epoch, observed_epoch, err, err_len) != 0) {
        return -1;
    }
    if (timing != NULL && start_ns != 0) {
        timing->cas_ns += td_now_ns() - start_ns;
    }
    return 0;
}

static int td_probe_slot(td_session_t *session, td_region_kind_t kind, uint64_t key_hash, td_slot_probe_t *probe, td_probe_profile_t *profile, char *err, size_t err_len) {
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
        uint64_t scan_start_ns;

        if (td_fetch_slot_at(session, kind, slot_index, &slot, profile, err, err_len) != 0) {
            return -1;
        }
        scan_start_ns = profile != NULL && profile->scan_ns != NULL ? td_now_ns() : 0;
        if (profile != NULL && profile->slots_examined != NULL) {
            ++(*profile->slots_examined);
        }

        if (slot.header.guard_epoch != slot.header.visible_epoch) {
            if (profile != NULL && profile->guard_mismatch_count != NULL) {
                ++(*profile->guard_mismatch_count);
            }
            if (profile != NULL && profile->scan_ns != NULL && scan_start_ns != 0) {
                *profile->scan_ns += td_now_ns() - scan_start_ns;
            }
            continue;
        }

        if ((slot.header.flags & TD_SLOT_FLAG_VALID) != 0 && slot.header.key_hash == key_hash) {
            probe->slot = slot;
            probe->slot_index = slot_index;
            probe->found = 1;
            probe->tombstone = (slot.header.flags & TD_SLOT_FLAG_TOMBSTONE) != 0;
            probe->candidate_slot = slot;
            probe->candidate_slot_index = slot_index;
            probe->candidate_valid = 1;
            if (profile != NULL && profile->scan_ns != NULL && scan_start_ns != 0) {
                *profile->scan_ns += td_now_ns() - scan_start_ns;
            }
            return 0;
        }

        if ((slot.header.flags & TD_SLOT_FLAG_VALID) != 0 && (slot.header.flags & TD_SLOT_FLAG_TOMBSTONE) != 0) {
            if (!probe->candidate_valid) {
                probe->candidate_slot = slot;
                probe->candidate_slot_index = slot_index;
                probe->candidate_valid = 1;
            }
            if (profile != NULL && profile->tombstone_count != NULL) {
                ++(*profile->tombstone_count);
            }
            if (profile != NULL && profile->scan_ns != NULL && scan_start_ns != 0) {
                *profile->scan_ns += td_now_ns() - scan_start_ns;
            }
            continue;
        }

        if (td_slot_is_empty(&slot)) {
            if (!probe->candidate_valid) {
                probe->candidate_slot = slot;
                probe->candidate_slot_index = slot_index;
                probe->candidate_valid = 1;
            }
            if (profile != NULL && profile->empty_count != NULL) {
                ++(*profile->empty_count);
            }
            if (profile != NULL && profile->scan_ns != NULL && scan_start_ns != 0) {
                *profile->scan_ns += td_now_ns() - scan_start_ns;
            }
            return 0;
        }

        if (profile != NULL && profile->scan_ns != NULL && scan_start_ns != 0) {
            *profile->scan_ns += td_now_ns() - scan_start_ns;
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

static int td_wait_for_primary_change(td_cluster_t *cluster, uint64_t key_hash, uint64_t old_epoch, td_latency_profile_t *profile) {
    td_session_t *primary = td_primary_session(cluster, key_hash);
    int attempts;
    char err[256];
    uint64_t start_ns = td_profile_begin(profile);

    for (attempts = 0; attempts < 50; ++attempts) {
        td_slot_probe_t probe;
        if (profile != NULL) {
            ++profile->wait_for_primary_change_attempts;
        }
        if (td_probe_slot(primary, TD_REGION_PRIME, key_hash, &probe, NULL, err, sizeof(err)) == 0 &&
            (!probe.found || probe.slot.header.guard_epoch != old_epoch)) {
            td_profile_end(profile, start_ns, profile != NULL ? &profile->wait_for_primary_change_ns : NULL);
            return 0;
        }
        usleep(10000);
    }
    td_profile_end(profile, start_ns, profile != NULL ? &profile->wait_for_primary_change_ns : NULL);
    return -1;
}

static void td_refresh_cache_best_effort(td_cluster_t *cluster, const char *key, const td_slot_t *slot, td_latency_profile_t *profile) {
    td_session_t *primary = td_primary_session(cluster, td_hash64_string(key));
    td_slot_probe_t probe;
    td_commit_timing_t timing = {0};
    uint64_t observed = 0;
    char err[256];
    uint64_t key_hash = td_hash64_string(key);

    if (cluster->config.cache == TD_CACHE_OFF) {
        return;
    }
    if (td_probe_slot(
            primary,
            TD_REGION_CACHE,
            key_hash,
            &probe,
            profile != NULL ? &(td_probe_profile_t){
                .total_ns = &profile->refresh_cache_probe_ns,
                .scan_ns = &profile->refresh_cache_probe_scan_ns,
                .read_count = &profile->refresh_cache_probe_reads,
                .slots_examined = &profile->refresh_cache_probe_slots_examined,
                .guard_mismatch_count = &profile->refresh_cache_probe_guard_mismatch,
                .tombstone_count = &profile->refresh_cache_probe_tombstones,
                .empty_count = &profile->refresh_cache_probe_empty_hits,
            } : NULL,
            err,
            sizeof(err)) != 0 || !probe.candidate_valid) {
        return;
    }
    (void)td_commit_slot_at(primary, TD_REGION_CACHE, probe.candidate_slot_index, slot, probe.candidate_slot.header.guard_epoch, &observed, profile != NULL ? &timing : NULL, err, sizeof(err));
    if (profile != NULL) {
        profile->refresh_cache_write_ns += timing.write_ns;
        profile->refresh_cache_cas_ns += timing.cas_ns;
    }
}

static int td_cluster_read_value(td_cluster_t *cluster, const char *key, unsigned char *value, size_t *value_len, int *found, td_latency_profile_t *profile, char *err, size_t err_len) {
    td_session_t *primary;
    td_slot_probe_t prime_probe;
    td_slot_probe_t cache_probe;
    uint64_t key_hash;
    uint64_t start_ns;

    *found = 0;
    if (profile != NULL) {
        memset(profile, 0, sizeof(*profile));
        profile->transport = cluster->config.transport;
        profile->cache_enabled = cluster->config.cache == TD_CACHE_ON;
        start_ns = td_now_ns();
        key_hash = td_hash64_string(key);
        profile->hash_ns += td_now_ns() - start_ns;
    } else {
        key_hash = td_hash64_string(key);
    }
    primary = td_primary_session(cluster, key_hash);

    if (td_probe_slot(
            primary,
            TD_REGION_PRIME,
            key_hash,
            &prime_probe,
            profile != NULL ? &(td_probe_profile_t){
                .total_ns = &profile->prime_probe_ns,
                .scan_ns = &profile->prime_probe_scan_ns,
                .read_count = &profile->prime_probe_reads,
                .slots_examined = &profile->prime_probe_slots_examined,
                .guard_mismatch_count = &profile->prime_probe_guard_mismatch,
                .tombstone_count = &profile->prime_probe_tombstones,
                .empty_count = &profile->prime_probe_empty_hits,
            } : NULL,
            err,
            err_len) != 0) {
        return -1;
    }

    if (cluster->config.cache == TD_CACHE_ON) {
        if (td_probe_slot(
                primary,
                TD_REGION_CACHE,
                key_hash,
                &cache_probe,
                profile != NULL ? &(td_probe_profile_t){
                    .total_ns = &profile->cache_probe_ns,
                    .scan_ns = &profile->cache_probe_scan_ns,
                    .read_count = &profile->cache_probe_reads,
                    .slots_examined = &profile->cache_probe_slots_examined,
                    .guard_mismatch_count = &profile->cache_probe_guard_mismatch,
                    .tombstone_count = &profile->cache_probe_tombstones,
                    .empty_count = &profile->cache_probe_empty_hits,
                } : NULL,
                err,
                err_len) == 0) {
            int cache_consistent = 0;

            start_ns = td_profile_begin(profile);
            cache_consistent = cache_probe.found &&
                prime_probe.found &&
                cache_probe.slot.header.guard_epoch == prime_probe.slot.header.guard_epoch &&
                cache_probe.slot.header.visible_epoch == prime_probe.slot.header.visible_epoch &&
                td_slot_present(&cache_probe.slot, key_hash);
            if (profile != NULL) {
                td_profile_end(profile, start_ns, &profile->cache_validation_ns);
            }
            if (cache_consistent) {
                td_crypto_profile_t crypto_profile;

                memset(&crypto_profile, 0, sizeof(crypto_profile));
                start_ns = td_profile_begin(profile);
                if (td_crypto_decode_slot_profiled(&cluster->crypto, key, &cache_probe.slot, value, value_len, profile != NULL ? &crypto_profile : NULL) == 0) {
                    if (profile != NULL) {
                        td_profile_end(profile, start_ns, &profile->cache_decode_ns);
                        profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
                        profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
                        profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
                        profile->crypto_verify_mac_ns += crypto_profile.verify_mac_ns;
                        profile->crypto_decrypt_setup_ns += crypto_profile.decrypt_setup_ns;
                        profile->crypto_decrypt_ns += crypto_profile.decrypt_ns;
                    }
                    if (profile != NULL) {
                        profile->cache_hit = 1;
                    }
                    *found = 1;
                    return 0;
                }
                if (profile != NULL) {
                    td_profile_end(profile, start_ns, &profile->cache_decode_ns);
                    profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
                    profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
                    profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
                    profile->crypto_verify_mac_ns += crypto_profile.verify_mac_ns;
                    profile->crypto_decrypt_setup_ns += crypto_profile.decrypt_setup_ns;
                    profile->crypto_decrypt_ns += crypto_profile.decrypt_ns;
                }
            }
        }
    }

    if (!prime_probe.found || !td_slot_present(&prime_probe.slot, key_hash)) {
        return 0;
    }
    {
        td_crypto_profile_t crypto_profile;

        memset(&crypto_profile, 0, sizeof(crypto_profile));
        start_ns = td_profile_begin(profile);
        if (td_crypto_decode_slot_profiled(&cluster->crypto, key, &prime_probe.slot, value, value_len, profile != NULL ? &crypto_profile : NULL) != 0) {
            if (profile != NULL) {
                td_profile_end(profile, start_ns, &profile->prime_decode_ns);
                profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
                profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
                profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
                profile->crypto_verify_mac_ns += crypto_profile.verify_mac_ns;
                profile->crypto_decrypt_setup_ns += crypto_profile.decrypt_setup_ns;
                profile->crypto_decrypt_ns += crypto_profile.decrypt_ns;
            }
            td_format_error(err, err_len, "mac verification failed for key %s", key);
            return -1;
        }
        if (profile != NULL) {
            td_profile_end(profile, start_ns, &profile->prime_decode_ns);
            profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
            profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
            profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
            profile->crypto_verify_mac_ns += crypto_profile.verify_mac_ns;
            profile->crypto_decrypt_setup_ns += crypto_profile.decrypt_setup_ns;
            profile->crypto_decrypt_ns += crypto_profile.decrypt_ns;
        }
    }
    td_refresh_cache_best_effort(cluster, key, &prime_probe.slot, profile);
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

static int td_cluster_write_value(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int update_only, int tombstone, td_latency_profile_t *profile, char *err, size_t err_len) {
    td_session_t *primary;
    td_slot_probe_t primary_probe;
    td_slot_t proposal;
    td_vote_t votes[TD_MAX_ENDPOINTS];
    size_t replica_count = (size_t)cluster->config.replication;
    size_t backup_count;
    size_t idx;
    uint64_t key_hash;
    uint64_t current_epoch;
    uint64_t observed = 0;
    uint64_t start_ns;
    int rule;

    if (profile != NULL) {
        memset(profile, 0, sizeof(*profile));
        profile->transport = cluster->config.transport;
        profile->cache_enabled = cluster->config.cache == TD_CACHE_ON;
        start_ns = td_now_ns();
        key_hash = td_hash64_string(key);
        profile->hash_ns += td_now_ns() - start_ns;
    } else {
        key_hash = td_hash64_string(key);
    }
    primary = td_primary_session(cluster, key_hash);

    if (replica_count > cluster->session_count) {
        replica_count = cluster->session_count;
    }
    backup_count = replica_count > 0 ? replica_count - 1 : 0;
    memset(votes, 0, sizeof(votes));
    if (profile != NULL) {
        profile->backup_targets = backup_count;
    }

    if (td_probe_slot(
            primary,
            TD_REGION_PRIME,
            key_hash,
            &primary_probe,
            profile != NULL ? &(td_probe_profile_t){
                .total_ns = &profile->prime_probe_ns,
                .scan_ns = &profile->prime_probe_scan_ns,
                .read_count = &profile->prime_probe_reads,
                .slots_examined = &profile->prime_probe_slots_examined,
                .guard_mismatch_count = &profile->prime_probe_guard_mismatch,
                .tombstone_count = &profile->prime_probe_tombstones,
                .empty_count = &profile->prime_probe_empty_hits,
            } : NULL,
            err,
            err_len) != 0) {
        return -1;
    }
    current_epoch = primary_probe.candidate_slot.header.guard_epoch;
    if (update_only && (!primary_probe.found || primary_probe.tombstone || !td_slot_present(&primary_probe.slot, key_hash))) {
        td_format_error(err, err_len, "update failed: key %s not found", key);
        return -1;
    }

    {
        td_crypto_profile_t crypto_profile;

        memset(&crypto_profile, 0, sizeof(crypto_profile));
        start_ns = td_profile_begin(profile);
        if (td_crypto_make_slot_profiled(
                &cluster->crypto,
                key,
                value,
                value_len,
                tombstone ? (TD_SLOT_FLAG_VALID | TD_SLOT_FLAG_TOMBSTONE) : TD_SLOT_FLAG_VALID,
                current_epoch + 1,
                &proposal,
                profile != NULL ? &crypto_profile : NULL) != 0) {
            if (profile != NULL) {
                td_profile_end(profile, start_ns, &profile->crypto_encode_ns);
                profile->crypto_slot_hash_ns += crypto_profile.slot_hash_ns;
                profile->crypto_tie_breaker_ns += crypto_profile.tie_breaker_ns;
                profile->crypto_iv_ns += crypto_profile.iv_ns;
                profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
                profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
                profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
                profile->crypto_encrypt_setup_ns += crypto_profile.encrypt_setup_ns;
                profile->crypto_encrypt_ns += crypto_profile.encrypt_ns;
                profile->crypto_mac_ns += crypto_profile.mac_ns;
            }
            td_format_error(err, err_len, "cannot prepare encrypted slot");
            return -1;
        }
        if (profile != NULL) {
            td_profile_end(profile, start_ns, &profile->crypto_encode_ns);
            profile->crypto_slot_hash_ns += crypto_profile.slot_hash_ns;
            profile->crypto_tie_breaker_ns += crypto_profile.tie_breaker_ns;
            profile->crypto_iv_ns += crypto_profile.iv_ns;
            profile->crypto_mac_setup_ns += crypto_profile.mac_setup_ns;
            profile->crypto_mac_body_ns += crypto_profile.mac_body_ns;
            profile->crypto_mac_finalize_ns += crypto_profile.mac_finalize_ns;
            profile->crypto_encrypt_setup_ns += crypto_profile.encrypt_setup_ns;
            profile->crypto_encrypt_ns += crypto_profile.encrypt_ns;
            profile->crypto_mac_ns += crypto_profile.mac_ns;
        }
    }

    for (idx = 0; idx < backup_count; ++idx) {
        td_session_t *backup = td_replica_session(cluster, key_hash, idx + 1);
        td_slot_probe_t probe;
        td_commit_timing_t timing = {0};
        uint64_t prior_epoch = 0;

        if (td_probe_slot(
                backup,
                TD_REGION_BACKUP,
                key_hash,
                &probe,
                profile != NULL ? &(td_probe_profile_t){
                    .total_ns = &profile->backup_probe_ns,
                    .scan_ns = &profile->backup_probe_scan_ns,
                    .read_count = &profile->backup_probe_reads,
                    .slots_examined = &profile->backup_probe_slots_examined,
                    .guard_mismatch_count = &profile->backup_probe_guard_mismatch,
                    .tombstone_count = &profile->backup_probe_tombstones,
                    .empty_count = &profile->backup_probe_empty_hits,
                } : NULL,
                err,
                err_len) != 0 || !probe.candidate_valid) {
            return -1;
        }
        prior_epoch = probe.candidate_slot.header.guard_epoch;
        if (td_commit_slot_at(backup, TD_REGION_BACKUP, probe.candidate_slot_index, &proposal, prior_epoch, &observed, profile != NULL ? &timing : NULL, err, err_len) == 0 &&
            observed == prior_epoch) {
            votes[idx].success = 1;
            if (profile != NULL) {
                ++profile->backup_successes;
            }
        } else {
            votes[idx].observed_epoch = observed;
            votes[idx].observed_tie = probe.candidate_slot.header.tie_breaker;
        }
        if (profile != NULL) {
            profile->backup_write_ns += timing.write_ns;
            profile->backup_cas_ns += timing.cas_ns;
        }
    }

    start_ns = td_profile_begin(profile);
    rule = td_evaluate_votes(votes, backup_count, proposal.header.tie_breaker);
    if (profile != NULL) {
        td_profile_end(profile, start_ns, &profile->rule_eval_ns);
    }
    if (rule == 0) {
        (void)td_wait_for_primary_change(cluster, key_hash, current_epoch, profile);
        td_format_error(err, err_len, "snapshot consensus lost for key %s", key);
        return -1;
    }

    {
        td_commit_timing_t timing = {0};
        if (td_commit_slot_at(primary, TD_REGION_PRIME, primary_probe.candidate_slot_index, &proposal, current_epoch, &observed, profile != NULL ? &timing : NULL, err, err_len) != 0 ||
            observed != current_epoch) {
            if (profile != NULL) {
                profile->primary_write_ns += timing.write_ns;
                profile->primary_cas_ns += timing.cas_ns;
            }
            td_format_error(err, err_len, "primary CAS failed for key %s", key);
            return -1;
        }
        if (profile != NULL) {
            profile->primary_write_ns += timing.write_ns;
            profile->primary_cas_ns += timing.cas_ns;
        }
    }

    for (idx = 0; idx < backup_count; ++idx) {
        if (!votes[idx].success) {
            td_session_t *backup = td_replica_session(cluster, key_hash, idx + 1);
            td_slot_probe_t probe;
            td_commit_timing_t timing = {0};

            if (profile != NULL) {
                ++profile->repair_attempts;
            }
            if (td_probe_slot(
                    backup,
                    TD_REGION_BACKUP,
                    key_hash,
                    &probe,
                    profile != NULL ? &(td_probe_profile_t){
                        .total_ns = &profile->repair_probe_ns,
                        .scan_ns = &profile->repair_probe_scan_ns,
                        .read_count = &profile->repair_probe_reads,
                        .slots_examined = &profile->repair_probe_slots_examined,
                        .guard_mismatch_count = &profile->repair_probe_guard_mismatch,
                        .tombstone_count = &profile->repair_probe_tombstones,
                        .empty_count = &profile->repair_probe_empty_hits,
                    } : NULL,
                    err,
                    err_len) == 0 && probe.candidate_valid) {
                (void)td_commit_slot_at(backup, TD_REGION_BACKUP, probe.candidate_slot_index, &proposal, probe.candidate_slot.header.guard_epoch, &observed, profile != NULL ? &timing : NULL, err, sizeof(err));
                if (profile != NULL) {
                    profile->repair_write_ns += timing.write_ns;
                    profile->repair_cas_ns += timing.cas_ns;
                }
            }
        }
    }

    td_refresh_cache_best_effort(cluster, key, &proposal, profile);
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
    return td_cluster_read_value(cluster, key, value, value_len, found, NULL, err, err_len);
}

int td_cluster_write_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, value, value_len, 0, 0, NULL, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_update_kv(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, value, value_len, 1, 0, NULL, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_delete_kv(td_cluster_t *cluster, const char *key, int *rule_out, char *err, size_t err_len) {
    int rule = td_cluster_write_value(cluster, key, (const unsigned char *)"", 0, 0, 1, NULL, err, err_len);
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_read_kv_profiled(td_cluster_t *cluster, const char *key, unsigned char *value, size_t *value_len, int *found, td_latency_profile_t *profile, char *err, size_t err_len) {
    uint64_t start_ns = td_profile_begin(profile);
    int rc;

    td_cluster_attach_transport_profile(cluster, profile != NULL ? &profile->transport_profile : NULL);
    rc = td_cluster_read_value(cluster, key, value, value_len, found, profile, err, err_len);
    td_cluster_attach_transport_profile(cluster, NULL);

    if (profile != NULL && start_ns != 0) {
        profile->total_ns = td_now_ns() - start_ns;
    }
    return rc;
}

int td_cluster_write_kv_profiled(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, td_latency_profile_t *profile, char *err, size_t err_len) {
    uint64_t start_ns = td_profile_begin(profile);
    int rule;

    td_cluster_attach_transport_profile(cluster, profile != NULL ? &profile->transport_profile : NULL);
    rule = td_cluster_write_value(cluster, key, value, value_len, 0, 0, profile, err, err_len);
    td_cluster_attach_transport_profile(cluster, NULL);

    if (profile != NULL && start_ns != 0) {
        profile->total_ns = td_now_ns() - start_ns;
    }
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_update_kv_profiled(td_cluster_t *cluster, const char *key, const unsigned char *value, size_t value_len, int *rule_out, td_latency_profile_t *profile, char *err, size_t err_len) {
    uint64_t start_ns = td_profile_begin(profile);
    int rule;

    td_cluster_attach_transport_profile(cluster, profile != NULL ? &profile->transport_profile : NULL);
    rule = td_cluster_write_value(cluster, key, value, value_len, 1, 0, profile, err, err_len);
    td_cluster_attach_transport_profile(cluster, NULL);

    if (profile != NULL && start_ns != 0) {
        profile->total_ns = td_now_ns() - start_ns;
    }
    if (rule < 0) {
        return -1;
    }
    if (rule_out != NULL) {
        *rule_out = rule;
    }
    return 0;
}

int td_cluster_delete_kv_profiled(td_cluster_t *cluster, const char *key, int *rule_out, td_latency_profile_t *profile, char *err, size_t err_len) {
    uint64_t start_ns = td_profile_begin(profile);
    int rule;

    td_cluster_attach_transport_profile(cluster, profile != NULL ? &profile->transport_profile : NULL);
    rule = td_cluster_write_value(cluster, key, (const unsigned char *)"", 0, 0, 1, profile, err, err_len);
    td_cluster_attach_transport_profile(cluster, NULL);

    if (profile != NULL && start_ns != 0) {
        profile->total_ns = td_now_ns() - start_ns;
    }
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
        if (session->endpoint.host[0] != '\0' && session->endpoint.port > 0) {
            fprintf(out, "mn[%zu] %s:%d node_id=%d prime=%llu cache=%llu backup=%llu bytes=%llu\n",
                idx,
                session->endpoint.host,
                session->endpoint.port,
                session->endpoint.node_id,
                (unsigned long long)session->header.prime_slot_count,
                (unsigned long long)session->header.cache_slot_count,
                (unsigned long long)session->header.backup_slot_count,
                (unsigned long long)session->header.region_size);
        } else {
            fprintf(out, "mn[%zu] node_id=%d prime=%llu cache=%llu backup=%llu bytes=%llu\n",
                idx,
                session->endpoint.node_id,
                (unsigned long long)session->header.prime_slot_count,
                (unsigned long long)session->header.cache_slot_count,
                (unsigned long long)session->header.backup_slot_count,
                (unsigned long long)session->header.region_size);
        }
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

static int td_consume_timing_flag(char *text) {
    char *trimmed;
    char *cursor;

    if (text == NULL) {
        return 0;
    }

    trimmed = td_trim(text);
    if (*trimmed == '\0') {
        return 0;
    }
    if (strcmp(trimmed, "-t") == 0) {
        *trimmed = '\0';
        return 1;
    }

    cursor = trimmed + strlen(trimmed);
    while (cursor > trimmed && !isspace((unsigned char)cursor[-1])) {
        --cursor;
    }
    if (strcmp(cursor, "-t") != 0) {
        return 0;
    }
    while (cursor > trimmed && isspace((unsigned char)cursor[-1])) {
        --cursor;
    }
    *cursor = '\0';
    return 1;
}

static void td_print_latency_line(FILE *out, const char *label, uint64_t ns, uint64_t total_ns) {
    if (ns == 0) {
        return;
    }
    fprintf(out, "latency.%s_us=%.2f (%.1f%%)\n", label, td_ns_to_us(ns), total_ns == 0 ? 0.0 : ((double)ns * 100.0) / (double)total_ns);
}

static void td_print_latency_value(FILE *out, const char *label, uint64_t ns) {
    if (ns == 0) {
        return;
    }
    fprintf(out, "latency.%s_us=%.2f\n", label, td_ns_to_us(ns));
}

static void td_print_latency_profile(FILE *out, const td_latency_profile_t *profile) {
    uint64_t accounted_ns = 0;
    uint64_t other_ns;

    fprintf(out, "latency.total_us=%.2f\n", td_ns_to_us(profile->total_ns));
    fprintf(out, "latency.transport kind=%s\n", profile->transport == TD_TRANSPORT_RDMA ? "rdma" : "tcp");
    td_print_latency_line(out, "hash", profile->hash_ns, profile->total_ns);
    accounted_ns += profile->hash_ns;
    td_print_latency_line(out, "probe_prime_scan", profile->prime_probe_scan_ns, profile->total_ns);
    accounted_ns += profile->prime_probe_scan_ns;
    td_print_latency_line(out, "probe_cache_scan", profile->cache_probe_scan_ns, profile->total_ns);
    accounted_ns += profile->cache_probe_scan_ns;
    td_print_latency_line(out, "probe_backup_scan", profile->backup_probe_scan_ns, profile->total_ns);
    accounted_ns += profile->backup_probe_scan_ns;
    td_print_latency_line(out, "probe_repair_scan", profile->repair_probe_scan_ns, profile->total_ns);
    accounted_ns += profile->repair_probe_scan_ns;
    td_print_latency_line(out, "probe_cache_refresh_scan", profile->refresh_cache_probe_scan_ns, profile->total_ns);
    accounted_ns += profile->refresh_cache_probe_scan_ns;
    td_print_latency_line(out, "cache_validation", profile->cache_validation_ns, profile->total_ns);
    accounted_ns += profile->cache_validation_ns;
    if (profile->transport == TD_TRANSPORT_TCP) {
        td_print_latency_line(out, "tcp_read_request_header_send", profile->transport_profile.tcp_read_request_header_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_read_request_header_send_ns;
        td_print_latency_line(out, "tcp_read_response_header_wait", profile->transport_profile.tcp_read_response_header_wait_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_read_response_header_wait_ns;
        td_print_latency_line(out, "tcp_read_response_payload_wait", profile->transport_profile.tcp_read_response_payload_wait_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_read_response_payload_wait_ns;
        td_print_latency_line(out, "tcp_write_request_header_send", profile->transport_profile.tcp_write_request_header_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_write_request_header_send_ns;
        td_print_latency_line(out, "tcp_write_request_payload_send", profile->transport_profile.tcp_write_request_payload_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_write_request_payload_send_ns;
        td_print_latency_line(out, "tcp_write_response_header_wait", profile->transport_profile.tcp_write_response_header_wait_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_write_response_header_wait_ns;
        td_print_latency_line(out, "tcp_cas_request_header_send", profile->transport_profile.tcp_cas_request_header_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_cas_request_header_send_ns;
        td_print_latency_line(out, "tcp_cas_response_header_wait", profile->transport_profile.tcp_cas_response_header_wait_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_cas_response_header_wait_ns;
        td_print_latency_line(out, "tcp_control_request_header_send", profile->transport_profile.tcp_control_request_header_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_control_request_header_send_ns;
        td_print_latency_line(out, "tcp_control_response_header_wait", profile->transport_profile.tcp_control_response_header_wait_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.tcp_control_response_header_wait_ns;
    } else {
        td_print_latency_line(out, "rdma_read_post_send", profile->transport_profile.rdma_read_post_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_read_post_send_ns;
        td_print_latency_line(out, "rdma_read_poll_cq", profile->transport_profile.rdma_read_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_read_poll_cq_ns;
        td_print_latency_line(out, "rdma_read_backoff", profile->transport_profile.rdma_read_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_read_backoff_ns;
        td_print_latency_line(out, "rdma_read_copy", profile->transport_profile.read_copy_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.read_copy_ns;
        td_print_latency_line(out, "rdma_write_copy", profile->transport_profile.write_copy_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.write_copy_ns;
        td_print_latency_line(out, "rdma_write_post_send", profile->transport_profile.rdma_write_post_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_write_post_send_ns;
        td_print_latency_line(out, "rdma_write_poll_cq", profile->transport_profile.rdma_write_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_write_poll_cq_ns;
        td_print_latency_line(out, "rdma_write_backoff", profile->transport_profile.rdma_write_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_write_backoff_ns;
        td_print_latency_line(out, "rdma_cas_request_post_send", profile->transport_profile.rdma_cas_request_post_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_cas_request_post_send_ns;
        td_print_latency_line(out, "rdma_cas_request_send_poll_cq", profile->transport_profile.rdma_cas_request_send_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_cas_request_send_poll_cq_ns;
        td_print_latency_line(out, "rdma_cas_request_send_backoff", profile->transport_profile.rdma_cas_request_send_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_cas_request_send_backoff_ns;
        td_print_latency_line(out, "rdma_cas_response_poll_cq", profile->transport_profile.rdma_cas_response_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_cas_response_poll_cq_ns;
        td_print_latency_line(out, "rdma_cas_response_backoff", profile->transport_profile.rdma_cas_response_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_cas_response_backoff_ns;
        td_print_latency_line(out, "rdma_control_request_post_send", profile->transport_profile.rdma_control_request_post_send_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_control_request_post_send_ns;
        td_print_latency_line(out, "rdma_control_request_send_poll_cq", profile->transport_profile.rdma_control_request_send_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_control_request_send_poll_cq_ns;
        td_print_latency_line(out, "rdma_control_request_send_backoff", profile->transport_profile.rdma_control_request_send_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_control_request_send_backoff_ns;
        td_print_latency_line(out, "rdma_control_response_poll_cq", profile->transport_profile.rdma_control_response_poll_cq_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_control_response_poll_cq_ns;
        td_print_latency_line(out, "rdma_control_response_backoff", profile->transport_profile.rdma_control_response_backoff_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_control_response_backoff_ns;
        td_print_latency_line(out, "rdma_response_copy", profile->transport_profile.rdma_response_copy_ns, profile->total_ns);
        accounted_ns += profile->transport_profile.rdma_response_copy_ns;
    }
    td_print_latency_line(out, "crypto_slot_hash", profile->crypto_slot_hash_ns, profile->total_ns);
    accounted_ns += profile->crypto_slot_hash_ns;
    td_print_latency_line(out, "crypto_tie_breaker", profile->crypto_tie_breaker_ns, profile->total_ns);
    accounted_ns += profile->crypto_tie_breaker_ns;
    td_print_latency_line(out, "crypto_iv", profile->crypto_iv_ns, profile->total_ns);
    accounted_ns += profile->crypto_iv_ns;
    td_print_latency_line(out, "crypto_encrypt_setup", profile->crypto_encrypt_setup_ns, profile->total_ns);
    accounted_ns += profile->crypto_encrypt_setup_ns;
    td_print_latency_line(out, "crypto_encrypt", profile->crypto_encrypt_ns, profile->total_ns);
    accounted_ns += profile->crypto_encrypt_ns;
    td_print_latency_line(out, "crypto_mac_setup", profile->crypto_mac_setup_ns, profile->total_ns);
    accounted_ns += profile->crypto_mac_setup_ns;
    td_print_latency_line(out, "crypto_mac_body", profile->crypto_mac_body_ns, profile->total_ns);
    accounted_ns += profile->crypto_mac_body_ns;
    td_print_latency_line(out, "crypto_mac_finalize", profile->crypto_mac_finalize_ns, profile->total_ns);
    accounted_ns += profile->crypto_mac_finalize_ns;
    td_print_latency_line(out, "crypto_decrypt_setup", profile->crypto_decrypt_setup_ns, profile->total_ns);
    accounted_ns += profile->crypto_decrypt_setup_ns;
    td_print_latency_line(out, "crypto_decrypt", profile->crypto_decrypt_ns, profile->total_ns);
    accounted_ns += profile->crypto_decrypt_ns;
    td_print_latency_line(out, "snapshot_consensus_eval", profile->rule_eval_ns, profile->total_ns);
    accounted_ns += profile->rule_eval_ns;
    td_print_latency_line(out, "snapshot_wait_primary_change", profile->wait_for_primary_change_ns, profile->total_ns);
    accounted_ns += profile->wait_for_primary_change_ns;
    other_ns = profile->total_ns > accounted_ns ? profile->total_ns - accounted_ns : 0;
    td_print_latency_line(out, "other", other_ns, profile->total_ns);
    td_print_latency_value(out, "transport_read_total", profile->transport_profile.read_send_ns + profile->transport_profile.read_wait_ns + profile->transport_profile.read_copy_ns);
    td_print_latency_value(out, "transport_write_total", profile->transport_profile.write_copy_ns + profile->transport_profile.write_send_ns + profile->transport_profile.write_wait_ns);
    td_print_latency_value(out, "transport_cas_total", profile->transport_profile.cas_send_ns + profile->transport_profile.cas_wait_ns);
    td_print_latency_value(out, "transport_control_total", profile->transport_profile.control_send_ns + profile->transport_profile.control_wait_ns);
    if (profile->transport == TD_TRANSPORT_TCP) {
        td_print_latency_value(out, "tcp_server_read_total", profile->transport_profile.tcp_server_read_total_ns);
        td_print_latency_value(out, "tcp_server_read_alloc", profile->transport_profile.tcp_server_read_alloc_ns);
        td_print_latency_value(out, "tcp_server_read_region", profile->transport_profile.tcp_server_read_region_ns);
        td_print_latency_value(out, "tcp_server_write_total", profile->transport_profile.tcp_server_write_total_ns);
        td_print_latency_value(out, "tcp_server_write_recv", profile->transport_profile.tcp_server_write_recv_ns);
        td_print_latency_value(out, "tcp_server_write_region", profile->transport_profile.tcp_server_write_region_ns);
        td_print_latency_value(out, "tcp_server_cas_total", profile->transport_profile.tcp_server_cas_total_ns);
        td_print_latency_value(out, "tcp_server_cas_region", profile->transport_profile.tcp_server_cas_region_ns);
        td_print_latency_value(out, "tcp_server_control_total", profile->transport_profile.tcp_server_control_total_ns);
        td_print_latency_value(out, "tcp_server_control_exec", profile->transport_profile.tcp_server_control_exec_ns);
        td_print_latency_value(out, "tcp_read_wire_estimate", profile->transport_profile.tcp_read_wire_estimate_ns);
        td_print_latency_value(out, "tcp_write_wire_estimate", profile->transport_profile.tcp_write_wire_estimate_ns);
        td_print_latency_value(out, "tcp_cas_wire_estimate", profile->transport_profile.tcp_cas_wire_estimate_ns);
        td_print_latency_value(out, "tcp_control_wire_estimate", profile->transport_profile.tcp_control_wire_estimate_ns);
    } else {
        td_print_latency_value(out, "rdma_read_completion_elapsed", profile->transport_profile.read_wait_ns);
        td_print_latency_value(out, "rdma_write_completion_elapsed", profile->transport_profile.write_wait_ns);
        td_print_latency_value(out, "rdma_cas_request_send_elapsed", profile->transport_profile.rdma_cas_request_send_wait_ns);
        td_print_latency_value(out, "rdma_cas_response_elapsed", profile->transport_profile.rdma_cas_response_wait_ns);
        td_print_latency_value(out, "rdma_control_request_send_elapsed", profile->transport_profile.rdma_control_request_send_wait_ns);
        td_print_latency_value(out, "rdma_control_response_elapsed", profile->transport_profile.rdma_control_response_wait_ns);
        td_print_latency_value(out, "rdma_server_cas_total", profile->transport_profile.rdma_server_cas_total_ns);
        td_print_latency_value(out, "rdma_server_cas_region", profile->transport_profile.rdma_server_cas_region_ns);
        td_print_latency_value(out, "rdma_server_control_total", profile->transport_profile.rdma_server_control_total_ns);
        td_print_latency_value(out, "rdma_server_control_exec", profile->transport_profile.rdma_server_control_exec_ns);
    }
    td_print_latency_value(out, "crypto_mac_total", profile->crypto_mac_ns);
    td_print_latency_value(out, "crypto_verify_mac_total", profile->crypto_verify_mac_ns);
    td_print_latency_value(out, "workload_prime_probe", profile->prime_probe_ns);
    td_print_latency_value(out, "workload_prime_probe_scan", profile->prime_probe_scan_ns);
    td_print_latency_value(out, "workload_cache_probe", profile->cache_probe_ns);
    td_print_latency_value(out, "workload_cache_probe_scan", profile->cache_probe_scan_ns);
    td_print_latency_value(out, "workload_cache_decode", profile->cache_decode_ns);
    td_print_latency_value(out, "workload_cache_validation", profile->cache_validation_ns);
    td_print_latency_value(out, "workload_prime_decode", profile->prime_decode_ns);
    td_print_latency_value(out, "workload_crypto_encode", profile->crypto_encode_ns);
    td_print_latency_value(out, "workload_snapshot_backup_probe", profile->backup_probe_ns);
    td_print_latency_value(out, "workload_snapshot_backup_probe_scan", profile->backup_probe_scan_ns);
    td_print_latency_value(out, "workload_snapshot_backup_write", profile->backup_write_ns);
    td_print_latency_value(out, "workload_snapshot_backup_cas", profile->backup_cas_ns);
    td_print_latency_value(out, "workload_snapshot_consensus_eval", profile->rule_eval_ns);
    td_print_latency_value(out, "workload_snapshot_wait_primary_change", profile->wait_for_primary_change_ns);
    td_print_latency_value(out, "workload_snapshot_primary_write", profile->primary_write_ns);
    td_print_latency_value(out, "workload_snapshot_primary_cas", profile->primary_cas_ns);
    td_print_latency_value(out, "workload_snapshot_repair_probe", profile->repair_probe_ns);
    td_print_latency_value(out, "workload_snapshot_repair_probe_scan", profile->repair_probe_scan_ns);
    td_print_latency_value(out, "workload_snapshot_repair_write", profile->repair_write_ns);
    td_print_latency_value(out, "workload_snapshot_repair_cas", profile->repair_cas_ns);
    td_print_latency_value(out, "workload_cache_refresh_probe", profile->refresh_cache_probe_ns);
    td_print_latency_value(out, "workload_cache_refresh_probe_scan", profile->refresh_cache_probe_scan_ns);
    td_print_latency_value(out, "workload_cache_refresh_write", profile->refresh_cache_write_ns);
    td_print_latency_value(out, "workload_cache_refresh_cas", profile->refresh_cache_cas_ns);
    fprintf(out, "latency.cache enabled=%s hit=%s\n",
        profile->cache_enabled ? "yes" : "no",
        profile->cache_hit ? "yes" : "no");
    fprintf(out, "latency.probe_reads prime=%zu cache=%zu backup=%zu repair=%zu cache_refresh=%zu\n",
        profile->prime_probe_reads,
        profile->cache_probe_reads,
        profile->backup_probe_reads,
        profile->repair_probe_reads,
        profile->refresh_cache_probe_reads);
    fprintf(out, "latency.probe_slots prime=%zu cache=%zu backup=%zu repair=%zu cache_refresh=%zu\n",
        profile->prime_probe_slots_examined,
        profile->cache_probe_slots_examined,
        profile->backup_probe_slots_examined,
        profile->repair_probe_slots_examined,
        profile->refresh_cache_probe_slots_examined);
    fprintf(out, "latency.probe_guard_mismatch prime=%zu cache=%zu backup=%zu repair=%zu cache_refresh=%zu\n",
        profile->prime_probe_guard_mismatch,
        profile->cache_probe_guard_mismatch,
        profile->backup_probe_guard_mismatch,
        profile->repair_probe_guard_mismatch,
        profile->refresh_cache_probe_guard_mismatch);
    fprintf(out, "latency.probe_tombstones prime=%zu cache=%zu backup=%zu repair=%zu cache_refresh=%zu\n",
        profile->prime_probe_tombstones,
        profile->cache_probe_tombstones,
        profile->backup_probe_tombstones,
        profile->repair_probe_tombstones,
        profile->refresh_cache_probe_tombstones);
    fprintf(out, "latency.probe_empty prime=%zu cache=%zu backup=%zu repair=%zu cache_refresh=%zu\n",
        profile->prime_probe_empty_hits,
        profile->cache_probe_empty_hits,
        profile->backup_probe_empty_hits,
        profile->repair_probe_empty_hits,
        profile->refresh_cache_probe_empty_hits);
    if (profile->transport == TD_TRANSPORT_RDMA) {
        fprintf(out, "latency.rdma_empty_polls read=%zu write=%zu cas_send=%zu cas_response=%zu control_send=%zu control_response=%zu\n",
            profile->transport_profile.rdma_read_empty_polls,
            profile->transport_profile.rdma_write_empty_polls,
            profile->transport_profile.rdma_cas_send_empty_polls,
            profile->transport_profile.rdma_cas_response_empty_polls,
            profile->transport_profile.rdma_control_send_empty_polls,
            profile->transport_profile.rdma_control_response_empty_polls);
        fprintf(out, "latency.rdma_backoffs read=%zu write=%zu cas_send=%zu cas_response=%zu control_send=%zu control_response=%zu\n",
            profile->transport_profile.rdma_read_backoff_count,
            profile->transport_profile.rdma_write_backoff_count,
            profile->transport_profile.rdma_cas_send_backoff_count,
            profile->transport_profile.rdma_cas_response_backoff_count,
            profile->transport_profile.rdma_control_send_backoff_count,
            profile->transport_profile.rdma_control_response_backoff_count);
    }
    if (profile->backup_targets > 0 || profile->repair_attempts > 0) {
        fprintf(out, "latency.replication backups=%zu successful=%zu repairs=%zu wait_primary_change_attempts=%zu\n",
            profile->backup_targets,
            profile->backup_successes,
            profile->repair_attempts,
            profile->wait_for_primary_change_attempts);
    }
}

int td_cluster_execute(td_cluster_t *cluster, const char *line, FILE *out) {
    char scratch[TD_CMD_BYTES];
    char *cmd = NULL;
    char *arg1 = NULL;
    char *arg2 = NULL;
    char err[256];
    int timing_enabled;

    snprintf(scratch, sizeof(scratch), "%s", line);
    if (td_split_command(scratch, &cmd, &arg1, &arg2) != 0) {
        return 1;
    }

    if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        return 0;
    }
    if (strcmp(cmd, "help") == 0) {
        fprintf(out, "commands: read <key> [-t], write <key> <value> [-t], update <key> <value> [-t], delete <key> [-t], status, evict, quit\n");
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

    timing_enabled = td_consume_timing_flag(arg2);

    if (strcmp(cmd, "read") == 0) {
        unsigned char value[TD_MAX_VALUE_SIZE + 1];
        size_t value_len = 0;
        int found = 0;
        td_latency_profile_t profile;

        if (arg2 != NULL && td_trim(arg2)[0] != '\0') {
            fprintf(out, "error: read accepts only read <key> or read <key> -t\n");
            return 1;
        }
        if ((timing_enabled
                ? td_cluster_read_kv_profiled(cluster, arg1, value, &value_len, &found, &profile, err, sizeof(err))
                : td_cluster_read_kv(cluster, arg1, value, &value_len, &found, err, sizeof(err))) != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        if (!found) {
            fprintf(out, "not_found %s\n", arg1);
            if (timing_enabled) {
                td_print_latency_profile(out, &profile);
            }
            return 1;
        }
        value[value_len] = '\0';
        fprintf(out, "value %s %s\n", arg1, value);
        if (timing_enabled) {
            td_print_latency_profile(out, &profile);
        }
        return 1;
    }

    if ((strcmp(cmd, "write") == 0 || strcmp(cmd, "update") == 0) && arg2 == NULL) {
        fprintf(out, "error: missing value\n");
        return 1;
    }

    if (strcmp(cmd, "write") == 0 || strcmp(cmd, "update") == 0) {
        td_latency_profile_t profile;
        uint64_t start_ns = td_now_ns();
        int rule = 0;
        char *value_arg = arg2 != NULL ? td_trim(arg2) : NULL;
        int rc;

        if (value_arg == NULL || *value_arg == '\0') {
            fprintf(out, "error: missing value\n");
            return 1;
        }
        rc = strcmp(cmd, "update") == 0
            ? (timing_enabled
                ? td_cluster_update_kv_profiled(cluster, arg1, (const unsigned char *)value_arg, strlen(value_arg), &rule, &profile, err, sizeof(err))
                : td_cluster_update_kv(cluster, arg1, (const unsigned char *)value_arg, strlen(value_arg), &rule, err, sizeof(err)))
            : (timing_enabled
                ? td_cluster_write_kv_profiled(cluster, arg1, (const unsigned char *)value_arg, strlen(value_arg), &rule, &profile, err, sizeof(err))
                : td_cluster_write_kv(cluster, arg1, (const unsigned char *)value_arg, strlen(value_arg), &rule, err, sizeof(err)));
        if (rc != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        if (timing_enabled) {
            fprintf(out, "ok %s rule=%d latency_us=%.2f\n", arg1, rule, td_ns_to_us(profile.total_ns));
            td_print_latency_profile(out, &profile);
        } else {
            fprintf(out, "ok %s rule=%d latency_us=%llu\n", arg1, rule, (unsigned long long)((td_now_ns() - start_ns) / 1000ULL));
        }
        return 1;
    }

    if (strcmp(cmd, "delete") == 0) {
        td_latency_profile_t profile;
        uint64_t start_ns = td_now_ns();
        int rule = 0;

        if (arg2 != NULL && td_trim(arg2)[0] != '\0') {
            fprintf(out, "error: delete accepts only delete <key> or delete <key> -t\n");
            return 1;
        }
        if ((timing_enabled
                ? td_cluster_delete_kv_profiled(cluster, arg1, &rule, &profile, err, sizeof(err))
                : td_cluster_delete_kv(cluster, arg1, &rule, err, sizeof(err))) != 0) {
            fprintf(out, "error: %s\n", err);
            return 1;
        }
        if (timing_enabled) {
            fprintf(out, "deleted %s rule=%d latency_us=%.2f\n", arg1, rule, td_ns_to_us(profile.total_ns));
            td_print_latency_profile(out, &profile);
        } else {
            fprintf(out, "deleted %s rule=%d latency_us=%llu\n", arg1, rule, (unsigned long long)((td_now_ns() - start_ns) / 1000ULL));
        }
        return 1;
    }

    fprintf(out, "error: unknown command %s\n", cmd);
    return 1;
}
