#include "td_config.h"
#include "td_layout.h"

#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int td_parse_mode(const char *value, td_mode_t *out) {
    if (strcmp(value, "cn") == 0) {
        *out = TD_MODE_CN;
        return 0;
    }
    if (strcmp(value, "mn") == 0) {
        *out = TD_MODE_MN;
        return 0;
    }
    return -1;
}

static int td_parse_transport(const char *value, td_transport_t *out) {
    if (strcmp(value, "tcp") == 0) {
        *out = TD_TRANSPORT_TCP;
        return 0;
    }
    if (strcmp(value, "rdma") == 0) {
        *out = TD_TRANSPORT_RDMA;
        return 0;
    }
    return -1;
}

static int td_parse_rdma_bootstrap(const char *value, td_rdma_bootstrap_t *out) {
    if (strcmp(value, "tcp") == 0) {
        *out = TD_RDMA_BOOTSTRAP_TCP;
        return 0;
    }
    if (strcmp(value, "file") == 0 || strcmp(value, "oob_file") == 0) {
        *out = TD_RDMA_BOOTSTRAP_OOB_FILE;
        return 0;
    }
    if (strcmp(value, "vsock") == 0) {
        *out = TD_RDMA_BOOTSTRAP_VSOCK;
        return 0;
    }
    return -1;
}

static int td_parse_toggle(const char *value, int *out) {
    if (strcmp(value, "on") == 0) {
        *out = 1;
        return 0;
    }
    if (strcmp(value, "off") == 0) {
        *out = 0;
        return 0;
    }
    return -1;
}

static char *td_normalize_value(char *value) {
    size_t len;

    value = td_trim(value);
    len = strlen(value);
    if (len >= 2 && value[0] == '"' && value[len - 1] == '"') {
        value[len - 1] = '\0';
        ++value;
    }
    return td_trim(value);
}

static int td_parse_size_literal(const char *text, size_t *out) {
    char *end = NULL;
    unsigned long long value;

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value > (unsigned long long)SIZE_MAX) {
        return -1;
    }
    *out = (size_t)value;
    return 0;
}

static int td_parse_size_bytes(const char *text, size_t *out) {
    char number[64];
    char unit[8];
    size_t text_len = strlen(text);
    size_t pos = 0;
    size_t number_len = 0;
    size_t unit_len = 0;
    unsigned long long magnitude;
    unsigned long long multiplier = 0;
    char *end = NULL;

    if (text_len == 0) {
        return -1;
    }

    while (pos < text_len && isdigit((unsigned char)text[pos])) {
        if (number_len + 1 >= sizeof(number)) {
            return -1;
        }
        number[number_len++] = text[pos++];
    }
    number[number_len] = '\0';

    while (pos < text_len && isspace((unsigned char)text[pos])) {
        ++pos;
    }
    while (pos < text_len && isalpha((unsigned char)text[pos])) {
        if (unit_len + 1 >= sizeof(unit)) {
            return -1;
        }
        unit[unit_len++] = (char)tolower((unsigned char)text[pos++]);
    }
    unit[unit_len] = '\0';

    while (pos < text_len && isspace((unsigned char)text[pos])) {
        ++pos;
    }
    if (number_len == 0 || unit_len == 0 || pos != text_len) {
        return -1;
    }

    errno = 0;
    magnitude = strtoull(number, &end, 10);
    if (errno != 0 || end == number || *end != '\0') {
        return -1;
    }

    if (strcmp(unit, "b") == 0) {
        multiplier = 1ULL;
    } else if (strcmp(unit, "kb") == 0) {
        multiplier = 1024ULL;
    } else if (strcmp(unit, "mb") == 0) {
        multiplier = 1024ULL * 1024ULL;
    } else if (strcmp(unit, "gb") == 0) {
        multiplier = 1024ULL * 1024ULL * 1024ULL;
    } else {
        return -1;
    }

    if (magnitude > ((unsigned long long)SIZE_MAX / multiplier)) {
        return -1;
    }
    *out = (size_t)(magnitude * multiplier);
    return 0;
}

static void td_allocate_weighted_slots(td_config_t *cfg, size_t remaining_slots) {
    typedef struct {
        size_t *slots;
        int explicit;
        size_t weight;
        size_t remainder;
        int priority;
    } td_slot_bucket_t;

    td_slot_bucket_t buckets[3];
    size_t weight_sum = 0;
    size_t allocated = 0;
    size_t idx;

    buckets[0].slots = &cfg->prime_slots;
    buckets[0].explicit = cfg->prime_slots_explicit;
    buckets[0].weight = 4;
    buckets[0].remainder = 0;
    buckets[0].priority = 0;
    buckets[1].slots = &cfg->cache_slots;
    buckets[1].explicit = cfg->cache_slots_explicit;
    buckets[1].weight = 1;
    buckets[1].remainder = 0;
    buckets[1].priority = 1;
    buckets[2].slots = &cfg->backup_slots;
    buckets[2].explicit = cfg->backup_slots_explicit;
    buckets[2].weight = 4;
    buckets[2].remainder = 0;
    buckets[2].priority = 2;

    for (idx = 0; idx < 3; ++idx) {
        if (!buckets[idx].explicit) {
            weight_sum += buckets[idx].weight;
            *buckets[idx].slots = 0;
        }
    }
    if (weight_sum == 0 || remaining_slots == 0) {
        return;
    }

    for (idx = 0; idx < 3; ++idx) {
        if (!buckets[idx].explicit) {
            *buckets[idx].slots += (remaining_slots * buckets[idx].weight) / weight_sum;
            buckets[idx].remainder = (remaining_slots * buckets[idx].weight) % weight_sum;
            allocated += *buckets[idx].slots;
        }
    }

    while (allocated < remaining_slots) {
        size_t best = 3;
        for (idx = 0; idx < 3; ++idx) {
            if (buckets[idx].explicit) {
                continue;
            }
            if (best == 3 ||
                buckets[idx].remainder > buckets[best].remainder ||
                (buckets[idx].remainder == buckets[best].remainder &&
                    buckets[idx].priority < buckets[best].priority)) {
                best = idx;
            }
        }
        if (best == 3) {
            break;
        }
        ++(*buckets[best].slots);
        buckets[best].remainder = 0;
        ++allocated;
    }
}

static int td_resolve_mn_slots(td_config_t *cfg, char *err, size_t err_len) {
    size_t slot_bytes = sizeof(td_slot_t);
    size_t capacity_slots;
    size_t manual_slots;
    size_t remaining_slots;

    if (cfg->mn_memory_size < slot_bytes) {
        td_format_error(err, err_len, "mn_memory_size must be at least %zu bytes", slot_bytes);
        return -1;
    }

    capacity_slots = cfg->mn_memory_size / slot_bytes;
    if (capacity_slots == 0) {
        td_format_error(err, err_len, "mn_memory_size is too small to fit any slots");
        return -1;
    }

    manual_slots = cfg->prime_slots + cfg->cache_slots + cfg->backup_slots;
    if (manual_slots > capacity_slots) {
        td_format_error(err, err_len,
            "configured slots need %zu bytes but mn_memory_size is %zu bytes",
            manual_slots * slot_bytes,
            cfg->mn_memory_size);
        return -1;
    }

    remaining_slots = capacity_slots - manual_slots;
    td_allocate_weighted_slots(cfg, remaining_slots);

    if (cfg->prime_slots == 0) {
        td_format_error(err, err_len, "resolved prime_slots is zero; increase mn_memory_size or set prime_slots");
        return -1;
    }
    if (cfg->replication > 1 && cfg->backup_slots == 0) {
        td_format_error(err, err_len, "resolved backup_slots is zero; increase mn_memory_size or set backup_slots");
        return -1;
    }
    return 0;
}

void td_config_init_defaults(td_config_t *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->mode = TD_MODE_CN;
    cfg->transport = TD_TRANSPORT_TCP;
    cfg->replication = 3;
    cfg->tdx = TD_TDX_OFF;
    cfg->cache = TD_CACHE_ON;
    cfg->mn_memory_size = 64ULL * 1024ULL * 1024ULL;
    cfg->rdma_bootstrap = TD_RDMA_BOOTSTRAP_OOB_FILE;
    cfg->rdma_gid_index = 0;
    cfg->rdma_port_num = 1;
    cfg->rdma_region_segment_bytes = 16ULL * 1024ULL * 1024ULL;
    cfg->rdma_oob_poll_ms = 100;
    cfg->rdma_oob_timeout_ms = 30000;
    cfg->listen_port = 0;
    cfg->node_id = -1;
    cfg->max_value_size = TD_MAX_VALUE_SIZE;
    cfg->eviction_threshold_pct = 80;
    cfg->recv_queue_depth = TD_RECV_QUEUE_DEPTH;
    snprintf(cfg->rdma_device, sizeof(cfg->rdma_device), "%s", "mlx5_0");
    snprintf(cfg->rdma_oob_dir, sizeof(cfg->rdma_oob_dir), "%s", "");
    snprintf(cfg->listen_host, sizeof(cfg->listen_host), "%s", "0.0.0.0");
    snprintf(cfg->memory_file, sizeof(cfg->memory_file), "%s", "/tmp/dist-td-mn.dat");
}

int td_config_load(const char *path, td_config_t *cfg, char *err, size_t err_len) {
    FILE *fp;
    char line[512];
    size_t line_no = 0;

    td_config_init_defaults(cfg);
    fp = fopen(path, "r");
    if (fp == NULL) {
        td_format_error(err, err_len, "cannot open config %s", path);
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *key;
        char *value;
        char *sep;

        ++line_no;
        key = td_trim(line);
        if (*key == '\0' || *key == '#') {
            continue;
        }

        sep = strchr(key, ':');
        if (sep == NULL) {
            td_format_error(err, err_len, "config parse error at line %zu", line_no);
            fclose(fp);
            return -1;
        }

        *sep = '\0';
        value = td_normalize_value(sep + 1);
        key = td_trim(key);

        if (strcmp(key, "mode") == 0) {
            if (td_parse_mode(value, &cfg->mode) != 0) {
                td_format_error(err, err_len, "invalid mode at line %zu", line_no);
                fclose(fp);
                return -1;
            }
        } else if (strcmp(key, "transport") == 0) {
            if (td_parse_transport(value, &cfg->transport) != 0) {
                td_format_error(err, err_len, "invalid transport at line %zu", line_no);
                fclose(fp);
                return -1;
            }
        } else if (strcmp(key, "replication") == 0) {
            cfg->replication = atoi(value);
        } else if (strcmp(key, "tdx") == 0) {
            int toggle = 0;
            if (td_parse_toggle(value, &toggle) != 0) {
                td_format_error(err, err_len, "invalid tdx at line %zu", line_no);
                fclose(fp);
                return -1;
            }
            cfg->tdx = toggle ? TD_TDX_ON : TD_TDX_OFF;
        } else if (strcmp(key, "cache") == 0) {
            int toggle = 0;
            if (td_parse_toggle(value, &toggle) != 0) {
                td_format_error(err, err_len, "invalid cache at line %zu", line_no);
                fclose(fp);
                return -1;
            }
            cfg->cache = toggle ? TD_CACHE_ON : TD_CACHE_OFF;
        } else if (strcmp(key, "mn_memory_size") == 0) {
            if (td_parse_size_bytes(value, &cfg->mn_memory_size) != 0) {
                td_format_error(err, err_len, "invalid mn_memory_size at line %zu", line_no);
                fclose(fp);
                return -1;
            }
        } else if (strcmp(key, "encryption_key_hex") == 0) {
            snprintf(cfg->encryption_key_hex, sizeof(cfg->encryption_key_hex), "%s", value);
        } else if (strcmp(key, "rdma_device") == 0) {
            snprintf(cfg->rdma_device, sizeof(cfg->rdma_device), "%s", value);
        } else if (strcmp(key, "rdma_bootstrap") == 0) {
            if (td_parse_rdma_bootstrap(value, &cfg->rdma_bootstrap) != 0) {
                td_format_error(err, err_len, "invalid rdma_bootstrap at line %zu", line_no);
                fclose(fp);
                return -1;
            }
        } else if (strcmp(key, "rdma_gid_index") == 0) {
            cfg->rdma_gid_index = atoi(value);
        } else if (strcmp(key, "rdma_port_num") == 0) {
            cfg->rdma_port_num = atoi(value);
        } else if (strcmp(key, "rdma_region_segment_bytes") == 0) {
            if (td_parse_size_bytes(value, &cfg->rdma_region_segment_bytes) != 0) {
                td_format_error(err, err_len, "invalid rdma_region_segment_bytes at line %zu", line_no);
                fclose(fp);
                return -1;
            }
        } else if (strcmp(key, "rdma_oob_dir") == 0) {
            snprintf(cfg->rdma_oob_dir, sizeof(cfg->rdma_oob_dir), "%s", value);
        } else if (strcmp(key, "rdma_oob_poll_ms") == 0) {
            cfg->rdma_oob_poll_ms = (size_t)strtoull(value, NULL, 10);
        } else if (strcmp(key, "rdma_oob_timeout_ms") == 0) {
            cfg->rdma_oob_timeout_ms = (size_t)strtoull(value, NULL, 10);
        } else if (strcmp(key, "listen_host") == 0) {
            snprintf(cfg->listen_host, sizeof(cfg->listen_host), "%s", value);
        } else if (strcmp(key, "listen_port") == 0) {
            cfg->listen_port = atoi(value);
        } else if (strcmp(key, "node_id") == 0) {
            cfg->node_id = atoi(value);
        } else if (strcmp(key, "mn_node_id") == 0) {
            int node_id = atoi(value);

            if (cfg->mn_count >= TD_MAX_ENDPOINTS) {
                td_format_error(err, err_len, "too many mn_node_id values");
                fclose(fp);
                return -1;
            }
            if (node_id < 0) {
                td_format_error(err, err_len, "invalid mn_node_id at line %zu", line_no);
                fclose(fp);
                return -1;
            }
            memset(&cfg->mn_endpoints[cfg->mn_count], 0, sizeof(cfg->mn_endpoints[cfg->mn_count]));
            snprintf(cfg->mn_endpoints[cfg->mn_count].host, sizeof(cfg->mn_endpoints[cfg->mn_count].host), "node-%d", node_id);
            cfg->mn_endpoints[cfg->mn_count].port = 0;
            cfg->mn_endpoints[cfg->mn_count].node_id = node_id;
            ++cfg->mn_count;
        } else if (strcmp(key, "memory_file") == 0) {
            snprintf(cfg->memory_file, sizeof(cfg->memory_file), "%s", value);
        } else if (strcmp(key, "prime_slots") == 0) {
            if (*value != '\0') {
                if (td_parse_size_literal(value, &cfg->prime_slots) != 0) {
                    td_format_error(err, err_len, "invalid prime_slots at line %zu", line_no);
                    fclose(fp);
                    return -1;
                }
                cfg->prime_slots_explicit = 1;
            }
        } else if (strcmp(key, "cache_slots") == 0) {
            if (*value != '\0') {
                if (td_parse_size_literal(value, &cfg->cache_slots) != 0) {
                    td_format_error(err, err_len, "invalid cache_slots at line %zu", line_no);
                    fclose(fp);
                    return -1;
                }
                cfg->cache_slots_explicit = 1;
            }
        } else if (strcmp(key, "backup_slots") == 0) {
            if (*value != '\0') {
                if (td_parse_size_literal(value, &cfg->backup_slots) != 0) {
                    td_format_error(err, err_len, "invalid backup_slots at line %zu", line_no);
                    fclose(fp);
                    return -1;
                }
                cfg->backup_slots_explicit = 1;
            }
        } else if (strcmp(key, "max_value_size") == 0) {
            cfg->max_value_size = (size_t)strtoull(value, NULL, 10);
        } else if (strcmp(key, "eviction_threshold_pct") == 0) {
            cfg->eviction_threshold_pct = (size_t)strtoull(value, NULL, 10);
        } else if (strcmp(key, "recv_queue_depth") == 0) {
            cfg->recv_queue_depth = (size_t)strtoull(value, NULL, 10);
        } else if (strcmp(key, "mn_endpoint") == 0) {
            if (cfg->mn_count >= TD_MAX_ENDPOINTS) {
                td_format_error(err, err_len, "too many mn_endpoint values");
                fclose(fp);
                return -1;
            }
            if (td_parse_host_port(value, &cfg->mn_endpoints[cfg->mn_count]) != 0) {
                td_format_error(err, err_len, "invalid mn_endpoint at line %zu", line_no);
                fclose(fp);
                return -1;
            }
            cfg->mn_endpoints[cfg->mn_count].node_id = (int)cfg->mn_count;
            ++cfg->mn_count;
        }
    }

    fclose(fp);

    if (cfg->max_value_size > TD_MAX_VALUE_SIZE) {
        td_format_error(err, err_len, "max_value_size exceeds built-in limit %d", TD_MAX_VALUE_SIZE);
        return -1;
    }
    if (cfg->replication <= 0) {
        cfg->replication = 1;
    }
    if (cfg->mode == TD_MODE_CN && cfg->tdx != TD_TDX_OFF) {
        td_format_error(err, err_len, "cn config requires tdx: off");
        return -1;
    }
    if (cfg->mode == TD_MODE_CN && cfg->mn_count == 0) {
        td_format_error(err, err_len, "cn config requires at least one mn_endpoint or mn_node_id");
        return -1;
    }
    if (cfg->mode == TD_MODE_MN && cfg->tdx != TD_TDX_ON) {
        td_format_error(err, err_len, "mn config requires tdx: on");
        return -1;
    }
    if (cfg->transport == TD_TRANSPORT_RDMA && cfg->rdma_port_num <= 0) {
        td_format_error(err, err_len, "rdma_port_num must be greater than zero");
        return -1;
    }
    if (cfg->transport == TD_TRANSPORT_RDMA && cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_OOB_FILE) {
        if (cfg->rdma_oob_dir[0] == '\0') {
            td_format_error(err, err_len, "rdma transport with file bootstrap requires rdma_oob_dir");
            return -1;
        }
        if (cfg->rdma_oob_poll_ms == 0) {
            td_format_error(err, err_len, "rdma_oob_poll_ms must be greater than zero");
            return -1;
        }
        if (cfg->rdma_oob_timeout_ms == 0) {
            td_format_error(err, err_len, "rdma_oob_timeout_ms must be greater than zero");
            return -1;
        }
        if (cfg->mode == TD_MODE_MN && cfg->node_id < 0) {
            td_format_error(err, err_len, "mn config with file bootstrap requires node_id");
            return -1;
        }
        if (cfg->mode == TD_MODE_CN) {
            size_t idx;

            for (idx = 0; idx < cfg->mn_count; ++idx) {
                if (cfg->mn_endpoints[idx].node_id < 0) {
                    td_format_error(err, err_len, "cn config with file bootstrap requires mn_node_id entries");
                    return -1;
                }
            }
        }
    }
    if (cfg->transport == TD_TRANSPORT_RDMA &&
        (cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_TCP || cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK) &&
        cfg->mode == TD_MODE_MN &&
        cfg->listen_port <= 0) {
        td_format_error(err, err_len, "mn config with %s bootstrap requires listen_port",
            cfg->rdma_bootstrap == TD_RDMA_BOOTSTRAP_VSOCK ? "vsock" : "tcp");
        return -1;
    }
    if (cfg->mode == TD_MODE_MN && td_resolve_mn_slots(cfg, err, err_len) != 0) {
        return -1;
    }
    if (cfg->encryption_key_hex[0] == '\0') {
        td_format_error(err, err_len, "encryption_key_hex must be set");
        return -1;
    }
    return 0;
}
