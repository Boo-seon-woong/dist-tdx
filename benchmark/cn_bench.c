#include "td_cluster.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    TD_BENCH_WRITE = 0,
    TD_BENCH_READ = 1,
    TD_BENCH_UPDATE = 2,
    TD_BENCH_DELETE = 3,
} td_bench_workload_t;

typedef struct {
    const char *config_path;
    td_bench_workload_t workload;
    const char *workload_name;
    size_t iterations;
    size_t bytes;
    size_t warmup_iterations;
    char key_prefix[TD_KEY_BYTES];
} td_bench_options_t;

typedef struct {
    uint64_t *samples_ns;
    size_t count;
    size_t bytes;
    const char *workload_name;
} td_bench_stats_t;

static void td_bench_usage(const char *argv0) {
    fprintf(stderr,
        "usage: %s --config <path> --workload <write|read|update|delete> [--iterations N] [--bytes N] [--warmup N] [--key-prefix prefix]\n",
        argv0);
}

static int td_parse_workload(const char *text, td_bench_workload_t *workload) {
    if (strcmp(text, "write") == 0) {
        *workload = TD_BENCH_WRITE;
        return 0;
    }
    if (strcmp(text, "read") == 0) {
        *workload = TD_BENCH_READ;
        return 0;
    }
    if (strcmp(text, "update") == 0) {
        *workload = TD_BENCH_UPDATE;
        return 0;
    }
    if (strcmp(text, "delete") == 0) {
        *workload = TD_BENCH_DELETE;
        return 0;
    }
    return -1;
}

static int td_parse_size_arg(const char *text, size_t *out) {
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (end == NULL || *end != '\0') {
        return -1;
    }
    *out = (size_t)value;
    return 0;
}

static int td_bench_parse_args(int argc, char **argv, td_bench_options_t *opts) {
    int i;

    memset(opts, 0, sizeof(*opts));
    opts->iterations = 1000;
    opts->bytes = 16;
    opts->warmup_iterations = 128;
    opts->workload = TD_BENCH_READ;
    opts->workload_name = "read";
    snprintf(opts->key_prefix, sizeof(opts->key_prefix), "%s", "bench");

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            opts->config_path = argv[++i];
        } else if (strcmp(argv[i], "--workload") == 0 && i + 1 < argc) {
            if (td_parse_workload(argv[++i], &opts->workload) != 0) {
                return -1;
            }
            opts->workload_name = argv[i];
        } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (td_parse_size_arg(argv[++i], &opts->iterations) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--bytes") == 0 && i + 1 < argc) {
            if (td_parse_size_arg(argv[++i], &opts->bytes) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            if (td_parse_size_arg(argv[++i], &opts->warmup_iterations) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--key-prefix") == 0 && i + 1 < argc) {
            snprintf(opts->key_prefix, sizeof(opts->key_prefix), "%s", argv[++i]);
        } else {
            return -1;
        }
    }

    if (opts->config_path == NULL || opts->iterations == 0 || opts->bytes == 0 || opts->bytes > TD_MAX_VALUE_SIZE) {
        return -1;
    }
    return 0;
}

static void td_bench_fill_value(unsigned char *buf, size_t bytes, size_t seed) {
    size_t idx;
    for (idx = 0; idx < bytes; ++idx) {
        buf[idx] = (unsigned char)('a' + ((seed + idx) % 26));
    }
}

static void td_bench_make_key(char *buf, size_t buf_len, const td_bench_options_t *opts, size_t idx) {
    snprintf(buf, buf_len, "%s-%s-%08zu", opts->key_prefix, opts->workload_name, idx);
}

static int td_bench_seed_keys(td_cluster_t *cluster, const td_bench_options_t *opts, unsigned char *value, char *err, size_t err_len) {
    size_t idx;
    size_t seed_count = opts->iterations + opts->warmup_iterations;

    for (idx = 0; idx < seed_count; ++idx) {
        char key[TD_KEY_BYTES];
        int rule = 0;
        td_bench_make_key(key, sizeof(key), opts, idx);
        td_bench_fill_value(value, opts->bytes, idx);
        if (td_cluster_write_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
            return -1;
        }
    }
    return 0;
}

static int td_bench_warmup(td_cluster_t *cluster, const td_bench_options_t *opts, unsigned char *value, char *err, size_t err_len) {
    size_t idx;

    if (opts->warmup_iterations == 0) {
        return 0;
    }

    for (idx = 0; idx < opts->warmup_iterations; ++idx) {
        char key[TD_KEY_BYTES];
        int rule = 0;
        int found = 0;
        size_t value_len = 0;
        unsigned char scratch[TD_MAX_VALUE_SIZE];

        td_bench_make_key(key, sizeof(key), opts, idx);
        td_bench_fill_value(value, opts->bytes, idx + 1000);

        switch (opts->workload) {
            case TD_BENCH_WRITE:
                if (td_cluster_write_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
            case TD_BENCH_READ:
                if (td_cluster_read_kv(cluster, key, scratch, &value_len, &found, err, err_len) != 0 || !found) {
                    td_format_error(err, err_len, "warmup read failed for key %s", key);
                    return -1;
                }
                break;
            case TD_BENCH_UPDATE:
                if (td_cluster_update_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
            case TD_BENCH_DELETE:
                if (td_cluster_delete_kv(cluster, key, &rule, err, err_len) != 0) {
                    return -1;
                }
                if (td_cluster_write_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
        }
    }
    return 0;
}

static int td_u64_compare(const void *lhs, const void *rhs) {
    const uint64_t *a = (const uint64_t *)lhs;
    const uint64_t *b = (const uint64_t *)rhs;
    return (*a > *b) - (*a < *b);
}

static double td_percentile_us(uint64_t *sorted_ns, size_t count, double pct) {
    size_t index;
    if (count == 0) {
        return 0.0;
    }
    index = (size_t)ceil(pct * (double)count) - 1;
    if (index >= count) {
        index = count - 1;
    }
    return (double)sorted_ns[index] / 1000.0;
}

static void td_bench_print_stats(const td_bench_stats_t *stats) {
    size_t idx;
    uint64_t min_ns;
    uint64_t max_ns;
    double avg_ns = 0.0;
    double variance = 0.0;
    double delta = 0.0;
    double typical_us;
    double p99_us;
    double p999_us;
    uint64_t *sorted;

    if (stats->count == 0) {
        return;
    }

    min_ns = stats->samples_ns[0];
    max_ns = stats->samples_ns[0];
    for (idx = 0; idx < stats->count; ++idx) {
        if (stats->samples_ns[idx] < min_ns) {
            min_ns = stats->samples_ns[idx];
        }
        if (stats->samples_ns[idx] > max_ns) {
            max_ns = stats->samples_ns[idx];
        }
        avg_ns += (double)stats->samples_ns[idx];
    }
    avg_ns /= (double)stats->count;

    for (idx = 0; idx < stats->count; ++idx) {
        delta = (double)stats->samples_ns[idx] - avg_ns;
        variance += delta * delta;
    }
    variance /= (double)stats->count;

    sorted = (uint64_t *)malloc(stats->count * sizeof(*sorted));
    if (sorted == NULL) {
        return;
    }
    memcpy(sorted, stats->samples_ns, stats->count * sizeof(*sorted));
    qsort(sorted, stats->count, sizeof(*sorted), td_u64_compare);

    typical_us = (double)sorted[stats->count / 2] / 1000.0;
    p99_us = td_percentile_us(sorted, stats->count, 0.99);
    p999_us = td_percentile_us(sorted, stats->count, 0.999);

    printf("workload #bytes #iterations    t_min[usec]    t_max[usec]  t_typical[usec]    t_avg[usec]    t_stdev[usec]   99%% percentile[usec]   99.9%% percentile[usec]\n");
    printf("%-8s %-6zu %-13zu %-14.2f %-12.2f %-17.2f %-14.2f %-16.2f %-22.2f %.2f\n",
        stats->workload_name,
        stats->bytes,
        stats->count,
        (double)min_ns / 1000.0,
        (double)max_ns / 1000.0,
        typical_us,
        avg_ns / 1000.0,
        sqrt(variance) / 1000.0,
        p99_us,
        p999_us);

    free(sorted);
}

static int td_bench_run(td_cluster_t *cluster, const td_bench_options_t *opts, td_bench_stats_t *stats, char *err, size_t err_len) {
    size_t idx;
    unsigned char value[TD_MAX_VALUE_SIZE];
    unsigned char read_buf[TD_MAX_VALUE_SIZE];
    stats->samples_ns = (uint64_t *)calloc(opts->iterations, sizeof(*stats->samples_ns));
    stats->count = opts->iterations;
    stats->bytes = opts->bytes;
    stats->workload_name = opts->workload_name;
    if (stats->samples_ns == NULL) {
        td_format_error(err, err_len, "out of memory");
        return -1;
    }

    if (opts->workload != TD_BENCH_WRITE) {
        if (td_bench_seed_keys(cluster, opts, value, err, err_len) != 0) {
            return -1;
        }
    }
    if (td_bench_warmup(cluster, opts, value, err, err_len) != 0) {
        return -1;
    }

    for (idx = 0; idx < opts->iterations; ++idx) {
        char key[TD_KEY_BYTES];
        uint64_t start_ns;
        int rule = 0;
        int found = 0;
        size_t value_len = 0;

        td_bench_make_key(key, sizeof(key), opts, idx + opts->warmup_iterations);
        td_bench_fill_value(value, opts->bytes, idx + 10000);

        start_ns = td_now_ns();
        switch (opts->workload) {
            case TD_BENCH_WRITE:
                if (td_cluster_write_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
            case TD_BENCH_READ:
                if (td_cluster_read_kv(cluster, key, read_buf, &value_len, &found, err, err_len) != 0 || !found) {
                    td_format_error(err, err_len, "bench read failed for key %s", key);
                    return -1;
                }
                break;
            case TD_BENCH_UPDATE:
                if (td_cluster_update_kv(cluster, key, value, opts->bytes, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
            case TD_BENCH_DELETE:
                if (td_cluster_delete_kv(cluster, key, &rule, err, err_len) != 0) {
                    return -1;
                }
                break;
        }
        stats->samples_ns[idx] = td_now_ns() - start_ns;
    }
    return 0;
}

int main(int argc, char **argv) {
    td_bench_options_t opts;
    td_config_t cfg;
    td_cluster_t cluster;
    td_bench_stats_t stats;
    char err[256];

    memset(&stats, 0, sizeof(stats));
    if (td_bench_parse_args(argc, argv, &opts) != 0) {
        td_bench_usage(argv[0]);
        return 1;
    }
    if (td_config_load(opts.config_path, &cfg, err, sizeof(err)) != 0) {
        fprintf(stderr, "config error: %s\n", err);
        return 1;
    }
    if (cfg.mode != TD_MODE_CN) {
        fprintf(stderr, "config error: mode must be cn\n");
        return 1;
    }
    if (td_cluster_init(&cluster, &cfg, err, sizeof(err)) != 0) {
        fprintf(stderr, "startup error: %s\n", err);
        return 1;
    }
    if (td_bench_run(&cluster, &opts, &stats, err, sizeof(err)) != 0) {
        fprintf(stderr, "benchmark error: %s\n", err);
        free(stats.samples_ns);
        td_cluster_close(&cluster);
        return 1;
    }

    td_bench_print_stats(&stats);
    free(stats.samples_ns);
    td_cluster_close(&cluster);
    return 0;
}
