#include "td_layout.h"
#include "td_config.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static td_region_header_t *td_region_header_mut(td_local_region_t *region) {
    return &region->private_state.header;
}

static pthread_mutex_t *td_region_private_lock(td_local_region_t *region) {
    return &region->private_state.lock;
}

static uint8_t **td_region_slot_state_array_mut(td_local_region_t *region, td_region_kind_t kind) {
    switch (kind) {
        case TD_REGION_PRIME:
            return &region->private_state.prime_slot_state;
        case TD_REGION_CACHE:
            return &region->private_state.cache_slot_state;
        case TD_REGION_BACKUP:
            return &region->private_state.backup_slot_state;
    }
    return NULL;
}

static uint8_t *td_region_slot_state_array(td_local_region_t *region, td_region_kind_t kind) {
    uint8_t **slot_state = td_region_slot_state_array_mut(region, kind);

    return slot_state != NULL ? *slot_state : NULL;
}

static td_slot_private_state_t td_region_slot_private_state_from_slot(const td_slot_t *slot) {
    if (slot == NULL) {
        return TD_SLOT_PRIVATE_EMPTY;
    }
    if ((slot->header.flags & TD_SLOT_FLAG_VALID) == 0) {
        return TD_SLOT_PRIVATE_EMPTY;
    }
    if ((slot->header.flags & TD_SLOT_FLAG_TOMBSTONE) != 0) {
        return TD_SLOT_PRIVATE_TOMBSTONE;
    }
    return TD_SLOT_PRIVATE_LIVE;
}

static int td_region_private_state_is_live(td_slot_private_state_t state) {
    return state == TD_SLOT_PRIVATE_LIVE;
}

static void td_region_set_slot_private_state_locked(td_local_region_t *region, td_region_kind_t kind, size_t slot_index, td_slot_private_state_t new_state) {
    uint8_t *slot_state = td_region_slot_state_array(region, kind);
    td_slot_private_state_t old_state;

    if (slot_state == NULL) {
        return;
    }

    old_state = (td_slot_private_state_t)slot_state[slot_index];
    slot_state[slot_index] = (uint8_t)new_state;
    if (kind == TD_REGION_CACHE) {
        if (td_region_private_state_is_live(old_state) && !td_region_private_state_is_live(new_state) && region->private_state.cache_usage > 0) {
            --region->private_state.cache_usage;
        } else if (!td_region_private_state_is_live(old_state) && td_region_private_state_is_live(new_state)) {
            ++region->private_state.cache_usage;
        }
    }
}

static int td_region_resolve_slot_offset(const td_region_header_t *header, size_t offset, td_region_kind_t *kind, size_t *slot_index) {
    td_region_kind_t candidate;

    for (candidate = TD_REGION_PRIME; candidate <= TD_REGION_BACKUP; candidate = (td_region_kind_t)(candidate + 1)) {
        size_t base = td_region_kind_base_offset(header, candidate);
        size_t count = td_region_kind_slot_count(header, candidate);
        size_t region_bytes = count * sizeof(td_slot_t);

        if (offset < base || offset >= base + region_bytes) {
            continue;
        }
        if (((offset - base) % sizeof(td_slot_t)) != 0) {
            return -1;
        }

        *kind = candidate;
        *slot_index = (offset - base) / sizeof(td_slot_t);
        return 0;
    }
    return -1;
}

static void td_region_refresh_private_slot_state_locked(td_local_region_t *region, td_region_kind_t kind, size_t slot_index) {
    td_slot_t *slot = td_region_slot_ptr(region, kind, slot_index);
    td_slot_private_state_t new_state = TD_SLOT_PRIVATE_EMPTY;

    if (slot->header.guard_epoch == slot->header.visible_epoch) {
        new_state = td_region_slot_private_state_from_slot(slot);
    }
    td_region_set_slot_private_state_locked(region, kind, slot_index, new_state);
}

static int td_region_alloc_slot_state(uint8_t **slot_state, size_t count, char *err, size_t err_len) {
    if (slot_state == NULL) {
        return -1;
    }
    *slot_state = NULL;
    if (count == 0) {
        return 0;
    }

    *slot_state = (uint8_t *)calloc(count, sizeof(**slot_state));
    if (*slot_state == NULL) {
        td_format_error(err, err_len, "cannot allocate private slot metadata for %zu slots", count);
        return -1;
    }
    return 0;
}

static void td_region_free_private_slot_state(td_local_region_t *region) {
    free(region->private_state.prime_slot_state);
    free(region->private_state.cache_slot_state);
    free(region->private_state.backup_slot_state);
    region->private_state.prime_slot_state = NULL;
    region->private_state.cache_slot_state = NULL;
    region->private_state.backup_slot_state = NULL;
}

const td_region_header_t *td_region_header_view(const td_local_region_t *region) {
    return &region->private_state.header;
}

void *td_region_shared_base(td_local_region_t *region) {
    return region->shared.base;
}

size_t td_region_shared_bytes(const td_local_region_t *region) {
    return region->shared.mapped_bytes;
}

const char *td_region_backing_path(const td_local_region_t *region) {
    return region->shared.backing_path;
}

const td_tdx_runtime_t *td_region_tdx_runtime(const td_local_region_t *region) {
    return &region->shared.tdx;
}

size_t td_region_private_bytes(const td_local_region_t *region) {
    const td_region_header_t *header = td_region_header_view(region);

    return sizeof(td_private_region_t) +
        td_region_kind_slot_count(header, TD_REGION_PRIME) +
        td_region_kind_slot_count(header, TD_REGION_CACHE) +
        td_region_kind_slot_count(header, TD_REGION_BACKUP);
}

size_t td_region_kind_slot_count(const td_region_header_t *header, td_region_kind_t kind) {
    switch (kind) {
        case TD_REGION_PRIME:
            return (size_t)header->prime_slot_count;
        case TD_REGION_CACHE:
            return (size_t)header->cache_slot_count;
        case TD_REGION_BACKUP:
            return (size_t)header->backup_slot_count;
    }
    return 0;
}

size_t td_region_required_bytes(const td_config_t *cfg) {
    return cfg->mn_memory_size;
}

int td_region_open(td_local_region_t *region, const td_config_t *cfg, char *err, size_t err_len) {
    size_t bytes = td_region_required_bytes(cfg);
    void *mapped;
    td_region_header_t *header;
    int force_shared_region = 0;
    int convert_after_init = 0;

    memset(region, 0, sizeof(*region));
    region->shared.fd = -1;

    if (td_tdx_runtime_init(&region->shared.tdx, cfg->mode, cfg->tdx, err, err_len) != 0) {
        return -1;
    }

    if (cfg->transport == TD_TRANSPORT_RDMA) {
        force_shared_region = cfg->tdx == TD_TDX_ON &&
            (getenv("DISTTDX_FORCE_SHARED_REGION") == NULL ||
             strcmp(getenv("DISTTDX_FORCE_SHARED_REGION"), "0") != 0);
        if (cfg->tdx == TD_TDX_ON && !force_shared_region) {
            mapped = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            region->shared.anonymous_mapping = 1;
            region->shared.shared_converted = 0;
            snprintf(region->shared.backing_path, sizeof(region->shared.backing_path), "%s", "[anonymous-private]");
            fprintf(stderr,
                "[MN-INFO] leaving slot region private because DISTTDX_FORCE_SHARED_REGION=0 disabled full shared conversion\n");
            fflush(stderr);
        } else {
            mapped = NULL;
            if (td_tdx_map_shared_memory(&region->shared.tdx, bytes, &mapped, err, err_len) != 0) {
                return -1;
            }
            fprintf(stderr, "[MN-DEBUG] shared region mapped base=%p bytes=%zu\n", mapped, bytes);
            fflush(stderr);
            region->shared.anonymous_mapping = 1;
            region->shared.shared_converted = 0;
            convert_after_init = 1;
            snprintf(region->shared.backing_path, sizeof(region->shared.backing_path), "%s", "[anonymous-shm]");
        }
    } else {
        region->shared.fd = open(cfg->memory_file, O_RDWR | O_CREAT, 0600);
        if (region->shared.fd < 0) {
            td_format_error(err, err_len, "cannot open backing file %s", cfg->memory_file);
            return -1;
        }
        if (ftruncate(region->shared.fd, (off_t)bytes) != 0) {
            td_format_error(err, err_len, "cannot size backing file %s", cfg->memory_file);
            close(region->shared.fd);
            region->shared.fd = -1;
            return -1;
        }
        mapped = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, region->shared.fd, 0);
        snprintf(region->shared.backing_path, sizeof(region->shared.backing_path), "%s", cfg->memory_file);
    }
    if (mapped == MAP_FAILED) {
        td_format_error(err, err_len, "mmap failed for %s", region->shared.anonymous_mapping ? "[anonymous-shm]" : cfg->memory_file);
        if (region->shared.fd >= 0) {
            close(region->shared.fd);
            region->shared.fd = -1;
        }
        return -1;
    }

    region->shared.base = mapped;
    region->shared.mapped_bytes = bytes;
    pthread_mutex_init(td_region_private_lock(region), NULL);

    if (!region->shared.anonymous_mapping) {
        memset(region->shared.base, 0, bytes);
    }
    header = td_region_header_mut(region);
    memset(header, 0, sizeof(*header));
    header->prime_slot_count = cfg->prime_slots;
    header->cache_slot_count = cfg->cache_slots;
    header->backup_slot_count = cfg->backup_slots;
    header->max_value_size = cfg->max_value_size;
    header->region_size = bytes;
    region->private_state.cache_usage = 0;
    region->private_state.eviction_cursor = 0;
    if (td_region_alloc_slot_state(&region->private_state.prime_slot_state, (size_t)header->prime_slot_count, err, err_len) != 0 ||
        td_region_alloc_slot_state(&region->private_state.cache_slot_state, (size_t)header->cache_slot_count, err, err_len) != 0 ||
        td_region_alloc_slot_state(&region->private_state.backup_slot_state, (size_t)header->backup_slot_count, err, err_len) != 0) {
        td_region_close(region);
        return -1;
    }
    if (convert_after_init) {
        if (td_tdx_accept_shared_memory(&region->shared.tdx, region->shared.base, region->shared.mapped_bytes, err, err_len) != 0) {
            td_region_close(region);
            return -1;
        }
        region->shared.shared_converted = 1;
        fprintf(stderr, "[MN-DEBUG] shared region converted after initialization base=%p bytes=%zu\n",
            region->shared.base,
            region->shared.mapped_bytes);
        fflush(stderr);
    }
    return 0;
}

void td_region_close(td_local_region_t *region) {
    td_region_free_private_slot_state(region);
    if (region->shared.base != NULL && region->shared.mapped_bytes > 0) {
        if (region->shared.shared_converted) {
            (void)td_tdx_release_shared_memory(&region->shared.tdx, region->shared.base, region->shared.mapped_bytes, NULL, 0);
        }
        td_tdx_unmap_shared_memory(&region->shared.tdx, region->shared.base, region->shared.mapped_bytes);
    }
    if (region->shared.fd >= 0) {
        close(region->shared.fd);
    }
    pthread_mutex_destroy(td_region_private_lock(region));
    memset(region, 0, sizeof(*region));
    region->shared.fd = -1;
}

size_t td_region_kind_base_offset(const td_region_header_t *header, td_region_kind_t kind) {
    size_t offset = 0;

    if (kind == TD_REGION_PRIME) {
        return offset;
    }
    offset += (size_t)header->prime_slot_count * sizeof(td_slot_t);
    if (kind == TD_REGION_CACHE) {
        return offset;
    }
    offset += (size_t)header->cache_slot_count * sizeof(td_slot_t);
    return offset;
}

size_t td_region_slot_index(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash) {
    size_t count = td_region_kind_slot_count(header, kind);
    return count == 0 ? 0 : (size_t)(key_hash % count);
}

size_t td_region_slot_offset_for_index(const td_region_header_t *header, td_region_kind_t kind, size_t slot_index) {
    return td_region_kind_base_offset(header, kind) + (slot_index * sizeof(td_slot_t));
}

size_t td_region_slot_offset(const td_region_header_t *header, td_region_kind_t kind, uint64_t key_hash) {
    return td_region_slot_offset_for_index(header, kind, td_region_slot_index(header, kind, key_hash));
}

td_slot_t *td_region_slot_ptr(td_local_region_t *region, td_region_kind_t kind, size_t slot_index) {
    size_t offset = td_region_kind_base_offset(td_region_header_view(region), kind) + (slot_index * sizeof(td_slot_t));
    return (td_slot_t *)((unsigned char *)region->shared.base + offset);
}

size_t td_slot_commit_offset(void) {
    return offsetof(td_slot_t, header) + offsetof(td_slot_shared_header_t, visible_epoch);
}

size_t td_slot_commit_length(void) {
    return sizeof(td_slot_t) - td_slot_commit_offset();
}

void *td_slot_commit_ptr(td_slot_t *slot) {
    return (unsigned char *)slot + td_slot_commit_offset();
}

const void *td_slot_commit_const_ptr(const td_slot_t *slot) {
    return (const unsigned char *)slot + td_slot_commit_offset();
}

int td_region_read_bytes(td_local_region_t *region, size_t offset, void *buf, size_t len) {
    if (offset + len > region->shared.mapped_bytes) {
        return -1;
    }
    pthread_mutex_lock(td_region_private_lock(region));
    memcpy(buf, (unsigned char *)region->shared.base + offset, len);
    pthread_mutex_unlock(td_region_private_lock(region));
    return 0;
}

int td_region_write_bytes(td_local_region_t *region, size_t offset, const void *buf, size_t len) {
    if (offset + len > region->shared.mapped_bytes) {
        return -1;
    }
    pthread_mutex_lock(td_region_private_lock(region));
    memcpy((unsigned char *)region->shared.base + offset, buf, len);
    pthread_mutex_unlock(td_region_private_lock(region));
    return 0;
}

int td_region_cas64(td_local_region_t *region, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value) {
    uint64_t *ptr;
    uint64_t observed;

    if (offset + sizeof(uint64_t) > region->shared.mapped_bytes || (offset % sizeof(uint64_t)) != 0) {
        return -1;
    }

    ptr = (uint64_t *)((unsigned char *)region->shared.base + offset);
    pthread_mutex_lock(td_region_private_lock(region));
    observed = *ptr;
    if (observed == compare) {
        td_region_kind_t kind;
        size_t slot_index;

        *ptr = swap;
        if (td_region_resolve_slot_offset(td_region_header_view(region), offset, &kind, &slot_index) == 0) {
            td_region_refresh_private_slot_state_locked(region, kind, slot_index);
        }
    }
    pthread_mutex_unlock(td_region_private_lock(region));

    if (old_value != NULL) {
        *old_value = observed;
    }
    return 0;
}

size_t td_region_count_cache_usage(td_local_region_t *region) {
    size_t cache_usage;

    pthread_mutex_lock(td_region_private_lock(region));
    cache_usage = region->private_state.cache_usage;
    pthread_mutex_unlock(td_region_private_lock(region));
    return cache_usage;
}

void td_region_evict_if_needed(td_local_region_t *region, size_t threshold_pct) {
    size_t used;
    td_region_header_t *header = td_region_header_mut(region);
    uint8_t *cache_slot_state = td_region_slot_state_array(region, TD_REGION_CACHE);
    size_t total = (size_t)header->cache_slot_count;
    size_t target;

    if (total == 0) {
        return;
    }

    used = td_region_count_cache_usage(region);
    if ((used * 100) < (threshold_pct * total)) {
        return;
    }

    target = used / 4;
    if (target == 0) {
        target = 1;
    }

    pthread_mutex_lock(td_region_private_lock(region));
    while (target > 0) {
        size_t idx = region->private_state.eviction_cursor % (size_t)header->cache_slot_count;
        td_slot_t *slot = td_region_slot_ptr(region, TD_REGION_CACHE, idx);
        if (cache_slot_state != NULL && cache_slot_state[idx] != TD_SLOT_PRIVATE_EMPTY) {
            memset(slot, 0, sizeof(*slot));
            td_region_set_slot_private_state_locked(region, TD_REGION_CACHE, idx, TD_SLOT_PRIVATE_EMPTY);
            --target;
        }
        region->private_state.eviction_cursor = (region->private_state.eviction_cursor + 1) % (size_t)header->cache_slot_count;
    }
    pthread_mutex_unlock(td_region_private_lock(region));
}
