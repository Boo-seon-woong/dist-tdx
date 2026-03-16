#include "td_layout.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
    size_t slots = cfg->prime_slots + cfg->cache_slots + cfg->backup_slots;
    return sizeof(td_region_header_t) + (slots * sizeof(td_slot_t));
}

int td_region_open(td_local_region_t *region, const td_config_t *cfg, char *err, size_t err_len) {
    size_t bytes = td_region_required_bytes(cfg);
    void *mapped;

    memset(region, 0, sizeof(*region));
    region->fd = -1;

    if (cfg->transport == TD_TRANSPORT_RDMA) {
        mapped = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        region->anonymous_mapping = 1;
        snprintf(region->backing_path, sizeof(region->backing_path), "%s", "[anonymous-shm]");
    } else {
        region->fd = open(cfg->memory_file, O_RDWR | O_CREAT, 0600);
        if (region->fd < 0) {
            td_format_error(err, err_len, "cannot open backing file %s", cfg->memory_file);
            return -1;
        }
        if (ftruncate(region->fd, (off_t)bytes) != 0) {
            td_format_error(err, err_len, "cannot size backing file %s", cfg->memory_file);
            close(region->fd);
            region->fd = -1;
            return -1;
        }
        mapped = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, region->fd, 0);
        snprintf(region->backing_path, sizeof(region->backing_path), "%s", cfg->memory_file);
    }
    if (mapped == MAP_FAILED) {
        td_format_error(err, err_len, "mmap failed for %s", region->anonymous_mapping ? "[anonymous-shm]" : cfg->memory_file);
        if (region->fd >= 0) {
            close(region->fd);
            region->fd = -1;
        }
        return -1;
    }

    region->base = mapped;
    region->mapped_bytes = bytes;
    region->header = (td_region_header_t *)mapped;
    pthread_mutex_init(&region->lock, NULL);

    memset(region->base, 0, bytes);
    region->header->magic = TD_PROJECT_MAGIC;
    region->header->version = 1;
    region->header->node_id = (uint64_t)(cfg->node_id < 0 ? 0 : cfg->node_id);
    region->header->prime_slot_count = cfg->prime_slots;
    region->header->cache_slot_count = cfg->cache_slots;
    region->header->backup_slot_count = cfg->backup_slots;
    region->header->max_value_size = cfg->max_value_size;
    region->header->cache_usage = 0;
    region->header->eviction_cursor = 0;
    region->header->cache_mode = cfg->cache;
    region->header->region_size = bytes;
    return 0;
}

void td_region_close(td_local_region_t *region) {
    if (region->base != NULL && region->mapped_bytes > 0) {
        munmap(region->base, region->mapped_bytes);
    }
    if (region->fd >= 0) {
        close(region->fd);
    }
    pthread_mutex_destroy(&region->lock);
    memset(region, 0, sizeof(*region));
    region->fd = -1;
}

size_t td_region_kind_base_offset(const td_region_header_t *header, td_region_kind_t kind) {
    size_t offset = sizeof(td_region_header_t);

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
    size_t offset = td_region_kind_base_offset(region->header, kind) + (slot_index * sizeof(td_slot_t));
    return (td_slot_t *)((unsigned char *)region->base + offset);
}

int td_region_read_bytes(td_local_region_t *region, size_t offset, void *buf, size_t len) {
    if (offset + len > region->mapped_bytes) {
        return -1;
    }
    pthread_mutex_lock(&region->lock);
    memcpy(buf, (unsigned char *)region->base + offset, len);
    pthread_mutex_unlock(&region->lock);
    return 0;
}

int td_region_write_bytes(td_local_region_t *region, size_t offset, const void *buf, size_t len) {
    if (offset + len > region->mapped_bytes) {
        return -1;
    }
    pthread_mutex_lock(&region->lock);
    memcpy((unsigned char *)region->base + offset, buf, len);
    pthread_mutex_unlock(&region->lock);
    return 0;
}

int td_region_cas64(td_local_region_t *region, size_t offset, uint64_t compare, uint64_t swap, uint64_t *old_value) {
    uint64_t *ptr;
    uint64_t observed;

    if (offset + sizeof(uint64_t) > region->mapped_bytes || (offset % sizeof(uint64_t)) != 0) {
        return -1;
    }

    ptr = (uint64_t *)((unsigned char *)region->base + offset);
    pthread_mutex_lock(&region->lock);
    observed = *ptr;
    if (observed == compare) {
        *ptr = swap;
    }
    pthread_mutex_unlock(&region->lock);

    if (old_value != NULL) {
        *old_value = observed;
    }
    return 0;
}

size_t td_region_count_cache_usage(td_local_region_t *region) {
    size_t used = 0;
    size_t idx;

    pthread_mutex_lock(&region->lock);
    for (idx = 0; idx < (size_t)region->header->cache_slot_count; ++idx) {
        td_slot_t *slot = td_region_slot_ptr(region, TD_REGION_CACHE, idx);
        if ((slot->flags & TD_SLOT_FLAG_VALID) != 0 && slot->guard_epoch == slot->visible_epoch) {
            ++used;
        }
    }
    region->header->cache_usage = used;
    pthread_mutex_unlock(&region->lock);
    return used;
}

void td_region_evict_if_needed(td_local_region_t *region, size_t threshold_pct) {
    size_t used;
    size_t total = (size_t)region->header->cache_slot_count;
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

    pthread_mutex_lock(&region->lock);
    while (target > 0) {
        size_t idx = (size_t)(region->header->eviction_cursor % region->header->cache_slot_count);
        td_slot_t *slot = td_region_slot_ptr(region, TD_REGION_CACHE, idx);
        if ((slot->flags & TD_SLOT_FLAG_VALID) != 0) {
            memset(slot, 0, sizeof(*slot));
            --target;
        }
        region->header->eviction_cursor = (region->header->eviction_cursor + 1) % region->header->cache_slot_count;
    }
    pthread_mutex_unlock(&region->lock);
    td_region_count_cache_usage(region);
}
