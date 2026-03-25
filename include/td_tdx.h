#ifndef TD_TDX_H
#define TD_TDX_H

#include <stddef.h>
#include <stdint.h>

#include "td_common.h"

typedef enum {
    TD_TDCALL_SUCCESS = 0,
    TD_TDCALL_BUSY = 1,
    TD_TDCALL_INTERRUPTED = 2,
    TD_TDCALL_UNSUPPORTED = 3,
    TD_TDCALL_FATAL = 4,
} td_tdcall_status_t;

typedef enum {
    TD_TDCALL_LEAF_VP_INFO = 1,
    TD_TDCALL_LEAF_VMCALL = 0,
    TD_TDCALL_LEAF_VEINFO_GET = 3,
    TD_TDCALL_LEAF_PAGE_ACCEPT = 6,
    TD_TDCALL_LEAF_PAGE_RELEASE = 30,
} td_tdcall_leaf_t;

typedef struct {
    td_tdx_mode_t enabled;
    int emulated;
    uint64_t gpa_width;
    uint64_t shared_mask;
} td_tdx_runtime_t;

int td_tdx_runtime_init(td_tdx_runtime_t *runtime, td_mode_t mode, td_tdx_mode_t tdx, char *err, size_t err_len);
int td_tdx_map_shared_memory(td_tdx_runtime_t *runtime, size_t bytes, void **out, char *err, size_t err_len);
void td_tdx_unmap_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes);
int td_tdx_accept_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes, char *err, size_t err_len);
int td_tdx_release_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes, char *err, size_t err_len);
const char *td_tdx_runtime_name(const td_tdx_runtime_t *runtime);

#endif
