#include "td_tdx.h"

#include <setjmp.h>
#include <signal.h>
#include <sys/mman.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint64_t rcx;
    uint64_t rdx;
    uint64_t r8;
    uint64_t r9;
} td_tdcall_args_t;

typedef struct {
    uint64_t rax;
    uint64_t rcx;
    uint64_t rdx;
    uint64_t r8;
    uint64_t r9;
} td_tdcall_result_t;

static td_tdcall_status_t td_tdx_status_from_rax(uint64_t rax) {
    switch (rax) {
        case 0:
            return TD_TDCALL_SUCCESS;
        case 1:
            return TD_TDCALL_BUSY;
        case 2:
            return TD_TDCALL_INTERRUPTED;
        case 3:
            return TD_TDCALL_UNSUPPORTED;
        default:
            return TD_TDCALL_FATAL;
    }
}

#ifdef TD_TDX_REAL_TDCALL
static sigjmp_buf td_tdx_fault_env;

static void td_tdx_fault_handler(int signo) {
    (void)signo;
    siglongjmp(td_tdx_fault_env, 1);
}

static td_tdcall_status_t td_tdx_module_call(td_tdcall_leaf_t leaf, const td_tdcall_args_t *args, td_tdcall_result_t *out) {
    struct sigaction sa_new;
    struct sigaction sa_old_ill;
    struct sigaction sa_old_segv;
    register uint64_t rax __asm__("rax") = (uint64_t)leaf;
    register uint64_t rcx __asm__("rcx") = args != NULL ? args->rcx : 0;
    register uint64_t rdx __asm__("rdx") = args != NULL ? args->rdx : 0;
    register uint64_t r8 __asm__("r8") = args != NULL ? args->r8 : 0;
    register uint64_t r9 __asm__("r9") = args != NULL ? args->r9 : 0;

    memset(&sa_new, 0, sizeof(sa_new));
    sa_new.sa_handler = td_tdx_fault_handler;
    sigemptyset(&sa_new.sa_mask);
    if (sigaction(SIGILL, &sa_new, &sa_old_ill) != 0) {
        return TD_TDCALL_FATAL;
    }
    if (sigaction(SIGSEGV, &sa_new, &sa_old_segv) != 0) {
        (void)sigaction(SIGILL, &sa_old_ill, NULL);
        return TD_TDCALL_FATAL;
    }

    if (sigsetjmp(td_tdx_fault_env, 1) != 0) {
        (void)sigaction(SIGSEGV, &sa_old_segv, NULL);
        (void)sigaction(SIGILL, &sa_old_ill, NULL);
        if (out != NULL) {
            memset(out, 0, sizeof(*out));
        }
        return TD_TDCALL_UNSUPPORTED;
    }

    asm volatile(".byte 0x66, 0x0f, 0x01, 0xcc"
                 : "+a"(rax), "+c"(rcx), "+d"(rdx), "+r"(r8), "+r"(r9)
                 :
                 : "r10", "r11", "r12", "r13", "r14", "r15", "memory", "cc");

    (void)sigaction(SIGSEGV, &sa_old_segv, NULL);
    (void)sigaction(SIGILL, &sa_old_ill, NULL);

    if (out != NULL) {
        out->rax = rax;
        out->rcx = rcx;
        out->rdx = rdx;
        out->r8 = r8;
        out->r9 = r9;
    }
    return td_tdx_status_from_rax(rax);
}
#endif

static td_tdcall_status_t td_tdx_raw_tdcall(td_tdx_runtime_t *runtime, td_tdcall_leaf_t leaf, const td_tdcall_args_t *args, td_tdcall_result_t *out) {
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (runtime == NULL || runtime->enabled != TD_TDX_ON) {
        return TD_TDCALL_SUCCESS;
    }

#ifdef TD_TDX_REAL_TDCALL
    return td_tdx_module_call(leaf, args, out);
#else
    runtime->emulated = 1;
    return TD_TDCALL_SUCCESS;
#endif
}

static int td_tdx_call_with_retry(td_tdx_runtime_t *runtime, td_tdcall_leaf_t leaf, const td_tdcall_args_t *args, td_tdcall_result_t *out, char *err, size_t err_len) {
    for (;;) {
        td_tdcall_status_t status = td_tdx_raw_tdcall(runtime, leaf, args, out);

        if (status == TD_TDCALL_SUCCESS) {
            return 0;
        }
        if (status == TD_TDCALL_BUSY || status == TD_TDCALL_INTERRUPTED) {
            continue;
        }

        td_format_error(err, err_len, "tdcall leaf=%u failed with status=%u", (unsigned int)leaf, (unsigned int)status);
        return -1;
    }
}

#ifdef TD_TDX_REAL_TDCALL
static int td_tdx_probe_runtime_info(td_tdx_runtime_t *runtime, char *err, size_t err_len) {
    td_tdcall_args_t args;
    td_tdcall_result_t result;
    uint64_t gpa_width;

    memset(&args, 0, sizeof(args));
    memset(&result, 0, sizeof(result));
    if (td_tdx_call_with_retry(runtime, TD_TDCALL_LEAF_VP_INFO, &args, &result, err, err_len) != 0) {
        return -1;
    }

    gpa_width = result.rcx & 0x3fu;
    if (gpa_width == 0 || gpa_width >= 64) {
        td_format_error(err, err_len, "tdx vp info returned invalid GPA width %llu", (unsigned long long)gpa_width);
        return -1;
    }

    runtime->gpa_width = gpa_width;
    runtime->shared_mask = 1ULL << (gpa_width - 1);
    runtime->emulated = 0;
    return 0;
}
#endif

int td_tdx_runtime_init(td_tdx_runtime_t *runtime, td_mode_t mode, td_tdx_mode_t tdx, char *err, size_t err_len) {
    memset(runtime, 0, sizeof(*runtime));
    runtime->enabled = tdx;
#ifndef TD_TDX_REAL_TDCALL
    runtime->emulated = tdx == TD_TDX_ON ? 1 : 0;
#endif

    if (mode == TD_MODE_CN && tdx == TD_TDX_ON) {
        td_format_error(err, err_len, "cn must run native; set tdx: off");
        return -1;
    }
    if (mode == TD_MODE_MN && tdx != TD_TDX_ON) {
        td_format_error(err, err_len, "mn must run inside TDX; set tdx: on");
        return -1;
    }

#ifdef TD_TDX_REAL_TDCALL
    if (tdx == TD_TDX_ON && td_tdx_probe_runtime_info(runtime, err, err_len) != 0) {
        return -1;
    }
#endif

    return 0;
}

int td_tdx_map_shared_memory(td_tdx_runtime_t *runtime, size_t bytes, void **out, char *err, size_t err_len) {
    void *mapped;

    (void)runtime;

    if (out == NULL) {
        td_format_error(err, err_len, "shared memory output pointer is required");
        return -1;
    }
    *out = NULL;
    if (bytes == 0) {
        td_format_error(err, err_len, "shared memory allocation size must be greater than zero");
        return -1;
    }

    /*
     * Linux TDX guests perform private/shared transitions in the guest kernel.
     * Userspace RDMA buffers become DMA-shareable when the kernel pins and DMA
     * maps them for MR registration, so keep the userspace mapping private here.
     */
    mapped = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) {
        td_format_error(err, err_len, "shared memory mmap failed");
        return -1;
    }
    memset(mapped, 0, bytes);

    *out = mapped;
    return 0;
}

void td_tdx_unmap_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes) {
    (void)runtime;

    if (base == NULL || bytes == 0) {
        return;
    }
    (void)munmap(base, bytes);
}

int td_tdx_accept_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes, char *err, size_t err_len) {
    /*
     * Raw userspace TDCALL is not sufficient to complete a safe private/shared
     * transition because Linux also has to update the guest page tables around
     * the conversion. Leave the transition to the guest-kernel DMA path.
     */
    (void)runtime;
    (void)base;
    (void)bytes;
    (void)err;
    (void)err_len;

    return 0;
}

int td_tdx_release_shared_memory(td_tdx_runtime_t *runtime, void *base, size_t bytes, char *err, size_t err_len) {
    (void)runtime;
    (void)base;
    (void)bytes;
    (void)err;
    (void)err_len;

    return 0;
}

const char *td_tdx_runtime_name(const td_tdx_runtime_t *runtime) {
    if (runtime == NULL || runtime->enabled != TD_TDX_ON) {
        return "native";
    }
    return runtime->emulated ? "tdx-emulated" : "tdx";
}
