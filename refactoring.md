Objective

Refactor the existing dist-td directory system to operate correctly inside an Intel TDX VM by:

introducing explicit shared/private memory separation
using TDCALL only for memory conversion
ensuring RDMA operates exclusively on shared memory
preserving one-sided RDMA semantics with minimal CPU involvement

The goal is not to make RDMA work via TDCALL,
but to make memory RDMA-compatible using TDCALL.

1. Design Principle (핵심 원칙)
1.1 Absolute Rule

RDMA MUST only touch shared memory

RDMA → shared memory only
CPU  → private + shared
TDCALL → only for conversion
1.2 Separation of Concerns
Layer	Responsibility
RDMA	data movement (shared only)
MN logic	metadata, control
TDCALL	memory state transition
CN	encryption / integrity
1.3 Non-goals
❌ TDCALL inside RDMA fast path
❌ encrypted memory RDMA access
❌ implicit memory conversion
2. Memory Architecture

(기존 문서 기반 확장)

2.1 Memory Regions
+------------------------+
|   Private Region       |
|------------------------|
| metadata               |
| lock / eviction state  |
| index / hash           |
+------------------------+

+------------------------+
|   Shared Region        |
|------------------------|
| PRIME slots            |
| CACHE slots            |
| BACKUP slots           |
| RDMA-visible buffers   |
+------------------------+
2.2 Allocation Rule
❗ 모든 slot은 shared memory에 존재해야 함
slot ∈ shared memory
metadata ∈ private memory
2.3 Forbidden
❌ RDMA → private memory
❌ slot pointer → private region
❌ mixed region struct
2.4 Shared Memory Definition (Critical)
In TDX, shared memory is NOT allocated directly.

All memory is initially private (encrypted), and becomes shared
only after explicit conversion via TDCALL.

Therefore:

    shared memory = private memory + TDCALL conversion
2.5 Memory Conversion Flow
 1. allocate memory (private)
 2. convert page → shared via TDCALL
 3. use as RDMA buffer
2.6 Code-level Contract
void* alloc_shared(size_t size)
{
    void* ptr = malloc(size);        // private

    for (each page in ptr)
        tdcall_convert_to_shared(page);

    return ptr;                      // now shared
}
Without TDCALL conversion, memory remains private and MUST NOT
be used for RDMA.

3. TDCALL Integration
3.1 Role of TDCALL

TDCALL은 "RDMA를 enable"하는 것이 아니라
memory를 RDMA-compatible 상태로 만드는 역할

3.2 Required Operations
TDG.MEM.PAGE.ACCEPT
TDG.MEM.PAGE.RELEASE
3.3 Wrapper Pattern (필수)
static inline void tdcall_retry(...)
{
    while (1) {
        ret = tdcall(...);

        if (ret == SUCCESS)
            return;

        if (ret == BUSY || ret == INTERRUPTED)
            continue;

        panic();
    }
}

(기존 정의 유지 )

3.4 When to Use
✔ 반드시
shared region 초기화 시
memory allocation 시
private ↔ shared 변환 시
3.5 When NOT to Use
❌ RDMA read/write path
❌ slot access
❌ hot data path

(기존 정의 유지 )

4. RDMA Refactoring
4.1 Critical Change

기존:

buf = malloc(size);
ibv_reg_mr(buf, ...);

변경:

buf = alloc_shared(size);   // 반드시 shared
ibv_reg_mr(buf, ...);
4.2 MR Registration Rule

ibv_reg_mr 대상은 반드시 shared memory

4.3 Why This Matters

TDX에서는:

encrypted memory → DMA write-back 실패

따라서:

shared memory → only valid RDMA target
4.4 RDMA Path
CN → RDMA → shared slot
            ↓
       MN private logic (control only)
5. Slot System Refactoring
5.1 Slot Placement
Component	Region
slot body	shared
slot header	shared
slot metadata	private
5.2 Pointer Safety
shared pointer → OK
private pointer → never exposed to RDMA
5.3 Example
struct slot {
    // shared
    char value[...];

    // NOT here:
    // lock, state → private로 분리
};
6. System Flow
6.1 Initialization
1. allocate memory
2. convert to shared (TDCALL)
3. register MR
4. expose to RDMA
6.2 Read (RDMA)
CN → RDMA READ → shared slot
(no MN CPU involvement)
6.3 Write (RDMA)
CN → RDMA WRITE → shared slot
6.4 Control Path
CN → request → MN
             → private metadata update
7. Security Model

(기존 유지 + 명확화)

shared memory = untrusted
CN = encryption / MAC 책임
MN = data 해석하지 않음
❗ 중요
shared memory에서 secret 사용 금지
8. Failure Modes (현재 네 문제 포함)
8.1 Symptom
ib_send_bw hang
ibping hang
no CQ completion
8.2 Root Cause
RDMA buffer = private (encrypted)
→ NIC write-back 실패
→ CQ 없음
8.3 Fix
shared memory로 MR 재구성
9. Implementation Strategy (현실적인 단계)
Phase 1 (필수)
shared allocator 구현
slot을 shared로 이동
Phase 2
RDMA buffer 분리
MR 재등록
Phase 3
metadata 완전 private화
Phase 4 (optional)
ODP support
performance tuning
10. Key Takeaways

RDMA 문제는 네트워크 문제가 아니라 memory model 문제


