Objective

Refactor `dist-td` into `dist-tdx` so that an MN running inside an Intel TDX guest can keep one-sided RDMA semantics without violating the TDX memory model.

The problem is not "how to do RDMA through TDCALL".
The problem is "how to expose only the correct memory to RDMA while the MN stays TDX-isolated".

1. Ground Truth

1.1 Fabric vs IPoIB

For InfiniBand, the following state is sufficient for verbs-level RDMA:

- `ibstat` reports `State: Active`
- `Physical state: LinkUp`
- a valid `SM lid`

This means the IB fabric is alive and routable.

`ip link show` reporting the IB netdev as `DOWN` only means IPoIB is not configured or not brought up.
It does NOT imply that verbs RDMA is unavailable.

Therefore:

- IPoIB is optional
- RDMA data path must not depend on IPoIB
- `rdma_cm` over IP is not the core transport model anymore

1.2 Absolute Rules

- RDMA MUST touch only the shared-exposed region
- MN private metadata MUST never be RDMA-visible
- TDCALL MUST NOT appear in the RDMA hot path
- CN remains responsible for encryption and integrity of shared slot contents

2. Transport Architecture

2.1 Split the transport into two planes

Control/bootstrap plane:

- ordinary TCP socket
- used only to exchange RC QP connection attributes
- may use any reachable management IP network

RDMA data plane:

- manual verbs RC QP setup
- one-sided RDMA READ/WRITE for slot data
- SEND/RECV only for control messages such as `HELLO`, `CAS`, `EVICT`, `CLOSE`

2.2 Why this change is required

The old `rdma_cm` path implicitly assumed an IP-based RDMA connection model.
That assumption breaks in the target deployment where:

- the IB fabric is active
- IPoIB is absent or intentionally left down
- RDMA must still work between CN and MN

So the design must become:

1. bootstrap over TCP
2. exchange `{LID, QPN, PSN, MTU, optional GID}`
3. move QP `INIT -> RTR -> RTS`
4. close the bootstrap socket
5. perform RDMA and control operations over verbs

2.3 Config semantics

Under `transport: rdma`:

- `listen_host` / `listen_port` mean "bootstrap listener", not "RDMA CM listener"
- `mn_endpoint` means "bootstrap endpoint", not "IPoIB data-plane address"
- `rdma_device` may be either:
  - a verbs device name such as `mlx5_0`
  - a netdev alias such as `ibp1s0` or `ibs3`
- `rdma_port_num` selects the verbs port on the HCA
- `rdma_gid_index` is only relevant when the path needs GRH/GID addressing

3. Memory Architecture

3.1 Shared vs private split

Shared region:

- PRIME slots
- CACHE slots
- BACKUP slots
- RDMA-visible local buffers
- remote-visible region header describing shared layout

Private region:

- slot sidecar metadata
- cache usage accounting
- eviction cursor
- locks and control state
- other MN-only management structures

3.2 Slot layout rule

Shared:

- slot shared header
- ciphertext/body payload

Private:

- per-slot sidecar metadata used only by the MN

Shared pointers may be exposed through RDMA.
Private pointers must never leave the MN.

3.3 Forbidden

- RDMA -> private memory
- mixed shared/private control fields inside the remote-visible region header
- storing MN-only metadata in the shared slot layout just because the old code did

4. TDX Integration

4.1 Correct role of TDCALL

TDCALL is for TDX guest interaction and TDX state probing.
It is not a substitute for the Linux guest kernel's private/shared memory management for DMA.

4.2 Important correction

The older mental model:

`shared memory = userspace malloc + raw TDCALL page conversion`

is not a safe or sufficient contract in a Linux TDX guest.

Reason:

- page-table updates and DMA visibility are coordinated by the guest kernel
- raw userspace `tdcall` alone is not enough to make a buffer safely usable for DMA

Therefore the implementation contract is:

- userspace can probe TDX capabilities with a real `tdcall` backend
- userspace allocators must keep RDMA-visible buffers on the TDX-aware allocation path
- actual DMA-shareable behavior for MR registration is delegated to the guest-kernel pinning and DMA-mapping path

4.3 Practical rule

- no raw userspace page-conversion loop in the RDMA fast path
- no attempt to implement Linux DMA semantics purely from userspace
- fail early if the runtime is not the expected TDX mode for the MN deployment

5. RDMA Refactoring Rules

5.1 Registration rule

Anything passed to `ibv_reg_mr()` for the RDMA path must come from the TDX-aware shared allocation contract.

That includes:

- MN shared slot region MR
- control SEND/RECV buffers
- local RDMA READ/WRITE staging buffer
- CAS helper buffer

5.2 Connection rule

Do not rely on:

- `rdma_getaddrinfo`
- `rdma_connect`
- `rdma_accept`
- IPoIB-only addressing assumptions

Do rely on:

- `ibv_open_device`
- `ibv_alloc_pd`
- `ibv_create_cq`
- `ibv_create_qp`
- `ibv_modify_qp`
- direct exchange of QP attributes over bootstrap TCP

5.3 Control-path rule

Control operations remain explicit:

- `HELLO`
- `CAS`
- `EVICT`
- `CLOSE`

These use SEND/RECV on the already-established RC QP.
Slot data movement continues to use one-sided RDMA.

6. Current Repository State

Implemented:

- shared/private region split in layout
- private sidecar metadata for slot management
- shared slot header/body separation
- RDMA-visible local buffers moved onto the shared allocator path
- region header slimmed to remote-visible layout only
- manual verbs RC setup with TCP bootstrap exchange
- support for resolving `rdma_device` from either verbs device names or IB netdev aliases
- `rdma_port_num` config
- real `tdcall` probe backend gated by `TDX_REAL_TDCALL=1`

Not a goal of the current code:

- enabling RDMA by bringing up IPoIB
- depending on `rdma_cm`
- performing Linux-safe DMA page conversion entirely from userspace with raw TDCALL

7. Failure Modes to Watch

7.1 Wrong mental model

Symptom:

- "IB netdev is DOWN, so RDMA must be down"

Reality:

- only IPoIB is down
- verbs RDMA may still be fully available if `ibstat` is active

7.2 Wrong transport assumption

Symptom:

- CN/MN config uses IP endpoint as if it were the RDMA path itself

Fix:

- treat it as bootstrap/control only
- keep RDMA data plane on verbs RC over the active fabric

7.3 Wrong memory assumption

Symptom:

- CQ hangs
- silent DMA failure
- MR registration succeeds but runtime behavior is unstable in TDX

Root cause:

- RDMA-visible memory or the conversion contract is wrong for the guest DMA path

8. Implementation Direction

Phase 1

- separate shared slot storage from private metadata
- move all NIC-visible local buffers onto the shared allocator path

Phase 2

- remove `rdma_cm`/IPoIB assumptions from RDMA transport
- adopt TCP bootstrap plus manual verbs QP connection

Phase 3

- tighten fail-fast behavior around TDX runtime expectations
- improve deployment observability for resolved RDMA device/port/bootstrap endpoint

Phase 4

- performance tuning
- optional transport/bootstrap hardening

9. Key Takeaways

- The critical distinction is fabric-level RDMA vs IPoIB
- The RDMA data plane must work without IPoIB
- The control/bootstrap plane may still use ordinary TCP
- The core problem remains memory-model correctness inside TDX
