Objective

Refactor `dist-td` into `dist-tdx` so that an MN running inside an Intel TDX guest can keep one-sided RDMA semantics without violating the TDX memory model.

The problem is not "how to do RDMA through TDCALL".
The problem is "how to expose only the correct memory to RDMA while the MN stays TDX-isolated and while the connection model does not depend on IPoIB".

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
- `rdma_cm` over IP is not the core transport model

1.2 Absolute Rules

- RDMA MUST touch only the shared-exposed region
- MN private metadata MUST never be RDMA-visible
- TDCALL MUST NOT appear in the RDMA hot path
- CN remains responsible for encryption and integrity of shared slot contents

2. Transport Architecture

2.1 Split the transport into two planes

Control/bootstrap plane:

- preferred in the current `run_td` host/guest deployment: TCP bootstrap over QEMU user-net host forwarding
- optional: file-based OOB rendezvous when CN and MN truly share a filesystem
- optional: vsock bootstrap when host-to-guest CID routing works in that QEMU/TDX environment
- used only to exchange RC QP connection attributes

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

1. exchange `{LID, QPN, PSN, MTU, optional GID}` through an OOB channel
2. move QP `INIT -> RTR -> RTS`
3. perform RDMA and control operations directly over verbs

2.3 OOB variants

For the current TDX guest deployment launched by `run_td`, the most practical automated bootstrap is TCP over QEMU user-net host forwarding:

1. MN listens on a configured TCP bootstrap port inside the guest
2. `run_td` forwards that same port from host to guest
3. CN connects to `127.0.0.1:forwarded_port` on the host
4. both sides exchange `{LID, QPN, PSN, MTU, optional GID}` and then move the RC QP to `RTR/RTS`

This avoids:

- any dependency on IPoIB
- any dependency on a shared host/guest filesystem

For example, if MN listens on `7301`, launch the guest with:

- same-host CN case:
  `run_td --tcp-hostfwd-ports 7301`

- external CN case:
  `run_td --tcp-hostfwd-ports 7301 --tcp-hostfwd-bind-addr <run_td-host-ip>`

Then the host CN uses:

- `mn_endpoint: 127.0.0.1:7301`

Then an external CN uses:

- `mn_endpoint: <run_td-host-ip>:7301`

In the current deployment:

- `run_td` runs on `simba`
- the TDX guest therefore physically lives on `simba`
- `genie` is an external CN/SM server
- the bootstrap endpoint must therefore target `simba` rather than `genie`

`vsock` bootstrap remains available as an optional path, but it must not be assumed. In some QEMU/TDX combinations the host fails to route to the guest CID even when `vhost-vsock-pci` is configured.

2.4 OOB file rendezvous

The file-based OOB mechanism is only valid when a shared directory is truly visible to both CN and MN.

Flow:

1. CN creates a request record for a target `mn_node_id`
2. MN polls the shared directory for requests addressed to its `node_id`
3. MN creates its local QP state and publishes a response record
4. CN reads the response record and completes QP transition

This removes the requirement that CN and MN share a reachable IP bootstrap path.

Important:

- creating `/shared/...` only inside the guest is insufficient
- the current `run_td` script does not automatically mount a shared directory for `/shared`
- without a real shared mount, file OOB will stall even if both sides have the same pathname

2.5 Config semantics

Under `transport: rdma` and `rdma_bootstrap: file`:

- `rdma_oob_dir` is mandatory
- `node_id` is mandatory on the MN
- `mn_node_id` entries are mandatory on the CN
- `listen_host` / `listen_port` are irrelevant
- `mn_endpoint` is irrelevant

Under `transport: rdma` and `rdma_bootstrap: tcp`:

- `listen_host` / `listen_port` mean bootstrap listener
- `mn_endpoint` means bootstrap endpoint

Under `transport: rdma` and `rdma_bootstrap: vsock`:

- `listen_port` is mandatory on the MN
- `mn_endpoint` means `guest_cid:port` on the CN
- use this only when host-to-guest CID routing is confirmed in the current QEMU/TDX environment

Shared RDMA settings:

- `rdma_device` may be either:
  - a verbs device name such as `mlx5_0`
  - a local netdev such as `ibp1s0` or `ibs3`
- `rdma_port_num` selects the verbs port on the HCA
- `rdma_gid_index` is only relevant when the path needs GRH/GID addressing

`rdma_device` is local to each process. CN and guest MN frequently need different values.

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
- OOB exchange of QP attributes

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
- manual verbs RC setup
- support for resolving `rdma_device` from either verbs device names or IB netdev aliases
- `rdma_port_num` config
- file-based OOB bootstrap using a shared rendezvous directory
- optional TCP bootstrap retained as fallback
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

7.2 Wrong bootstrap assumption

Symptom:

- CN/MN config still assumes RDMA requires an IP address

Fix:

- use `rdma_bootstrap: file`
- use `rdma_oob_dir`
- identify peers by `mn_node_id` / `node_id`

7.3 Wrong OOB assumption

Symptom:

- file bootstrap times out

Root cause:

- CN and MN are not actually looking at the same shared directory

Fix:

- mount or expose the same rendezvous directory to both systems
- verify permissions and stale file cleanup

7.4 Wrong memory assumption

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
- adopt OOB exchange of QP state

Phase 3

- tighten fail-fast behavior around TDX runtime expectations
- improve deployment observability for resolved RDMA device/port/OOB path

Phase 4

- performance tuning
- optional additional OOB backends if a shared directory is not available

9. Key Takeaways

- The critical distinction is fabric-level RDMA vs IPoIB
- The RDMA data plane must work without IPoIB
- QP setup still needs an OOB exchange path
- In this refactor, the default OOB path is a shared directory rather than TCP
- The core problem remains memory-model correctness inside TDX
