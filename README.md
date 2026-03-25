# dist-tdx

`dist-tdx` is the TDX-focused refactoring branch of `dist-td`.

The main problem is not "how to make RDMA use IP" but "how to keep RDMA on the InfiniBand fabric while respecting TDX shared/private memory boundaries".

## Current direction

- MN runs inside an Intel TDX guest.
- CN remains outside the TDX trust boundary.
- RDMA touches only the shared-exposed slot region and RDMA-visible local buffers.
- MN metadata, eviction state, and other control-plane state stay private.

## RDMA connection model

RDMA data path no longer depends on IPoIB and does not require `rdma_cm`.

- Data plane: manual verbs RC QP setup plus one-sided RDMA READ/WRITE.
- Preferred bootstrap plane for the current `run_td` host/guest deployment: TCP bootstrap over QEMU user-net host forwarding.
- Optional shared-filesystem bootstrap plane: file-based OOB rendezvous through a directory that is truly shared between CN and MN.
- Optional bootstrap plane: `vsock`, when the host can reach the guest CID in that QEMU/TDX environment.

The OOB rendezvous exchanges the minimum RC connection state:

- `LID`
- `QPN`
- `PSN`
- `MTU`
- `GID` when the fabric path needs GRH/GID addressing

Implication:

- `ibstat` showing `State: Active` and a valid `SM lid` is enough for verbs-level RDMA setup on InfiniBand.
- `ip link show` reporting the IB netdev as `DOWN` does not by itself block verbs RDMA.
- IPoIB is not required unless a separate IP-based management path is explicitly chosen.

## Config meaning

For `transport: rdma` with `rdma_bootstrap: file`:

- `rdma_oob_dir`: shared directory used for QP attribute rendezvous
- `mn_node_id`: CN-side target MN identifier
- `node_id`: MN-side local node identifier
- `rdma_device`: local device name for that process, either a verbs device name such as `mlx5_0` or a local netdev such as `ibp1s0` or `ibs3`
- `rdma_port_num`: verbs port number to use on that device
- `rdma_gid_index`: only needed when the fabric path requires a GRH/GID-based address

Important:

- creating `/shared/...` manually inside the guest is not enough
- `rdma_oob_dir` must be backed by the same shared filesystem on both host CN and guest MN
- the current `run_td` script does not set up that shared filesystem automatically

For `transport: rdma` with `rdma_bootstrap: tcp`:

- `listen_host` / `listen_port`: TCP bootstrap listener on the MN side
- `mn_endpoint`: TCP bootstrap endpoint on the CN side
- for same-host testing, `run_td --tcp-hostfwd-ports 7301` lets the host CN use `mn_endpoint: 127.0.0.1:7301`
- for an external CN server, bind the forward on the host that actually runs `run_td`; in the current deployment that host is `simba`, so use `run_td --tcp-hostfwd-ports 7301 --tcp-hostfwd-bind-addr 10.20.18.199`, then point the CN to `mn_endpoint: 10.20.18.199:7301`
- sample files:
  `build/config/cn.rdma.local*.conf` are for same-host testing on the `run_td` host
  `build/config/cn.rdma*.conf` currently match the `genie` CN to `simba` TDX-host deployment

For `transport: rdma` with `rdma_bootstrap: vsock`:

- `listen_port`: vsock listener port on the MN side
- `mn_endpoint`: `guest_cid:port` on the CN side
- in some QEMU/TDX combinations the host may still fail to route to the guest CID, so this path is optional rather than assumed

## TDX memory model

- Shared slot storage is separated from private metadata.
- Slot header/body visible to RDMA remain in the shared region.
- MN-only sidecar metadata remains private.

Userspace raw `TDCALL` is not treated as the mechanism that makes RDMA buffers safe in Linux guests.

- The code can probe TDX with a real `tdcall` backend when built with `TDX_REAL_TDCALL=1`.
- Safe private/shared handling for DMA registration is left to the guest-kernel DMA/pinning path.
- `td_tdx_map_shared_memory()` therefore represents the allocator contract for NIC-visible memory, not a promise that userspace completed a raw page conversion by itself.
- The allocator now uses shared-anonymous shmem mappings instead of private COW anonymous mappings for RDMA-visible buffers.
- The shared slot region is exported to CN as segmented MRs rather than one giant MR, so large MN regions do not rely on a single `ibv_reg_mr()` succeeding.
- `rdma_region_segment_bytes` controls the target MR chunk size for the shared slot region and is the main tuning knob when large TDX guest MRs fail with `Input/output error`.

## Non-goals

- No TDCALL in the RDMA hot path.
- No RDMA access to private metadata.
- No dependency on IPoIB for the RDMA data plane.
