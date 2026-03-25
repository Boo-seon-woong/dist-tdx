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
- Default bootstrap plane: file-based OOB rendezvous through a shared directory visible to both CN and MN.
- Optional fallback bootstrap plane: TCP, if a management IP path is intentionally available.

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
- `rdma_device`: either a verbs device name such as `mlx5_0` or a netdev alias such as `ibp1s0` or `ibs3`
- `rdma_port_num`: verbs port number to use on that device
- `rdma_gid_index`: only needed when the fabric path requires a GRH/GID-based address

For `transport: rdma` with `rdma_bootstrap: tcp`:

- `listen_host` / `listen_port`: TCP bootstrap listener on the MN side
- `mn_endpoint`: TCP bootstrap endpoint on the CN side

## TDX memory model

- Shared slot storage is separated from private metadata.
- Slot header/body visible to RDMA remain in the shared region.
- MN-only sidecar metadata remains private.

Userspace raw `TDCALL` is not treated as the mechanism that makes RDMA buffers safe in Linux guests.

- The code can probe TDX with a real `tdcall` backend when built with `TDX_REAL_TDCALL=1`.
- Safe private/shared handling for DMA registration is left to the guest-kernel DMA/pinning path.
- `td_tdx_map_shared_memory()` therefore represents the allocator contract for NIC-visible memory, not a promise that userspace completed a raw page conversion by itself.

## Non-goals

- No TDCALL in the RDMA hot path.
- No RDMA access to private metadata.
- No dependency on IPoIB for the RDMA data plane.
