# dist-tdx

`dist-tdx` is the TDX-focused refactoring branch of `dist-td`.

The main constraint is not "how to make RDMA work in TDX" but "how to expose only the correct memory to RDMA inside a TDX guest".

## Current direction

- MN runs inside an Intel TDX guest.
- CN remains outside the TDX trust boundary.
- RDMA touches only the shared-exposed slot region and RDMA-visible local buffers.
- MN metadata, eviction state, and other control-plane state stay private.

## RDMA connection model

RDMA data path no longer assumes `rdma_cm` or IPoIB.

- Data plane: manual verbs RC QP setup plus one-sided RDMA READ/WRITE.
- Bootstrap/control plane: ordinary TCP socket used only to exchange QP attributes.
- Required bootstrap data: `LID`, `QPN`, `PSN`, `MTU`, and `GID` when needed.

Implication:

- `ibstat` showing `State: Active` and a valid `SM lid` is enough for verbs-level RDMA setup on InfiniBand.
- `ip link show` reporting the IB netdev as `DOWN` does not by itself block verbs RDMA.
- IPoIB is not required unless a separate IP-based tool or protocol needs it.

## Config meaning

- `listen_host` / `listen_port`: TCP bootstrap listener on the MN side.
- `mn_endpoint`: TCP bootstrap endpoint on the CN side.
- `rdma_device`: either a verbs device name such as `mlx5_0` or a netdev alias such as `ibp1s0` or `ibs3`.
- `rdma_port_num`: verbs port number to use on that device.
- `rdma_gid_index`: only needed when the fabric path requires a GRH/GID-based address.

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
