# dist-tdx Current Status and Open Problem

Date: 2026-03-26

## Goal

Refactor `dist-tdx` so the CN can access the MN shared slot region through direct RDMA while the MN runs inside a TDX guest and private metadata remains non-RDMA-visible.

## Work Completed

- Reintroduced direct RDMA `READ/WRITE` for the shared slot region while keeping `HELLO/CAS/EVICT/CLOSE` on `SEND/RECV` control messages.
- Restored `remote_addr` / `rkey` exchange in the RDMA `HELLO` path.
- Wired the TDX runtime to use the validated external shared-memory conversion device `/dev/tdx_shmem` instead of leaving `accept/release` as stubs.
- Converted RDMA-local control buffers before MR registration.
- Converted the MN shared region during region open so it can be exposed as an RDMA-visible window.
- Added `MADV_NOHUGEPAGE` to the shared mapping path.
- Changed `/dev/tdx_shmem` conversion requests to run in 64KB chunks instead of converting the full range in one ioctl.
- Added temporary `[MN-DEBUG]` logging around region open and shared-region MR registration.

## Previously Observed Failure

Initial behavior after wiring region-wide shared conversion:

- Guest command:
  `./bin/mn --config build/config/mn.rdma.off.conf`
- Output stopped at:
  `[MN-DEBUG] opening region transport=rdma tdx=on bytes=16777216`
- The TDX VM became unreachable immediately after that point.

Interpretation:

- The guest was dying during `td_region_open()`, before RDMA bootstrap and before `ibv_reg_mr()`.
- The most likely failure point was region-wide shared conversion inside `td_tdx_accept_shared_memory()`.

## Current Behavior

After adding `MADV_NOHUGEPAGE` and chunked conversion, the guest no longer dies immediately.

MN-side output:

```text
dist-td mn node_id=0 transport=rdma bootstrap=tcp 0.0.0.0:7301 rdma_device=ibp1s0 rdma_port=1 gid_index=0 segment_bytes=16777216 backing=[anonymous-shm] bytes=17179869184
dist-td mn runtime=tdx-emulated shared_only_exposure=yes
dist-td mn layout mode=auto shared=16GB(17179869184) private_meta=49941600B prime=22196214:7635497616B cache=5549053:1908874232B backup=22196213:7635497272B used=17179869120B unused=64B slot_bytes=344
rdma connection setup failed: ibv_reg_mr failed for region segment 1/1024 offset=0 length=16777224: Input/output error
```

CN-side output:

```text
[DEBUG] connecting to 10.20.18.199:7301...
[DEBUG] TCP connected, setting up QP...
[DEBUG] QP setup done, exchanging bootstrap...
[DEBUG] bootstrap done, local lid=2 qpn=81 psn=8268449
[DEBUG] sending HELLO via RDMA...
[DEBUG] HELLO send FAILED: rdma completion failed status=12 (transport retry counter exceeded)
startup error: rdma completion failed status=12 (transport retry counter exceeded)
```

## Current Interpretation

The primary blocker is now:

- Guest-side `ibv_reg_mr()` for the shared region fails on the first segment with `Input/output error`.

The CN failure is secondary:

- TCP bootstrap succeeds.
- QP bootstrap succeeds.
- The MN fails to present a usable remote region / QP state for subsequent data-path operation.
- CN `HELLO` then times out with `transport retry counter exceeded`.

## Important Observations

- The old immediate guest crash was mitigated enough to let the MN reach the RDMA setup path.
- The environment is still not providing a working direct user-memory MR path for the converted shared region.
- The failure happens at the first region segment, not later in the segmented registration loop.
- The shared-memory conversion prototype in `~/2026/share` was validated for smaller test allocations, but the application path still fails when registering the real RDMA window.

## Most Likely Root Cause

One of the following is still true:

- `/dev/tdx_shmem` conversion is sufficient for simple tests but not sufficient for `mlx5` user MR registration on this TDX guest.
- The guest RDMA driver stack still falls back to a DMA path that cannot handle this converted userspace memory correctly.
- The converted pages are visible enough to avoid a hard guest crash, but not valid enough for `ibv_reg_mr()` to build an RDMA MR.

## Current Blocker Statement

`dist-tdx` no longer fails at TCP bootstrap or at basic guest region setup, but direct registration of the TDX guest shared slot region still fails at:

`ibv_reg_mr(...): Input/output error`

This is the active blocker preventing end-to-end direct RDMA access to the guest shared memory.

## Suggested Next Debug Direction

- Capture guest `dmesg` immediately after the `ibv_reg_mr()` failure.
- Check whether `mlx5`, `ib_core`, `iommu`, `swiotlb`, or TDX-related kernel logs explain the EIO.
- Verify whether the failing path is the same with a much smaller application-exposed shared region.
- If direct user MR is still unsupported in this environment, fall back to a smaller explicitly managed RDMA-visible window or revisit a hybrid design.
