# dist-tdx Problem Status

Date: 2026-03-26

## Summary

`dist-tdx` now gets past:

- TDX guest region creation
- full shared-region conversion
- RDMA QP bootstrap
- control-buffer MR registration
- optional direct shared-region MR registration
- CN startup without `HELLO`

The active blocker is still the same end-to-end requirement:

- `read/write/update/delete` do not yet work between external CN and TDX guest MN over RDMA

At this point the failure is no longer explained by config mismatch, TCP bootstrap, or missing shared conversion. The remaining failure is in the actual RDMA packet/data path into the TDX guest.

## Current Deployment

- `genie`: CN host
- TDX guest: MN
- TDX guest launched from the host with:

```bash
sudo /home/seonung/2026/tdx/guest-tools/run_td \
  --rdma-nics 0000:6f:00.0 \
  --sm-mode external \
  --tcp-hostfwd-ports 7301 \
  --tcp-hostfwd-bind-addr 10.20.18.199
```

- Guest RDMA device: `ibp1s0`
- CN RDMA device: `ibp23s0`
- Active configs:
  - `build/config/mn.rdma.off.conf`
  - `build/config/cn.rdma.off.conf`

## Current Config Direction

The current `.conf` files intentionally use:

- `rdma_bootstrap: tcp`
- `rdma_control_mode: write_imm`
- `rdma_data_mode: rpc`
- `rdma_skip_hello: on`

Meaning:

- bootstrap is TCP
- control messages use `RDMA_WRITE_WITH_IMM`
- `READ/WRITE` use RPC fallback instead of one-sided direct region access
- CN does not send a separate `HELLO`

Also, `DISTTDX_FORCE_SHARED_REGION` currently defaults to enabled in code for TDX+RDMA. Unless explicitly set to `0`, the MN tries to convert the full slot region to shared memory during startup.

## What Has Been Verified

### 1. Full shared-region conversion works

The guest no longer crashes during region open.

Observed MN log:

```text
[MN-DEBUG] shared region mapped base=... bytes=16777216
[MN-DEBUG] shared region converted after initialization base=... bytes=16777216
[MN-DEBUG] region opened successfully
```

This means the earlier crash was caused by conversion timing during initialization, not by the concept of full-region conversion itself.

### 2. Direct region MR registration is not reliable

Two states have been observed:

- sometimes full shared-region `ibv_reg_mr()` succeeds
- sometimes it fails with:

```text
[MN-WARN] shared region MR registration failed: Input/output error; falling back to SEND/RECV path for READ/WRITE
```

So direct-region exposure in the guest is not stable enough to rely on.

### 3. `HELLO` is no longer the main blocker

Earlier, CN hung on:

```text
[DEBUG] sending HELLO via RDMA...
```

That path is now bypassed by configuration:

```text
[DEBUG] skipping HELLO; using bootstrap metadata directly
```

So the current failure is after bootstrap and after session setup.

### 4. RPC fallback is really selected

Recent CN log confirms that the client is not taking the one-sided direct read/write path:

```text
[DEBUG] rdma session modes: control=write_imm data=rpc skip_hello=on
[DEBUG] using RDMA RPC fallback for READ/WRITE (data_mode=rpc, direct_flag=1)
```

This is important because it rules out "CN is still accidentally trying direct RDMA read/write" as the main explanation.

### 5. `write_imm` access rights were fixed

The control path originally used `RDMA_WRITE_WITH_IMM`, but the responder-side QP and MRs did not allow remote writes.

That was fixed by:

- enabling `IBV_ACCESS_REMOTE_WRITE` on the guest/shared control buffers
- enabling `IBV_ACCESS_REMOTE_WRITE` in QP INIT when `write_imm` mode is active

This fixed the immediate QP-to-ERR transition on first RPC.

## Current Failure

Current CN behavior:

```text
td> read a
[RDMA-DEBUG] post_recv qpn=96 ...
[RDMA-DEBUG] post_write_imm op=READ(2) qpn=96 payload_len=0 ctrl_remote=... op_remote=...
[RDMA-DEBUG] pre-write-imm qp state=RTS ...
[RDMA-DEBUG] waiting for CQ completion expected=RDMA_WRITE empty_polls=50000000 qpn=96
...
```

Current MN behavior at the same time:

```text
[MN-DEBUG] server thread started, qpn=40 lid=1
[MN-DEBUG] still polling CQ, no completions yet ...
```

What that means:

- CN posts `RDMA_WRITE_WITH_IMM`
- CN does not get a local `RDMA_WRITE` completion
- MN does not get `RECV_RDMA_WITH_IMM`
- both QPs are already in `RTS`
- bootstrap metadata is already exchanged

So the current failure point is:

- the first inbound RDMA packet to guest shared memory never completes

## Current Interpretation

The remaining blocker is below the application protocol layer.

The evidence now supports this interpretation:

- TDX guest shared conversion is happening
- RDMA buffers are shared and MR-registered
- QP bootstrap is correct enough to reach `RTS`
- RPC fallback is selected correctly
- control-path permissions are configured correctly

But:

- external CN still cannot complete a remote RDMA write into the TDX guest's shared userspace buffer

That is consistent with a guest/driver/runtime limitation such as:

- inbound DMA into TDX shared-converted userspace pages is not actually functional in this environment
- the passthrough mlx5 path does not complete RC traffic targeting these guest pages
- the memory is "shared enough" for conversion/MR registration but not actually usable as an RDMA destination

## Non-Issues Already Eliminated

These are no longer the leading suspects:

- wrong `rdma_device` naming
- TCP bootstrap failure
- `HELLO` handshake logic
- missing shared conversion of control buffers
- missing `REMOTE_WRITE` access on control MRs
- QP not reaching `RTR/RTS`
- CN accidentally selecting direct one-sided read/write while `rpc` mode is configured

## Practical Conclusion

`dist-tdx` is currently blocked by the guest RDMA data path, not by high-level protocol code.

More specifically:

- outbound setup from the CN is fine
- inbound RDMA traffic into guest shared userspace memory is not completing

Until that environment-level limitation is resolved, end-to-end CRUD over RDMA will remain broken even with RPC fallback.

## Suggested Next Step

The next meaningful test is not another protocol change inside `dist-tdx`.

Instead, validate the environment directly with a minimal verbs test that does only this:

- CN issues `RDMA_WRITE_WITH_IMM` or `SEND`
- target buffer lives in the TDX guest and is shared-converted the same way `dist-tdx` uses it

If that minimal test also hangs, the blocker is definitively outside `dist-tdx`.
