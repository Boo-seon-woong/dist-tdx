# dist-tdx

`dist-tdx` is an RDMA-capable branch of `dist-td` for the case where:

- the `MN` runs inside an Intel TDX guest
- the `CN` runs outside the guest
- only shared memory is allowed to be RDMA-visible
- MN-private metadata must remain non-RDMA-visible

This repository currently supports both TCP and RDMA transports, but the TDX+RDMA path has an important known limitation documented below.

## Current Status

As of 2026-03-26:

- TCP bootstrap works
- RDMA QP setup works
- TDX full shared-region conversion during MN startup works
- shared control buffers are converted and MR-registered
- optional direct shared-region MR registration sometimes works and sometimes fails with `Input/output error`
- CN can skip `HELLO` and use bootstrap metadata directly
- RPC fallback for `READ/WRITE` is implemented

What still does not work reliably in the current external-CN -> TDX-guest-MN deployment:

- inbound RDMA traffic into the guest shared buffers
- end-to-end `read/write/update/delete` over RDMA

In practice, this means the code is past the obvious application-layer failures, but the actual guest RDMA target path is still blocked.

## Build

Requirements:

- C compiler available as `cc`
- `libibverbs`
- `pthread`
- OpenSSL `libcrypto`

Build:

```bash
make
```

Binaries:

- `bin/mn`
- `bin/cn`
- `bin/cn_bench`

## Architecture

### Memory model

The design splits MN state into:

- shared slot region
- private metadata

The shared slot region contains the user-visible slot storage. Private metadata includes eviction/accounting state that must not be exposed through RDMA.

For TDX+RDMA:

- the slot region is mapped through the TDX shared-memory helper
- the region is converted to shared memory after initialization
- small RDMA control buffers are also mapped in shared memory

The current code enables full shared-region conversion by default for TDX+RDMA. To disable that behavior explicitly:

```bash
DISTTDX_FORCE_SHARED_REGION=0 ./bin/mn --config build/config/mn.rdma.off.conf
```

### RDMA model

There are two independent choices in the RDMA path.

Control path:

- `send`
- `write_imm`

Data path:

- `direct`
- `rpc`

Current recommended config for the real TDX deployment uses:

- `rdma_control_mode: write_imm`
- `rdma_data_mode: rpc`
- `rdma_skip_hello: on`

Meaning:

- control operations use `RDMA_WRITE_WITH_IMM`
- `READ/WRITE` are issued as RPCs instead of one-sided direct region access
- CN skips `HELLO` and uses bootstrap metadata directly

## Config Files

Available sample configs:

- `build/config/mn.rdma.conf`
- `build/config/mn.rdma.off.conf`
- `build/config/cn.rdma.conf`
- `build/config/cn.rdma.off.conf`
- `build/config/cn.rdma.local.conf`
- `build/config/cn.rdma.local.off.conf`

The `*.off.conf` files disable the application cache. The `local` CN configs are for same-host bootstrap testing; the non-`local` CN configs match the external `genie` -> TDX guest deployment.

## Important Config Keys

Common:

- `transport: tcp|rdma`
- `tdx: on|off`
- `mn_memory_size`
- `cache: on|off`

RDMA:

- `rdma_bootstrap: tcp|file|vsock`
- `rdma_device`
- `rdma_port_num`
- `rdma_gid_index`
- `rdma_control_mode: send|write_imm`
- `rdma_data_mode: direct|rpc`
- `rdma_skip_hello: on|off`

TCP bootstrap:

- MN:
  - `listen_host`
  - `listen_port`
- CN:
  - `mn_endpoint`

OOB file bootstrap:

- `rdma_oob_dir`
- `node_id`

## Current Deployment Notes

The deployment currently being debugged is:

- CN host: `genie`
- MN: TDX guest
- TCP bootstrap endpoint exposed via the host that runs `run_td`

Guest-side RDMA device:

- `ibp1s0`

CN-side RDMA device:

- `ibp23s0`

Example TDX launch used in this setup:

```bash
sudo /home/seonung/2026/tdx/guest-tools/run_td \
  --rdma-nics 0000:6f:00.0 \
  --sm-mode external \
  --tcp-hostfwd-ports 7301 \
  --tcp-hostfwd-bind-addr 10.20.18.199
```

Example runtime commands:

MN inside guest:

```bash
./bin/mn --config build/config/mn.rdma.off.conf
```

CN on `genie`:

```bash
./bin/cn --config build/config/cn.rdma.off.conf
```

## What Works Today

- MN startup in TDX guest
- shared slot-region creation and conversion
- RDMA bootstrap metadata exchange
- RC QP transition to `RTR/RTS`
- shared control-buffer registration
- `rdma_skip_hello: on`
- mode selection and debug logging for `direct` vs `rpc`

## Known Limitation

The main unresolved issue is this:

- external CN still cannot complete inbound RDMA operations targeting the TDX guest shared userspace buffers

Symptoms:

- CN can connect and enter the CLI
- first `read` or `write` over RDMA stalls or fails
- MN sees no corresponding completion

This affects both:

- earlier `SEND/RECV`-based control flow
- current `write_imm + rpc` control/data flow

So the active blocker appears to be the guest RDMA target path itself, not the higher-level `dist-tdx` protocol design.

## Recommended Way To Read Current Logs

During connection, these lines are the highest-signal ones:

- `impl modes control=... data=... skip_hello=...`
- `bootstrap ... local ...`
- `bootstrap ... remote ...`
- `qp->RTR ...`
- `qp->RTS ...`
- `using direct RDMA region path ...`
- `using RDMA RPC fallback for READ/WRITE ...`
- `post_write_imm ...`
- `waiting for CQ completion ...`

If `data=rpc` is configured and the log still shows direct-path behavior, the binary is stale.

## Debugging Guidance

When debugging the current environment, separate the problem into layers:

1. TDX shared conversion
2. MR registration
3. bootstrap metadata exchange
4. QP state transitions
5. inbound RDMA completion into guest shared buffers

The project is currently blocked at layer 5.
