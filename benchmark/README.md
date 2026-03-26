# Benchmark

`cn_bench`는 CN 노드에서 직접 workload를 반복 실행하고 latency 통계를 출력하는 벤치마크 러너다.

## Build

```bash
make
```

## Run

```bash
./bin/cn_bench --config build/config/cn.rdma.conf --workload read --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.rdma.conf --workload write --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.rdma.conf --workload update --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.rdma.conf --workload delete --iterations 1000 --bytes 32 --warmup 128
```

출력 컬럼:

- `workload`
- `#bytes`
- `#iterations`
- `t_min[usec]`
- `t_max[usec]`
- `t_typical[usec]`
- `t_avg[usec]`
- `t_stdev[usec]`
- `99% percentile[usec]`
- `99.9% percentile[usec]`

## Warm-up / Seed

- `write`: warm-up만 수행하고 측정은 새 key에 대해 실행
- `read`: 측정 전에 key/value를 미리 써둔 뒤 read 수행
- `update`: 측정 전에 key/value를 미리 써둔 뒤 update 수행
- `delete`: 측정 전에 key/value를 미리 써둔 뒤 delete 수행

`t_typical`은 중앙값(p50)이다.

## Minimal RDMA Repro

`rdma_min`은 `dist-tdx` 프로토콜을 거치지 않고 다음만 검증한다.

- TCP bootstrap
- RC QP 연결
- small MR 등록
- `write`, `read`, `send`, `write_imm` 중 하나의 단발 RDMA 연산

Server 쪽은 TDX guest에서 `--tdx on`으로 실행하면 `/dev/tdx_shmem` shared-convert 경로를 그대로 사용한다.

Build:

```bash
make bin/rdma_min
```

Guest server 예시:

```bash
./bin/rdma_min \
  --mode server \
  --listen 0.0.0.0 \
  --tcp-port 7301 \
  --rdma-device ibp1s0 \
  --rdma-port 1 \
  --gid-index 0 \
  --tdx on \
  --op write \
  --bytes 32
```

Native client 예시:

```bash
./bin/rdma_min \
  --mode client \
  --connect 10.20.18.199 \
  --tcp-port 7301 \
  --rdma-device ibp23s0 \
  --rdma-port 1 \
  --gid-index 0 \
  --op write \
  --bytes 32
```

연산별 기대 결과:

- `write`: client가 `RDMA_WRITE` local completion을 받고 server data buffer가 바뀜
- `read`: server가 seed한 data buffer를 client가 `RDMA_READ` completion 뒤 동일하게 읽어옴
- `send`: server가 `RECV` completion과 payload dump를 찍음
- `write_imm`: client가 `RDMA_WRITE` local completion을 받고 server가 `RECV_RDMA_WITH_IMM` completion과 ctrl/data buffer dump를 찍음

권장 확인 순서:

- `write`
- `read`
- `send`
- `write_imm`
