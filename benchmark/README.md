# Benchmark

`cn_bench`는 CN 노드에서 직접 workload를 반복 실행하고 latency 통계를 출력하는 벤치마크 러너다.

## Build

```bash
make
```

## Run

```bash
./bin/cn_bench --config build/config/cn.conf --workload read --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.conf --workload write --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.conf --workload update --iterations 1000 --bytes 32 --warmup 128
./bin/cn_bench --config build/config/cn.conf --workload delete --iterations 1000 --bytes 32 --warmup 128
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
