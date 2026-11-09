### round 3 load: 0.90 1.18 0.77 rtt: 0.000426s
server: http://172.31.37.64:18090
YCSB, 1000000 records, 64 clients, 60s per workload, 8 cores

load: 1000000 records in 62.6s (15986 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9334 | 12.36 ms | 17.42 ms | 24.92 ms | read 0.17 / 0.25 ms, update 13.25 / 21.65 ms | 0 |
| B | 43597 | 1.46 ms | 2.30 ms | 4.80 ms | read 1.45 / 2.15 ms, update 1.82 / 6.29 ms | 0 |
| C | 46256 | 1.39 ms | 2.04 ms | 2.18 ms | read 1.39 / 2.04 ms | 0 |
| D | 42073 | 1.49 ms | 2.77 ms | 10.22 ms | insert 2.01 / 13.84 ms, read 1.47 / 2.24 ms | 0 |
| E | 2824 | 19.37 ms | 66.09 ms | 86.52 ms | insert 16.74 / 48.71 ms, scan 19.55 / 66.54 ms | 0 |
| F | 7989 | 14.53 ms | 20.02 ms | 25.42 ms | read 0.18 / 0.26 ms, rmw 15.53 / 21.86 ms | 0 |
