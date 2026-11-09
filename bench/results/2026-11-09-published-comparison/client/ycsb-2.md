### round 2 load: 0.76 0.96 0.49 rtt: 0.000384s
server: http://172.31.37.64:18090
YCSB, 1000000 records, 64 clients, 60s per workload, 8 cores

load: 1000000 records in 62.5s (15999 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9309 | 12.37 ms | 17.55 ms | 23.08 ms | read 0.17 / 0.25 ms, update 13.27 / 21.37 ms | 0 |
| B | 45609 | 1.38 ms | 2.64 ms | 7.72 ms | read 1.36 / 2.06 ms, update 1.83 / 10.08 ms | 0 |
| C | 45719 | 1.40 ms | 2.06 ms | 2.19 ms | read 1.40 / 2.06 ms | 0 |
| D | 43956 | 1.42 ms | 3.11 ms | 9.93 ms | insert 2.01 / 13.33 ms, read 1.40 / 2.15 ms | 0 |
| E | 2859 | 19.10 ms | 65.47 ms | 85.06 ms | insert 16.49 / 48.42 ms, scan 19.28 / 65.90 ms | 0 |
| F | 8138 | 14.39 ms | 19.99 ms | 26.44 ms | read 0.18 / 0.26 ms, rmw 15.22 / 23.69 ms | 0 |
