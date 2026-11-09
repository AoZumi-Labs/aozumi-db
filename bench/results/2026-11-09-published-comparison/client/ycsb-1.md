### round 1 load: 0.00 0.00 0.03 rtt: 0.000382s
server: http://172.31.37.64:18090
YCSB, 1000000 records, 64 clients, 60s per workload, 8 cores

load: 1000000 records in 62.7s (15937 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9877 | 11.59 ms | 16.79 ms | 22.32 ms | read 0.17 / 0.25 ms, update 12.52 / 21.21 ms | 0 |
| B | 42758 | 1.50 ms | 2.29 ms | 4.06 ms | read 1.48 / 2.20 ms, update 1.82 / 5.02 ms | 0 |
| C | 47015 | 1.37 ms | 2.00 ms | 2.12 ms | read 1.37 / 2.00 ms | 0 |
| D | 41517 | 1.53 ms | 2.51 ms | 8.56 ms | insert 1.97 / 11.19 ms, read 1.51 / 2.27 ms | 0 |
| E | 2853 | 19.25 ms | 64.61 ms | 83.91 ms | insert 16.64 / 47.30 ms, scan 19.43 / 65.06 ms | 0 |
| F | 8111 | 14.34 ms | 19.98 ms | 24.96 ms | read 0.18 / 0.26 ms, rmw 15.27 / 21.85 ms | 0 |
