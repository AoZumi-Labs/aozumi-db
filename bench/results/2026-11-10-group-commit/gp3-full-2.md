### gp3 full repeat 2 load: 4.64 3.71 2.72
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 318 | 2.93 ms | 3.16 ms | 13.46 ms | txn 2.93 / 3.16 ms | 0 |
| 4 | 416 | 9.23 ms | 19.98 ms | 23.12 ms | txn 9.23 / 19.98 ms | 0 |
| 16 | 414 | 37.66 ms | 51.90 ms | 52.98 ms | txn 37.66 / 51.90 ms | 0 |
| 64 | 416 | 151.03 ms | 166.90 ms | 168.06 ms | txn 151.03 / 166.90 ms | 0 |

rows: 3614900 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.8s (17349 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1020 | 0.19 ms | 75.17 ms | 333.88 ms | read 0.13 / 0.19 ms, update 61.82 / 75.84 ms | 0 |
| B | 10088 | 0.12 ms | 62.28 ms | 72.08 ms | read 0.12 / 0.21 ms, update 60.91 / 74.59 ms | 0 |
| C | 42836 | 0.74 ms | 1.29 ms | 1.64 ms | read 0.74 / 1.29 ms | 0 |
