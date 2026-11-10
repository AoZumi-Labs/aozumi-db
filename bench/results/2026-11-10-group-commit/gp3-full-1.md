### gp3 full repeat 1 load: 4.17 3.48 2.18
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 315 | 2.96 ms | 3.18 ms | 13.55 ms | txn 2.96 / 3.18 ms | 0 |
| 4 | 419 | 9.18 ms | 19.92 ms | 23.02 ms | txn 9.18 / 19.92 ms | 0 |
| 16 | 417 | 37.41 ms | 51.76 ms | 52.28 ms | txn 37.41 / 51.76 ms | 0 |
| 64 | 418 | 149.84 ms | 165.39 ms | 177.15 ms | txn 149.84 / 165.39 ms | 0 |

rows: 3627400 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.7s (17559 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1026 | 0.20 ms | 73.92 ms | 195.98 ms | read 0.13 / 0.20 ms, update 61.98 / 75.50 ms | 0 |
| B | 10067 | 0.12 ms | 62.37 ms | 71.73 ms | read 0.12 / 0.22 ms, update 60.92 / 74.01 ms | 0 |
| C | 43379 | 0.73 ms | 1.32 ms | 1.65 ms | read 0.73 / 1.32 ms | 0 |
