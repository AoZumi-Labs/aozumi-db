### nvme group repeat 1 load: 4.25 2.21 1.11
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 801 | 1.07 ms | 1.21 ms | 3.56 ms | txn 1.07 / 1.21 ms | 0 |
| 4 | 2110 | 1.28 ms | 4.17 ms | 8.96 ms | txn 1.28 / 4.17 ms | 0 |
| 16 | 1708 | 9.07 ms | 15.81 ms | 17.98 ms | txn 9.07 / 15.81 ms | 0 |
| 64 | 1551 | 42.06 ms | 63.00 ms | 65.48 ms | txn 42.06 / 63.00 ms | 0 |

rows: 14157300 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.4s (18558 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 20795 | 1.36 ms | 3.42 ms | 11.14 ms | read 0.19 / 0.41 ms, update 2.80 / 4.09 ms | 0 |
| B | 41082 | 0.75 ms | 1.51 ms | 4.47 ms | read 0.74 / 1.34 ms, update 1.09 / 4.04 ms | 0 |
| C | 42975 | 0.74 ms | 1.32 ms | 1.68 ms | read 0.74 / 1.32 ms | 0 |
