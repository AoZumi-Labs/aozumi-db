### nvme full repeat 1 load: 1.28 1.23 0.66
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 798 | 1.07 ms | 1.30 ms | 4.58 ms | txn 1.07 / 1.30 ms | 0 |
| 4 | 1673 | 1.87 ms | 4.74 ms | 6.54 ms | txn 1.87 / 4.74 ms | 0 |
| 16 | 1756 | 8.54 ms | 15.03 ms | 16.57 ms | txn 8.54 / 15.03 ms | 0 |
| 64 | 1551 | 41.53 ms | 62.57 ms | 65.01 ms | txn 41.53 / 62.57 ms | 0 |

rows: 13291500 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.4s (18647 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9503 | 5.67 ms | 10.11 ms | 15.63 ms | read 0.14 / 0.24 ms, update 6.44 / 10.64 ms | 0 |
| B | 41691 | 0.74 ms | 1.66 ms | 4.39 ms | read 0.73 / 1.29 ms, update 1.13 / 5.06 ms | 0 |
| C | 42940 | 0.74 ms | 1.33 ms | 1.67 ms | read 0.74 / 1.33 ms | 0 |
