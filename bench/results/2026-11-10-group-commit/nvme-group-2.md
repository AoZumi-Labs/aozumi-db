### nvme group repeat 2 load: 5.27 3.09 1.59
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 804 | 1.06 ms | 1.23 ms | 3.74 ms | txn 1.06 / 1.23 ms | 0 |
| 4 | 1885 | 1.27 ms | 4.24 ms | 8.02 ms | txn 1.27 / 4.24 ms | 0 |
| 16 | 1677 | 9.23 ms | 15.89 ms | 17.29 ms | txn 9.23 / 15.89 ms | 0 |
| 64 | 1518 | 43.18 ms | 64.24 ms | 66.90 ms | txn 43.18 / 64.24 ms | 0 |

rows: 13748600 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.3s (18715 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 20785 | 1.36 ms | 3.43 ms | 11.09 ms | read 0.19 / 0.41 ms, update 2.79 / 4.24 ms | 0 |
| B | 41468 | 0.75 ms | 1.49 ms | 4.18 ms | read 0.73 / 1.32 ms, update 1.08 / 3.83 ms | 0 |
| C | 42982 | 0.74 ms | 1.31 ms | 1.65 ms | read 0.74 / 1.31 ms | 0 |
