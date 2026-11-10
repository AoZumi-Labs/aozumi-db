### nvme full repeat 2 load: 4.93 3.46 1.95
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 803 | 1.06 ms | 1.31 ms | 4.45 ms | txn 1.06 / 1.31 ms | 0 |
| 4 | 1762 | 1.85 ms | 4.62 ms | 6.41 ms | txn 1.85 / 4.62 ms | 0 |
| 16 | 1719 | 8.68 ms | 15.33 ms | 17.00 ms | txn 8.68 / 15.33 ms | 0 |
| 64 | 1580 | 40.89 ms | 61.20 ms | 63.51 ms | txn 40.89 / 61.20 ms | 0 |

rows: 13502500 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.3s (18741 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9420 | 5.51 ms | 10.33 ms | 15.69 ms | read 0.14 / 0.24 ms, update 6.48 / 10.99 ms | 0 |
| B | 42903 | 0.70 ms | 1.88 ms | 5.71 ms | read 0.69 / 1.32 ms, update 1.16 / 5.64 ms | 0 |
| C | 43157 | 0.73 ms | 1.33 ms | 1.70 ms | read 0.73 / 1.33 ms | 0 |
