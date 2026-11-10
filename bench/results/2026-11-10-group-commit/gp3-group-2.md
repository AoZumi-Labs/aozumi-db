### gp3 group repeat 2 load: 5.00 3.63 2.54
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 317 | 2.93 ms | 3.22 ms | 13.48 ms | txn 2.93 / 3.22 ms | 0 |
| 4 | 775 | 4.77 ms | 8.97 ms | 15.64 ms | txn 4.77 / 8.97 ms | 0 |
| 16 | 1734 | 8.66 ms | 15.07 ms | 33.77 ms | txn 8.66 / 15.07 ms | 0 |
| 64 | 1582 | 40.20 ms | 60.71 ms | 81.82 ms | txn 40.20 / 60.71 ms | 0 |

rows: 10173900 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.8s (17095 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 8676 | 2.95 ms | 57.73 ms | 84.13 ms | read 0.21 / 0.52 ms, update 4.79 / 71.61 ms | 0 |
| B | 41923 | 0.57 ms | 5.07 ms | 6.46 ms | read 0.56 / 1.07 ms, update 4.36 / 7.40 ms | 0 |
| C | 43716 | 0.72 ms | 1.30 ms | 1.64 ms | read 0.72 / 1.30 ms | 0 |
