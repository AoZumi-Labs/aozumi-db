### gp3 group repeat 1 load: 3.33 3.07 2.21
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 20s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 317 | 2.93 ms | 3.24 ms | 13.49 ms | txn 2.93 / 3.24 ms | 0 |
| 4 | 775 | 4.69 ms | 8.83 ms | 15.48 ms | txn 4.69 / 8.83 ms | 0 |
| 16 | 1755 | 8.54 ms | 15.24 ms | 32.63 ms | txn 8.54 / 15.24 ms | 0 |
| 64 | 1572 | 40.39 ms | 61.01 ms | 81.19 ms | txn 40.39 / 61.01 ms | 0 |

rows: 10208500 (100 per committed transaction, warmups included)
server: http://127.0.0.1:18091
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.7s (17433 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 8850 | 2.94 ms | 57.42 ms | 84.38 ms | read 0.21 / 0.52 ms, update 4.81 / 82.81 ms | 0 |
| B | 39195 | 0.63 ms | 5.10 ms | 6.50 ms | read 0.61 / 1.11 ms, update 4.39 / 7.32 ms | 0 |
| C | 40997 | 0.77 ms | 1.30 ms | 1.65 ms | read 0.77 / 1.30 ms | 0 |
