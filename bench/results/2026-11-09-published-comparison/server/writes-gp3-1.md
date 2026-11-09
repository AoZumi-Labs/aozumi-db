### gp3 repeat 1 sweep 1,2,4,8,16,32,64 load: 2.02 1.81 1.20
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 319 | 2.93 ms | 3.28 ms | 13.22 ms | txn 2.93 / 3.28 ms | 0 |
| 2 | 422 | 4.47 ms | 5.75 ms | 17.93 ms | txn 4.47 / 5.75 ms | 0 |
| 4 | 421 | 9.14 ms | 19.63 ms | 22.83 ms | txn 9.14 / 19.63 ms | 0 |
| 8 | 420 | 18.49 ms | 32.03 ms | 32.57 ms | txn 18.49 / 32.03 ms | 0 |
| 16 | 420 | 37.13 ms | 51.37 ms | 51.91 ms | txn 37.13 / 51.37 ms | 0 |
| 32 | 420 | 74.56 ms | 89.27 ms | 90.07 ms | txn 74.56 / 89.27 ms | 0 |
| 64 | 419 | 149.56 ms | 165.11 ms | 166.10 ms | txn 149.56 / 165.11 ms | 0 |

rows: 9401000 (100 per committed transaction, warmups included)
