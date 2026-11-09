### gp3 repeat 2 sweep 64,32,16,8,4,2,1 load: 1.15 1.46 1.19
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 64 | 422 | 148.73 ms | 164.02 ms | 165.89 ms | txn 148.73 / 164.02 ms | 0 |
| 32 | 421 | 74.31 ms | 89.08 ms | 90.40 ms | txn 74.31 / 89.08 ms | 0 |
| 16 | 420 | 37.12 ms | 51.39 ms | 53.43 ms | txn 37.12 / 51.39 ms | 0 |
| 8 | 419 | 18.50 ms | 32.05 ms | 32.72 ms | txn 18.50 / 32.05 ms | 0 |
| 4 | 420 | 9.15 ms | 19.69 ms | 23.00 ms | txn 9.15 / 19.69 ms | 0 |
| 2 | 419 | 4.49 ms | 6.37 ms | 17.91 ms | txn 4.49 / 6.37 ms | 0 |
| 1 | 318 | 2.92 ms | 3.33 ms | 13.15 ms | txn 2.92 / 3.33 ms | 0 |

rows: 9395100 (100 per committed transaction, warmups included)
