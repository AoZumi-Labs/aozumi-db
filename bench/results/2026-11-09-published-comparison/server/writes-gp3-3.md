### gp3 repeat 3 sweep 1,2,4,8,16,32,64 load: 1.03 1.34 1.20
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 320 | 2.91 ms | 3.55 ms | 13.15 ms | txn 2.91 / 3.55 ms | 0 |
| 2 | 418 | 4.52 ms | 6.34 ms | 17.93 ms | txn 4.52 / 6.34 ms | 0 |
| 4 | 418 | 9.20 ms | 19.70 ms | 22.91 ms | txn 9.20 / 19.70 ms | 0 |
| 8 | 417 | 18.61 ms | 32.18 ms | 32.73 ms | txn 18.61 / 32.18 ms | 0 |
| 16 | 418 | 37.38 ms | 51.45 ms | 52.09 ms | txn 37.38 / 51.45 ms | 0 |
| 32 | 419 | 74.83 ms | 89.74 ms | 90.24 ms | txn 74.83 / 89.74 ms | 0 |
| 64 | 417 | 150.68 ms | 166.11 ms | 166.92 ms | txn 150.68 / 166.11 ms | 0 |

rows: 9348400 (100 per committed transaction, warmups included)
