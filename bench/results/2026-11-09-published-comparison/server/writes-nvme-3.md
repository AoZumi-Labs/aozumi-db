### nvme repeat 3 sweep 1,2,4,8,16,32,64 load: 1.35 1.56 0.96
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 795 | 1.07 ms | 1.27 ms | 3.58 ms | txn 1.07 / 1.27 ms | 0 |
| 2 | 1566 | 1.07 ms | 1.60 ms | 4.91 ms | txn 1.07 / 1.60 ms | 0 |
| 4 | 1527 | 1.84 ms | 4.58 ms | 6.96 ms | txn 1.84 / 4.58 ms | 0 |
| 8 | 1753 | 3.94 ms | 7.99 ms | 10.29 ms | txn 3.94 / 7.99 ms | 0 |
| 16 | 1644 | 9.35 ms | 16.46 ms | 18.49 ms | txn 9.35 / 16.46 ms | 0 |
| 32 | 1587 | 19.91 ms | 31.35 ms | 33.44 ms | txn 19.91 / 31.35 ms | 0 |
| 64 | 1487 | 43.84 ms | 65.14 ms | 67.76 ms | txn 43.84 / 65.14 ms | 0 |

rows: 34371200 (100 per committed transaction, warmups included)
