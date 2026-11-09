### nvme repeat 1 sweep 1,2,4,8,16,32,64 load: 0.00 0.05 0.24
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 1 | 801 | 1.06 ms | 1.23 ms | 3.79 ms | txn 1.06 / 1.23 ms | 0 |
| 2 | 1522 | 1.10 ms | 1.60 ms | 5.54 ms | txn 1.10 / 1.60 ms | 0 |
| 4 | 1620 | 1.91 ms | 4.44 ms | 6.54 ms | txn 1.91 / 4.44 ms | 0 |
| 8 | 1847 | 4.01 ms | 7.89 ms | 10.53 ms | txn 4.01 / 7.89 ms | 0 |
| 16 | 1749 | 8.63 ms | 14.76 ms | 16.60 ms | txn 8.63 / 14.76 ms | 0 |
| 32 | 1583 | 19.93 ms | 31.46 ms | 33.95 ms | txn 19.93 / 31.46 ms | 0 |
| 64 | 1521 | 42.67 ms | 63.78 ms | 66.66 ms | txn 42.67 / 63.78 ms | 0 |

rows: 35209600 (100 per committed transaction, warmups included)
