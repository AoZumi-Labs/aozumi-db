### nvme repeat 2 sweep 64,32,16,8,4,2,1 load: 2.02 1.17 0.66
server: http://127.0.0.1:18091
writes: each transaction inserts 100 rows; 30s per step after a 3 s warmup

| connections | tx/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| 64 | 1590 | 40.84 ms | 61.21 ms | 64.72 ms | txn 40.84 / 61.21 ms | 0 |
| 32 | 1670 | 18.90 ms | 30.00 ms | 32.48 ms | txn 18.90 / 30.00 ms | 0 |
| 16 | 1725 | 8.75 ms | 15.21 ms | 16.96 ms | txn 8.75 / 15.21 ms | 0 |
| 8 | 1820 | 3.95 ms | 8.06 ms | 10.00 ms | txn 3.95 / 8.06 ms | 0 |
| 4 | 1691 | 1.88 ms | 4.79 ms | 6.81 ms | txn 1.88 / 4.79 ms | 0 |
| 2 | 1517 | 1.09 ms | 1.55 ms | 4.97 ms | txn 1.09 / 1.55 ms | 0 |
| 1 | 777 | 1.09 ms | 1.25 ms | 4.03 ms | txn 1.09 / 1.25 ms | 0 |

rows: 35620000 (100 per committed transaction, warmups included)
