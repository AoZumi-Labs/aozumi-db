### round 1 libsql ycsb load: 6.36 20.84 28.20
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 12.7s (7863 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 944 | 9.68 ms | 85.96 ms | 477.97 ms | read 0.39 / 9.88 ms, update 44.79 / 112.03 ms | 57 (0.6%) |
| B | 4582 | 6.65 ms | 13.94 ms | 25.39 ms | read 6.63 / 13.63 ms, update 7.10 / 32.85 ms | 2 (0.0%) |
| C | 4358 | 7.05 ms | 14.13 ms | 19.27 ms | read 7.05 / 14.13 ms | 0 |
| D | 1087 | 6.85 ms | 17.23 ms | 65.62 ms | insert 8.76 / 68.36 ms, read 6.77 / 14.61 ms | 37 (0.3%), 32 timed out |
| E | 40 | 5.83 ms | 38.32 ms | 58.74 ms | scan 5.83 / 38.32 ms | 32 (7.2%), 32 timed out |
| F | 5 | 6.29 ms | 28.02 ms | 28.02 ms | read 6.29 / 28.02 ms | 32 (40.5%), 32 timed out |
### round 1 libsql latency load: 30.78 22.98 28.22
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 782/s | 1640.33 ms | 2963.91 ms | 3253.04 ms | 3322.10 ms | 3377.25 ms | 2184 (21.8%), 2184 timed out |
### round 1 aozumi-full ycsb load: 23.94 41.81 44.82
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 3.3s (30072 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 15311 | 0.95 ms | 6.49 ms | 38.82 ms | read 0.13 / 0.44 ms, update 3.52 / 9.98 ms | 0 |
| B | 36482 | 0.81 ms | 2.03 ms | 8.49 ms | read 0.80 / 1.92 ms, update 0.97 / 7.60 ms | 0 |
| C | 39851 | 0.78 ms | 1.66 ms | 2.69 ms | read 0.78 / 1.66 ms | 0 |
| D | 32160 | 0.87 ms | 4.35 ms | 12.16 ms | insert 1.14 / 9.32 ms, read 0.86 / 3.95 ms | 0 |
| E | 5099 | 5.89 ms | 14.59 ms | 29.81 ms | insert 5.34 / 49.10 ms, scan 5.92 / 14.51 ms | 0 |
| F | 13254 | 0.62 ms | 7.39 ms | 38.67 ms | read 0.15 / 0.31 ms, rmw 4.12 / 22.97 ms | 0 |
### round 1 aozumi-full latency load: 12.83 34.99 42.03
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.70 ms | 12.52 ms | 217.32 ms | 281.38 ms | 282.50 ms | 0 |
### round 1 aozumi-normal ycsb load: 11.25 33.92 41.57
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 4.1s (24197 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 13678 | 1.70 ms | 11.79 ms | 82.28 ms | read 0.16 / 0.66 ms, update 2.98 / 18.44 ms | 0 |
| B | 23161 | 0.90 ms | 7.12 ms | 43.70 ms | read 0.89 / 6.79 ms, update 0.98 / 12.02 ms | 0 |
| C | 37436 | 0.79 ms | 1.82 ms | 5.42 ms | read 0.79 / 1.82 ms | 0 |
| D | 31585 | 0.86 ms | 4.55 ms | 13.02 ms | insert 1.01 / 9.02 ms, read 0.86 / 4.38 ms | 0 |
| E | 2234 | 8.79 ms | 168.19 ms | 568.66 ms | insert 7.73 / 180.49 ms, scan 8.84 / 164.93 ms | 0 |
| F | 11759 | 2.61 ms | 17.30 ms | 82.09 ms | read 0.18 / 0.89 ms, rmw 3.98 / 37.83 ms | 0 |
### round 1 aozumi-normal latency load: 21.77 32.17 40.31
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.47 ms | 6.45 ms | 229.45 ms | 258.40 ms | 263.34 ms | 0 |
### round 2 aozumi-normal ycsb load: 18.81 31.19 39.87
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 11.4s (8749 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 6752 | 2.44 ms | 28.11 ms | 62.38 ms | read 0.18 / 1.94 ms, update 4.86 / 40.12 ms | 0 |
| B | 30609 | 0.89 ms | 3.95 ms | 13.50 ms | read 0.89 / 3.72 ms, update 1.01 / 8.65 ms | 0 |
| C | 33395 | 0.88 ms | 2.57 ms | 5.32 ms | read 0.88 / 2.57 ms | 0 |
| D | 26589 | 0.97 ms | 5.91 ms | 24.53 ms | insert 1.19 / 25.77 ms, read 0.96 / 5.04 ms | 0 |
| E | 4646 | 6.37 ms | 17.97 ms | 35.72 ms | insert 5.43 / 14.64 ms, scan 6.42 / 18.03 ms | 0 |
| F | 12480 | 1.35 ms | 13.63 ms | 59.13 ms | read 0.17 / 0.70 ms, rmw 3.83 / 23.19 ms | 0 |
### round 2 aozumi-normal latency load: 15.90 27.62 37.82
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.16 ms | 2.07 ms | 18.53 ms | 58.76 ms | 66.86 ms | 0 |
### round 2 aozumi-full ycsb load: 14.50 26.94 37.46
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 4.0s (25010 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 11714 | 1.76 ms | 12.08 ms | 58.69 ms | read 0.16 / 0.81 ms, update 4.33 / 18.16 ms | 0 |
| B | 32063 | 0.88 ms | 3.16 ms | 11.49 ms | read 0.87 / 2.94 ms, update 1.07 / 8.41 ms | 0 |
| C | 35022 | 0.84 ms | 2.39 ms | 6.54 ms | read 0.84 / 2.39 ms | 0 |
| D | 28723 | 0.93 ms | 4.99 ms | 14.40 ms | insert 1.25 / 11.51 ms, read 0.92 / 4.67 ms | 0 |
| E | 4085 | 7.00 ms | 21.06 ms | 119.57 ms | insert 6.24 / 21.92 ms, scan 7.05 / 20.99 ms | 0 |
| F | 11498 | 0.94 ms | 8.98 ms | 58.99 ms | read 0.15 / 0.40 ms, rmw 4.74 / 23.41 ms | 0 |
### round 2 aozumi-full latency load: 10.78 23.39 35.35
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.43 ms | 3.38 ms | 213.01 ms | 283.47 ms | 285.76 ms | 0 |
### round 2 libsql ycsb load: 10.22 22.87 35.02
YCSB, 100000 records, 32 clients, 10s per workload, 6 cores

load: 100000 records in 13.6s (7377 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1103 | 8.06 ms | 108.47 ms | 159.57 ms | read 0.38 / 9.00 ms, update 45.61 / 132.41 ms | 34 (0.3%) |
| B | 1895 | 6.55 ms | 22.43 ms | 87.73 ms | read 6.52 / 21.58 ms, update 7.08 / 32.49 ms | 49 (0.1%), 32 timed out |
| C | 3510 | 7.79 ms | 35.09 ms | 73.11 ms | read 7.79 / 35.09 ms | 0 |
| D | 47 | 3.56 ms | 16.60 ms | 361.74 ms | read 3.56 / 16.60 ms | 32 (6.1%), 32 timed out |
| E | 38 | 15.41 ms | 112.98 ms | 123.79 ms | scan 15.41 / 112.98 ms | 32 (7.2%), 32 timed out |
| F | 5 | 11.91 ms | 32.98 ms | 32.98 ms | read 11.91 / 32.98 ms | 32 (40.5%), 32 timed out |
### round 2 libsql latency load: 36.74 26.59 35.11
Open-loop write latency, 1000 tx/s, 32 connections, 10s, 6 cores

