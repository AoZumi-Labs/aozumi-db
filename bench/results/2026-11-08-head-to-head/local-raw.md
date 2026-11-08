### round 1 libsql ycsb load: 1.40, 2.35, 2.32
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 24.5s (4087 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 850 | 10.04 ms | 109.21 ms | 316.26 ms | read 1.30 / 10.04 ms, update 40.20 / 187.71 ms | 73 (0.3%), 31 timed out |
| B | 48 | 4.36 ms | 24.06 ms | 37.00 ms | read 4.36 / 24.06 ms | 64 (6.1%), 64 timed out |
| C | 2792 | 10.35 ms | 31.62 ms | 50.08 ms | read 10.35 / 31.62 ms | 0 |
| D | 50 | 4.76 ms | 24.74 ms | 35.44 ms | read 4.76 / 24.74 ms | 64 (5.9%), 64 timed out |
| E | 55 | 14.14 ms | 74.69 ms | 103.46 ms | scan 14.14 / 74.69 ms | 64 (5.2%), 64 timed out |
| F | 4 | 5.99 ms | 36.46 ms | 36.46 ms | read 5.99 / 36.46 ms | 64 (44.8%), 64 timed out |
### round 1 libsql latency load: 85.99, 31.45, 12.73
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 3362.12 ms | 5838.33 ms | 6414.00 ms | 6635.08 ms | 6711.68 ms | 3 (0.0%) |
### round 1 aozumi-full ycsb load: 56.93, 28.99, 12.41
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 6.2s (16155 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 3029 | 3.92 ms | 35.58 ms | 48.34 ms | read 0.30 / 1.58 ms, update 19.61 / 41.64 ms | 0 |
| B | 19329 | 0.78 ms | 21.20 ms | 34.72 ms | read 0.75 / 3.92 ms, update 15.05 / 38.79 ms | 0 |
| C | 25396 | 1.11 ms | 3.79 ms | 21.32 ms | read 1.11 / 3.79 ms | 0 |
| D | 16998 | 0.76 ms | 25.17 ms | 47.07 ms | insert 18.14 / 50.46 ms, read 0.73 / 5.08 ms | 0 |
| E | 2264 | 13.16 ms | 33.88 ms | 45.55 ms | insert 12.85 / 28.85 ms, scan 13.17 / 34.10 ms | 0 |
| F | 2720 | 3.56 ms | 36.48 ms | 61.44 ms | read 0.32 / 1.79 ms, rmw 22.04 / 45.01 ms | 0 |
### round 1 aozumi-full latency load: 12.90, 21.68, 11.81
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 2.09 ms | 3.41 ms | 21.03 ms | 37.68 ms | 45.26 ms | 0 |
### round 1 aozumi-normal ycsb load: 9.60, 20.35, 11.59
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 6.0s (16807 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 11548 | 2.92 ms | 10.25 ms | 40.11 ms | read 0.31 / 1.29 ms, update 4.64 / 23.29 ms | 0 |
| B | 22490 | 1.20 ms | 5.17 ms | 30.63 ms | read 1.19 / 4.95 ms, update 1.34 / 8.96 ms | 0 |
| C | 23981 | 1.11 ms | 5.60 ms | 27.95 ms | read 1.11 / 5.60 ms | 0 |
| D | 18824 | 1.19 ms | 15.30 ms | 55.72 ms | insert 1.48 / 28.98 ms, read 1.18 / 13.73 ms | 0 |
| E | 2342 | 12.68 ms | 33.31 ms | 43.39 ms | insert 11.06 / 25.12 ms, scan 12.79 / 33.49 ms | 0 |
| F | 9590 | 3.26 ms | 13.29 ms | 29.70 ms | read 0.31 / 1.15 ms, rmw 5.73 / 24.94 ms | 0 |
### round 1 aozumi-normal latency load: 8.44, 16.40, 11.24
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.28 ms | 1.86 ms | 3.66 ms | 9.17 ms | 19.34 ms | 0 |
### round 1 sqlite-full ycsb load: 6.04, 15.33, 11.00
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 10118 | 0.09 ms | 1.85 ms | 1030.63 ms | read 0.01 / 0.04 ms, update 0.15 / 34.07 ms | 1 (0.0%) |
| B | 52459 | 0.01 ms | 3.65 ms | 129.17 ms | read 0.01 / 0.08 ms, update 0.30 / 180.48 ms | 0 |
| C | 124456 | 0.18 ms | 1.20 ms | 4.85 ms | read 0.18 / 1.20 ms | 0 |
| D | 46450 | 0.01 ms | 4.13 ms | 129.44 ms | insert 0.34 / 229.62 ms, read 0.01 / 0.07 ms | 0 |
| E | 5171 | 5.19 ms | 20.97 ms | 57.52 ms | insert 2.87 / 83.89 ms, scan 5.35 / 19.83 ms | 0 |
| F | 8919 | 0.09 ms | 33.96 ms | 930.21 ms | read 0.01 / 0.05 ms, rmw 0.16 / 129.31 ms | 1 (0.0%) |
### round 1 sqlite-normal ycsb load: 8.81, 13.31, 10.78
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 43085 | 0.01 ms | 18.39 ms | 104.30 ms | read 0.01 / 0.12 ms, update 0.02 / 33.68 ms | 0 |
| B | 76732 | 0.03 ms | 4.01 ms | 55.65 ms | read 0.03 / 0.74 ms, update 1.16 / 84.22 ms | 0 |
| C | 124656 | 0.18 ms | 1.23 ms | 4.81 ms | read 0.18 / 1.23 ms | 0 |
| D | 76804 | 0.02 ms | 4.17 ms | 73.58 ms | insert 1.18 / 104.75 ms, read 0.02 / 0.58 ms | 0 |
| E | 5200 | 5.13 ms | 21.05 ms | 27.06 ms | insert 1.04 / 10.54 ms, scan 5.53 / 21.16 ms | 0 |
| F | 31638 | 0.02 ms | 18.78 ms | 104.01 ms | read 0.01 / 0.19 ms, rmw 0.04 / 34.09 ms | 0 |
### round 2 aozumi-full ycsb load: 11.75, 12.96, 10.96
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 6.0s (16796 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 3301 | 3.41 ms | 32.18 ms | 47.49 ms | read 0.29 / 0.85 ms, update 18.14 / 38.82 ms | 0 |
| B | 19673 | 0.76 ms | 20.52 ms | 34.69 ms | read 0.73 / 3.58 ms, update 14.94 / 39.41 ms | 0 |
| C | 26447 | 1.05 ms | 4.21 ms | 26.86 ms | read 1.05 / 4.21 ms | 0 |
| D | 17315 | 0.66 ms | 26.21 ms | 48.39 ms | insert 18.37 / 54.13 ms, read 0.63 / 5.54 ms | 0 |
| E | 2261 | 13.21 ms | 34.47 ms | 45.13 ms | insert 13.06 / 31.30 ms, scan 13.21 / 34.58 ms | 0 |
| F | 2946 | 3.56 ms | 34.61 ms | 47.86 ms | read 0.31 / 1.15 ms, rmw 20.34 / 43.41 ms | 0 |
### round 2 aozumi-full latency load: 6.65, 10.60, 10.34
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.96 ms | 3.18 ms | 15.85 ms | 25.74 ms | 35.31 ms | 0 |
### round 2 aozumi-normal ycsb load: 5.03, 9.97, 10.14
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 5.8s (17327 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 12127 | 2.88 ms | 9.95 ms | 38.14 ms | read 0.30 / 1.04 ms, update 4.42 / 23.01 ms | 0 |
| B | 24342 | 1.04 ms | 5.77 ms | 42.60 ms | read 1.04 / 5.31 ms, update 1.21 / 12.97 ms | 0 |
| C | 26704 | 1.02 ms | 4.54 ms | 27.08 ms | read 1.02 / 4.54 ms | 0 |
| D | 18878 | 1.25 ms | 13.74 ms | 44.63 ms | insert 1.52 / 21.87 ms, read 1.24 / 12.50 ms | 0 |
| E | 2331 | 12.72 ms | 33.33 ms | 50.72 ms | insert 10.84 / 26.20 ms, scan 12.85 / 33.50 ms | 0 |
| F | 9800 | 3.56 ms | 12.38 ms | 25.97 ms | read 0.31 / 1.10 ms, rmw 5.69 / 24.25 ms | 0 |
### round 2 aozumi-normal latency load: 8.08, 9.55, 9.96
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.76 ms | 2.34 ms | 4.56 ms | 10.76 ms | 21.11 ms | 0 |
### round 2 sqlite-full ycsb load: 5.94, 8.97, 9.76
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9663 | 0.09 ms | 1.53 ms | 1031.24 ms | read 0.01 / 0.05 ms, update 0.15 / 33.81 ms | 1 (0.0%) |
| B | 48243 | 0.01 ms | 3.71 ms | 129.38 ms | read 0.01 / 0.08 ms, update 0.33 / 229.69 ms | 0 |
| C | 120368 | 0.19 ms | 1.30 ms | 4.98 ms | read 0.19 / 1.30 ms | 0 |
| D | 44380 | 0.01 ms | 3.90 ms | 130.43 ms | insert 0.36 / 230.45 ms, read 0.01 / 0.07 ms | 0 |
| E | 4754 | 5.58 ms | 23.18 ms | 58.05 ms | insert 3.09 / 84.04 ms, scan 5.78 / 22.32 ms | 0 |
| F | 9053 | 0.09 ms | 33.85 ms | 930.65 ms | read 0.01 / 0.05 ms, rmw 0.16 / 129.29 ms | 0 |
### round 2 sqlite-normal ycsb load: 7.11, 8.55, 9.50
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 42091 | 0.01 ms | 18.45 ms | 104.59 ms | read 0.01 / 0.12 ms, update 0.02 / 33.75 ms | 0 |
| B | 76627 | 0.03 ms | 4.07 ms | 55.95 ms | read 0.03 / 0.73 ms, update 1.16 / 104.30 ms | 0 |
| C | 119379 | 0.19 ms | 1.34 ms | 5.20 ms | read 0.19 / 1.34 ms | 0 |
| D | 74809 | 0.02 ms | 4.17 ms | 78.72 ms | insert 1.19 / 104.85 ms, read 0.02 / 0.59 ms | 0 |
| E | 5207 | 5.15 ms | 21.12 ms | 27.44 ms | insert 1.05 / 10.58 ms, scan 5.52 / 21.27 ms | 0 |
| F | 31410 | 0.02 ms | 18.81 ms | 104.07 ms | read 0.01 / 0.20 ms, rmw 0.04 / 34.12 ms | 0 |
### round 2 libsql ycsb load: 9.25, 8.98, 9.54
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 24.6s (4061 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 256 | 8.87 ms | 85.43 ms | 99.53 ms | read 1.24 / 9.25 ms, update 40.05 / 89.63 ms | 67 (1.1%), 64 timed out |
| B | 48 | 4.37 ms | 22.88 ms | 44.35 ms | read 4.37 / 22.88 ms | 64 (6.1%), 64 timed out |
| C | 2600 | 11.31 ms | 29.81 ms | 42.05 ms | read 11.31 / 29.81 ms | 0 |
| D | 50 | 4.44 ms | 18.02 ms | 26.69 ms | read 4.44 / 18.02 ms | 64 (5.9%), 64 timed out |
| E | 55 | 14.17 ms | 55.93 ms | 68.61 ms | scan 14.17 / 55.93 ms | 64 (5.2%), 64 timed out |
| F | 4 | 9.69 ms | 28.67 ms | 28.67 ms | read 9.69 / 28.67 ms | 64 (44.8%), 64 timed out |
### round 2 libsql latency load: 119.88, 44.81, 22.13
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

bench failed (exit 101)
**server exited during the run (crashed)**: sqld hit its 4,096-thread cap and aborted. The end of its log:
    Thread count overflowed the configured max count. Thread index = 4096, max threads = 4096.
    thread caused non-unwinding panic. aborting.
### round 3 aozumi-normal ycsb load: 1.62, 32.75, 52.57
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 5.6s (17750 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 11468 | 3.00 ms | 11.41 ms | 28.37 ms | read 0.31 / 1.17 ms, update 4.68 / 23.43 ms | 0 |
| B | 23827 | 1.08 ms | 6.30 ms | 32.49 ms | read 1.07 / 5.75 ms, update 1.24 / 19.29 ms | 0 |
| C | 23717 | 1.11 ms | 5.40 ms | 34.94 ms | read 1.11 / 5.40 ms | 0 |
| D | 18289 | 1.16 ms | 19.13 ms | 53.10 ms | insert 1.51 / 35.20 ms, read 1.14 / 16.79 ms | 0 |
| E | 2297 | 12.96 ms | 34.58 ms | 46.50 ms | insert 11.02 / 25.77 ms, scan 13.08 / 34.89 ms | 0 |
| F | 9673 | 3.54 ms | 11.50 ms | 33.31 ms | read 0.31 / 1.17 ms, rmw 5.74 / 24.68 ms | 0 |
### round 3 aozumi-normal latency load: 6.98, 24.46, 47.02
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.89 ms | 2.51 ms | 4.85 ms | 14.69 ms | 23.79 ms | 0 |
### round 3 sqlite-full ycsb load: 5.13, 22.91, 46.03
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 9667 | 0.09 ms | 2.00 ms | 1030.84 ms | read 0.01 / 0.05 ms, update 0.15 / 34.47 ms | 5 (0.0%) |
| B | 49609 | 0.01 ms | 3.72 ms | 129.27 ms | read 0.01 / 0.08 ms, update 0.32 / 229.44 ms | 0 |
| C | 119108 | 0.19 ms | 1.38 ms | 5.69 ms | read 0.19 / 1.38 ms | 0 |
| D | 45441 | 0.01 ms | 4.23 ms | 129.47 ms | insert 0.35 / 230.16 ms, read 0.01 / 0.07 ms | 0 |
| E | 5116 | 5.21 ms | 21.23 ms | 58.08 ms | insert 3.02 / 84.68 ms, scan 5.40 / 20.05 ms | 0 |
| F | 8985 | 0.09 ms | 33.99 ms | 930.07 ms | read 0.01 / 0.05 ms, rmw 0.16 / 129.65 ms | 0 |
### round 3 sqlite-normal ycsb load: 9.70, 18.49, 41.46
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 4 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 42483 | 0.01 ms | 18.44 ms | 104.17 ms | read 0.01 / 0.12 ms, update 0.02 / 33.70 ms | 0 |
| B | 77259 | 0.03 ms | 4.02 ms | 55.59 ms | read 0.03 / 0.73 ms, update 1.16 / 104.24 ms | 0 |
| C | 121667 | 0.18 ms | 1.36 ms | 5.31 ms | read 0.18 / 1.36 ms | 0 |
| D | 75298 | 0.02 ms | 4.21 ms | 79.06 ms | insert 1.18 / 104.89 ms, read 0.02 / 0.60 ms | 0 |
| E | 5209 | 5.02 ms | 21.81 ms | 28.86 ms | insert 1.00 / 11.21 ms, scan 5.42 / 21.95 ms | 0 |
| F | 31425 | 0.02 ms | 18.80 ms | 104.19 ms | read 0.01 / 0.20 ms, rmw 0.04 / 34.12 ms | 0 |
### round 3 libsql ycsb load: 10.91, 16.01, 37.64
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 23.7s (4213 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 378 | 9.49 ms | 124.10 ms | 5254.01 ms | read 1.20 / 10.44 ms, update 39.74 / 372.24 ms | 63 (0.7%), 32 timed out |
| B | 48 | 3.48 ms | 17.85 ms | 24.33 ms | read 3.48 / 17.85 ms | 64 (6.1%), 64 timed out |
| C | 2698 | 10.83 ms | 29.97 ms | 63.92 ms | read 10.83 / 29.97 ms | 0 |
| D | 51 | 3.76 ms | 17.52 ms | 26.66 ms | read 3.76 / 17.52 ms | 64 (5.9%), 64 timed out |
| E | 56 | 12.19 ms | 57.96 ms | 70.73 ms | scan 12.19 / 57.96 ms | 64 (5.2%), 64 timed out |
| F | 4 | 7.23 ms | 30.10 ms | 30.10 ms | read 7.23 / 30.10 ms | 64 (44.8%), 64 timed out |
### round 3 libsql latency load: 83.38, 35.83, 41.22
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

**stalled: no result within 140s, killed**
### round 3 aozumi-full ycsb load: 132.50, 65.56, 50.93
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 4 cores

load: 100000 records in 6.5s (15332 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 2979 | 3.24 ms | 35.21 ms | 60.94 ms | read 0.31 / 1.58 ms, update 20.02 / 42.02 ms | 0 |
| B | 19174 | 0.77 ms | 21.64 ms | 36.30 ms | read 0.73 / 3.99 ms, update 15.50 / 39.12 ms | 0 |
| C | 24280 | 1.17 ms | 4.07 ms | 21.19 ms | read 1.17 / 4.07 ms | 0 |
| D | 17384 | 0.64 ms | 26.60 ms | 47.33 ms | insert 19.61 / 56.55 ms, read 0.61 / 4.59 ms | 0 |
| E | 2221 | 13.40 ms | 34.87 ms | 45.69 ms | insert 13.25 / 30.13 ms, scan 13.42 / 35.01 ms | 0 |
| F | 2668 | 5.98 ms | 39.88 ms | 63.10 ms | read 0.33 / 1.76 ms, rmw 22.38 / 48.92 ms | 0 |
### round 3 aozumi-full latency load: 21.19, 45.28, 45.31
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 4 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.72 ms | 2.99 ms | 11.91 ms | 20.98 ms | 26.06 ms | 0 |
### done 2026-11-08T04:22:12Z
