### round 1 libsql ycsb load: 0.00, 0.16, 0.31
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 14.0s (7166 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 101 | 2.57 ms | 51.27 ms | 79.07 ms | read 0.70 / 9.60 ms, update 36.94 / 65.52 ms | 64 (2.9%), 64 timed out |
| B | 48 | 3.42 ms | 12.55 ms | 14.99 ms | read 3.42 / 12.55 ms | 64 (6.1%), 64 timed out |
| C | 3628 | 8.26 ms | 20.02 ms | 25.69 ms | read 8.26 / 20.02 ms | 0 |
| D | 51 | 3.94 ms | 13.41 ms | 17.08 ms | read 3.94 / 13.41 ms | 64 (5.9%), 64 timed out |
| E | 56 | 9.19 ms | 24.62 ms | 29.19 ms | scan 9.19 / 24.62 ms | 64 (5.2%), 64 timed out |
| F | 4 | 5.93 ms | 15.04 ms | 15.04 ms | read 5.93 / 15.04 ms | 64 (44.8%), 64 timed out |
### round 1 libsql latency load: 25.67, 11.45, 4.49
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

bench failed (exit 101)
### round 1 aozumi-full ycsb load: 16.06, 17.55, 14.23
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.8s (17242 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1051 | 0.19 ms | 74.58 ms | 136.72 ms | read 0.12 / 0.19 ms, update 61.36 / 75.57 ms | 0 |
| B | 10291 | 0.12 ms | 61.62 ms | 70.55 ms | read 0.11 / 0.20 ms, update 60.19 / 73.53 ms | 0 |
| C | 44532 | 0.71 ms | 1.27 ms | 1.61 ms | read 0.71 / 1.27 ms | 0 |
| D | 9541 | 0.12 ms | 66.05 ms | 78.02 ms | insert 64.54 / 78.92 ms, read 0.12 / 0.21 ms | 0 |
| E | 3661 | 8.21 ms | 20.56 ms | 25.97 ms | insert 9.77 / 19.81 ms, scan 8.13 / 20.59 ms | 0 |
| F | 1025 | 0.19 ms | 73.06 ms | 76.97 ms | read 0.13 / 0.19 ms, rmw 62.56 / 75.36 ms | 0 |
### round 1 aozumi-full latency load: 4.13, 12.40, 12.75
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 8949.16 ms | 16120.84 ms | 17733.04 ms | 17894.47 ms | 17923.36 ms | 0 |
### round 1 aozumi-normal ycsb load: 2.61, 10.97, 12.25
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.5s (18148 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 11365 | 2.35 ms | 3.19 ms | 433.42 ms | read 0.15 / 0.28 ms, update 2.83 / 3.70 ms | 0 |
| B | 43317 | 0.72 ms | 1.28 ms | 2.58 ms | read 0.72 / 1.25 ms, update 0.77 / 5.90 ms | 0 |
| C | 44257 | 0.71 ms | 1.27 ms | 1.60 ms | read 0.71 / 1.27 ms | 0 |
| D | 43054 | 0.72 ms | 1.33 ms | 5.10 ms | insert 0.82 / 8.54 ms, read 0.72 / 1.28 ms | 0 |
| E | 3699 | 8.11 ms | 20.65 ms | 26.46 ms | insert 6.99 / 15.59 ms, scan 8.18 / 20.76 ms | 0 |
| F | 10939 | 3.17 ms | 4.24 ms | 234.55 ms | read 0.16 / 0.28 ms, rmw 3.72 / 22.04 ms | 0 |
### round 1 aozumi-normal latency load: 4.36, 8.89, 11.32
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 0.94 ms | 1.38 ms | 1.52 ms | 4.86 ms | 10.50 ms | 0 |
### round 1 sqlite-full ycsb load: 3.19, 8.33, 11.08
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1011 | 0.04 ms | 833.15 ms | 3334.24 ms | read 0.02 / 0.04 ms, update 4.10 / 1533.18 ms | 5 (0.0%) |
| B | 9288 | 0.01 ms | 22.02 ms | 933.06 ms | read 0.01 / 0.04 ms, update 4.19 / 1333.04 ms | 1 (0.0%) |
| C | 179037 | 0.17 ms | 0.41 ms | 0.55 ms | read 0.17 / 0.41 ms | 0 |
| D | 8379 | 0.01 ms | 7.81 ms | 1033.67 ms | insert 4.75 / 1634.59 ms, read 0.01 / 0.04 ms | 3 (0.0%) |
| E | 7989 | 0.23 ms | 58.42 ms | 831.06 ms | insert 6.37 / 1132.11 ms, scan 0.22 / 0.84 ms | 0 |
| F | 962 | 0.04 ms | 733.50 ms | 3634.39 ms | read 0.01 / 0.03 ms, rmw 4.04 / 1633.23 ms | 14 (0.1%) |
### round 1 sqlite-normal ycsb load: 5.25, 7.71, 10.51
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 55590 | 0.01 ms | 8.23 ms | 103.55 ms | read 0.01 / 0.04 ms, update 0.02 / 18.35 ms | 0 |
| B | 124172 | 0.02 ms | 3.19 ms | 36.19 ms | read 0.02 / 0.15 ms, update 0.12 / 78.55 ms | 0 |
| C | 184158 | 0.17 ms | 0.42 ms | 0.53 ms | read 0.17 / 0.42 ms | 0 |
| D | 137219 | 0.02 ms | 3.21 ms | 37.42 ms | insert 0.11 / 78.56 ms, read 0.02 / 0.11 ms | 0 |
| E | 9131 | 3.34 ms | 8.26 ms | 11.20 ms | insert 1.60 / 9.47 ms, scan 3.48 / 8.25 ms | 0 |
| F | 33605 | 0.02 ms | 18.29 ms | 78.51 ms | read 0.01 / 0.04 ms, rmw 0.02 / 33.35 ms | 0 |
### round 2 aozumi-full ycsb load: 10.85, 9.11, 10.63
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.7s (17571 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1046 | 0.19 ms | 72.07 ms | 102.02 ms | read 0.12 / 0.19 ms, update 61.37 / 73.32 ms | 0 |
| B | 10233 | 0.11 ms | 61.64 ms | 67.63 ms | read 0.11 / 0.21 ms, update 60.23 / 70.82 ms | 0 |
| C | 44234 | 0.72 ms | 1.27 ms | 1.61 ms | read 0.72 / 1.27 ms | 0 |
| D | 9653 | 0.12 ms | 65.97 ms | 77.73 ms | insert 64.47 / 78.74 ms, read 0.12 / 0.21 ms | 0 |
| E | 3681 | 8.17 ms | 20.60 ms | 26.11 ms | insert 9.76 / 19.40 ms, scan 8.09 / 20.69 ms | 0 |
| F | 1026 | 0.19 ms | 73.84 ms | 77.11 ms | read 0.13 / 0.19 ms, rmw 62.49 / 75.25 ms | 0 |
### round 2 aozumi-full latency load: 4.50, 7.22, 9.74
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 8946.40 ms | 16093.55 ms | 17699.39 ms | 17858.83 ms | 17876.47 ms | 0 |
### round 2 aozumi-normal ycsb load: 2.79, 6.44, 9.37
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.4s (18417 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 11370 | 2.38 ms | 3.26 ms | 433.64 ms | read 0.15 / 0.27 ms, update 2.86 / 3.82 ms | 0 |
| B | 44491 | 0.70 ms | 1.31 ms | 3.58 ms | read 0.69 / 1.27 ms, update 0.76 / 7.43 ms | 0 |
| C | 42560 | 0.74 ms | 1.28 ms | 1.61 ms | read 0.74 / 1.28 ms | 0 |
| D | 41522 | 0.75 ms | 1.35 ms | 7.02 ms | insert 0.85 / 11.44 ms, read 0.74 / 1.29 ms | 0 |
| E | 3704 | 8.10 ms | 20.73 ms | 26.52 ms | insert 6.86 / 15.88 ms, scan 8.17 / 20.85 ms | 0 |
| F | 10230 | 3.16 ms | 4.25 ms | 334.02 ms | read 0.16 / 0.27 ms, rmw 3.76 / 58.87 ms | 0 |
### round 2 aozumi-normal latency load: 4.93, 6.21, 8.92
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.15 ms | 1.60 ms | 1.73 ms | 10.11 ms | 20.51 ms | 0 |
### round 2 sqlite-full ycsb load: 3.59, 5.82, 8.74
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 980 | 0.06 ms | 1031.10 ms | 3335.42 ms | read 0.02 / 0.04 ms, update 4.14 / 1534.01 ms | 4 (0.0%) |
| B | 9448 | 0.01 ms | 12.41 ms | 933.08 ms | read 0.01 / 0.04 ms, update 3.98 / 1433.10 ms | 0 |
| C | 181158 | 0.17 ms | 0.43 ms | 0.56 ms | read 0.17 / 0.43 ms | 0 |
| D | 8317 | 0.01 ms | 7.88 ms | 1034.05 ms | insert 4.76 / 1635.12 ms, read 0.01 / 0.04 ms | 1 (0.0%) |
| E | 8004 | 0.23 ms | 58.08 ms | 831.36 ms | insert 6.41 / 1131.26 ms, scan 0.22 / 0.77 ms | 0 |
| F | 996 | 0.04 ms | 730.85 ms | 3437.54 ms | read 0.01 / 0.03 ms, rmw 2.89 / 1633.31 ms | 24 (0.1%) |
### round 2 sqlite-normal ycsb load: 5.21, 6.04, 8.46
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 56548 | 0.01 ms | 8.23 ms | 103.53 ms | read 0.01 / 0.04 ms, update 0.02 / 18.33 ms | 0 |
| B | 122809 | 0.02 ms | 3.20 ms | 41.05 ms | read 0.02 / 0.15 ms, update 0.13 / 78.56 ms | 0 |
| C | 177154 | 0.18 ms | 0.40 ms | 0.50 ms | read 0.18 / 0.40 ms | 0 |
| D | 137691 | 0.02 ms | 3.21 ms | 37.80 ms | insert 0.12 / 78.54 ms, read 0.02 / 0.11 ms | 0 |
| E | 8840 | 3.47 ms | 8.56 ms | 11.75 ms | insert 1.58 / 8.89 ms, scan 3.62 / 8.55 ms | 0 |
| F | 35025 | 0.02 ms | 18.29 ms | 78.51 ms | read 0.01 / 0.04 ms, rmw 0.02 / 33.35 ms | 0 |
### round 2 libsql ycsb load: 10.51, 7.97, 8.83
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 14.0s (7138 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 179 | 4.04 ms | 52.28 ms | 304.13 ms | read 0.69 / 7.15 ms, update 36.95 / 78.11 ms | 94 (2.2%), 64 timed out |
| B | 48 | 3.53 ms | 12.80 ms | 20.12 ms | read 3.53 / 12.80 ms | 64 (6.1%), 64 timed out |
| C | 3696 | 8.06 ms | 20.13 ms | 25.59 ms | read 8.06 / 20.13 ms | 0 |
| D | 51 | 3.96 ms | 13.64 ms | 18.35 ms | read 3.96 / 13.64 ms | 64 (5.9%), 64 timed out |
| E | 56 | 9.34 ms | 26.89 ms | 34.79 ms | scan 9.34 / 26.89 ms | 64 (5.2%), 64 timed out |
| F | 4 | 6.10 ms | 18.94 ms | 18.94 ms | read 6.10 / 18.94 ms | 64 (44.8%), 64 timed out |
### round 2 libsql latency load: 29.04, 17.29, 12.19
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

**stalled: no result after 15 minutes of a 20 s run; the run was stopped by hand.**
sqld (pid 12409) was still running: 618% CPU, 3855 threads, up 19:47
The end of its log:
    2026-11-08T03:46:53.367527Z  INFO restore: libsql_server::namespace::meta_store: restoring meta store
    2026-11-08T03:46:53.367567Z  INFO restore: libsql_server::namespace::meta_store: meta store restore completed
    2026-11-08T03:46:53.368222Z  INFO libsql_server: Server sending heartbeat to URL <not supplied> every 30s
    2026-11-08T03:46:53.378432Z  INFO create:try_new_primary:make_primary_connection_maker: libsql_server::replication::primary::logger: SQLite autocheckpoint: 1000
### round 3 aozumi-normal ycsb load: 14.13, 16.05, 15.15
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 5.3s (18749 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 10793 | 2.34 ms | 3.26 ms | 433.58 ms | read 0.15 / 0.27 ms, update 2.83 / 4.36 ms | 0 |
| B | 44145 | 0.71 ms | 1.29 ms | 3.50 ms | read 0.70 / 1.25 ms, update 0.76 / 9.15 ms | 0 |
| C | 43784 | 0.72 ms | 1.27 ms | 1.61 ms | read 0.72 / 1.27 ms | 0 |
| D | 39169 | 0.80 ms | 1.36 ms | 10.40 ms | insert 0.90 / 15.62 ms, read 0.79 / 1.32 ms | 0 |
| E | 3656 | 8.21 ms | 20.96 ms | 26.68 ms | insert 7.10 / 16.06 ms, scan 8.28 / 21.10 ms | 0 |
| F | 10429 | 3.15 ms | 4.23 ms | 334.06 ms | read 0.16 / 0.28 ms, rmw 3.73 / 58.75 ms | 0 |
### round 3 aozumi-normal latency load: 6.50, 12.47, 13.95
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 1.13 ms | 1.58 ms | 1.72 ms | 5.35 ms | 11.01 ms | 0 |
### round 3 sqlite-full ycsb load: 4.79, 11.70, 13.66
YCSB on SQLite 3.46.0 embedded (synchronous = full), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1019 | 0.05 ms | 934.05 ms | 3232.38 ms | read 0.02 / 0.04 ms, update 3.84 / 1631.42 ms | 8 (0.0%) |
| B | 9468 | 0.01 ms | 11.99 ms | 934.03 ms | read 0.01 / 0.04 ms, update 3.95 / 1531.44 ms | 1 (0.0%) |
| C | 173974 | 0.18 ms | 0.45 ms | 0.60 ms | read 0.18 / 0.45 ms | 0 |
| D | 8480 | 0.01 ms | 7.93 ms | 1034.14 ms | insert 4.73 / 1731.52 ms, read 0.01 / 0.04 ms | 2 (0.0%) |
| E | 8031 | 0.24 ms | 57.16 ms | 831.88 ms | insert 6.27 / 1131.32 ms, scan 0.23 / 0.86 ms | 0 |
| F | 990 | 0.04 ms | 830.78 ms | 3533.86 ms | read 0.01 / 0.04 ms, rmw 3.57 / 1633.06 ms | 19 (0.1%) |
### round 3 sqlite-normal ycsb load: 5.61, 9.95, 12.77
YCSB on SQLite 3.46.0 embedded (synchronous = normal), 100000 records, 32 threads, 20s per workload, 8 cores

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 55996 | 0.01 ms | 8.23 ms | 103.55 ms | read 0.01 / 0.04 ms, update 0.02 / 18.34 ms | 0 |
| B | 123468 | 0.02 ms | 3.19 ms | 36.95 ms | read 0.02 / 0.15 ms, update 0.12 / 78.57 ms | 0 |
| C | 181524 | 0.17 ms | 0.43 ms | 0.54 ms | read 0.17 / 0.43 ms | 0 |
| D | 137220 | 0.02 ms | 3.21 ms | 33.63 ms | insert 0.11 / 78.53 ms, read 0.02 / 0.11 ms | 0 |
| E | 8760 | 3.47 ms | 8.82 ms | 12.05 ms | insert 1.58 / 9.29 ms, scan 3.62 / 8.81 ms | 0 |
| F | 34312 | 0.02 ms | 18.29 ms | 78.51 ms | read 0.01 / 0.04 ms, rmw 0.02 / 33.35 ms | 0 |
### round 3 libsql ycsb load: 8.62, 9.92, 12.38
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 14.0s (7120 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 102 | 2.07 ms | 52.57 ms | 105.60 ms | read 0.69 / 8.31 ms, update 36.92 / 66.57 ms | 64 (2.9%), 64 timed out |
| B | 48 | 3.38 ms | 12.16 ms | 15.07 ms | read 3.38 / 12.16 ms | 64 (6.1%), 64 timed out |
| C | 3636 | 8.24 ms | 20.05 ms | 25.88 ms | read 8.24 / 20.05 ms | 0 |
| D | 51 | 4.00 ms | 14.75 ms | 23.81 ms | read 4.00 / 14.75 ms | 64 (5.9%), 64 timed out |
| E | 56 | 9.07 ms | 26.19 ms | 31.23 ms | scan 9.07 / 26.19 ms | 64 (5.2%), 64 timed out |
| F | 4 | 5.70 ms | 18.10 ms | 18.10 ms | read 5.70 / 18.10 ms | 64 (44.8%), 64 timed out |
### round 3 libsql latency load: 26.44, 17.45, 14.85
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

**stalled: no result within 140s, killed**
### round 3 aozumi-full ycsb load: 14.25, 15.86, 14.61
server: http://127.0.0.1:18090
YCSB, 100000 records, 32 clients, 20s per workload, 8 cores

load: 100000 records in 6.0s (16794 records/s, 1000 per transaction)

| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |
|---|---:|---:|---:|---:|---|---:|
| A | 1045 | 0.19 ms | 71.81 ms | 148.37 ms | read 0.13 / 0.19 ms, update 61.44 / 75.01 ms | 0 |
| B | 10191 | 0.12 ms | 61.71 ms | 69.65 ms | read 0.12 / 0.21 ms, update 60.33 / 72.91 ms | 0 |
| C | 42611 | 0.74 ms | 1.26 ms | 1.60 ms | read 0.74 / 1.26 ms | 0 |
| D | 9537 | 0.12 ms | 66.16 ms | 77.87 ms | insert 64.64 / 78.72 ms, read 0.12 / 0.21 ms | 0 |
| E | 3670 | 8.25 ms | 20.43 ms | 26.40 ms | insert 9.73 / 19.53 ms, scan 8.16 / 20.51 ms | 0 |
| F | 1024 | 0.19 ms | 74.12 ms | 76.09 ms | read 0.13 / 0.19 ms, rmw 62.63 / 75.34 ms | 0 |
### round 3 aozumi-full latency load: 4.32, 11.44, 13.13
server: http://127.0.0.1:18090
Open-loop write latency, 1000 tx/s, 32 connections, 20s, 8 cores

| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000/s | 1000/s | 8948.41 ms | 16131.55 ms | 17744.32 ms | 17906.46 ms | 17934.46 ms | 0 |
### done 2026-11-08T04:20:50Z
