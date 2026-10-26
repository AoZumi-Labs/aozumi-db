# Benchmarks

aozumi-db is measured with standard workloads, over HTTP, the way clients
use it:

- **YCSB core workloads A–F**: the common benchmark for databases served
  over a network. Ten 100-byte fields per record, Zipfian key popularity,
  the standard read/update/insert/scan mixes (see
  `examples/bench/ycsb.rs`). Closed loop: each client sends its next
  request as soon as the last returns.
- **Open-loop write latency**: single-row write transactions arriving on a
  fixed schedule, timed from when each was due, so time spent queued behind
  another writer counts (no coordinated omission).
- **Micro**: point reads, single-row inserts and 100-row transactions, and
  the bare storage engine's point-read rate on one thread, to show how
  much the server adds.

```bash
cargo run --release --example bench -- ycsb ABCDEF --records 100000 --clients 32 --seconds 10
cargo run --release --example bench -- latency --rate 1000 --clients 32 --seconds 10
cargo run --release --example bench -- micro --clients 32 --seconds 5
```

The server runs in the benchmark process on 4 threads and the clients on
2, on the same machine, so numbers compare across runs on one machine,
not across machines. Next to add: TATP, the OLTP benchmark used in the
SQLite VLDB paper, to compare against the engine's own embedded numbers.

## Results

Machine: Apple M-series MacBook, 6 cores, 8 GB RAM, macOS. 100,000 records
(about 100 MB), 32 clients, 10 s per workload.

### Baseline (2026-10-26, before tuning)

| YCSB | ops/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| A (50% update) | 2,042 | 1.14 ms | 326.02 ms | 1,346.51 ms |
| B (5% update) | 18,428 | 1.49 ms | 6.74 ms | 17.86 ms |
| C (read only) | 16,498 | 1.65 ms | 7.19 ms | 15.85 ms |
| D (read latest, 5% insert) | 12,095 | 1.93 ms | 12.46 ms | 40.76 ms |
| E (short scans, 5% insert) | 3,222 | 8.34 ms | 35.82 ms | 103.48 ms |
| F (read-modify-write) | 2,386 | 0.60 ms | 286.54 ms | 1,321.31 ms |

| open-loop writes | completed | p50 | p90 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|---:|
| 1000/s, 32 connections | 1000/s | 2.04 ms | 211.08 ms | 1,102.76 ms | 1,454.33 ms | 1,716.81 ms |

| micro | ops/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| point read | 32,468 | 0.95 ms | 1.96 ms | 5.73 ms |
| single-row insert | 1,870 | 0.47 ms | 378.91 ms | 1,468.99 ms |
| 100-row transaction | 525 | 3.02 ms | 340.89 ms | 1,372.51 ms |

Bare engine, one thread, same point read: 144,196 ops/s.

What the baseline shows: concurrent writers race for the database's single
write lock, and the losers sleep in SQLite-style backoff (1 ms rising to
100 ms per retry), so write tails reach seconds even at 1,000 writes/s.
