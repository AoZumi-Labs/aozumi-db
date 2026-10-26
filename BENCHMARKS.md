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

Machine: Apple A18 Pro laptop, 6 cores, 8 GB RAM, macOS, shared with other
apps (load average 4 to 13 during these runs, swap in use). Expect
run-to-run swings of 20 to 50% on this machine; only large differences, or
differences repeated in paired runs, mean anything. 100,000 records (about
100 MB), 32 clients, 10 s per workload.

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

### After tuning (2026-10-26)

Changes: writers queue per database instead of racing for the write lock;
connections are pooled; compiled statements are cached per connection;
commits no longer checkpoint the WAL inline (a background checkpointer does).
Run at load average 4.4.

| YCSB | ops/s | p50 | p99 | p99.9 | vs baseline |
|---|---:|---:|---:|---:|---|
| A (50% update) | 13,955 | 0.40 ms | 21.64 ms | 26.10 ms | 6.8x ops/s, p99.9 52x lower |
| B (5% update) | 34,455 | 0.85 ms | 1.96 ms | 20.35 ms | 1.9x |
| C (read only) | 36,413 | 0.86 ms | 1.70 ms | 2.02 ms | 2.2x |
| D (read latest, 5% insert) | 31,485 | 0.88 ms | 5.43 ms | 17.27 ms | 2.6x |
| E (short scans, 5% insert) | 4,287 | 6.91 ms | 19.62 ms | 44.93 ms | 1.3x |
| F (read-modify-write) | 12,224 | 0.41 ms | 24.03 ms | 28.62 ms | 5.1x ops/s, p99.9 46x lower |

| micro | ops/s | p50 | p99 | p99.9 | vs baseline |
|---|---:|---:|---:|---:|---|
| point read | 53,747 | 0.58 ms | 1.17 ms | 1.44 ms | 1.7x |
| single-row insert | 12,677 | 2.39 ms | 6.12 ms | 9.23 ms | 6.8x ops/s, p99.9 159x lower |
| 100-row transaction | 2,726 | 2.67 ms | 6.76 ms | 13.51 ms | 5.2x |

Bare engine, one thread, same point read, same run: 148,027 ops/s.

| open-loop writes, 1000/s, 32 connections | p50 | p90 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| baseline | 2.04 ms | 211.08 ms | 1,102.76 ms | 1,454.33 ms | 1,716.81 ms |
| writer queue | 2.00 ms | 3.31 ms | 43.99 ms | 74.15 ms | 82.73 ms |
| all changes, 3 runs at load 9.3 | 0.91–1.44 ms | 1.59–2.09 ms | 6.81–34.74 ms | 18.14–99.21 ms | 19.94–104.30 ms |

Paired runs (alternating, three pairs, 5 s each) isolate connection and
statement reuse: YCSB C median 32.7k vs 22.7k ops/s (+44%), YCSB A median
12.5k vs 9.3k (+35%), better in every pair. The background checkpointer
consistently lowers open-loop p50 (about 1.0 ms vs 1.6–1.9 ms in paired
runs); its effect on p99 is within this machine's noise.

What is left on the table: the server reaches about a third of the bare
engine's single-thread point-read rate on a shared 6-core laptop where the
clients run too. Next: a quiet machine for numbers worth publishing, TATP,
and profiling the read path (JSON encoding, the blocking-pool hop).
