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
# as production runs it: checked tokens, backups, the WAL shipped every 5 s
cargo run --release --example bench -- micro --clients 32 --seconds 5 --prod
# a server elsewhere
cargo run --release --example bench -- micro --url https://db.example.com --token "$TOKEN"
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

## The path apps use (2026-11-04)

The numbers above run client and server in one process. What an app sees
also includes the network in front of the server. Two probes measure it with
the requests `@libsql/client` sends (`POST /v2/pipeline` with `execute` and
`close`), one request at a time over a kept-alive connection:

- `bench/probe/pipeline-latency.mjs` (Node's `fetch`): from where an app
  runs.
- `bench/probe/pipeline-latency.py` (Python's standard library only): on the
  server's own machine, against its loopback, to see the server alone.

Both use one table of 10,000 rows (`--setup` makes it) and print JSON.

AOZUMI's hosted server, release 289c07c: a 2-vCPU ARM VM (AWS t4g.small,
EBS gp3, ext4) behind a Cloudflare tunnel. The app side was a 0.5-vCPU ARM
container (AWS Fargate) in the same availability zone, Node 22. The VM was
idle (load 0.00) with one database.

| Per request, ms | from the app, p50 / p99 | on the server's loopback, p50 / p99 | network's share of p50 |
|---|---:|---:|---:|
| `GET /health` | 11.33 / 26.07 | 0.17 / 0.28 | 11.2 |
| point read | 10.90 / 16.87 | 0.30 / 0.41 | 10.6 (97%) |
| single-row insert | 14.03 / 32.19 | 3.57 / 4.35 | 10.5 (75%) |
| 10-row transaction | 13.85 / 18.45 | 3.69 / 4.91 | 10.2 (73%) |
| point reads, 8 clients, 10 s | 594 ops/s, p50 10.9, p99 57.8 | | |

What this shows:

- **The tunnel is most of every request.** A point read takes 0.30 ms on the
  server and 10.9 ms from the app, and 8 clients get 594 reads/s. A path
  inside the network, without the tunnel, is where most of the time is.
- **Writes are a flush.** A commit of one row costs what a commit of ten does
  (3.57 vs 3.69 ms). With `synchronous = FULL`, each commit waits for the
  disk.

A flush on that volume, 200 writes of one 4,120-byte WAL frame each:

| Pattern | p50 | p99 |
|---|---:|---:|
| append + `fsync` (the WAL grows after each truncating checkpoint) | 3.05 ms | 3.32 ms |
| overwrite in place + `fsync` | 2.81 ms | 3.05 ms |
| append + `fdatasync` | 3.02 ms | 3.41 ms |
| overwrite in place + `fdatasync` | 0.94 ms | 1.06 ms |

Every commit today appends to a file that grows, and `fsync` also flushes
its metadata: the 3 ms row. Reusing the WAL in place and syncing it with
`fdatasync`, as SQLite itself does on Linux, would make it about 1 ms, with
the same durability.

The YCSB harness runs the server in dev mode by default: no token check, no
backups. `--prod` runs it with production's settings instead (checked
tokens, backups with the WAL shipped every 5 s), and `--url … --token …`
points it at a server elsewhere.

## Head-to-head with libSQL's server (2026-10-26)

`bench/head-to-head.sh` runs the same YCSB and open-loop client against
libSQL's own server (`sqld` 0.24.32, default settings, one local database)
and aozumi-db, each on a fresh empty directory, alternating the order per
round. aozumi-db is run twice: with its default durability (`synchronous =
FULL`) and with `AOZUMI_DB_SYNCHRONOUS=normal`, which is what `sqld` uses.
Raw output, medians and charts: `bench/results/2026-10-26-head-to-head/`.

Two rounds completed (the third was stopped), at load average 6 to 37, so
read these as large-gap results only.

| YCSB, 32 clients, 100k records | aozumi-db | aozumi-db (normal) | libSQL server, per round |
|---|---:|---:|---:|
| A (50% update) | 13,512 ops/s | 10,215 | 944 / 1,103, plus 57 / 34 refused busy |
| B (5% update) | 34,272 | 26,885 | 4,582 / 1,895 (stalled in round 2) |
| C (read only) | 37,436 | 35,416 | 4,358 / 3,510 |
| A p99 | 9.29 ms | 19.95 ms | 85.96 / 108.47 ms |

What happened on libSQL's server: under 32 concurrent clients it refuses
some writes with `SQLITE_BUSY`, and in every round it then stopped answering
altogether: all 32 clients timed out (10 s) from workload D or earlier, and
an earlier run stayed stuck for over an hour with the server spinning on 5
cores in its write-lock acquire path. Its D, E, F and open-loop numbers
therefore measure that stall, not its speed, and are left out above.
aozumi-db refused nothing and timed out nothing: writers queue per database
instead of racing for the lock.

Not yet done: a quiet machine, more rounds, and `sqld` tuning beyond its
defaults.
