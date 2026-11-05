# aozumi-db design

aozumi-db is a SQLite-compatible database server for small apps, written in
Rust. Each app gets its own database file on the server's disk. Apps reach it
over HTTP with the libSQL protocol (Hrana over HTTP, version 2), so
`@libsql/client`, Drizzle and other libSQL clients work without changes.

Status: experimental and pre-1.0.

## Goals

1. A database per app that costs nothing while idle: a file, not an instance.
2. Unchanged client code: point `@libsql/client` at `https://db.example.com`
   with a token, and it works.
3. One Rust process that is small to read and to run, and that sits next to
   AOZUMI's Rust gateway without being part of it.

Not goals yet: replication, protocol v3 (cursors, protobuf), `describe`.

## Shape

```
client (@libsql/client)
  │  POST /v2/pipeline   Authorization: Bearer <database token>
  ▼
aozumi-db (hyper + tokio)
  ├─ auth: token → database name
  ├─ streams: baton → open connection (interactive transactions)
  ├─ registry: database name → database handle (opened once, shared)
  └─ <data-dir>/<name>.db   (one database file each, plus its WAL)
```

## Decisions

| # | Decision | Why |
|---|----------|-----|
| D1 | Separate process, not code inside the AOZUMI gateway | A gateway deploy, crash or slow query must not take app data down, and the gateway must stay stateless so it can scale out. |
| D2 | Speak libSQL's Hrana-over-HTTP v2 (JSON) | `@libsql/client` defaults to v2 over JSON. Matching it means no client SDK to ship. |
| D3 | The token names the database | The client resolves `v2/pipeline` relative to the URL, so a database name in the path is dropped. One URL for every app, one token per database. |
| D4 | Tokens: `adb1.<base64url(JSON claims)>.<base64url(HMAC-SHA256)>`, claims `{db, exp}` | The AOZUMI control plane holds the same secret and mints tokens itself, so creating an app needs no round trip. Moving to Ed25519 (public verification) is a later, compatible change. |
| D5 | Databases are created explicitly through the admin API | A valid token for a deleted database must not quietly recreate an empty one. |
| D6 | Batons are random, single-use, and rotated on every request; idle streams close after 10 seconds | Matches libSQL's server. A rotated baton stops two requests from driving one connection at once. |
| D7 | Error codes use SQLite names (`SQLITE_CONSTRAINT`, ...) | `@libsql/client` passes them through as `LibsqlError.code`, which apps already check. |
| D8 | Named arguments without a prefix match `:name`, `@name` or `$name` in the SQL | `@libsql/client` sends object keys without the prefix. |
| D9 | Dev mode (no auth, one database named `default`) only with `AOZUMI_DB_DEV=1`; otherwise a missing or empty secret stops the server from starting | Local development needs no secret. A secret that never reached the process (a missing line in an env file, an empty value) must not leave every database open, so running without one is something to ask for. |
| D10 | A backup copies the database and WAL files under `BEGIN IMMEDIATE`, then checkpoints the copy into one file | Writers wait only for a local file copy; readers never wait. The result is one self-contained file. Every connection gets a 5 s busy timeout so writes wait instead of failing. |
| D11 | Scheduled backups skip databases whose files have not changed (size and mtime) | Idle apps cost nothing, including in backup storage and requests. |
| D12 | Restores into a new name create a new database; a restore in place (D26) first keeps what it replaces as a backup | No restore can destroy data. |
| D13 | Our own small S3 client (SigV4, IMDSv2) instead of the AWS SDK | We need four operations; the SDK is a large dependency for that. Signing is checked against AWS's published examples. |
| D14 | Drive the storage engine's core API directly, pinned to an exact version | Point-in-time restore needs WAL frame access and checkpoint control, and statement limits need interrupts; the high-level crate exposes neither. Fewer layers on the hot path too. |
| D15 | A statement policy for tenant SQL: no `ATTACH`/`DETACH`/`VACUUM INTO`, pragmas from an allowlist | Without it a tenant could write files on the server, read another database, lift its own limits or switch a read-only token back. The policy parses only SQL that names one of those words, so most queries skip it. |
| D16 | Read-only tokens use the engine's query-only mode | Enforced by the engine on every write path, not by guessing from SQL text. |
| D17 | Limits: size via the engine's page limit, statement time via its interrupt, request rate via GCRA, all per database | Each is enforced where it is exact and cheap. Limits live beside the database in `<name>.db-limits.json`; server defaults apply at creation. |
| D18 | Point-in-time restore ships committed WAL frames per snapshot "generation"; the shipper is the only checkpointer | A frame is never folded into the database file before it is backed up. If a commit slips in between shipping and a checkpoint, the checkpoint's frame count shows it and a new generation (snapshot) starts. Restore = newest snapshot at or before the time + its page images shipped by then. |
| D19 | Writers queue per database (fair, first come first served); reads never wait | The engine's busy handler sleeps up to 100 ms per retry, which put write tails in seconds under contention. Streams inside a transaction skip the queue so they can always reach their `COMMIT`. |
| D20 | Pooled connections and cached compiled programs | Opening a connection registers every SQL function; parsing and planning dominate short queries. Statements themselves are not cached: one kept across uses pins an old pager and stalls writers. A stream that ran a pragma or touched `TEMP` does not return its connection. |
| D21 | Commits never checkpoint; a background task does (passive, then a short truncate) | A checkpoint inside a commit stalls every writer queued behind it. |
| D22 | A console API (`/v1/console/*`) beside the pipeline, behind the same database tokens | The AOZUMI DB console needs scripts split the way SQLite splits them, each statement's outcome, capped results and a table browser. Doing it here uses the engine's own parser and keeps the control plane a thin, authorized proxy. Its JSON is camelCase, shaped for display (integers as strings, long text and blobs cut down), and never a substitute for the pipeline in apps. A read-write script that destroys data (found in the parse, not the text) runs only when confirmed. |
| D23 | Tokens carry their database's token generation (claim `gen`, absent = 0). Rotating a database's tokens raises its generation by one, and tokens below it are refused | Tokens are stateless, so a leaked one could only be revoked by deleting its database or by rotating the secret, which breaks every app and the admin API at once. One number per database revokes all of that database's tokens and nothing else. It is checked from memory on every request, before the token counts against the rate limit, and lives in `<name>.tokens.json` beside the database, synced when written. It stays when the database is deleted, so a revoked token never opens a database created later under the same name, while an in-place restore (D12) keeps the current tokens working. Tokens above the current generation pass: only a holder of the secret can mint them. Generations are not in backups yet: a database restored into a new data directory starts again at 0, and its revoked tokens work again until it is rotated past them. |
| D24 | Each backup's kind is in its name (`<id>.db` scheduled, `<id>.manual.db`, `<id>.before-restore.db`); a database's backup status is what the store holds plus what the server saw since it started | The console must tell people when their data was last safe and why a backup exists. A name is enough for the kind, and servers that do not know a name leave the file alone. The newest backup comes from the store, so it survives restarts; whether everything is in the store (`up_to_date_ms`) and the last failure are only known to the running server, so they are kept in memory. |
| D25 | Each database has its own backup policy, set through the admin API and kept beside it: retention by age, and how often and how many backups may be asked for. With WAL shipping, only the server's own snapshots start a generation | A hosted product promises "restore to any moment in the last N days", which a count cannot: 48 snapshots were two days for a busy database and weeks for an idle one. Pruning keeps the newest scheduled snapshot from before the window, so the window's first moment stays restorable, and never the database's newest. A generation is a snapshot plus the WAL after it, so a manual snapshot that started one would cut the history wherever it was pruned. Without a window, the count applies to each side apart, the server's snapshots and the backups asked for, so asking for backups never prunes the generation WAL is shipped into. Spacing counts from the last manual backup this server took as well as the newest one stored, since a window shorter than the spacing can prune that backup first. A server without a schedule still prunes on a timer, or nothing would age out of an idle database's window. The policy has its own endpoint, not `limits`, so a control plane can send it to an older server and get 404 instead of breaking database creation. |
| D26 | A restore in place is one admin call. It stores what it replaces as a `before_restore` backup before swapping the file, and holds the database's writer turn and write lock from that copy to the swap. A write that waited for its turn looks the database up again | Deleting and restoring under the same name left a gap where the name 404'd and nothing to undo with. Holding both locks means every acknowledged commit is either in the undo backup or in the restored database: commits before the copy are in the copy, none can land on the old file after it, and a writer queued behind the restore finds its database closing and moves to the new file. Its open transactions end first, or a parked one would hold the write lock until it idled out. The old WAL goes before the new file moves in, so it is never replayed onto it. |

## HTTP surface

- `GET /health`: `ok`
- `GET /v2`: version check, 200
- `POST /v2/pipeline`: Hrana v2 pipeline (`execute`, `batch`, `sequence`,
  `store_sql`, `close_sql`, `get_autocommit`, `close`)
- `POST /v1/console/run`, `GET /v1/console/schema`, `POST /v1/console/rows`:
  the console API (D22), database token
- Admin, `Authorization: Bearer <admin secret>`: see the API table in the README.

Database names: `[a-z0-9][a-z0-9-]{0,62}`.

## Configuration

| Env | Default | Meaning |
|-----|---------|---------|
| `AOZUMI_DB_LISTEN` | `127.0.0.1:8080` | Listen address |
| `AOZUMI_DB_DATA_DIR` | `./data` | Where database files live |
| `AOZUMI_DB_SECRET` | required | HMAC key for database tokens and the admin API |
| `AOZUMI_DB_DEV` | unset | `1` = dev mode without a secret (D9) |

Backup and limit settings are listed in the README.

## Roadmap

1. Done: server, protocol v2, tokens, admin API, tests against the real client.
2. Done: snapshots to S3 or a directory, retention, point-in-time restore.
   Next: stream large backups instead of holding them in memory; compression;
   token generations kept with the backups (D23).
3. Done: read-only tokens, revocable tokens (D23), statement policy, size/time/rate limits.
   Next: rows-read and rows-written budgets, metrics.
4. Done: benchmarks (YCSB, open-loop latency) and the first tuning pass.
   Next: TATP, a quiet machine for publishable numbers, the read path.
5. AOZUMI integration as an experimental option: one database per app and
   preview, a `DATABASE_URL` + token injected as secrets, gateway route
   `db.aozumi.dev`.
6. Fold host: use the same engine for fold data and its change log for live updates.
7. A standby machine with replication, then failover.
