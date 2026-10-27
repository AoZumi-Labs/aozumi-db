# aozumi-db

A database for every app, as a file. aozumi-db is a SQLite-compatible
database server written in Rust. It speaks libSQL's HTTP protocol, so the
libSQL clients you already use work without changes.

> **Experimental.** aozumi-db is pre-1.0 and has no replication yet. Turn on
> backups.

## Why

Small apps need a real database, but a database instance per app costs money
even while nobody uses it. In aozumi-db each database is one file on disk: an
idle database costs nothing, and a new one is ready in milliseconds.

## Quick start

```bash
cargo run --release
# aozumi-db: AOZUMI_DB_SECRET is unset, running in dev mode: no auth, one database named `default`
```

```js
import { createClient } from "@libsql/client";

const db = createClient({ url: "http://127.0.0.1:8080" });
await db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
await db.execute({ sql: "INSERT INTO notes (body) VALUES (:body)", args: { body: "hello" } });
console.log((await db.execute("SELECT * FROM notes")).rows);
```

Drizzle works the same way through `drizzle-orm/libsql`.

## Running with auth

Set a secret, create a database, and mint a token for it:

```bash
export AOZUMI_DB_SECRET="$(openssl rand -hex 32)"
cargo run --release &

curl -X POST localhost:8080/v1/databases \
  -H "authorization: Bearer $AOZUMI_DB_SECRET" -d '{"name":"notes"}'

cargo run --release -- token notes 3600   # or POST /v1/databases/notes/tokens
```

The token names its database, so every app uses the same URL with its own
token:

```js
const db = createClient({ url: "https://db.example.com", authToken: process.env.DB_TOKEN });
```

| Env | Default | Meaning |
|-----|---------|---------|
| `AOZUMI_DB_LISTEN` | `127.0.0.1:8080` | Listen address |
| `AOZUMI_DB_DATA_DIR` | `./data` | Where database files live |
| `AOZUMI_DB_SECRET` | unset (dev mode) | Signs tokens; also the admin API credential |
| `AOZUMI_DB_BACKUP_URL` | unset (no backups) | `s3://bucket/prefix` or `file:///path` |
| `AOZUMI_DB_BACKUP_EVERY` | `300` | Seconds between backups of changed databases; `0` = only on request |
| `AOZUMI_DB_BACKUP_KEEP` | `24` | Backups kept per database |
| `AOZUMI_DB_BACKUP_WAL_MS` | `1000` | Ship the WAL this often for point-in-time restore; `0` = off |
| `AOZUMI_DB_S3_ENDPOINT` | AWS S3 | An S3-compatible endpoint, e.g. Cloudflare R2 or MinIO |
| `AOZUMI_DB_MAX_SIZE_BYTES` | `0` (none) | Default size limit for new databases |
| `AOZUMI_DB_MAX_STATEMENT_MS` | `10000` | Default statement time limit; `0` = none |
| `AOZUMI_DB_MAX_REQUESTS_PER_SECOND` | `0` (none) | Default request rate limit |

## Read-only tokens

```bash
cargo run --release -- token notes 3600 --read-only
# or: POST /v1/databases/notes/tokens with {"read_only": true}
```

A read-only token can run any query, but every write fails with
`SQLITE_READONLY`. Give it to dashboards, analytics and anything that should
never change data.

## What tenant SQL can do

Each database is isolated from the server and from other databases. Tenant
SQL that would reach outside it is refused with `SQLITE_AUTH`: `ATTACH`,
`DETACH`, `VACUUM INTO`, and any `PRAGMA` outside a short list of harmless
ones (`table_info`, `user_version`, `foreign_keys`, `integrity_check` and the
like). That also stops a tenant from lifting its own limits or switching a
read-only token to read-write.

## Limits

Every database has three limits, each optional:

| Limit | What happens |
|---|---|
| `max_size_bytes` | Writes that would grow the database past it fail with `SQLITE_FULL` |
| `max_statement_ms` | A statement running longer is stopped with `SQLITE_INTERRUPT` |
| `max_requests_per_second` | Requests over the rate get HTTP 429 with `Retry-After` |

New databases get the server's defaults (the `AOZUMI_DB_MAX_*` settings),
changed per database at creation or later:

```bash
curl -X POST localhost:8080/v1/databases -H "authorization: Bearer $AOZUMI_DB_SECRET" \
  -d '{"name": "notes", "limits": {"max_size_bytes": 104857600, "max_requests_per_second": 200}}'

# a missing field keeps its value, null lifts the limit
curl -X PATCH localhost:8080/v1/databases/notes/limits -H "authorization: Bearer $AOZUMI_DB_SECRET" \
  -d '{"max_size_bytes": null}'

curl localhost:8080/v1/databases/notes -H "authorization: Bearer $AOZUMI_DB_SECRET"
# {"name": "notes", "size_bytes": 8192, "limits": {...}}
```

## Backups

With `AOZUMI_DB_BACKUP_URL` set, every database that changed is backed up on
a schedule, and the oldest backups past `AOZUMI_DB_BACKUP_KEEP` are deleted.
A backup is a consistent copy of the whole database as one file. Writes keep
working while it is taken: they wait for the local file copy instead of failing.

For S3, credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
(and `AWS_SESSION_TOKEN`), or from the EC2 instance role when those are unset.
The region comes from `AWS_REGION`.

```bash
# take one now
curl -X POST localhost:8080/v1/databases/notes/backups -H "authorization: Bearer $AOZUMI_DB_SECRET"

# list them, newest first
curl localhost:8080/v1/databases/notes/backups -H "authorization: Bearer $AOZUMI_DB_SECRET"

# restore into a new database (or under the old name, once it is gone)
curl -X POST localhost:8080/v1/databases -H "authorization: Bearer $AOZUMI_DB_SECRET" \
  -d '{"name": "notes-restored", "restore": {"database": "notes", "backup": "latest"}}'
```

Restores never overwrite a live database. A new machine pointed at the same
backup store can restore every database it finds there.

### Point-in-time restore

With backups on, the WAL is also shipped every second
(`AOZUMI_DB_BACKUP_WAL_MS`), so a database can be restored to any moment
since its oldest kept backup, to within about a second:

```bash
curl -X POST localhost:8080/v1/databases -H "authorization: Bearer $AOZUMI_DB_SECRET" \
  -d '{"name": "notes-before-the-bad-deploy", "restore": {"database": "notes", "at": "2026-10-26T09:30:00Z"}}'
# {"name": "notes-before-the-bad-deploy", "restored_to_ms": 1792999799512}
```

`at` takes an RFC 3339 UTC time or unix milliseconds. The response says
exactly which moment the copy holds. Listing a database's backups also
reports `point_in_time.earliest_ms` and `latest_ms`, the window you can
restore from.

## API

| Method | Path | Auth | |
|--------|------|------|---|
| `POST` | `/v2/pipeline` | database token | libSQL Hrana v2 pipeline |
| `GET` | `/v2` | none | protocol version check |
| `GET` | `/health` | none | liveness |
| `GET` | `/v1/databases` | admin | list databases |
| `POST` | `/v1/databases` | admin | create `{"name"}`, optionally `"limits"` and `"restore": {"database", "backup" or "at"}` |
| `GET` | `/v1/databases/{name}` | admin | size and limits |
| `PATCH` | `/v1/databases/{name}/limits` | admin | change limits |
| `DELETE` | `/v1/databases/{name}` | admin | delete a database and its files |
| `POST` | `/v1/databases/{name}/tokens` | admin | `{"ttl_seconds": N, "read_only": bool}` → `{"token": "..."}` |
| `POST` | `/v1/databases/{name}/backups` | admin | back up now → `{"id", "size_bytes"}` |
| `GET` | `/v1/databases/{name}/backups` | admin | backups, newest first, and the point-in-time window |
| `POST` | `/v1/console/run` | database token | `{"sql", "confirm_destructive"?}`: run a script statement by statement for the AOZUMI DB console |
| `GET` | `/v1/console/schema` | database token | the database's tables and their columns |
| `POST` | `/v1/console/rows` | database token | `{"table", "page"}`: 50 rows of a table and its row count |

The console endpoints are for people reading results, not for apps: a script
is split by the engine's own parser, statements run in order and the first
error stops the rest, a transaction left open is rolled back, and each
statement keeps at most 1,000 rows (4 MB across a request) while counting
them all. A read-only token reads only, as everywhere. With a read-write
token, a script that drops a table, index, view, trigger or column, or deletes
or updates every row of a table, answers 409 `confirm_destructive` with what it
would destroy, and runs only when sent again with `"confirm_destructive": true`.

Supported pipeline requests: `execute`, `batch` (with conditions), `sequence`,
`store_sql`, `close_sql`, `get_autocommit`, `close`. Interactive
transactions work through batons. Protocol v3 (cursors, protobuf) and
`describe` are not supported yet.

### Tokens

```
adb1.<base64url(JSON claims)>.<base64url(HMAC-SHA256(secret, "adb1.<claims>"))>
claims: {"db": "notes", "exp": 4102444800, "read_only": true}   // exp and read_only optional
```

Base64url without padding. Anything that holds the secret can mint tokens,
so a control plane can create them without calling the server.

## Tests

```bash
cargo test                                         # HTTP-level tests
cargo build && cd tests/client && npm install && node --test   # real @libsql/client
```

## Performance

Measured with YCSB, open-loop write latency and micro benchmarks; method and
numbers in [BENCHMARKS.md](BENCHMARKS.md). On a shared 6-core laptop, with
clients on the same machine: 36k reads/s on YCSB C, 14k ops/s on YCSB A
(50% updates) with p99.9 at 26 ms.

## Design

See [DESIGN.md](DESIGN.md) for the decisions and the roadmap.

## License

MIT.
