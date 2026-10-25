# aozumi-db design

aozumi-db is a SQLite-compatible database server for small apps, written in
Rust. Each app gets its own database file on the server's disk. Apps reach it
over HTTP with the libSQL protocol (Hrana over HTTP, version 2), so
`@libsql/client`, Drizzle and other libSQL clients work without changes.

Status: experimental. The engine is pre-1.0, so keep independent backups.

## Goals

1. A database per app that costs nothing while idle: a file, not an instance.
2. Unchanged client code: point `@libsql/client` at `https://db.example.com`
   with a token, and it works.
3. One Rust process that is small to read and to run, and that sits next to
   AOZUMI's Rust gateway without being part of it.

Not goals for v0: replication, point-in-time restore, read-only tokens,
protocol v3 (cursors, protobuf), `describe`.

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
| D9 | No secret configured = dev mode: no auth, one database named `default` | Local development and tests need no setup. The server logs a warning. |

## HTTP surface

- `GET /health`: `ok`
- `GET /v2`: version check, 200
- `POST /v2/pipeline`: Hrana v2 pipeline (`execute`, `batch`, `sequence`,
  `store_sql`, `close_sql`, `get_autocommit`, `close`)
- Admin, `Authorization: Bearer <admin secret>`:
  - `GET /v1/databases`
  - `POST /v1/databases` with `{"name": "..."}`
  - `DELETE /v1/databases/{name}`
  - `POST /v1/databases/{name}/tokens` with `{"ttl_seconds": N}` → `{"token": "..."}`

Database names: `[a-z0-9][a-z0-9-]{0,62}`.

## Configuration

| Env | Default | Meaning |
|-----|---------|---------|
| `AOZUMI_DB_LISTEN` | `127.0.0.1:8080` | Listen address |
| `AOZUMI_DB_DATA_DIR` | `./data` | Where database files live |
| `AOZUMI_DB_SECRET` | unset (dev mode) | HMAC key for database tokens and the admin API |

## Roadmap

1. v0 (this): server, protocol v2, tokens, admin API, tests against the real client.
2. Backups: ship WAL frames and snapshots to S3; restore to a point in time.
3. AOZUMI integration as an experimental option: one database per app and
   preview, a `DATABASE_URL` + token injected as secrets, gateway route
   `db.aozumi.dev`.
4. Fold host: use the same engine for fold data and its change log for live updates.
5. Read-only tokens, quotas (size, rows read and written), metrics.
6. A standby machine with replication, then failover.
