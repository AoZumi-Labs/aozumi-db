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
| `AOZUMI_DB_S3_ENDPOINT` | AWS S3 | An S3-compatible endpoint, e.g. Cloudflare R2 or MinIO |

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

## API

| Method | Path | Auth | |
|--------|------|------|---|
| `POST` | `/v2/pipeline` | database token | libSQL Hrana v2 pipeline |
| `GET` | `/v2` | none | protocol version check |
| `GET` | `/health` | none | liveness |
| `GET` | `/v1/databases` | admin | list databases |
| `POST` | `/v1/databases` | admin | create `{"name": "..."}`, optionally `"restore": {"database", "backup"}` |
| `DELETE` | `/v1/databases/{name}` | admin | delete a database and its files |
| `POST` | `/v1/databases/{name}/tokens` | admin | `{"ttl_seconds": N}` → `{"token": "..."}` |
| `POST` | `/v1/databases/{name}/backups` | admin | back up now → `{"id", "size_bytes"}` |
| `GET` | `/v1/databases/{name}/backups` | admin | list backups, newest first |

Supported pipeline requests: `execute`, `batch` (with conditions), `sequence`,
`store_sql`, `close_sql`, `get_autocommit`, `close`. Interactive
transactions work through batons. Protocol v3 (cursors, protobuf) and
`describe` are not supported yet.

### Tokens

```
adb1.<base64url(JSON claims)>.<base64url(HMAC-SHA256(secret, "adb1.<claims>"))>
claims: {"db": "notes", "exp": 4102444800}   // exp optional, unix seconds
```

Base64url without padding. Anything that holds the secret can mint tokens,
so a control plane can create them without calling the server.

## Tests

```bash
cargo test                                         # HTTP-level tests
cargo build && cd tests/client && npm install && node --test   # real @libsql/client
```

## Design

See [DESIGN.md](DESIGN.md) for the decisions and the roadmap (backups,
replication, quotas).

## License

MIT.
