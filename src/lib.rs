//! aozumi-db: a database per app, served over libSQL's HTTP protocol.
//! See DESIGN.md for the decisions behind this file.

mod auth;
mod backup;
mod console;
mod engine;
mod io;
mod limits;
mod policy;
mod proto;
mod registry;
mod stream;

use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;

pub use auth::{mint as mint_token, Claims};
pub use backup::{BackupConfig, BackupTarget, Credentials, S3Options};
use backup::{BackupError, BackupInfo, Backups, Kind};
use proto::{PipelineReq, PipelineResp};
pub use engine::Synchronous;
pub use io::SyncMethod;
pub use limits::{BackupPolicy, Limits};
use limits::{BackupPolicyPatch, LimitsPatch, RateLimiter};
pub use registry::token_generation;
use registry::{Registry, RegistryError};
use stream::{Stream, Streams};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
/// Backup downloads sent at once. Each holds its whole backup in memory
/// until the last byte is sent, so a few slow clients cannot use it all up.
const DOWNLOADS_AT_ONCE: usize = 2;
const DEV_DATABASE: &str = "default";
/// How often the background checkpointer looks, and from what WAL size
/// (about 4 MB of pages) it checkpoints.
const CHECKPOINT_EVERY: Duration = Duration::from_millis(250);
const CHECKPOINT_FRAMES: u64 = 1000;
/// How long a statement waits for another writer (or a backup) to finish.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Config {
    pub data_dir: PathBuf,
    /// HMAC key for tokens and the admin API. `None` runs in dev mode.
    pub secret: Option<Vec<u8>>,
    /// How long a stream may sit unused before it is closed.
    pub stream_idle_timeout: Duration,
    pub backup: Option<BackupConfig>,
    /// Limits for new databases; each database keeps its own copy.
    pub default_limits: Limits,
    /// The backup policy of databases that have none of their own.
    pub default_backup_policy: BackupPolicy,
    /// When commits reach the disk. `Full` unless chosen otherwise.
    pub synchronous: Synchronous,
    /// How they get there: `fdatasync` and a WAL reused in place, unless chosen otherwise.
    pub sync_method: SyncMethod,
}

impl Config {
    pub fn new(data_dir: PathBuf, secret: Option<Vec<u8>>) -> Self {
        Self {
            data_dir,
            secret,
            stream_idle_timeout: Duration::from_secs(10),
            backup: None,
            default_limits: Limits::default(),
            default_backup_policy: BackupPolicy::default(),
            synchronous: Synchronous::Full,
            sync_method: SyncMethod::default(),
        }
    }
}

struct State {
    secret: Option<Vec<u8>>,
    registry: Registry,
    rates: RateLimiter,
    streams: Arc<Streams>,
    backups: Backups,
    downloads: Arc<tokio::sync::Semaphore>,
}

pub async fn serve(listener: TcpListener, config: Config) -> anyhow::Result<()> {
    let state = Arc::new(State {
        secret: config.secret,
        registry: Registry::new(config.data_dir, config.default_limits, config.default_backup_policy, config.synchronous, config.sync_method),
        rates: RateLimiter::default(),
        streams: Streams::new(config.stream_idle_timeout),
        backups: Backups::new(config.backup),
        downloads: Arc::new(tokio::sync::Semaphore::new(DOWNLOADS_AT_ONCE)),
    });
    // Exactly one task checkpoints the WAL: the shipper when point-in-time
    // restore is on, otherwise a background checkpointer.
    match state.backups.wal_every() {
        Some(every) => {
            let state = state.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    state.backups.ship_all(&state.registry).await;
                }
            });
        }
        None => {
            let state = state.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(CHECKPOINT_EVERY);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    state.registry.checkpoint_all(CHECKPOINT_FRAMES).await;
                }
            });
        }
    }
    if let Some(every) = state.backups.every() {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                state.backups.run_scheduled(&state.registry).await;
            }
        });
    }
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle(state, req).await) }
            });
            // A client that hangs up mid-request is not a server error.
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}

/// Every response's body: bytes in memory, boxed so a backup download can
/// carry its place in the download limit (`Permitted`) until it is sent.
type Body = BoxBody<Bytes, Infallible>;
type Resp = Response<Body>;

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).boxed()
}

/// A body that keeps a download permit until hyper drops it: after its last
/// byte is sent, or when the client goes away.
struct Permitted {
    body: Full<Bytes>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl hyper::body::Body for Permitted {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

async fn handle(state: Arc<State>, req: Request<Incoming>) -> Resp {
    let path = req.uri().path().to_string();
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match (req.method(), segments.as_slice()) {
        (&Method::GET, ["health"]) => json(StatusCode::OK, &"ok"),
        (&Method::GET, ["v2"]) => json(StatusCode::OK, &json!({})),
        (&Method::POST, ["v2", "pipeline"]) => pipeline(&state, req).await,
        (&Method::POST, ["v1", "console", "run"]) => console_run(&state, req).await,
        (&Method::GET, ["v1", "console", "schema"]) => console_schema(&state, req).await,
        (&Method::POST, ["v1", "console", "rows"]) => console_rows(&state, req).await,
        (_, ["v1", "databases", ..]) => {
            if !is_admin(&state, &req) {
                return text(StatusCode::UNAUTHORIZED, "admin credentials required");
            }
            match (req.method(), &segments[2..]) {
                (&Method::GET, []) => list_databases(&state).await,
                (&Method::POST, []) => create_database(&state, req).await,
                (&Method::GET, [name]) => database_info(&state, name).await,
                (&Method::DELETE, [name]) => delete_database(&state, name, req.uri().query()).await,
                (&Method::PATCH, [name, "limits"]) => update_limits(&state, name, req).await,
                (&Method::POST, [name, "tokens"]) => create_token(&state, name, req).await,
                (&Method::POST, [name, "tokens", "rotate"]) => rotate_tokens(&state, name).await,
                (&Method::POST, [name, "backups"]) => match state.backups.snapshot(&state.registry, name, Kind::Manual).await {
                    Ok(info) => json(StatusCode::CREATED, &info),
                    Err(err) => backup_error(err),
                },
                (&Method::GET, [name, "backups"]) => list_backups(&state, name).await,
                (&Method::GET, [name, "backups", id]) => download_backup(&state, name, id).await,
                (&Method::PATCH, [name, "backups"]) => update_backup_policy(&state, name, req).await,
                (&Method::POST, [name, "restore"]) => restore_in_place(&state, name, req).await,
                _ => text(StatusCode::NOT_FOUND, "not found"),
            }
        }
        _ => text(StatusCode::NOT_FOUND, "not found"),
    }
}

/// The database a request's token opens, and whether the token is
/// read-only, with the database's request rate counted. A token from before
/// the database's last rotation is refused before it counts. Dev mode serves
/// its one database to anyone.
async fn token_database(state: &State, req: &Request<Incoming>) -> Result<(String, bool, registry::OpenDb), Resp> {
    let (database, read_only, generation, create) = match &state.secret {
        None => (DEV_DATABASE.to_string(), false, None, true),
        Some(secret) => match bearer(req).and_then(|t| auth::verify(secret, t, unix_now())) {
            Some(claims) => (claims.db, claims.read_only, Some(claims.generation), false),
            None => return Err(text(StatusCode::UNAUTHORIZED, "a valid database token is required")),
        },
    };
    let db = state.registry.open(&database, create).await.map_err(registry_error)?;
    if generation.is_some_and(|minted| minted < db.token_generation) {
        return Err(text(StatusCode::UNAUTHORIZED, "this database token was revoked; use a newer one"));
    }
    if let Some(rate) = db.limits.max_requests_per_second {
        if let Err(wait) = state.rates.check(&database, rate) {
            let retry_after = wait.as_secs_f64().ceil().max(1.0) as u64;
            return Err(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", retry_after.to_string())
                .body(full("this database is over its request rate limit"))
                .unwrap());
        }
    }
    Ok((database, read_only, db))
}

async fn pipeline(state: &State, req: Request<Incoming>) -> Resp {
    let (database, read_only, db) = match token_database(state, &req).await {
        Ok(found) => found,
        Err(resp) => return resp,
    };

    let msg: PipelineReq = match read_json(req).await {
        Ok(msg) => msg,
        Err(resp) => return resp,
    };

    let mut stream = match &msg.baton {
        Some(baton) => match state.streams.take(baton).await {
            // A stream opened before the database's last rotation is over,
            // even for a token minted after it, and so is one whose database
            // was deleted, even if another has been created under its name.
            Some(stream)
                if stream.db == database
                    && stream.read_only == read_only
                    && stream.token_generation >= db.token_generation
                    && Arc::ptr_eq(&stream.file, &db.db) =>
            {
                stream
            }
            _ => return text(StatusCode::BAD_REQUEST, "unknown or expired baton"),
        },
        None => match open_stream(&database, &db, read_only) {
            Ok(stream) => stream,
            Err(resp) => return resp,
        },
    };

    // Writers queue in arrival order; reads, and streams already holding a
    // transaction, go straight through.
    let mut db = db;
    let turn = match stream.may_write(&msg.requests) && !stream.in_transaction() {
        true => match writer_turn(state, &database, &mut db, &mut stream, msg.baton.is_none(), read_only).await {
            Ok(turn) => Some(turn),
            Err(resp) => return resp,
        },
        false => None,
    };
    // Statements block on disk and CPU, so they run on the blocking pool.
    let (stream, results) = match tokio::task::spawn_blocking(move || {
        let results = msg.requests.iter().map(|request| stream.handle(request)).collect::<Vec<_>>();
        (stream, results)
    })
    .await
    {
        Ok(done) => done,
        Err(err) => return text(StatusCode::INTERNAL_SERVER_ERROR, &format!("statement execution failed: {err}")),
    };
    drop(turn);

    let baton = match stream.is_closed() {
        true => {
            stream.release();
            None
        }
        false => {
            // Rotating the database's tokens or deleting it drops only parked
            // streams, so it misses a stream that is busy with a request. Look
            // again once this one is parked: both change the database before
            // they drop its streams, so either this sees the change or they
            // find the stream parked. A database deleted and already created
            // again under its name keeps its token generation, not its file.
            let (generation, file) = (stream.token_generation, stream.file.clone());
            let baton = state.streams.park(stream).await;
            let current = state
                .registry
                .open(&database, false)
                .await
                .is_ok_and(|now| now.token_generation <= generation && Arc::ptr_eq(&now.db, &file) && !now.is_closing());
            // Looking can wait on the registry for longer than the idle
            // timeout (a restore holds it while it checks the whole copy), and
            // the idle reaper may drop the stream meanwhile: the baton goes
            // out only if its stream is still parked.
            match current && state.streams.is_parked(&baton).await {
                true => Some(baton),
                // Taking the stream back drops it, rolling back its
                // transaction, and no baton tells the client it is closed.
                false => {
                    drop(state.streams.take(&baton).await);
                    None
                }
            }
        }
    };
    json(StatusCode::OK, &PipelineResp { baton, base_url: None, results })
}

/// The console runs a script: `{"sql": "...", "confirm_destructive"?: bool}`
/// (see src/console.rs). With writes allowed, a script that destroys data
/// runs only when confirmed; otherwise 409 `confirm_destructive` lists what it
/// would destroy and nothing runs.
async fn console_run(state: &State, req: Request<Incoming>) -> Resp {
    #[derive(Deserialize)]
    struct Body {
        sql: String,
        #[serde(default)]
        confirm_destructive: bool,
    }
    let (database, read_only, db) = match token_database(state, &req).await {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let body: Body = match read_json(req).await {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    if body.sql.len() > console::MAX_SCRIPT_BYTES {
        return console_error(StatusCode::PAYLOAD_TOO_LARGE, &format!("Keep a script under {} KB.", console::MAX_SCRIPT_BYTES / 1024), "sql_too_long");
    }
    let split = console::split(&body.sql);
    if split.is_empty() {
        return console_error(StatusCode::BAD_REQUEST, "There is no SQL to run.", "empty");
    }
    if split.len() > console::MAX_STATEMENTS {
        return console_error(StatusCode::BAD_REQUEST, &format!("Run at most {} statements at a time.", console::MAX_STATEMENTS), "too_many_statements");
    }
    if !read_only && !body.confirm_destructive {
        let destructive = console::destructive(&split);
        if !destructive.is_empty() {
            return json(
                StatusCode::CONFLICT,
                &json!({"error": "This script destroys data. Confirm it to run it.", "code": "confirm_destructive", "destructive": destructive}),
            );
        }
    }
    let mut stream = match open_stream(&database, &db, read_only) {
        Ok(stream) => stream,
        Err(resp) => return resp,
    };
    // A read-write console request takes its turn with the app's writers,
    // whatever its SQL: the person asked to write.
    let mut db = db;
    let turn = match read_only {
        false => match writer_turn(state, &database, &mut db, &mut stream, true, read_only).await {
            Ok(turn) => Some(turn),
            Err(resp) => return resp,
        },
        true => None,
    };
    let started = Instant::now();
    let (stream, statements) = match tokio::task::spawn_blocking(move || {
        let statements = stream.run_console(split);
        (stream, statements)
    })
    .await
    {
        Ok(done) => done,
        Err(err) => return text(StatusCode::INTERNAL_SERVER_ERROR, &format!("statement execution failed: {err}")),
    };
    drop(turn);
    stream.release();
    let duration_ms = started.elapsed().as_millis() as u64;
    json(StatusCode::OK, &console::RunResult { read_only, duration_ms, statements })
}

/// The console's schema: `{"tables": [{"name", "columns": [...]}]}`.
async fn console_schema(state: &State, req: Request<Incoming>) -> Resp {
    let (_, _, db) = match token_database(state, &req).await {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    match console_read(db, console::schema).await {
        Ok(tables) => json(StatusCode::OK, &json!({"tables": tables})),
        Err(resp) => resp,
    }
}

/// One page of a table for the console: `{"table": "...", "page": 1}`.
async fn console_rows(state: &State, req: Request<Incoming>) -> Resp {
    #[derive(Deserialize)]
    struct Body {
        table: String,
        page: Option<u64>,
    }
    let (_, _, db) = match token_database(state, &req).await {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let body: Body = match read_json(req).await {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    if !console::valid_table_name(&body.table) {
        return console_error(StatusCode::BAD_REQUEST, "Name a table.", "table_invalid");
    }
    let page = body.page.unwrap_or(1).clamp(1, console::MAX_PAGE);
    match console_read(db, move |conn: &engine::Conn| console::page(conn, &body.table, page)).await {
        Ok(page) => json(StatusCode::OK, &page),
        Err(resp) => resp,
    }
}

/// Runs a console lookup on a read-only connection, whatever the token
/// allows: the lookups only read. A missing table answers 404.
async fn console_read<T: Send + 'static>(
    db: registry::OpenDb,
    read: impl FnOnce(&engine::Conn) -> Result<T, proto::Error> + Send + 'static,
) -> Result<T, Resp> {
    let conn = db
        .checkout(BUSY_TIMEOUT, true)
        .map_err(|err| text(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot connect: {err}")))?;
    let pool = db.pool.clone();
    let (conn, result) = tokio::task::spawn_blocking(move || {
        let result = read(&conn);
        (conn, result)
    })
    .await
    .map_err(|err| text(StatusCode::INTERNAL_SERVER_ERROR, &format!("statement execution failed: {err}")))?;
    pool.put(conn, true);
    result.map_err(|error| match console::is_missing_table(&error) {
        true => console_error(StatusCode::NOT_FOUND, "That table is not in this database.", "table_not_found"),
        false => console_error(StatusCode::UNPROCESSABLE_ENTITY, &error.message, error.code.as_deref().unwrap_or("SQLITE_ERROR")),
    })
}

/// Take the database's writer turn. When the database was deleted or
/// restored in place while this request waited, its file is gone: a stream
/// this request opened moves to the database that has the name now, and a
/// stream from an earlier request (a baton) ends, as it would at its next
/// request. No write lands on a file that was replaced.
async fn writer_turn(
    state: &State,
    database: &str,
    db: &mut registry::OpenDb,
    stream: &mut Stream,
    fresh: bool,
    read_only: bool,
) -> Result<tokio::sync::OwnedMutexGuard<()>, Resp> {
    for _ in 0..3 {
        let turn = db.writers.clone().lock_owned().await;
        if !db.is_closing() {
            return Ok(turn);
        }
        drop(turn);
        if !fresh {
            return Err(text(StatusCode::BAD_REQUEST, "unknown or expired baton"));
        }
        let now = state.registry.open(database, false).await.map_err(registry_error)?;
        *stream = open_stream(database, &now, read_only)?;
        *db = now;
    }
    Err(text(StatusCode::SERVICE_UNAVAILABLE, "the database is being replaced; try again"))
}

fn open_stream(database: &str, db: &registry::OpenDb, read_only: bool) -> Result<Stream, Resp> {
    let conn = db
        .checkout(BUSY_TIMEOUT, read_only)
        .map_err(|err| text(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot connect: {err}")))?;
    Ok(Stream::new(database.to_string(), read_only, db.token_generation, db.db.clone(), conn, db.pool.clone()))
}

async fn list_databases(state: &State) -> Resp {
    match state.registry.list().await {
        Ok(names) => {
            let databases = names.into_iter().map(|name| json!({"name": name})).collect::<Vec<_>>();
            json(StatusCode::OK, &json!({"databases": databases}))
        }
        Err(err) => registry_error(err),
    }
}

async fn create_database(state: &State, req: Request<Incoming>) -> Resp {
    #[derive(Deserialize)]
    struct Body {
        name: String,
        /// Start from a backup instead of empty.
        restore: Option<Restore>,
        /// Changes to the server's default limits for this database.
        limits: Option<LimitsPatch>,
    }
    #[derive(Deserialize)]
    struct Restore {
        database: String,
        /// A snapshot id, or `latest`.
        backup: Option<String>,
        /// A moment to restore to: unix milliseconds or an RFC 3339 time.
        at: Option<serde_json::Value>,
    }
    let body: Body = match read_json(req).await {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    let limits = match state.registry.limits_for_new(body.limits.as_ref()) {
        Ok(limits) => limits,
        Err(err) => return registry_error(err),
    };
    let mut restored_to_ms = None;
    let created = match body.restore {
        None => state.registry.create(&body.name, limits).await,
        Some(from) => {
            // Refuse early so a doomed restore does not download a backup.
            match state.registry.open(&body.name, false).await {
                Ok(_) => return registry_error(RegistryError::Exists),
                Err(RegistryError::NotFound) => {}
                Err(err) => return registry_error(err),
            }
            let fetched = match (&from.at, &from.backup) {
                (Some(at), _) => match backup::parse_time(at) {
                    Ok(at) => state.backups.fetch_at(&from.database, at).await.map(|restored| {
                        restored_to_ms = Some(restored.restored_to_ms);
                        restored.bytes
                    }),
                    Err(err) => Err(err),
                },
                (None, Some(id)) => state.backups.fetch(&from.database, id).await.map(|(bytes, _)| bytes),
                (None, None) => return text(StatusCode::BAD_REQUEST, "a restore needs `backup` or `at`"),
            };
            match fetched {
                Ok(bytes) => state.registry.create_from(&body.name, bytes, limits).await,
                Err(err) => return backup_error(err),
            }
        }
    };
    match created {
        Ok(()) => match restored_to_ms {
            Some(ms) => json(StatusCode::CREATED, &json!({"name": body.name, "restored_to_ms": ms})),
            None => json(StatusCode::CREATED, &json!({"name": body.name})),
        },
        Err(err) => registry_error(err),
    }
}

async fn database_info(state: &State, name: &str) -> Resp {
    let db = match state.registry.open(name, false).await {
        Ok(db) => db,
        Err(err) => return registry_error(err),
    };
    match state.registry.size(name).await {
        Ok(size_bytes) => json(
            StatusCode::OK,
            &json!({"name": name, "size_bytes": size_bytes, "limits": db.limits, "token_generation": db.token_generation}),
        ),
        Err(err) => registry_error(err),
    }
}

async fn update_limits(state: &State, name: &str, req: Request<Incoming>) -> Resp {
    let patch: LimitsPatch = match read_json(req).await {
        Ok(patch) => patch,
        Err(resp) => return resp,
    };
    match state.registry.set_limits(name, &patch).await {
        Ok(limits) => json(StatusCode::OK, &json!({"name": name, "limits": limits})),
        Err(err) => registry_error(err),
    }
}

/// `{"backups", "point_in_time", "status"}`: see `backup::Listing`.
async fn list_backups(state: &State, name: &str) -> Resp {
    match state.backups.listing(&state.registry, name).await {
        Ok(listing) => json(StatusCode::OK, &listing),
        Err(err) => backup_error(err),
    }
}

/// Restore a database in place: `{"backup": id | "latest"}` or `{"at": time}`
/// → `{"name", "restored_to_ms", "before_restore": backup}` (D26). The
/// database keeps its name, tokens, limits and backup policy.
async fn restore_in_place(state: &State, name: &str, req: Request<Incoming>) -> Resp {
    #[derive(Deserialize)]
    struct Body {
        backup: Option<String>,
        at: Option<serde_json::Value>,
    }
    let body: Body = match read_json(req).await {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    if let Err(err) = state.registry.open(name, false).await {
        return registry_error(err);
    }
    // The backup is read first: a backup that cannot be read changes nothing.
    let fetched = match (&body.at, &body.backup) {
        (Some(at), _) => match backup::parse_time(at) {
            Ok(at) => state.backups.fetch_at(name, at).await.map(|restored| (restored.bytes, restored.restored_to_ms)),
            Err(err) => Err(err),
        },
        (None, Some(id)) => state.backups.fetch(name, id).await.map(|(bytes, info)| (bytes, info.taken_ms)),
        (None, None) => return text(StatusCode::BAD_REQUEST, "a restore needs `backup` or `at`"),
    };
    let (bytes, restored_to_ms) = match fetched {
        Ok(fetched) => fetched,
        Err(err) => return backup_error(err),
    };
    match replace_with_backup(state, name, bytes).await {
        Ok(before) => json(StatusCode::OK, &json!({"name": name, "restored_to_ms": restored_to_ms, "before_restore": before})),
        Err(err) => backup_error(err),
    }
}

/// The restore in place itself. The copy is written and checked without any
/// lock. Then, holding the database's backup lock and its writer turn, it
/// ends the database's open transactions, takes the write lock on the old
/// file and copies it, stores that copy as the `before_restore` backup (and
/// stops, changing nothing, if it cannot), and swaps the file. Writes wait
/// meanwhile; reads go on. A write that waited lands in the restored file.
async fn replace_with_backup(state: &State, name: &str, bytes: Vec<u8>) -> Result<BackupInfo, BackupError> {
    let staged = state.registry.stage(name, bytes).await?;
    let lock = state.backups.lock(name);
    let _backing_up = lock.lock().await;
    let old = match state.registry.open(name, false).await {
        Ok(db) => db,
        Err(err) => {
            staged.remove().await;
            return Err(err.into());
        }
    };
    let turn = old.writers.clone().lock_owned().await;
    old.set_closing(true);
    // A parked transaction would hold the write lock until it idled out.
    state.streams.close_database(name).await;
    let held = match state.registry.hold(name, &old).await {
        Ok(held) => held,
        Err(err) => {
            old.set_closing(false);
            staged.remove().await;
            return Err(err.into());
        }
    };
    let (held, before) = state.backups.store_before_restore(&state.registry, name, held).await;
    let before = match before {
        Ok(before) => before,
        Err(err) => {
            let _ = tokio::task::spawn_blocking(move || held.release()).await;
            old.set_closing(false);
            staged.remove().await;
            return Err(err);
        }
    };
    let replaced = state.registry.replace(name, staged).await;
    let _ = tokio::task::spawn_blocking(move || held.release()).await;
    drop(turn);
    state.streams.close_database(name).await;
    state.backups.forget_generation(name).await;
    replaced?;
    Ok(before)
}

/// `{"retention_hours", "manual_every_seconds", "manual_keep"}`, each a
/// positive number or `null`; a field left out keeps its value.
async fn update_backup_policy(state: &State, name: &str, req: Request<Incoming>) -> Resp {
    let patch: BackupPolicyPatch = match read_json(req).await {
        Ok(patch) => patch,
        Err(resp) => return resp,
    };
    match state.registry.set_backup_policy(name, &patch).await {
        Ok(policy) => json(StatusCode::OK, &json!({"name": name, "policy": policy})),
        Err(err) => registry_error(err),
    }
}

/// A backup as a database file: `application/vnd.sqlite3`, `latest` or an id.
async fn download_backup(state: &State, name: &str, id: &str) -> Resp {
    let permit = state.downloads.clone().acquire_owned().await.expect("the download limit is never closed");
    match state.backups.fetch(name, id).await {
        Ok((bytes, info)) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/vnd.sqlite3")
            .header("content-disposition", format!("attachment; filename=\"{name}-{}.db\"", info.id))
            .body(Permitted { body: Full::new(Bytes::from(bytes)), _permit: permit }.boxed())
            .unwrap(),
        Err(err) => backup_error(err),
    }
}

/// Delete a database and its files. With `?backups=delete` its backups go
/// too, WAL included: those there now are listed before this answers and
/// deleted in the background, so a database created again under the name
/// keeps its own. Without, they stay, for a restore later.
async fn delete_database(state: &Arc<State>, name: &str, query: Option<&str>) -> Resp {
    let with_backups = match query {
        None | Some("") | Some("backups=keep") => false,
        Some("backups=delete") => true,
        Some(_) => return text(StatusCode::BAD_REQUEST, "a delete takes `?backups=delete` or `?backups=keep`"),
    };
    match state.registry.delete(name).await {
        Ok(()) => {
            state.streams.close_database(name).await;
            state.rates.forget(name);
            state.backups.forget(name).await;
            if with_backups {
                match state.backups.backup_keys(name).await {
                    Ok(keys) => {
                        let (state, name) = (state.clone(), name.to_string());
                        tokio::spawn(async move { state.backups.delete_keys(&name, keys).await });
                    }
                    Err(BackupError::NotConfigured) => {}
                    Err(err) => return backup_error(err),
                }
            }
            Response::builder().status(StatusCode::NO_CONTENT).body(full(Bytes::new())).unwrap()
        }
        Err(err) => registry_error(err),
    }
}

async fn create_token(state: &State, name: &str, req: Request<Incoming>) -> Resp {
    #[derive(Deserialize)]
    struct Body {
        ttl_seconds: Option<u64>,
        #[serde(default)]
        read_only: bool,
    }
    let Some(secret) = &state.secret else {
        return text(StatusCode::BAD_REQUEST, "tokens need a secret; dev mode has none");
    };
    let body: Body = match read_json(req).await {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    let db = match state.registry.open(name, false).await {
        Ok(db) => db,
        Err(err) => return registry_error(err),
    };
    let claims = Claims {
        db: name.to_string(),
        generation: db.token_generation,
        exp: body.ttl_seconds.map(|ttl| unix_now() + ttl),
        read_only: body.read_only,
    };
    json(StatusCode::OK, &json!({"token": auth::mint(secret, &claims)}))
}

/// Revoke every token of a database (`Registry::rotate_tokens`) and drop its
/// open streams, rolling back their transactions; a stream busy with a
/// request ends when that request does (`pipeline`). Tokens minted from now
/// on carry the new generation.
async fn rotate_tokens(state: &State, name: &str) -> Resp {
    if state.secret.is_none() {
        return text(StatusCode::BAD_REQUEST, "tokens need a secret; dev mode has none");
    }
    match state.registry.rotate_tokens(name).await {
        Ok(generation) => {
            state.streams.close_database(name).await;
            json(StatusCode::OK, &json!({"name": name, "token_generation": generation}))
        }
        Err(err) => registry_error(err),
    }
}

fn is_admin(state: &State, req: &Request<Incoming>) -> bool {
    match &state.secret {
        None => true,
        Some(secret) => bearer(req).is_some_and(|t| auth::is_admin(secret, t)),
    }
}

fn bearer(req: &Request<Incoming>) -> Option<&str> {
    req.headers().get("authorization")?.to_str().ok()?.strip_prefix("Bearer ")
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

async fn read_json<T: serde::de::DeserializeOwned>(req: Request<Incoming>) -> Result<T, Resp> {
    let body = Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
        .map_err(|_| text(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"))?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|err| text(StatusCode::BAD_REQUEST, &format!("invalid JSON body: {err}")))
}

fn registry_error(err: RegistryError) -> Resp {
    match err {
        RegistryError::InvalidName => text(StatusCode::BAD_REQUEST, "database names match [a-z0-9][a-z0-9-]{0,62}"),
        RegistryError::InvalidLimits(message) => text(StatusCode::BAD_REQUEST, &message),
        RegistryError::Exists => text(StatusCode::CONFLICT, "database already exists"),
        RegistryError::NotFound => text(StatusCode::NOT_FOUND, "database not found"),
        RegistryError::Other(err) => text(StatusCode::INTERNAL_SERVER_ERROR, &format!("database error: {err}")),
    }
}

fn backup_error(err: BackupError) -> Resp {
    match err {
        BackupError::NotConfigured => text(StatusCode::BAD_REQUEST, "backups are not configured on this server"),
        BackupError::NotFound => text(StatusCode::NOT_FOUND, "backup not found"),
        BackupError::InvalidId => text(StatusCode::BAD_REQUEST, "backup ids are digits, or `latest`"),
        BackupError::InvalidTime => {
            text(StatusCode::BAD_REQUEST, "`at` is unix milliseconds or an RFC 3339 UTC time like 2026-10-26T09:30:00Z")
        }
        BackupError::TooSoon { retry_after } => {
            let seconds = retry_after.as_secs_f64().ceil().max(1.0) as u64;
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", seconds.to_string())
                .header("content-type", "text/plain; charset=utf-8")
                .body(full(format!("a backup of this database was asked for recently; try again in {seconds} s")))
                .unwrap()
        }
        BackupError::Registry(err) => registry_error(err),
        BackupError::Other(err) => text(StatusCode::INTERNAL_SERVER_ERROR, &format!("backup error: {err}")),
    }
}

fn json<T: serde::Serialize>(status: StatusCode, body: &T) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

/// A console error: `{"error": "...", "code": "..."}`.
fn console_error(status: StatusCode, message: &str, code: &str) -> Resp {
    json(status, &json!({"error": message, "code": code}))
}

fn text(status: StatusCode, body: &str) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(full(body.to_string()))
        .unwrap()
}
