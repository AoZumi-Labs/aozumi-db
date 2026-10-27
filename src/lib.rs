//! aozumi-db: a database per app, served over libSQL's HTTP protocol.
//! See DESIGN.md for the decisions behind this file.

mod auth;
mod backup;
mod console;
mod engine;
mod limits;
mod policy;
mod proto;
mod registry;
mod stream;

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;

pub use auth::{mint as mint_token, Claims};
pub use backup::{BackupConfig, BackupTarget, Credentials, S3Options};
use backup::{BackupError, Backups};
use proto::{PipelineReq, PipelineResp};
pub use engine::Synchronous;
pub use limits::Limits;
use limits::{LimitsPatch, RateLimiter};
use registry::{Registry, RegistryError};
use stream::{Stream, Streams};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
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
    /// When commits reach the disk. `Full` unless chosen otherwise.
    pub synchronous: Synchronous,
}

impl Config {
    pub fn new(data_dir: PathBuf, secret: Option<Vec<u8>>) -> Self {
        Self {
            data_dir,
            secret,
            stream_idle_timeout: Duration::from_secs(10),
            backup: None,
            default_limits: Limits::default(),
            synchronous: Synchronous::Full,
        }
    }
}

struct State {
    secret: Option<Vec<u8>>,
    registry: Registry,
    rates: RateLimiter,
    streams: Arc<Streams>,
    backups: Backups,
}

pub async fn serve(listener: TcpListener, config: Config) -> anyhow::Result<()> {
    let state = Arc::new(State {
        secret: config.secret,
        registry: Registry::new(config.data_dir, config.default_limits, config.synchronous),
        rates: RateLimiter::default(),
        streams: Streams::new(config.stream_idle_timeout),
        backups: Backups::new(config.backup),
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

type Resp = Response<Full<Bytes>>;

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
                (&Method::DELETE, [name]) => delete_database(&state, name).await,
                (&Method::PATCH, [name, "limits"]) => update_limits(&state, name, req).await,
                (&Method::POST, [name, "tokens"]) => create_token(&state, name, req).await,
                (&Method::POST, [name, "backups"]) => match state.backups.snapshot(&state.registry, name).await {
                    Ok(info) => json(StatusCode::CREATED, &info),
                    Err(err) => backup_error(err),
                },
                (&Method::GET, [name, "backups"]) => list_backups(&state, name).await,
                _ => text(StatusCode::NOT_FOUND, "not found"),
            }
        }
        _ => text(StatusCode::NOT_FOUND, "not found"),
    }
}

/// The database a request's token opens, and whether the token is
/// read-only, with the database's request rate counted. Dev mode serves its
/// one database to anyone.
async fn token_database(state: &State, req: &Request<Incoming>) -> Result<(String, bool, registry::OpenDb), Resp> {
    let (database, read_only, create) = match &state.secret {
        None => (DEV_DATABASE.to_string(), false, true),
        Some(secret) => match bearer(req).and_then(|t| auth::verify(secret, t, unix_now())) {
            Some(claims) => (claims.db, claims.read_only, false),
            None => return Err(text(StatusCode::UNAUTHORIZED, "a valid database token is required")),
        },
    };
    let db = state.registry.open(&database, create).await.map_err(registry_error)?;
    if let Some(rate) = db.limits.max_requests_per_second {
        if let Err(wait) = state.rates.check(&database, rate) {
            let retry_after = wait.as_secs_f64().ceil().max(1.0) as u64;
            return Err(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", retry_after.to_string())
                .body(Full::new(Bytes::from("this database is over its request rate limit")))
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
            Some(stream) if stream.db == database && stream.read_only == read_only => stream,
            _ => return text(StatusCode::BAD_REQUEST, "unknown or expired baton"),
        },
        None => match open_stream(&database, &db, read_only) {
            Ok(stream) => stream,
            Err(resp) => return resp,
        },
    };

    // Writers queue in arrival order; reads, and streams already holding a
    // transaction, go straight through.
    let turn = match stream.may_write(&msg.requests) && !stream.in_transaction() {
        true => Some(db.writers.clone().lock_owned().await),
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
        false => Some(state.streams.park(stream).await),
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
    let turn = match read_only {
        false => Some(db.writers.clone().lock_owned().await),
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

fn open_stream(database: &str, db: &registry::OpenDb, read_only: bool) -> Result<Stream, Resp> {
    let conn = db
        .checkout(BUSY_TIMEOUT, read_only)
        .map_err(|err| text(StatusCode::INTERNAL_SERVER_ERROR, &format!("cannot connect: {err}")))?;
    Ok(Stream::new(database.to_string(), read_only, conn, db.pool.clone()))
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
                (None, Some(id)) => state.backups.fetch(&from.database, id).await,
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
        Ok(size_bytes) => json(StatusCode::OK, &json!({"name": name, "size_bytes": size_bytes, "limits": db.limits})),
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

async fn list_backups(state: &State, name: &str) -> Resp {
    let backups = match state.backups.list(name).await {
        Ok(backups) => backups,
        Err(err) => return backup_error(err),
    };
    match state.backups.window(name).await {
        Ok(window) => json(StatusCode::OK, &json!({"backups": backups, "point_in_time": window})),
        Err(err) => backup_error(err),
    }
}

async fn delete_database(state: &State, name: &str) -> Resp {
    match state.registry.delete(name).await {
        Ok(()) => {
            state.streams.close_database(name).await;
            state.rates.forget(name);
            state.backups.forget(name).await;
            Response::builder().status(StatusCode::NO_CONTENT).body(Full::default()).unwrap()
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
    if let Err(err) = state.registry.open(name, false).await {
        return registry_error(err);
    }
    let claims = Claims { db: name.to_string(), exp: body.ttl_seconds.map(|ttl| unix_now() + ttl), read_only: body.read_only };
    json(StatusCode::OK, &json!({"token": auth::mint(secret, &claims)}))
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
        BackupError::Registry(err) => registry_error(err),
        BackupError::Other(err) => text(StatusCode::INTERNAL_SERVER_ERROR, &format!("backup error: {err}")),
    }
}

fn json<T: serde::Serialize>(status: StatusCode, body: &T) -> Resp {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(body).unwrap())))
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
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
