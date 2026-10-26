//! Database name → open database. Each database is one file in the data
//! directory, opened once and shared by every connection to it, with its
//! limits beside it in `<name>.db-limits.json`.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::Mutex;
use turso_core::CheckpointMode;

use crate::engine::{self, Conn, ConnOptions, Db, Synchronous, WalPosition};
use crate::limits::{Limits, LimitsPatch};
use crate::proto::Value;

/// How long a snapshot waits for in-flight writes before giving up.
const SNAPSHOT_LOCK_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
pub struct Fingerprint(Vec<(u64, SystemTime)>);

/// An open database and what connections to it need to know.
#[derive(Clone)]
pub struct OpenDb {
    pub db: Db,
    pub limits: Limits,
    pub page_size: u64,
    /// Writers take turns here, first come first served, instead of racing
    /// for the engine's write lock and sleeping in its busy backoff.
    pub writers: Arc<Mutex<()>>,
    /// Idle connections, reused instead of opened per request.
    pub pool: Arc<Pool>,
    synchronous: Synchronous,
}

/// Idle connections of one database, kept per access level. Connections
/// are expensive to open (every SQL function is registered on each), so
/// streams return theirs here when they close cleanly.
#[derive(Default)]
pub struct Pool {
    read_write: std::sync::Mutex<Vec<Conn>>,
    read_only: std::sync::Mutex<Vec<Conn>>,
}

/// Idle connections kept per database and access level.
const POOL_SIZE: usize = 32;

impl Pool {
    fn side(&self, read_only: bool) -> &std::sync::Mutex<Vec<Conn>> {
        if read_only { &self.read_only } else { &self.read_write }
    }

    pub fn take(&self, read_only: bool) -> Option<Conn> {
        self.side(read_only).lock().unwrap().pop()
    }

    /// Keep a connection for reuse. Only connections in autocommit mode,
    /// with no state a stream set on them, belong here.
    pub fn put(&self, conn: Conn, read_only: bool) {
        let mut idle = self.side(read_only).lock().unwrap();
        if idle.len() < POOL_SIZE {
            idle.push(conn);
        }
    }
}

/// A consistent copy of a database, and the WAL position it includes.
pub struct Snapshot {
    pub bytes: Vec<u8>,
    pub wal: WalPosition,
}

impl OpenDb {
    /// Connection settings that enforce this database's limits.
    pub fn conn_options(&self, busy_timeout: Duration, read_only: bool) -> ConnOptions {
        ConnOptions {
            synchronous: self.synchronous,
            busy_timeout,
            read_only,
            max_pages: self.limits.max_size_bytes.map(|bytes| (bytes / self.page_size).max(1)),
            statement_timeout: self.limits.max_statement_ms.map(Duration::from_millis),
        }
    }

    /// A connection for a tenant stream: an idle one if there is one.
    pub fn checkout(&self, busy_timeout: Duration, read_only: bool) -> Result<Conn, turso_core::LimboError> {
        match self.pool.take(read_only) {
            Some(conn) => Ok(conn),
            None => Conn::open(&self.db, &self.conn_options(busy_timeout, read_only)),
        }
    }

    /// Settings for the server's own connections: no tenant limits.
    pub fn internal_options(&self) -> ConnOptions {
        ConnOptions { synchronous: self.synchronous, ..Default::default() }
    }
}

pub struct Registry {
    dir: PathBuf,
    defaults: Limits,
    synchronous: Synchronous,
    open: Mutex<HashMap<String, OpenDb>>,
}

#[derive(Debug)]
pub enum RegistryError {
    InvalidName,
    InvalidLimits(String),
    Exists,
    NotFound,
    Other(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for RegistryError {
    fn from(err: E) -> Self {
        RegistryError::Other(err.into())
    }
}

/// `[a-z0-9][a-z0-9-]{0,62}`: safe as a file name and as a DNS label.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes[0] != b'-'
        && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

impl Registry {
    pub fn new(dir: PathBuf, defaults: Limits, synchronous: Synchronous) -> Self {
        Self { dir, defaults, synchronous, open: Mutex::new(HashMap::new()) }
    }

    /// Checkpoint every open database whose WAL has grown past
    /// `threshold` frames: a passive pass that blocks no one, then a short
    /// truncating one. Skipped for a database when a transaction is in the
    /// way; the next round tries again. Only for servers without WAL
    /// shipping, which checkpoints on its own schedule.
    pub async fn checkpoint_all(&self, threshold: u64) {
        let open = self.open.lock().await.values().cloned().collect::<Vec<_>>();
        for db in open {
            let checkpointed = tokio::task::spawn_blocking(move || {
                let conn = Conn::open(&db.db, &db.internal_options())?;
                if conn.wal_position()?.frame < threshold {
                    return anyhow::Ok(());
                }
                conn.checkpoint_passive(None)?;
                match conn.checkpoint_truncate() {
                    Ok(_) => Ok(()),
                    Err(err) if err.downcast_ref::<turso_core::LimboError>().is_some_and(|e| matches!(e, turso_core::LimboError::Busy)) => Ok(()),
                    Err(err) => Err(err),
                }
            })
            .await;
            match checkpointed {
                Ok(Ok(())) => {}
                Ok(Err(err)) => eprintln!("aozumi-db: checkpoint failed: {err:?}"),
                Err(err) => eprintln!("aozumi-db: checkpoint task failed: {err:?}"),
            }
        }
    }

    /// The server's limits with a create request's changes applied.
    pub fn limits_for_new(&self, patch: Option<&LimitsPatch>) -> Result<Limits, RegistryError> {
        match patch {
            Some(patch) => self.defaults.apply(patch).map_err(RegistryError::InvalidLimits),
            None => Ok(self.defaults),
        }
    }

    /// Open an existing database. With `create`, a missing one is created
    /// (dev mode only; otherwise databases are created through `create`).
    pub async fn open(&self, name: &str, create: bool) -> Result<OpenDb, RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if let Some(db) = open.get(name) {
            return Ok(db.clone());
        }
        if !create && !tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::NotFound);
        }
        let limits = match tokio::fs::read(self.limits_path(name)).await {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(err) if err.kind() == ErrorKind::NotFound => self.defaults,
            Err(err) => return Err(err.into()),
        };
        let db = self.build(name, limits).await?;
        open.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn create(&self, name: &str, limits: Limits) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::Exists);
        }
        let db = self.build(name, limits).await?;
        // The engine creates the file lazily; a first statement makes it real.
        Conn::open(&db.db, &db.internal_options())?.execute_script("PRAGMA user_version = 0")?;
        self.write_limits(name, &limits).await?;
        open.insert(name.to_string(), db);
        Ok(())
    }

    /// Create a database from the bytes of a backup.
    pub async fn create_from(&self, name: &str, bytes: Vec<u8>, limits: Limits) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::Exists);
        }
        tokio::fs::create_dir_all(&self.dir).await?;
        let partial = self.dir.join(format!(".{name}.restore"));
        tokio::fs::write(&partial, bytes).await?;
        tokio::fs::rename(&partial, self.path(name)).await?;
        let checked = async {
            let db = self.build(name, limits).await?;
            let check = Conn::open(&db.db, &db.internal_options())?.execute("PRAGMA quick_check", &[], &[], true)?;
            match check.rows.first().and_then(|row| row.first()) {
                Some(Value::Text(ok)) if ok == "ok" => Ok(db),
                other => anyhow::bail!("the backup is not a valid database: {other:?}"),
            }
        }
        .await;
        match checked {
            Ok(db) => {
                self.write_limits(name, &limits).await?;
                open.insert(name.to_string(), db);
                Ok(())
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(self.path(name)).await;
                Err(err.into())
            }
        }
    }

    /// Change a database's limits. New streams get them; open ones keep theirs.
    pub async fn set_limits(&self, name: &str, patch: &LimitsPatch) -> Result<Limits, RegistryError> {
        let current = self.open(name, false).await?;
        let limits = current.limits.apply(patch).map_err(RegistryError::InvalidLimits)?;
        self.write_limits(name, &limits).await?;
        if let Some(db) = self.open.lock().await.get_mut(name) {
            db.limits = limits;
            // Idle connections carry the old limits.
            db.pool = Arc::default();
        }
        Ok(limits)
    }

    /// The database's size in bytes (its pages, including those in the WAL).
    pub async fn size(&self, name: &str) -> Result<u64, RegistryError> {
        let db = self.open(name, false).await?;
        let pages = tokio::task::spawn_blocking(move || {
            let result = Conn::open(&db.db, &db.internal_options())?.execute("PRAGMA page_count", &[], &[], true)?;
            match result.rows.first().and_then(|row| row.first()) {
                Some(Value::Integer(pages)) => Ok(*pages as u64 * db.page_size),
                other => anyhow::bail!("unexpected page count {other:?}"),
            }
        })
        .await??;
        Ok(pages)
    }

    /// Close and delete a database with its WAL and other side files.
    pub async fn delete(&self, name: &str) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if !tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::NotFound);
        }
        open.remove(name);
        let file = format!("{name}.db");
        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let entry_name = entry.file_name();
            let entry_name = entry_name.to_string_lossy();
            if entry_name == file || entry_name.starts_with(&format!("{file}-")) {
                match tokio::fs::remove_file(entry.path()).await {
                    Err(err) if err.kind() != ErrorKind::NotFound => return Err(err.into()),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<String>, RegistryError> {
        let mut names = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(names),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_string_lossy().strip_suffix(".db") {
                if valid_name(name) {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        Ok(names)
    }

    /// A consistent copy of a database as one file. Writers wait (they do
    /// not fail) while the files are copied; readers carry on.
    pub async fn snapshot(&self, name: &str) -> Result<Snapshot, RegistryError> {
        let db = self.open(name, false).await?;
        let scratch = self.dir.join(".tmp").join(format!("{name}-{}", rand::random::<u64>()));
        tokio::fs::create_dir_all(&scratch).await?;
        let (dir, file, work) = (self.dir.clone(), format!("{name}.db"), scratch.clone());
        let result = tokio::task::spawn_blocking(move || snapshot_files(&db, &dir, &file, &work)).await?;
        let _ = tokio::fs::remove_dir_all(&scratch).await;
        Ok(result?)
    }

    /// Size and modification time of a database's files: changes whenever
    /// the database is written.
    pub async fn fingerprint(&self, name: &str) -> anyhow::Result<Fingerprint> {
        let mut parts = Vec::new();
        for suffix in ["", "-wal"] {
            match tokio::fs::metadata(self.dir.join(format!("{name}.db{suffix}"))).await {
                Ok(meta) => parts.push((meta.len(), meta.modified()?)),
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(Fingerprint(parts))
    }

    async fn build(&self, name: &str, limits: Limits) -> anyhow::Result<OpenDb> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.path(name);
        let synchronous = self.synchronous;
        tokio::task::spawn_blocking(move || {
            let db = engine::open(&path)?;
            let result = Conn::open(&db, &ConnOptions::default())?.execute("PRAGMA page_size", &[], &[], true)?;
            let page_size = match result.rows.first().and_then(|row| row.first()) {
                Some(Value::Integer(size)) if *size > 0 => *size as u64,
                other => anyhow::bail!("unexpected page size {other:?}"),
            };
            Ok(OpenDb { db, limits, page_size, writers: Arc::new(Mutex::new(())), pool: Arc::default(), synchronous })
        })
        .await?
    }

    async fn write_limits(&self, name: &str, limits: &Limits) -> anyhow::Result<()> {
        let partial = self.dir.join(format!(".{name}.limits"));
        tokio::fs::write(&partial, serde_json::to_vec(limits)?).await?;
        tokio::fs::rename(&partial, self.limits_path(name)).await?;
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db"))
    }

    fn limits_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db-limits.json"))
    }
}

/// Copy a database's files while holding its write lock, then fold the
/// copied WAL into the copied database file and return that file. Blocks.
fn snapshot_files(db: &OpenDb, dir: &std::path::Path, file: &str, work: &std::path::Path) -> anyhow::Result<Snapshot> {
    let conn = Conn::open(&db.db, &ConnOptions { busy_timeout: SNAPSHOT_LOCK_WAIT, ..db.internal_options() })?;
    conn.execute_script("BEGIN IMMEDIATE")?;
    let copied = (|| {
        // Taken under the write lock: exactly what the copy will contain.
        let wal = conn.wal_position()?;
        for suffix in ["", "-wal"] {
            let from = dir.join(format!("{file}{suffix}"));
            if from.exists() {
                std::fs::copy(&from, work.join(format!("{file}{suffix}")))?;
            }
        }
        anyhow::Ok(wal)
    })();
    conn.execute_script("ROLLBACK")?;
    let wal = copied?;

    let copy = work.join(file);
    let snapshot = engine::open(&copy)?;
    snapshot.connect()?.checkpoint(CheckpointMode::Truncate { upper_bound_inclusive: None })?;
    drop(snapshot);
    Ok(Snapshot { bytes: std::fs::read(&copy)?, wal })
}
