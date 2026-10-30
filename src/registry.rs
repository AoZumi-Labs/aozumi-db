//! Database name → open database. Each database is one file in the data
//! directory, opened once and shared by every connection to it, with its
//! limits beside it in `<name>.db-limits.json` and its token generation in
//! `<name>.tokens.json`, which outlives the database (`rotate_tokens`).

use std::collections::HashMap;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
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
    /// Tokens minted below this generation are refused (`rotate_tokens`).
    pub token_generation: u64,
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

    /// Create a database from the bytes of a backup. The copy is written
    /// and checked in a scratch directory without the registry, so a large
    /// restore holds up no other database; only moving it into place takes
    /// the registry.
    pub async fn create_from(&self, name: &str, bytes: Vec<u8>, limits: Limits) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        if tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::Exists);
        }
        let scratch = self.dir.join(".tmp").join(format!("{name}-{}", rand::random::<u64>()));
        tokio::fs::create_dir_all(&scratch).await?;
        let created = self.create_checked(name, &scratch.join(format!("{name}.db")), bytes, limits).await;
        let _ = tokio::fs::remove_dir_all(&scratch).await;
        created
    }

    /// Write the backup to `copy` and check it, then move it into place
    /// unless the name was taken meanwhile.
    async fn create_checked(&self, name: &str, copy: &Path, bytes: Vec<u8>, limits: Limits) -> Result<(), RegistryError> {
        tokio::fs::write(copy, bytes).await?;
        let checked = copy.to_path_buf();
        tokio::task::spawn_blocking(move || quick_check(&checked)).await??;
        // Every create checks that its name is free and makes its file while
        // holding the registry, so a database made while this copy was
        // written and checked is seen here and never overwritten.
        let mut open = self.open.lock().await;
        if tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::Exists);
        }
        tokio::fs::rename(copy, self.path(name)).await?;
        let placed = async {
            let db = self.build(name, limits).await?;
            self.write_limits(name, &limits).await?;
            anyhow::Ok(db)
        }
        .await;
        match placed {
            Ok(db) => {
                open.insert(name.to_string(), db);
                Ok(())
            }
            Err(err) => {
                let _ = self.remove_files(name).await;
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

    /// Revoke every token of a database: its token generation goes up by
    /// one, and tokens minted below it are refused from now on. Deleting a
    /// database keeps its generation, so a revoked token stays revoked for a
    /// database created later under the same name, while an in-place restore
    /// (delete, then create from a backup) keeps the current tokens working.
    pub async fn rotate_tokens(&self, name: &str) -> Result<u64, RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        // Held throughout, so no open reads the generation halfway.
        let mut open = self.open.lock().await;
        if !tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::NotFound);
        }
        let generation = self
            .read_token_generation(name)
            .await?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("the token generation of {name} cannot go higher"))?;
        self.write_token_generation(name, generation).await?;
        if let Some(db) = open.get_mut(name) {
            db.token_generation = generation;
        }
        Ok(generation)
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

    /// Close and delete a database with its WAL and other side files. Its
    /// token generation stays (see `rotate_tokens`).
    pub async fn delete(&self, name: &str) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if !tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::NotFound);
        }
        open.remove(name);
        self.remove_files(name).await?;
        Ok(())
    }

    /// Remove a database's file, its WAL and its other side files.
    async fn remove_files(&self, name: &str) -> anyhow::Result<()> {
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
        let token_generation = self.read_token_generation(name).await?;
        tokio::task::spawn_blocking(move || {
            let db = engine::open(&path)?;
            let result = Conn::open(&db, &ConnOptions::default())?.execute("PRAGMA page_size", &[], &[], true)?;
            let page_size = match result.rows.first().and_then(|row| row.first()) {
                Some(Value::Integer(size)) if *size > 0 => *size as u64,
                other => anyhow::bail!("unexpected page size {other:?}"),
            };
            Ok(OpenDb { db, limits, token_generation, page_size, writers: Arc::new(Mutex::new(())), pool: Arc::default(), synchronous })
        })
        .await?
    }

    async fn write_limits(&self, name: &str, limits: &Limits) -> anyhow::Result<()> {
        let partial = self.dir.join(format!(".{name}.limits"));
        tokio::fs::write(&partial, serde_json::to_vec(limits)?).await?;
        tokio::fs::rename(&partial, self.limits_path(name)).await?;
        Ok(())
    }

    async fn read_token_generation(&self, name: &str) -> anyhow::Result<u64> {
        generation_from(tokio::fs::read(generation_path(&self.dir, name)).await)
    }

    /// Written like the limits, and synced on both sides of the rename: a
    /// rotation that a crash undid would let revoked tokens back in.
    async fn write_token_generation(&self, name: &str, generation: u64) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(&TokenGeneration { generation })?;
        let (dir, partial, path) = (self.dir.clone(), self.dir.join(format!(".{name}.tokens")), generation_path(&self.dir, name));
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::create(&partial)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&partial, &path)?;
            std::fs::File::open(&dir)?.sync_all()?;
            anyhow::Ok(())
        })
        .await?
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db"))
    }

    fn limits_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db-limits.json"))
    }
}

/// A database's token generation as a server with data directory `dir`
/// keeps it, for minting tokens without the server (`aozumi-db token`).
pub fn token_generation(dir: &Path, name: &str) -> anyhow::Result<u64> {
    anyhow::ensure!(valid_name(name), "database names match [a-z0-9][a-z0-9-]{{0,62}}");
    generation_from(std::fs::read(generation_path(dir, name)))
}

#[derive(Serialize, Deserialize)]
struct TokenGeneration {
    generation: u64,
}

fn generation_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.tokens.json"))
}

/// The generation in `<name>.tokens.json`: 0 when the database's tokens were never rotated.
fn generation_from(read: std::io::Result<Vec<u8>>) -> anyhow::Result<u64> {
    match read {
        Ok(bytes) => Ok(serde_json::from_slice::<TokenGeneration>(&bytes)?.generation),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err.into()),
    }
}

/// Check a database file (`PRAGMA quick_check`) and close it again. Blocks.
/// The engine shares one open database per file, found by device and inode,
/// which a rename keeps: the file moves into place only after this returns,
/// so it is opened there afresh.
fn quick_check(path: &Path) -> anyhow::Result<()> {
    let db = engine::open(path)?;
    let check = Conn::open(&db, &ConnOptions::default())?.execute("PRAGMA quick_check", &[], &[], true)?;
    match check.rows.first().and_then(|row| row.first()) {
        Some(Value::Text(ok)) if ok == "ok" => Ok(()),
        other => anyhow::bail!("the backup is not a valid database: {other:?}"),
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
