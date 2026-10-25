//! Database name → open database. Each database is one file in the
//! data directory, opened once and shared by every connection to it.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use tokio::sync::Mutex;

/// How long a snapshot waits for in-flight writes before giving up.
const SNAPSHOT_LOCK_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
pub struct Fingerprint(Vec<(u64, SystemTime)>);

pub struct Registry {
    dir: PathBuf,
    open: Mutex<HashMap<String, turso::Database>>,
}

#[derive(Debug)]
pub enum RegistryError {
    InvalidName,
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
    pub fn new(dir: PathBuf) -> Self {
        Self { dir, open: Mutex::new(HashMap::new()) }
    }

    /// Open an existing database. With `create`, a missing one is created
    /// (dev mode only; otherwise databases are created through `create`).
    pub async fn open(&self, name: &str, create: bool) -> Result<turso::Database, RegistryError> {
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
        let db = self.build(name).await?;
        open.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub async fn create(&self, name: &str) -> Result<(), RegistryError> {
        if !valid_name(name) {
            return Err(RegistryError::InvalidName);
        }
        let mut open = self.open.lock().await;
        if tokio::fs::try_exists(self.path(name)).await? {
            return Err(RegistryError::Exists);
        }
        let db = self.build(name).await?;
        // The engine creates the file lazily; a first statement makes it real.
        db.connect()?.execute("PRAGMA user_version = 0", ()).await?;
        open.insert(name.to_string(), db);
        Ok(())
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

    /// Create a database from the bytes of a backup.
    pub async fn create_from(&self, name: &str, bytes: Vec<u8>) -> Result<(), RegistryError> {
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
            let db = self.build(name).await?;
            let mut rows = db.connect()?.query("PRAGMA quick_check", ()).await?;
            let verdict = rows.next().await?.map(|row| row.get_value(0)).transpose()?;
            drop(rows);
            match verdict {
                Some(turso::Value::Text(ok)) if ok == "ok" => Ok(db),
                other => anyhow::bail!("the backup is not a valid database: {other:?}"),
            }
        }
        .await;
        match checked {
            Ok(db) => {
                open.insert(name.to_string(), db);
                Ok(())
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(self.path(name)).await;
                Err(err.into())
            }
        }
    }

    /// A consistent copy of a database as one file. Writers wait (they do
    /// not fail) while the files are copied; readers carry on.
    pub async fn snapshot(&self, name: &str) -> Result<Vec<u8>, RegistryError> {
        let db = self.open(name, false).await?;
        let scratch = self.dir.join(".tmp").join(format!("{name}-{}", rand::random::<u64>()));
        tokio::fs::create_dir_all(&scratch).await?;
        let result = async {
            let file = format!("{name}.db");
            let conn = db.connect()?;
            conn.busy_timeout(SNAPSHOT_LOCK_WAIT)?;
            conn.execute("BEGIN IMMEDIATE", ()).await?;
            let copied = async {
                for suffix in ["", "-wal"] {
                    let from = self.dir.join(format!("{file}{suffix}"));
                    if tokio::fs::try_exists(&from).await? {
                        tokio::fs::copy(&from, scratch.join(format!("{file}{suffix}"))).await?;
                    }
                }
                anyhow::Ok(())
            }
            .await;
            conn.execute("ROLLBACK", ()).await?;
            copied?;

            // Fold the copied WAL into the copy so the backup is one file.
            let copy = scratch.join(&file);
            let snapshot = turso::Builder::new_local(copy.to_str().unwrap()).build().await?;
            let mut rows = snapshot.connect()?.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
            while rows.next().await?.is_some() {}
            drop(rows);
            drop(snapshot);
            Ok::<_, anyhow::Error>(tokio::fs::read(&copy).await?)
        }
        .await;
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

    async fn build(&self, name: &str) -> anyhow::Result<turso::Database> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.path(name);
        Ok(turso::Builder::new_local(path.to_str().unwrap()).build().await?)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db"))
    }
}
