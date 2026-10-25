//! Backups: consistent snapshots of a database file, kept in a backup store
//! (a directory or an S3-compatible bucket) under `<database>/<id>.db`.
//! Ids are zero-padded unix milliseconds, so they sort by age.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::registry::{Fingerprint, Registry, RegistryError};

pub struct BackupConfig {
    pub target: BackupTarget,
    /// How many backups to keep per database; older ones are deleted.
    pub keep: usize,
    /// Back up every changed database this often. `None` = only on request.
    pub every: Option<Duration>,
}

pub struct BackupTarget(Store);

enum Store {
    Directory(PathBuf),
}

impl BackupTarget {
    pub fn directory(path: PathBuf) -> Self {
        Self(Store::Directory(path))
    }

    /// `file:///absolute/path` or a plain path.
    pub fn parse(url: &str) -> anyhow::Result<Self> {
        match url.strip_prefix("file://") {
            Some(path) => Ok(Self::directory(PathBuf::from(path))),
            None if url.contains("://") => anyhow::bail!("unsupported backup URL {url:?}"),
            None => Ok(Self::directory(PathBuf::from(url))),
        }
    }

    async fn put(&self, db: &str, id: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        match &self.0 {
            Store::Directory(root) => {
                let dir = root.join(db);
                tokio::fs::create_dir_all(&dir).await?;
                let partial = dir.join(format!(".{id}.partial"));
                tokio::fs::write(&partial, bytes).await?;
                tokio::fs::rename(&partial, dir.join(format!("{id}.db"))).await?;
                Ok(())
            }
        }
    }

    async fn get(&self, db: &str, id: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match &self.0 {
            Store::Directory(root) => match tokio::fs::read(root.join(db).join(format!("{id}.db"))).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err.into()),
            },
        }
    }

    /// Backups of one database, newest first.
    async fn list(&self, db: &str) -> anyhow::Result<Vec<BackupInfo>> {
        let mut out = Vec::new();
        match &self.0 {
            Store::Directory(root) => {
                let mut entries = match tokio::fs::read_dir(root.join(db)).await {
                    Ok(entries) => entries,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                    Err(err) => return Err(err.into()),
                };
                while let Some(entry) = entries.next_entry().await? {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if let Some(id) = name.strip_suffix(".db").filter(|id| valid_id(id)) {
                        out.push(BackupInfo { id: id.to_string(), size_bytes: entry.metadata().await?.len() });
                    }
                }
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    async fn delete(&self, db: &str, id: &str) -> anyhow::Result<()> {
        match &self.0 {
            Store::Directory(root) => match tokio::fs::remove_file(root.join(db).join(format!("{id}.db"))).await {
                Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err.into()),
                _ => Ok(()),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BackupInfo {
    pub id: String,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub enum BackupError {
    NotConfigured,
    NotFound,
    InvalidId,
    Registry(RegistryError),
    Other(anyhow::Error),
}

impl From<RegistryError> for BackupError {
    fn from(err: RegistryError) -> Self {
        BackupError::Registry(err)
    }
}

impl From<anyhow::Error> for BackupError {
    fn from(err: anyhow::Error) -> Self {
        BackupError::Other(err)
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 20 && id.bytes().all(|b| b.is_ascii_digit())
}

pub struct Backups {
    config: Option<BackupConfig>,
    last_id: Mutex<u64>,
    /// What each database looked like at its last scheduled backup.
    seen: Mutex<HashMap<String, Fingerprint>>,
}

impl Backups {
    pub fn new(config: Option<BackupConfig>) -> Self {
        Self { config, last_id: Mutex::new(0), seen: Mutex::new(HashMap::new()) }
    }

    pub fn every(&self) -> Option<Duration> {
        self.config.as_ref().and_then(|c| c.every)
    }

    fn config(&self) -> Result<&BackupConfig, BackupError> {
        self.config.as_ref().ok_or(BackupError::NotConfigured)
    }

    pub async fn snapshot(&self, registry: &Registry, db: &str) -> Result<BackupInfo, BackupError> {
        let config = self.config()?;
        let bytes = registry.snapshot(db).await?;
        let id = self.next_id().await;
        let info = BackupInfo { id: id.clone(), size_bytes: bytes.len() as u64 };
        config.target.put(db, &id, bytes).await?;
        self.prune(db).await?;
        Ok(info)
    }

    pub async fn list(&self, db: &str) -> Result<Vec<BackupInfo>, BackupError> {
        if !crate::registry::valid_name(db) {
            return Err(RegistryError::InvalidName.into());
        }
        Ok(self.config()?.target.list(db).await?)
    }

    /// The bytes of a backup; `id` may be `latest`.
    pub async fn fetch(&self, db: &str, id: &str) -> Result<Vec<u8>, BackupError> {
        let target = &self.config()?.target;
        let id = match id {
            "latest" => self.list(db).await?.into_iter().next().ok_or(BackupError::NotFound)?.id,
            id if valid_id(id) => id.to_string(),
            _ => return Err(BackupError::InvalidId),
        };
        target.get(db, &id).await?.ok_or(BackupError::NotFound)
    }

    /// Back up every database that changed since its last scheduled backup.
    pub async fn run_scheduled(&self, registry: &Registry) {
        let names = match registry.list().await {
            Ok(names) => names,
            Err(err) => return eprintln!("aozumi-db: backup: cannot list databases: {err:?}"),
        };
        for name in names {
            // Taken before the snapshot: a write that lands during it changes
            // the fingerprint again, so the next round backs it up.
            let Ok(fingerprint) = registry.fingerprint(&name).await else { continue };
            if self.seen.lock().await.get(&name) == Some(&fingerprint) {
                continue;
            }
            match self.snapshot(registry, &name).await {
                Ok(_) => {
                    self.seen.lock().await.insert(name, fingerprint);
                }
                Err(err) => eprintln!("aozumi-db: backup of {name} failed: {err:?}"),
            }
        }
    }

    async fn prune(&self, db: &str) -> Result<(), BackupError> {
        let config = self.config()?;
        for old in config.target.list(db).await?.into_iter().skip(config.keep.max(1)) {
            config.target.delete(db, &old.id).await?;
        }
        Ok(())
    }

    async fn next_id(&self) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut last = self.last_id.lock().await;
        *last = now.max(*last + 1);
        format!("{:013}", *last)
    }
}
