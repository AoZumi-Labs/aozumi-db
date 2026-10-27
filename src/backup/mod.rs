//! Backups and point-in-time restore.
//!
//! A snapshot is a consistent copy of a database as one file, stored as
//! `<database>/<id>.db`; ids are zero-padded unix milliseconds, so they sort
//! by age. With WAL shipping on, each snapshot also starts a generation: the
//! WAL frames committed after it, uploaded every few moments as segments
//! `<database>/wal/<snapshot id>/<segment no>-<shipped at ms>.wal`. Restoring
//! to a time takes the newest snapshot at or before it and replays the
//! page images of its segments shipped by then.

mod s3;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::engine::{Conn, WalPosition, FRAME_HEADER};
use crate::registry::{Fingerprint, OpenDb, Registry, RegistryError};

pub use s3::{Credentials, S3Options};
use s3::S3;

/// Checkpoint once the WAL holds this many shipped frames (about 4 MB).
const CHECKPOINT_FRAMES: u64 = 1000;
/// Past this many frames (about 64 MB), checkpoint even when that costs a
/// new generation: when shipping keeps failing, or writes never pause.
const MAX_UNSHIPPED_FRAMES: u64 = 16_384;

pub struct BackupConfig {
    pub target: BackupTarget,
    /// How many snapshots to keep per database; older ones are deleted,
    /// with their WAL segments.
    pub keep: usize,
    /// Snapshot every changed database this often. `None` = only on request.
    pub every: Option<Duration>,
    /// Ship committed WAL frames this often, for point-in-time restore.
    /// `None` = off.
    pub wal_every: Option<Duration>,
}

pub struct BackupTarget(Store);

enum Store {
    Directory(PathBuf),
    S3(Box<S3>),
}

impl BackupTarget {
    pub fn directory(path: PathBuf) -> Self {
        Self(Store::Directory(path))
    }

    pub fn s3(options: S3Options) -> anyhow::Result<Self> {
        Ok(Self(Store::S3(Box::new(S3::new(options)?))))
    }

    /// `s3://bucket/prefix` (credentials and region from the AWS environment
    /// variables or the instance role), `file:///absolute/path`, or a path.
    pub fn parse(url: &str) -> anyhow::Result<Self> {
        if url.starts_with("s3://") {
            return Self::s3(S3Options::from_url(url)?);
        }
        match url.strip_prefix("file://") {
            Some(path) => Ok(Self::directory(PathBuf::from(path))),
            None if url.contains("://") => anyhow::bail!("unsupported backup URL {url:?}"),
            None => Ok(Self::directory(PathBuf::from(url))),
        }
    }

    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        match &self.0 {
            Store::Directory(root) => {
                let path = root.join(key);
                let dir = path.parent().expect("keys have a directory");
                tokio::fs::create_dir_all(dir).await?;
                let partial = dir.join(format!(".{}.partial", rand::random::<u64>()));
                tokio::fs::write(&partial, bytes).await?;
                tokio::fs::rename(&partial, path).await?;
                Ok(())
            }
            Store::S3(s3) => s3.put(key, bytes).await,
        }
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match &self.0 {
            Store::Directory(root) => match tokio::fs::read(root.join(key)).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err.into()),
            },
            Store::S3(s3) => s3.get(key).await,
        }
    }

    /// Names and sizes of the objects directly inside `dir` (ending in `/`).
    async fn list(&self, dir: &str) -> anyhow::Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        match &self.0 {
            Store::Directory(root) => {
                let mut entries = match tokio::fs::read_dir(root.join(dir)).await {
                    Ok(entries) => entries,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                    Err(err) => return Err(err.into()),
                };
                while let Some(entry) = entries.next_entry().await? {
                    let meta = entry.metadata().await?;
                    if meta.is_file() {
                        out.push((entry.file_name().to_string_lossy().into_owned(), meta.len()));
                    }
                }
            }
            Store::S3(s3) => {
                for (key, size) in s3.list(dir).await? {
                    if let Some(name) = key.strip_prefix(dir) {
                        out.push((name.to_string(), size));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match &self.0 {
            Store::Directory(root) => match tokio::fs::remove_file(root.join(key)).await {
                Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err.into()),
                _ => Ok(()),
            },
            Store::S3(s3) => s3.delete(key).await,
        }
    }
}

fn snapshot_key(db: &str, id: &str) -> String {
    format!("{db}/{id}.db")
}

fn wal_dir(db: &str, generation: &str) -> String {
    format!("{db}/wal/{generation}/")
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BackupInfo {
    pub id: String,
    pub size_bytes: u64,
}

/// The range of times a database can be restored to.
#[derive(Debug, Clone, Serialize)]
pub struct Window {
    pub earliest_ms: u64,
    pub latest_ms: u64,
}

/// A restored database's bytes, and the moment they represent.
pub struct Restored {
    pub bytes: Vec<u8>,
    pub restored_to_ms: u64,
}

#[derive(Debug)]
pub enum BackupError {
    NotConfigured,
    NotFound,
    InvalidId,
    InvalidTime,
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

impl From<tokio::task::JoinError> for BackupError {
    fn from(err: tokio::task::JoinError) -> Self {
        BackupError::Other(err.into())
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 20 && id.bytes().all(|b| b.is_ascii_digit())
}

/// Where WAL shipping stands for one database.
struct Cursor {
    generation: String,
    wal_seq: u32,
    /// The next WAL frame to ship.
    next_frame: u64,
    next_segment: u64,
}

pub struct Backups {
    config: Option<BackupConfig>,
    last_id: Mutex<u64>,
    /// What each database looked like at its last scheduled snapshot.
    seen: Mutex<HashMap<String, Fingerprint>>,
    cursors: Mutex<HashMap<String, Cursor>>,
    /// Snapshots and WAL shipping of one database never overlap.
    locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl Backups {
    pub fn new(config: Option<BackupConfig>) -> Self {
        Self {
            config,
            last_id: Mutex::new(0),
            seen: Mutex::new(HashMap::new()),
            cursors: Mutex::new(HashMap::new()),
            locks: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn every(&self) -> Option<Duration> {
        self.config.as_ref().and_then(|c| c.every)
    }

    pub fn wal_every(&self) -> Option<Duration> {
        self.config.as_ref().and_then(|c| c.wal_every)
    }

    fn config(&self) -> Result<&BackupConfig, BackupError> {
        self.config.as_ref().ok_or(BackupError::NotConfigured)
    }

    fn lock(&self, db: &str) -> Arc<Mutex<()>> {
        self.locks.lock().unwrap().entry(db.to_string()).or_default().clone()
    }

    pub async fn snapshot(&self, registry: &Registry, db: &str) -> Result<BackupInfo, BackupError> {
        let lock = self.lock(db);
        let _held = lock.lock().await;
        self.snapshot_locked(registry, db).await
    }

    async fn snapshot_locked(&self, registry: &Registry, db: &str) -> Result<BackupInfo, BackupError> {
        let config = self.config()?;
        let snapshot = registry.snapshot(db).await?;
        let id = self.next_id().await;
        let info = BackupInfo { id: id.clone(), size_bytes: snapshot.bytes.len() as u64 };
        config.target.put(&snapshot_key(db, &id), snapshot.bytes).await?;
        if config.wal_every.is_some() {
            let cursor = Cursor { generation: id, wal_seq: snapshot.wal.seq, next_frame: snapshot.wal.frame + 1, next_segment: 0 };
            self.cursors.lock().await.insert(db.to_string(), cursor);
        }
        self.prune(db).await?;
        Ok(info)
    }

    /// Stop shipping a deleted database's WAL.
    pub async fn forget(&self, db: &str) {
        self.cursors.lock().await.remove(db);
        self.seen.lock().await.remove(db);
    }

    /// Snapshots of one database, newest first.
    pub async fn list(&self, db: &str) -> Result<Vec<BackupInfo>, BackupError> {
        if !crate::registry::valid_name(db) {
            return Err(RegistryError::InvalidName.into());
        }
        let mut out = Vec::new();
        for (name, size_bytes) in self.config()?.target.list(&format!("{db}/")).await? {
            if let Some(id) = name.strip_suffix(".db").filter(|id| valid_id(id)) {
                out.push(BackupInfo { id: id.to_string(), size_bytes });
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    /// The times a database can be restored to, with WAL shipping on.
    pub async fn window(&self, db: &str) -> Result<Option<Window>, BackupError> {
        if self.config()?.wal_every.is_none() {
            return Ok(None);
        }
        let snapshots = self.list(db).await?;
        let (Some(newest), Some(oldest)) = (snapshots.first(), snapshots.last()) else { return Ok(None) };
        let segments = self.segments(db, &newest.id).await?;
        let shipped = segments.last().map(|s| s.shipped_ms).unwrap_or(0);
        Ok(Some(Window {
            earliest_ms: oldest.id.parse().unwrap_or(0),
            latest_ms: shipped.max(newest.id.parse().unwrap_or(0)),
        }))
    }

    /// The bytes of a snapshot; `id` may be `latest`.
    pub async fn fetch(&self, db: &str, id: &str) -> Result<Vec<u8>, BackupError> {
        // The name becomes part of a path in the store; checked before any read.
        if !crate::registry::valid_name(db) {
            return Err(RegistryError::InvalidName.into());
        }
        let target = &self.config()?.target;
        let id = match id {
            "latest" => self.list(db).await?.into_iter().next().ok_or(BackupError::NotFound)?.id,
            id if valid_id(id) => id.to_string(),
            _ => return Err(BackupError::InvalidId),
        };
        target.get(&snapshot_key(db, &id)).await?.ok_or(BackupError::NotFound)
    }

    /// The database as it was at `at_ms`: the newest snapshot at or before
    /// it, plus the WAL shipped after that snapshot and by that time.
    pub async fn fetch_at(&self, db: &str, at_ms: u64) -> Result<Restored, BackupError> {
        let target = &self.config()?.target;
        let snapshot = self
            .list(db)
            .await?
            .into_iter()
            .find(|s| s.id.parse::<u64>().is_ok_and(|ms| ms <= at_ms))
            .ok_or(BackupError::NotFound)?;
        let mut bytes = target.get(&snapshot_key(db, &snapshot.id)).await?.ok_or(BackupError::NotFound)?;
        let mut restored_to_ms = snapshot.id.parse().unwrap_or(0);
        for (n, segment) in self.segments(db, &snapshot.id).await?.into_iter().enumerate() {
            // Segments are numbered from 0; stop at a gap or past the time.
            if segment.number != n as u64 || segment.shipped_ms > at_ms {
                break;
            }
            let key = format!("{}{}", wal_dir(db, &snapshot.id), segment.name);
            let frames = target.get(&key).await?.ok_or(BackupError::NotFound)?;
            apply_frames(&mut bytes, &frames)?;
            restored_to_ms = segment.shipped_ms;
        }
        Ok(Restored { bytes, restored_to_ms })
    }

    async fn segments(&self, db: &str, generation: &str) -> Result<Vec<Segment>, BackupError> {
        let mut segments = self
            .config()?
            .target
            .list(&wal_dir(db, generation))
            .await?
            .into_iter()
            .filter_map(|(name, _)| Segment::parse(name))
            .collect::<Vec<_>>();
        segments.sort_by_key(|s| s.number);
        Ok(segments)
    }

    /// Snapshot every database that changed since its last scheduled snapshot.
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

    /// Ship every database's newly committed WAL frames.
    pub async fn ship_all(&self, registry: &Registry) {
        let names = match registry.list().await {
            Ok(names) => names,
            Err(err) => return eprintln!("aozumi-db: WAL shipping: cannot list databases: {err:?}"),
        };
        for name in names {
            if let Err(err) = self.ship(registry, &name).await {
                eprintln!("aozumi-db: WAL shipping for {name} failed: {err:?}");
            }
        }
    }

    async fn ship(&self, registry: &Registry, db: &str) -> Result<(), BackupError> {
        let lock = self.lock(db);
        let _held = lock.lock().await;
        let open = registry.open(db, false).await?;

        let Some((generation, wal_seq, next_frame, next_segment)) = self
            .cursors
            .lock()
            .await
            .get(db)
            .map(|c| (c.generation.clone(), c.wal_seq, c.next_frame, c.next_segment))
        else {
            // No generation yet (a new database, or a restarted server): a
            // snapshot starts one and covers everything written so far.
            self.snapshot_locked(registry, db).await?;
            return Ok(());
        };

        let mut next_frame = next_frame;
        let mut next_segment = next_segment;
        let mut wal_frames = 0;
        // Ship until nothing new is left (bounded), so the checkpoint below
        // races with as little as possible.
        for _ in 0..3 {
            let (position, frames) = {
                let open = open.clone();
                tokio::task::spawn_blocking(move || read_new_frames(&open, next_frame)).await??
            };
            if position.seq != wal_seq {
                // The WAL restarted under us; its unshipped frames are gone.
                self.snapshot_locked(registry, db).await?;
                return Ok(());
            }
            wal_frames = position.frame;
            let Some((bytes, last)) = frames else { break };
            let key = format!("{}{}", wal_dir(db, &generation), Segment::name(next_segment, now_ms()));
            match self.config()?.target.put(&key, bytes).await {
                Ok(()) => {
                    next_frame = last + 1;
                    next_segment += 1;
                    if let Some(cursor) = self.cursors.lock().await.get_mut(db) {
                        cursor.next_frame = next_frame;
                        cursor.next_segment = next_segment;
                    }
                }
                Err(err) if position.frame > MAX_UNSHIPPED_FRAMES => {
                    // Keep the WAL from growing without end while the store
                    // is down; the generation is broken either way.
                    eprintln!("aozumi-db: WAL shipping for {db} failed, checkpointing anyway: {err:?}");
                    self.cursors.lock().await.remove(db);
                    tokio::task::spawn_blocking(move || checkpoint(&open, u64::MAX)).await??;
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
            if position.frame < CHECKPOINT_FRAMES {
                break;
            }
        }

        let shipped_to = next_frame - 1;
        if wal_frames < CHECKPOINT_FRAMES || shipped_to < wal_frames {
            return Ok(());
        }
        // Under writes that never pause, frames keep arriving before every
        // checkpoint; past the cap, checkpoint anyway and let the check
        // below start a new generation if one slipped through.
        let wait_for = if wal_frames > MAX_UNSHIPPED_FRAMES { u64::MAX } else { shipped_to };
        let outcome = tokio::task::spawn_blocking({
            let open = open.clone();
            move || checkpoint(&open, wait_for)
        })
        .await??;
        match outcome {
            // An open transaction, or frames arrived since shipping: try
            // again next round.
            Checkpoint::Skipped => {}
            // A commit landed in the instant before the checkpoint, which
            // folded it in unshipped: start a new generation.
            Checkpoint::Done { backfilled } if backfilled > shipped_to => {
                self.snapshot_locked(registry, db).await?;
            }
            Checkpoint::Done { .. } => {
                let position = tokio::task::spawn_blocking(move || wal_position(&open)).await??;
                if let Some(cursor) = self.cursors.lock().await.get_mut(db) {
                    cursor.wal_seq = position.seq;
                    cursor.next_frame = 1;
                }
            }
        }
        Ok(())
    }

    async fn prune(&self, db: &str) -> Result<(), BackupError> {
        let config = self.config()?;
        for old in self.list(db).await?.into_iter().skip(config.keep.max(1)) {
            for segment in self.segments(db, &old.id).await? {
                config.target.delete(&format!("{}{}", wal_dir(db, &old.id), segment.name)).await?;
            }
            config.target.delete(&snapshot_key(db, &old.id)).await?;
        }
        Ok(())
    }

    async fn next_id(&self) -> String {
        let mut last = self.last_id.lock().await;
        *last = now_ms().max(*last + 1);
        format!("{:013}", *last)
    }
}

struct Segment {
    name: String,
    number: u64,
    shipped_ms: u64,
}

impl Segment {
    fn name(number: u64, shipped_ms: u64) -> String {
        format!("{number:010}-{shipped_ms:013}.wal")
    }

    fn parse(name: String) -> Option<Self> {
        let (number, shipped) = name.strip_suffix(".wal")?.split_once('-')?;
        Some(Self { number: number.parse().ok()?, shipped_ms: shipped.parse().ok()?, name })
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// The WAL position, and the committed frames from `next` on if there are
/// any. Blocks.
fn read_new_frames(open: &OpenDb, next: u64) -> anyhow::Result<(WalPosition, Option<(Vec<u8>, u64)>)> {
    let conn = Conn::open(&open.db, &open.internal_options())?;
    let position = conn.wal_position()?;
    if position.frame < next {
        return Ok((position, None));
    }
    let (bytes, last) = conn.committed_frames(next, position.frame, open.page_size as usize)?;
    Ok((position, (!bytes.is_empty()).then_some((bytes, last))))
}

fn wal_position(open: &OpenDb) -> anyhow::Result<WalPosition> {
    Conn::open(&open.db, &open.internal_options())?.wal_position()
}

enum Checkpoint {
    Skipped,
    Done { backfilled: u64 },
}

/// Checkpoint and empty the WAL, unless frames past `shipped_to` exist or a
/// transaction is in the way. Blocks.
fn checkpoint(open: &OpenDb, shipped_to: u64) -> anyhow::Result<Checkpoint> {
    let conn = Conn::open(&open.db, &open.internal_options())?;
    if conn.wal_position()?.frame > shipped_to {
        return Ok(Checkpoint::Skipped);
    }
    // One truncating pass, not passive-then-truncate: the frame count it
    // reports must cover the whole WAL for the check against `shipped_to`.
    match conn.checkpoint_truncate() {
        Ok(backfilled) => Ok(Checkpoint::Done { backfilled }),
        Err(err) if err.downcast_ref::<turso_core::LimboError>().is_some_and(|e| matches!(e, turso_core::LimboError::Busy)) => {
            Ok(Checkpoint::Skipped)
        }
        Err(err) => Err(err),
    }
}

/// Replay WAL frames onto a database file: each commit's page images are
/// written in place, and the file is cut to the size the commit records.
/// Frames after the last commit are ignored.
fn apply_frames(db: &mut Vec<u8>, frames: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(db.len() >= 100, "the snapshot is not a database file");
    let page_size = match u16::from_be_bytes([db[16], db[17]]) {
        1 => 65_536,
        size => size as usize,
    };
    let frame_size = FRAME_HEADER + page_size;
    anyhow::ensure!(frames.len() % frame_size == 0, "a WAL segment does not hold whole frames");
    let mut pending: Vec<(usize, &[u8])> = Vec::new();
    for frame in frames.chunks(frame_size) {
        let page = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap()) as usize;
        anyhow::ensure!(page > 0, "a WAL frame names page 0");
        pending.push((page, &frame[FRAME_HEADER..]));
        if db_size > 0 {
            for (page, image) in pending.drain(..) {
                let at = (page - 1) * page_size;
                if db.len() < at + page_size {
                    db.resize(at + page_size, 0);
                }
                db[at..at + page_size].copy_from_slice(image);
            }
            db.resize(db_size * page_size, 0);
        }
    }
    Ok(())
}

/// Unix milliseconds, from a number or an RFC 3339 UTC time such as
/// `2026-10-26T09:30:00Z` or `2026-10-26T09:30:00.250+00:00`.
pub fn parse_time(value: &serde_json::Value) -> Result<u64, BackupError> {
    match value {
        serde_json::Value::Number(n) => n.as_u64().ok_or(BackupError::InvalidTime),
        serde_json::Value::String(s) => parse_rfc3339(s).ok_or(BackupError::InvalidTime),
        _ => Err(BackupError::InvalidTime),
    }
}

fn parse_rfc3339(s: &str) -> Option<u64> {
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix("+00:00"))?;
    let (date, time) = s.split_once(['T', 't', ' '])?;
    let mut date = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (year, month, day) = (date.next()??, date.next()??, date.next()??);
    let (time, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut time = time.splitn(3, ':').map(|p| p.parse::<i64>().ok());
    let (hour, minute, second) = (time.next()??, time.next()??, time.next()??);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    if !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let millis = format!("{fraction:0<3}")[..3].parse::<i64>().ok()?;
    // Days from 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let ms = ((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1000) + millis;
    u64::try_from(ms).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_times_parse_to_unix_milliseconds() {
        // Values checked with Python's calendar.timegm.
        assert_eq!(parse_rfc3339("2013-05-24T00:00:00Z"), Some(1_369_353_600_000));
        assert_eq!(parse_rfc3339("2024-02-29T23:59:59.5Z"), Some(1_709_251_199_500));
        assert_eq!(parse_rfc3339("2024-02-29T23:59:59.123+00:00"), Some(1_709_251_199_123));
        assert_eq!(parse_rfc3339("2024-02-29T23:59:59"), None, "a zone is required");
        assert_eq!(parse_rfc3339("yesterday"), None);
    }
}
