//! Group commit (`synchronous = group`, D33): commits are written to the WAL
//! without a flush of their own, and one flush makes every commit queued
//! behind it durable at once. Writers no longer hold the writer turn while
//! the disk flushes, so the next writer runs during the flush, and a flush
//! covers as many commits as arrived while the one before it ran.
//!
//! The engine makes a commit visible as soon as it is written, so a commit
//! is seen before it is durable. Nothing that has seen one leaves the
//! server before it is: every answer waits until the WAL is flushed as far
//! as what its connection read, and backups and copies flush the WAL before
//! they read it. So what `full` promises still holds: nothing a client was
//! told, and nothing in a backup, is lost on power loss.
//!
//! A request that only reads would then often wait for a flush. Instead it
//! reads the database as of the last flushed commit (`ReadCap`, a patch to
//! the engine in vendor/turso_core), which is on disk already. Writers and
//! transactions read everything committed and wait as above.

use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::engine::{Conn, ConnOptions, Db, WalPosition};

pub struct Durability {
    /// False unless the database runs with group commit: then every commit
    /// is already durable when it is visible, and waiting is a no-op.
    enabled: bool,
    wal: PathBuf,
    db: Db,
    options: ConnOptions,
    state: Mutex<State>,
    flushed: Notify,
    /// The flusher's own connection, to read how far the WAL is committed.
    probe: Mutex<Option<Conn>>,
    /// The last flushed commit: requests that only read see the database as
    /// of here (the engine patch in vendor/turso_core), so they never wait.
    cap: Arc<turso_core::ReadCap>,
}

#[derive(Default)]
struct State {
    /// The WAL was committed this far before a flush that has finished.
    durable: Option<WalPosition>,
    /// Flushes are numbered; `completed` have finished, and waiters need
    /// the WAL flushed up to flush number `requested`.
    completed: u64,
    requested: u64,
    flushing: bool,
    /// How far the WAL was committed when the running flush began: that
    /// flush covers whoever saw no further.
    in_flight: Option<WalPosition>,
}

/// Whether a WAL flushed when it stood at `durable` holds what a connection
/// that read up to `seen` saw. A WAL that restarted since (a new `seq`) was
/// flushed and copied into the database file first.
fn covers(durable: WalPosition, seen: WalPosition) -> bool {
    durable.seq > seen.seq || (durable.seq == seen.seq && durable.frame >= seen.frame)
}

impl Durability {
    pub fn new(enabled: bool, db: Db, wal: PathBuf, options: ConnOptions) -> Arc<Self> {
        Arc::new(Self {
            enabled,
            wal,
            db,
            options,
            state: Mutex::default(),
            flushed: Notify::new(),
            probe: Mutex::new(None),
            cap: Arc::new(turso_core::ReadCap::new()),
        })
    }

    /// Flush what a previous run of the server left unflushed, so readers
    /// start from a cap that holds every commit on disk. Blocks.
    pub fn start(&self) {
        if !self.enabled {
            return;
        }
        let position = self.flush();
        let mut state = self.state.lock().unwrap();
        state.in_flight = None;
        if let Some(position) = position {
            state.durable = Some(position);
            self.cap.set(position.seq, position.frame);
        }
    }

    /// The cap for connections that only read, with group commit on.
    pub fn read_cap(&self) -> Option<Arc<turso_core::ReadCap>> {
        self.enabled.then(|| self.cap.clone())
    }

    /// Wait until what a connection saw (`seen`, its WAL position after a
    /// request) is on disk. `None` when the position is not known: then wait
    /// for a flush that starts after now, which covers everything committed.
    pub async fn wait(self: &Arc<Self>, seen: Option<WalPosition>) {
        if !self.enabled {
            return;
        }
        let (need, lead) = {
            let mut state = self.state.lock().unwrap();
            if let (Some(durable), Some(seen)) = (state.durable, seen) {
                if covers(durable, seen) {
                    return;
                }
            }
            // A flush already running covers what it found committed when it
            // began; anything newer needs the flush after it.
            let running_covers = matches!((state.in_flight, seen), (Some(at), Some(seen)) if covers(at, seen));
            let need = state.completed + if state.flushing && !running_covers { 2 } else { 1 };
            state.requested = state.requested.max(need);
            let lead = !state.flushing;
            state.flushing = true;
            (need, lead)
        };
        if lead {
            // Its own task: a request that goes away mid-wait (the client
            // hung up) must not leave the others waiting on a flush.
            tokio::spawn(self.clone().lead());
        }
        loop {
            let notified = self.flushed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().completed >= need {
                return;
            }
            notified.await;
        }
    }

    /// Flush until no waiter needs another flush.
    async fn lead(self: Arc<Self>) {
        loop {
            let number = {
                let mut state = self.state.lock().unwrap();
                if state.completed >= state.requested {
                    state.flushing = false;
                    return;
                }
                state.completed + 1
            };
            let this = self.clone();
            let position = match tokio::task::spawn_blocking(move || this.flush()).await {
                Ok(position) => position,
                Err(err) => fatal(&self.wal, &err.to_string()),
            };
            {
                let mut state = self.state.lock().unwrap();
                state.completed = number;
                state.in_flight = None;
                if let Some(position) = position {
                    if state.durable.is_none_or(|durable| !covers(durable, position)) {
                        state.durable = Some(position);
                        self.cap.set(position.seq, position.frame);
                    }
                }
            }
            self.flushed.notify_waiters();
        }
    }

    /// Read how far the WAL is committed, then flush it: everything up to
    /// that position is then on disk. Blocks.
    fn flush(&self) -> Option<WalPosition> {
        let position = {
            let mut probe = self.probe.lock().unwrap();
            if probe.is_none() {
                *probe = Conn::open(&self.db, &self.options).ok();
            }
            // Unknown: the flush still covers its waiters, but no later
            // request can skip waiting on its account.
            probe.as_ref().and_then(|conn| conn.wal_position().ok())
        };
        self.state.lock().unwrap().in_flight = position;
        self.sync_blocking();
        position
    }

    /// Flush the WAL as it is now. For readers of the WAL file itself (WAL
    /// shipping, copies), which must not take a commit that is not durable.
    /// Blocks.
    pub fn sync_blocking(&self) {
        if !self.enabled {
            return;
        }
        match std::fs::File::open(&self.wal).and_then(|file| file.sync_data()) {
            Ok(()) => {}
            // No WAL: nothing written since it was emptied, or the database
            // was deleted. Either way nothing is waiting on it.
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => fatal(&self.wal, &err.to_string()),
        }
    }
}

/// A failed flush leaves unknown what reached the disk, and a later flush
/// can succeed without writing the lost pages again, so carrying on could
/// acknowledge commits that are gone. Stop instead: on restart the engine
/// recovers the WAL from what is actually on disk.
fn fatal(wal: &std::path::Path, err: &str) -> ! {
    eprintln!("aozumi-db: flushing {} failed ({err}); stopping so recovery starts from the disk", wal.display());
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flushed_position_covers_earlier_frames_and_older_wals() {
        let at = |seq, frame| WalPosition { seq, frame };
        assert!(covers(at(1, 10), at(1, 10)));
        assert!(covers(at(1, 10), at(1, 3)));
        assert!(!covers(at(1, 10), at(1, 11)));
        assert!(covers(at(2, 0), at(1, 50)), "a restarted WAL was flushed and copied in first");
        assert!(!covers(at(1, 50), at(2, 1)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn waiters_share_flushes_and_a_covered_read_does_not_wait() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = crate::engine::open(&path).unwrap();
        let options = ConnOptions { synchronous: crate::engine::Synchronous::Group, ..Default::default() };
        let conn = Conn::open(&db, &options).unwrap();
        conn.execute_script("create table t (x); insert into t values (1)").unwrap();
        let durability = Durability::new(true, db.clone(), PathBuf::from(format!("{}-wal", path.display())), options);

        let waiters = (0..64).map(|_| {
            let durability = durability.clone();
            tokio::spawn(async move { durability.wait(None).await })
        });
        for waiter in waiters.collect::<Vec<_>>() {
            waiter.await.unwrap();
        }
        let flushes = durability.state.lock().unwrap().completed;
        assert!((1..64).contains(&flushes), "64 waiters, {flushes} flushes");

        // What the connection saw is on disk now: no further flush.
        durability.wait(conn.seen()).await;
        assert_eq!(durability.state.lock().unwrap().completed, flushes);

        // A new commit is not, until a flush covers it.
        conn.execute_script("insert into t values (2)").unwrap();
        durability.wait(conn.seen()).await;
        assert_eq!(durability.state.lock().unwrap().completed, flushes + 1);
    }

    fn count(conn: &Conn) -> i64 {
        match conn.execute("select count(*) from t", &[], &[], true).unwrap().rows[0][0] {
            crate::proto::Value::Integer(n) => n,
            ref other => panic!("{other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capped_reader_sees_only_flushed_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = crate::engine::open(&path).unwrap();
        let options = ConnOptions { synchronous: crate::engine::Synchronous::Group, ..Default::default() };
        let writer = Conn::open(&db, &options).unwrap();
        writer.execute_script("create table t (x); insert into t values (1)").unwrap();
        let durability = Durability::new(true, db.clone(), PathBuf::from(format!("{}-wal", path.display())), options.clone());
        durability.start();

        let reader = Conn::open(&db, &options).unwrap();
        reader.set_read_cap(durability.read_cap());
        assert_eq!(count(&reader), 1, "what was there at start is flushed");

        writer.execute_script("insert into t values (2); insert into t values (3)").unwrap();
        assert_eq!(count(&writer), 3, "the writer sees its own commits");
        assert_eq!(count(&reader), 1, "a capped reader does not see unflushed commits");
        let seen = reader.seen();
        durability.wait(seen).await;
        assert_eq!(durability.state.lock().unwrap().completed, 0, "a capped read waits for no flush");

        durability.wait(writer.seen()).await;
        assert_eq!(count(&reader), 3, "once flushed, they are seen");

        // A capped connection cannot become a writer on a stale snapshot.
        writer.execute_script("insert into t values (4)").unwrap();
        assert!(reader.execute_script("insert into t values (5)").is_err());
        reader.set_read_cap(None);
        reader.execute_script("insert into t values (5)").unwrap();
        assert_eq!(count(&writer), 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capped_reader_stays_consistent_across_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = crate::engine::open(&path).unwrap();
        let options = ConnOptions { synchronous: crate::engine::Synchronous::Group, ..Default::default() };
        let writer = Conn::open(&db, &options).unwrap();
        writer.execute_script("create table t (x)").unwrap();
        let durability = Durability::new(true, db.clone(), PathBuf::from(format!("{}-wal", path.display())), options.clone());
        durability.start();
        let reader = Conn::open(&db, &options).unwrap();
        reader.set_read_cap(durability.read_cap());
        for round in 1..=3i64 {
            writer.execute_script("insert into t values (1)").unwrap();
            durability.wait(writer.seen()).await;
            writer.checkpoint_truncate().unwrap();
            writer.execute_script("insert into t values (1)").unwrap();
            // The WAL restarted: the reader sees what the database file holds.
            assert_eq!(count(&reader), round * 2 - 1, "round {round}");
            durability.wait(writer.seen()).await;
            assert_eq!(count(&reader), round * 2, "round {round}");
        }
    }

    #[tokio::test]
    async fn waiting_is_free_without_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = crate::engine::open(&path).unwrap();
        let durability = Durability::new(false, db, PathBuf::from(format!("{}-wal", path.display())), ConnOptions::default());
        durability.wait(None).await;
        assert_eq!(durability.state.lock().unwrap().completed, 0);
    }
}
