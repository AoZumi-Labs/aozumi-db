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
        })
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
