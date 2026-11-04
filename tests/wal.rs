//! How commits reach the disk (`SyncMethod`): with the default, a
//! checkpoint starts the WAL over in place, so later commits overwrite it
//! instead of growing it, which makes their sync cheaper; with `Fsync`,
//! checkpoints truncate it, as the server did before.

mod common;

use std::time::Duration;

use aozumi_db::SyncMethod;
use common::*;
use serde_json::Value;

/// 1,200 commits of one row each, past the checkpoint threshold (1,000 frames).
async fn past_a_checkpoint(server: &TestServer) {
    server.run(None, vec![execute("CREATE TABLE t (i INTEGER)")]).await;
    for chunk in 0..12 {
        let rows = (0..100).map(|i| execute(&format!("INSERT INTO t VALUES ({})", chunk * 100 + i))).collect();
        server.run(None, rows).await;
    }
}

fn wal_size(server: &TestServer) -> u64 {
    std::fs::metadata(server.dir.path().join("default.db-wal")).map(|m| m.len()).unwrap_or(0)
}

/// The database file holds the table once a checkpoint copied it in.
async fn checkpointed(server: &TestServer) {
    for _ in 0..200 {
        let file = std::fs::metadata(server.dir.path().join("default.db")).map(|m| m.len()).unwrap_or(0);
        if file > 4 * 4096 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no checkpoint copied the WAL into the database file");
}

async fn count_and_check(server: &TestServer) -> (Value, Value) {
    let results = server.run(None, vec![execute("SELECT count(*) FROM t"), execute("PRAGMA integrity_check")]).await;
    (scalar(&results), results[1]["response"]["result"]["rows"][0][0]["value"].clone())
}

#[tokio::test]
async fn after_a_checkpoint_commits_overwrite_the_wal_instead_of_growing_it() {
    let server = TestServer::start_with(None, |c| c.sync_method = SyncMethod::DataSync).await;
    past_a_checkpoint(&server).await;
    checkpointed(&server).await;
    // The checkpoint may have started while the last rows were committing; let it settle.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let size = wal_size(&server);
    assert!(size > 0, "the WAL keeps its size");
    for i in 0..50 {
        server.run(None, vec![execute(&format!("INSERT INTO t VALUES ({})", 2000 + i))]).await;
    }
    assert_eq!(wal_size(&server), size, "50 commits wrote over the WAL in place");
    assert_eq!(count_and_check(&server).await, (Value::from("1250"), Value::from("ok")));
}

#[tokio::test]
async fn with_fsync_checkpoints_truncate_the_wal_as_before() {
    let server = TestServer::start_with(None, |c| c.sync_method = SyncMethod::Fsync).await;
    past_a_checkpoint(&server).await;
    checkpointed(&server).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let size = wal_size(&server);
    for i in 0..50 {
        server.run(None, vec![execute(&format!("INSERT INTO t VALUES ({})", 2000 + i))]).await;
    }
    assert!(wal_size(&server) > size, "commits after a truncating checkpoint grow the WAL again");
    assert_eq!(count_and_check(&server).await, (Value::from("1250"), Value::from("ok")));
}
