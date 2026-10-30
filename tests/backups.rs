mod common;

use std::time::Duration;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

fn backups(keep: usize, every: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().to_path_buf(), keep, every, wal: None };
    (dir, cfg)
}

async fn snapshot(server: &TestServer, db: &str) -> Value {
    let (status, body) = server.admin(Method::POST, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn list(server: &TestServer, db: &str) -> Vec<Value> {
    let (status, body) = server.admin(Method::GET, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["backups"].as_array().unwrap().clone()
}

async fn restore(server: &TestServer, into: &str, from: &str, backup: &str) -> (StatusCode, Value) {
    server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": into, "restore": {"database": from, "backup": backup}})))
        .await
}

async fn count(server: &TestServer, token: &str) -> Value {
    scalar(&server.run(Some(token), vec![execute("SELECT count(*) FROM t")]).await)
}

#[tokio::test]
async fn a_backup_restores_into_a_new_database_as_it_was() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1), (2)")]).await;

    let backup = snapshot(&server, "app").await;
    assert!(backup["size_bytes"].as_u64().unwrap() > 0);
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (3)")]).await;

    let id = backup["id"].as_str().unwrap();
    assert_eq!(list(&server, "app").await, vec![backup.clone()]);
    let (status, body) = restore(&server, "app-copy", "app", id).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let copy = server.token("app-copy").await;
    assert_eq!(count(&server, &copy).await, "2", "the copy is the database as it was at the backup");
    assert_eq!(count(&server, &app).await, "3", "the original is untouched");
}

#[tokio::test]
async fn latest_picks_the_newest_backup_and_backups_outlive_their_database() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)")]).await;
    snapshot(&server, "app").await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (1)")]).await;
    snapshot(&server, "app").await;

    let ids = list(&server, "app").await.iter().map(|b| b["id"].as_str().unwrap().to_string()).collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert!(ids[0] > ids[1], "newest first: {ids:?}");

    // Losing the database does not lose its backups; restore it under its own name.
    server.admin(Method::DELETE, "/v1/databases/app", None).await;
    assert_eq!(list(&server, "app").await.len(), 2);
    let (status, body) = restore(&server, "app", "app", "latest").await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, &server.token("app").await).await, "1");
}

#[tokio::test]
async fn a_new_server_can_restore_from_the_same_backup_store() {
    let (_dir, cfg) = backups(10, None);
    let first = TestServer::with_backups(&cfg).await;
    let app = first.database("app").await;
    first.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    snapshot(&first, "app").await;

    // A replacement machine with an empty data directory.
    let second = TestServer::with_backups(&cfg).await;
    let (status, body) = restore(&second, "app", "app", "latest").await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&second, &second.token("app").await).await, "1");
}

#[tokio::test]
async fn restore_refuses_bad_requests() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let id = snapshot(&server, "app").await["id"].as_str().unwrap().to_string();

    assert_eq!(restore(&server, "app", "app", &id).await.0, StatusCode::CONFLICT, "target exists");
    assert_eq!(restore(&server, "copy", "app", "1").await.0, StatusCode::NOT_FOUND, "unknown backup");
    assert_eq!(restore(&server, "copy", "nothing", "latest").await.0, StatusCode::NOT_FOUND, "no backups");
    assert_eq!(restore(&server, "copy", "app", "../app/x").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        server.admin(Method::POST, "/v1/databases/missing/backups", None).await.0,
        StatusCode::NOT_FOUND
    );

    let plain = TestServer::with_secret().await;
    plain.database("app").await;
    assert_eq!(
        plain.admin(Method::POST, "/v1/databases/app/backups", None).await.0,
        StatusCode::BAD_REQUEST,
        "backups are not configured"
    );
}

#[tokio::test]
async fn restore_reads_only_from_the_backup_store() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().join("store"), keep: 10, every: None, wal: None };
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let id = snapshot(&server, "app").await["id"].as_str().unwrap().to_string();
    // A valid backup file beside the store, not in it.
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::copy(cfg.dir.join("app").join(format!("{id}.db")), outside.join(format!("{id}.db"))).unwrap();

    for source in ["../outside", outside.to_str().unwrap()] {
        let (status, body) = restore(&server, "copy", source, &id).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{source}: {body}");
    }
}

#[tokio::test]
async fn backups_taken_during_writes_are_consistent_and_writes_do_not_fail() {
    let (_dir, cfg) = backups(100, None);
    let server = std::sync::Arc::new(TestServer::with_backups(&cfg).await);
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)")]).await;

    let writer = {
        let (server, app) = (server.clone(), app.clone());
        tokio::spawn(async move {
            for i in 0..200 {
                let results = server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i})"))]).await;
                assert_eq!(results[0]["type"], "ok", "write {i} failed during a backup: {}", results[0]);
            }
        })
    };
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(snapshot(&server, "app").await["id"].as_str().unwrap().to_string());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    writer.await.unwrap();

    let mut last = -1;
    for (i, id) in ids.iter().enumerate() {
        let name = format!("copy-{i}");
        assert_eq!(restore(&server, &name, "app", id).await.0, StatusCode::CREATED);
        let token = server.token(&name).await;
        let results = server
            .run(Some(&token), vec![execute("SELECT count(*), coalesce(max(x), -1) FROM t"), execute("PRAGMA integrity_check")])
            .await;
        let row = &results[0]["response"]["result"]["rows"][0];
        let (n, max) = (row[0]["value"].as_str().unwrap().parse::<i64>().unwrap(), row[1]["value"].as_str().unwrap().parse::<i64>().unwrap());
        assert_eq!(n, max + 1, "backup {i} holds a whole prefix of the writes");
        assert!(n >= last, "later backups never go back in time");
        assert_eq!(results[1]["response"]["result"]["rows"][0][0]["value"], "ok");
        last = n;
    }
}

#[tokio::test]
async fn scheduled_backups_skip_unchanged_databases_and_keep_the_newest() {
    let (_dir, cfg) = backups(2, Some(Duration::from_millis(150)));
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)")]).await;

    // A tick may land between creating the database and its table, so
    // there may be one or two backups; after that, nothing while unchanged.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let settled = list(&server, "app").await;
    assert!(!settled.is_empty());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(list(&server, "app").await, settled, "no backups while the database is unchanged");

    for i in 0..3 {
        server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i})"))]).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
    }
    let kept = list(&server, "app").await;
    assert_eq!(kept.len(), 2, "only the newest two are kept: {kept:?}");

    let (status, _) = restore(&server, "copy", "app", "latest").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(count(&server, &server.token("copy").await).await, "3");
}

/// A database of one 64 MB row, backed up: writing and checking a copy of
/// it takes a while.
async fn large_backup(server: &TestServer, db: &str) {
    let token = server.database(db).await;
    server.run(Some(&token), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (zeroblob(64000000))")]).await;
    let backup = snapshot(server, db).await;
    assert!(backup["size_bytes"].as_u64().unwrap() > 60_000_000, "{backup}");
}

/// Files in the data directory, at any depth, that are none of these
/// databases' own.
fn stray_files(server: &TestServer, databases: &[&str]) -> Vec<std::path::PathBuf> {
    let root = server.dir.path();
    let own = |name: &str| databases.iter().any(|db| name.starts_with(&format!("{db}.db")) || name == format!("{db}.tokens.json"));
    let (mut dirs, mut stray) = (vec![root.to_path_buf()], Vec::new());
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.parent() != Some(root) || !own(&path.file_name().unwrap().to_string_lossy()) {
                stray.push(path);
            }
        }
    }
    stray
}

#[tokio::test]
async fn a_large_restore_does_not_hold_up_other_databases() {
    let (_dir, cfg) = backups(10, None);
    let server = std::sync::Arc::new(TestServer::with_backups(&cfg).await);
    large_backup(&server, "big").await;
    let app = server.database("app").await;
    // Leaves a pooled connection for the request made during the restore.
    server.run(Some(&app), vec![execute("SELECT 1")]).await;

    let restoring = {
        let server = server.clone();
        tokio::spawn(async move { restore(&server, "copy", "big", "latest").await })
    };
    // The restore is writing its copy once a file of neither database shows up.
    while stray_files(&server, &["big", "app"]).is_empty() {
        assert!(!restoring.is_finished(), "the restore never wrote a copy in the data directory");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Another database's request is answered while that copy is still being
    // written or checked: before the restored database exists.
    let results = server.run(Some(&app), vec![execute("SELECT 1")]).await;
    assert_eq!(scalar(&results), "1");
    assert_eq!(server.admin(Method::GET, "/v1/databases/copy", None).await.0, StatusCode::NOT_FOUND, "answered only after the restore");

    let (status, body) = restoring.await.unwrap();
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, &server.token("copy").await).await, "1");
}

#[tokio::test]
async fn racing_restores_and_a_create_of_one_name_have_one_winner() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    large_backup(&server, "big").await;

    // Two restores into one name, and a create of it that lands while they
    // are writing their copies.
    let ((first, _), (second, _), (created, _)) = tokio::join!(
        restore(&server, "copy", "big", "latest"),
        restore(&server, "copy", "big", "latest"),
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            server.admin(Method::POST, "/v1/databases", Some(json!({"name": "copy"}))).await
        },
    );
    let statuses = [first, second, created];
    assert_eq!(statuses.iter().filter(|s| **s == StatusCode::CREATED).count(), 1, "one winner: {statuses:?}");
    assert!(statuses.iter().all(|s| [StatusCode::CREATED, StatusCode::CONFLICT].contains(s)), "{statuses:?}");

    // Nothing overwrote the winner, and the losers left no files.
    let tables = scalar(&server.run(Some(&server.token("copy").await), vec![execute("SELECT count(*) FROM sqlite_schema")]).await);
    assert_eq!(tables, if created == StatusCode::CREATED { "0" } else { "1" }, "{statuses:?}");
    assert_eq!(stray_files(&server, &["big", "copy"]), Vec::<std::path::PathBuf>::new());
}

#[tokio::test]
async fn a_backup_that_is_not_a_database_is_refused_and_leaves_no_files() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let id = snapshot(&server, "app").await["id"].as_str().unwrap().to_string();
    std::fs::write(cfg.dir.join("app").join(format!("{id}.db")), vec![7u8; 64 * 1024]).unwrap();

    let (status, body) = restore(&server, "copy", "app", &id).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(stray_files(&server, &["app"]), Vec::<std::path::PathBuf>::new());
    assert_eq!(server.admin(Method::GET, "/v1/databases/copy", None).await.0, StatusCode::NOT_FOUND);
    assert_eq!(server.admin(Method::POST, "/v1/databases", Some(json!({"name": "copy"}))).await.0, StatusCode::CREATED);
}

#[tokio::test]
async fn writes_to_a_restored_database_reach_its_own_files() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    snapshot(&server, "app").await;
    assert_eq!(restore(&server, "copy", "app", "latest").await.0, StatusCode::CREATED);

    // The copy was checked elsewhere before it moved into place. A backup of
    // it reads its files in the data directory, WAL included.
    let copy = server.token("copy").await;
    server.run(Some(&copy), vec![execute("INSERT INTO t VALUES (2)")]).await;
    snapshot(&server, "copy").await;
    assert_eq!(restore(&server, "copy-of-copy", "copy", "latest").await.0, StatusCode::CREATED);
    assert_eq!(count(&server, &server.token("copy-of-copy").await).await, "2");
}

#[tokio::test]
async fn an_in_place_restore_keeps_the_current_tokens_and_not_revoked_ones() {
    let (_dir, cfg) = backups(10, None);
    let server = TestServer::with_backups(&cfg).await;
    let revoked = server.database("app").await;
    server.run(Some(&revoked), vec![execute("CREATE TABLE t (x)")]).await;
    snapshot(&server, "app").await;
    assert_eq!(server.admin(Method::POST, "/v1/databases/app/tokens/rotate", None).await.0, StatusCode::OK);
    let current = server.token("app").await;

    assert_eq!(server.admin(Method::DELETE, "/v1/databases/app", None).await.0, StatusCode::NO_CONTENT);
    let (status, body) = restore(&server, "app", "app", "latest").await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, &current).await, "0", "the current generation carries over");
    let (status, _) = server.pipeline(Some(&revoked), json!({"baton": null, "requests": [{"type": "close"}]})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "revoked stays revoked");
}
