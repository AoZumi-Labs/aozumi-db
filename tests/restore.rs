//! Restoring a database in place: `POST /v1/databases/{name}/restore`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

fn store(wal: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().join("store"), keep: 20, every: None, wal };
    (dir, cfg)
}

async fn snapshot(server: &TestServer, db: &str) -> Value {
    let (status, body) = server.admin(Method::POST, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn restore(server: &TestServer, db: &str, from: Value) -> (StatusCode, Value) {
    server.admin(Method::POST, &format!("/v1/databases/{db}/restore"), Some(from)).await
}

async fn count(server: &TestServer, token: &str) -> String {
    scalar(&server.run(Some(token), vec![execute("SELECT count(*) FROM t")]).await).as_str().unwrap().to_string()
}

async fn listed(server: &TestServer, db: &str) -> Vec<Value> {
    let (status, body) = server.admin(Method::GET, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["backups"].as_array().unwrap().clone()
}

#[tokio::test]
async fn restoring_in_place_puts_the_database_back_and_keeps_an_undo_backup() {
    let (_dir, cfg) = store(None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    let limits = json!({"max_statement_ms": 1234});
    assert_eq!(server.admin(Method::PATCH, "/v1/databases/app/limits", Some(limits)).await.0, StatusCode::OK);
    assert_eq!(server.admin(Method::PATCH, "/v1/databases/app/backups", Some(json!({"manual_keep": 5}))).await.0, StatusCode::OK);
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let good = snapshot(&server, "app").await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)"), execute("INSERT INTO t VALUES (3)")]).await;

    let (status, body) = restore(&server, "app", json!({"backup": good["id"]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["name"], "app");
    assert_eq!(body["restored_to_ms"], good["taken_ms"], "{body}");
    let before = &body["before_restore"];
    assert_eq!(before["kind"], "before_restore", "{body}");
    assert!(before["size_bytes"].as_u64().unwrap() > 0);

    // The same token reaches the restored database, and its settings stayed.
    assert_eq!(count(&server, &app).await, "1");
    let (_, info) = server.admin(Method::GET, "/v1/databases/app", None).await;
    assert_eq!(info["limits"]["max_statement_ms"], 1234, "{info}");
    let (_, backups) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(backups["status"]["policy"]["manual_keep"], 5, "{backups}");
    assert_eq!(listed(&server, "app").await[0], *before, "the undo backup is the newest");

    // Undo: the backup taken just before has every row the restore replaced.
    let (status, body) = restore(&server, "app", json!({"backup": before["id"]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(count(&server, &app).await, "3");
    let results = server.run(Some(&app), vec![execute("PRAGMA integrity_check"), execute("INSERT INTO t VALUES (4)")]).await;
    assert_eq!(scalar(&results), "ok");
    assert_eq!(results[1]["type"], "ok", "{}", results[1]);
}

#[tokio::test]
async fn restoring_in_place_to_a_moment() {
    let (_dir, cfg) = store(Some(Duration::from_millis(50)));
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let moment = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let (status, body) = restore(&server, "app", json!({"at": moment})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["restored_to_ms"].as_u64().unwrap() <= moment, "{body}");
    assert_eq!(count(&server, &app).await, "1");

    // Writes after the restore are shipped from it, as a new history.
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (3)")]).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "now", "restore": {"database": "app", "at": now_ms()}})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let now = server.token("now").await;
    let rows = server.run(Some(&now), vec![execute("SELECT group_concat(x) FROM (SELECT x FROM t ORDER BY x)")]).await;
    assert_eq!(scalar(&rows), "1,3", "the restored history, not the replaced one");
}

#[tokio::test]
async fn a_refused_restore_changes_nothing() {
    let (_dir, cfg) = store(None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let only = snapshot(&server, "app").await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;

    for (from, expected) in [
        (json!({}), StatusCode::BAD_REQUEST),
        (json!({"backup": "../x"}), StatusCode::BAD_REQUEST),
        (json!({"backup": "1"}), StatusCode::NOT_FOUND),
        (json!({"at": "yesterday"}), StatusCode::BAD_REQUEST),
        (json!({"at": 1000}), StatusCode::NOT_FOUND),
    ] {
        let (status, body) = restore(&server, "app", from.clone()).await;
        assert_eq!(status, expected, "{from}: {body}");
    }
    assert_eq!(restore(&server, "missing", json!({"backup": "latest"})).await.0, StatusCode::NOT_FOUND);
    assert_eq!(count(&server, &app).await, "2", "untouched");
    assert_eq!(listed(&server, "app").await, vec![only], "no undo backup for a restore that never ran");

    let plain = TestServer::with_secret().await;
    plain.database("app").await;
    assert_eq!(restore(&plain, "app", json!({"backup": "latest"})).await.0, StatusCode::BAD_REQUEST, "backups are not configured");
}

#[tokio::test]
async fn an_open_transaction_ends_and_does_not_hold_up_the_restore() {
    let dir = tempfile::tempdir().unwrap();
    let backup = aozumi_db::BackupConfig {
        target: aozumi_db::BackupTarget::directory(dir.path().join("store")),
        keep: 10,
        every: None,
        wal_every: None,
    };
    // Streams idle long enough that waiting for one to time out would show.
    let server = TestServer::with_secret_and(move |c| {
        c.backup = Some(backup);
        c.stream_idle_timeout = Duration::from_secs(60);
    })
    .await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    snapshot(&server, "app").await;

    // A transaction that has written, parked between requests.
    let (status, open) = server
        .pipeline(Some(&app), json!({"baton": null, "requests": [execute("BEGIN"), execute("INSERT INTO t VALUES (99)")]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{open}");
    let baton = open["baton"].as_str().unwrap().to_string();

    let started = Instant::now();
    let (status, body) = restore(&server, "app", json!({"backup": "latest"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(started.elapsed() < Duration::from_secs(20), "did not wait for the stream to idle out: {:?}", started.elapsed());

    let (status, _) = server.pipeline(Some(&app), json!({"baton": baton, "requests": [execute("COMMIT")]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "the transaction ended with the old file");
    assert_eq!(count(&server, &app).await, "1");
}

/// The files in the data directory below `.tmp`, where restores write their copies.
fn scratch(server: &TestServer) -> Vec<String> {
    let mut found = Vec::new();
    let mut dirs = vec![server.dir.path().join(".tmp")];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            found.push(path.to_string_lossy().into_owned());
            if path.is_dir() {
                dirs.push(path);
            }
        }
    }
    found
}

#[tokio::test]
async fn a_write_that_waited_for_the_restore_lands_in_the_restored_database() {
    let (_dir, cfg) = store(None);
    let server = Arc::new(TestServer::with_backups(&cfg).await);
    let app = server.database("app").await;
    // Large enough that copying it for the undo backup takes a while.
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("CREATE TABLE big (b)"), execute("INSERT INTO big VALUES (zeroblob(64000000))")]).await;
    snapshot(&server, "app").await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (1)")]).await;

    let restoring = {
        let server = server.clone();
        tokio::spawn(async move { restore(&server, "app", json!({"backup": "latest"})).await })
    };
    // The restore holds the database's writes once it copies it for the undo backup.
    while !scratch(&server).iter().any(|path| path.contains("-before-")) {
        assert!(!restoring.is_finished(), "the restore never copied the database: {:?}", restoring.await);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let written = server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;
    assert_eq!(written[0]["type"], "ok", "{}", written[0]);

    let (status, body) = restoring.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = server.run(Some(&app), vec![execute("SELECT group_concat(x) FROM t"), execute("PRAGMA integrity_check")]).await;
    assert_eq!(scalar(&rows), "2", "the write that waited is in the restored database, not the replaced file");
    assert_eq!(rows[1]["response"]["result"]["rows"][0][0]["value"], "ok");

    // The undo backup was taken before that write, after the one before it.
    let (status, undo) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "undo", "restore": {"database": "app", "backup": body["before_restore"]["id"]}})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{undo}");
    assert_eq!(count(&server, &server.token("undo").await).await, "1");
    assert!(scratch(&server).iter().all(|path| path.ends_with(".tmp")), "no copies left behind: {:?}", scratch(&server));
}
