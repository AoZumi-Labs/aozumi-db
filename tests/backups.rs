mod common;

use std::time::Duration;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

fn backups(keep: usize, every: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().to_path_buf(), keep, every };
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

    tokio::time::sleep(Duration::from_millis(400)).await;
    let after_first = list(&server, "app").await;
    assert_eq!(after_first.len(), 1, "one backup, then nothing while unchanged: {after_first:?}");

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
