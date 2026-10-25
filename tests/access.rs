//! What a tenant's SQL may do: read-only tokens, and statements that would
//! reach outside the tenant's own database or undo its limits.

mod common;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

async fn read_only_token(server: &TestServer, db: &str) -> String {
    let (status, body) = server
        .admin(Method::POST, &format!("/v1/databases/{db}/tokens"), Some(json!({"read_only": true})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["token"].as_str().unwrap().to_string()
}

fn error_code(result: &Value) -> &str {
    assert_eq!(result["type"], "error", "expected an error: {result}");
    result["error"]["code"].as_str().unwrap()
}

#[tokio::test]
async fn a_read_only_token_reads_but_cannot_write() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    server.run(Some(&rw), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let ro = read_only_token(&server, "app").await;

    let results = server
        .run(Some(&ro), vec![
            execute("SELECT count(*) FROM t"),
            execute("INSERT INTO t VALUES (2)"),
            execute("CREATE TABLE u (x)"),
            execute("PRAGMA query_only = 0"),
            execute("INSERT INTO t VALUES (3)"),
            json!({"type": "sequence", "sql": "PRAGMA query_only = 0; INSERT INTO t VALUES (4);"}),
        ])
        .await;

    assert_eq!(scalar(&results), "1");
    assert_eq!(error_code(&results[1]), "SQLITE_READONLY");
    assert_eq!(error_code(&results[2]), "SQLITE_READONLY");
    assert_eq!(error_code(&results[3]), "SQLITE_AUTH", "read-only cannot be switched off");
    assert_eq!(error_code(&results[4]), "SQLITE_READONLY");
    assert_eq!(error_code(&results[5]), "SQLITE_AUTH");
    assert_eq!(scalar(&server.run(Some(&rw), vec![execute("SELECT count(*) FROM t")]).await), "1");
}

#[tokio::test]
async fn read_only_and_read_write_streams_do_not_mix() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    let ro = read_only_token(&server, "app").await;

    let (_, open) = server.pipeline(Some(&ro), json!({"baton": null, "requests": []})).await;
    let baton = open["baton"].as_str().unwrap();
    let (status, _) = server.pipeline(Some(&rw), json!({"baton": baton, "requests": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a read-only stream cannot be taken over by a read-write token");

    let (_, open) = server.pipeline(Some(&rw), json!({"baton": null, "requests": []})).await;
    let baton = open["baton"].as_str().unwrap();
    let (status, _) = server.pipeline(Some(&ro), json!({"baton": baton, "requests": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a read-only token cannot borrow a read-write stream");
}

#[tokio::test]
async fn tenants_cannot_reach_outside_their_database_or_change_server_settings() {
    let server = TestServer::with_secret().await;
    let token = server.database("app").await;
    let outside = server.dir.path().join("planted.db");

    let denied = [
        format!("VACUUM INTO '{}'", outside.display()),
        format!("ATTACH DATABASE '{}' AS other", outside.display()),
        "DETACH DATABASE other".to_string(),
        "PRAGMA max_page_count = 1000000".to_string(),
        "PRAGMA query_only = 1".to_string(),
        "PRAGMA journal_mode = DELETE".to_string(),
        "PRAGMA wal_checkpoint(TRUNCATE)".to_string(),
        "PRAGMA synchronous = OFF".to_string(),
        "PRAGMA cache_size = -10000000".to_string(),
        "PRAGMA database_list".to_string(),
        "pRaGmA   Main.Journal_Mode = off".to_string(),
        "EXPLAIN VACUUM INTO 'x.db'".to_string(),
    ];
    let results = server.run(Some(&token), denied.iter().map(|sql| execute(sql)).collect()).await;
    for (sql, result) in denied.iter().zip(&results) {
        assert_eq!(error_code(result), "SQLITE_AUTH", "{sql} must be refused");
    }
    assert!(!outside.exists(), "nothing was written outside the database");

    let results = server
        .run(Some(&token), vec![json!({"type": "sequence", "sql": "CREATE TABLE a (x); PRAGMA synchronous = OFF;"})])
        .await;
    assert_eq!(error_code(&results[0]), "SQLITE_AUTH", "scripts are checked statement by statement");

    let allowed = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)",
        "PRAGMA table_info(t)",
        "PRAGMA user_version = 7",
        "PRAGMA user_version",
        "PRAGMA foreign_keys = ON",
        "PRAGMA integrity_check",
        "PRAGMA page_count",
        "SELECT * FROM pragma_table_info('t')",
        "SELECT 'PRAGMA journal_mode = off'",
    ];
    let results = server.run(Some(&token), allowed.iter().map(|sql| execute(sql)).collect()).await;
    for (sql, result) in allowed.iter().zip(&results) {
        assert_eq!(result["type"], "ok", "{sql} must be allowed: {result}");
    }
    assert_eq!(results[3]["response"]["result"]["rows"][0][0]["value"], "7");
}
