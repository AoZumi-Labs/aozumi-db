//! Point-in-time restore: snapshots plus the shipped WAL between them.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

const SHIP_EVERY: Duration = Duration::from_millis(50);
/// Comfortably longer than one shipping round.
const SETTLE: Duration = Duration::from_millis(400);

async fn pitr_server() -> (tempfile::TempDir, TestServer) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().to_path_buf(), keep: 10, every: None, wal: Some(SHIP_EVERY) };
    (dir, TestServer::with_backups(&cfg).await)
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Note the time between two writes, well clear of both.
async fn mark() -> u64 {
    tokio::time::sleep(SETTLE).await;
    let at = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    at
}

async fn restore_at(server: &TestServer, into: &str, from: &str, at: Value) -> (StatusCode, Value) {
    server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": into, "restore": {"database": from, "at": at}})))
        .await
}

async fn rows(server: &TestServer, db: &str, sql: &str) -> Vec<Value> {
    let token = server.token(db).await;
    let results = server.run(Some(&token), vec![execute(sql)]).await;
    assert_eq!(results[0]["type"], "ok", "{}", results[0]);
    results[0]["response"]["result"]["rows"].as_array().unwrap().clone()
}

async fn notes(server: &TestServer, db: &str) -> Vec<String> {
    rows(server, db, "SELECT body FROM notes ORDER BY rowid")
        .await
        .iter()
        .map(|row| row[0]["value"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn restores_the_database_as_it_was_at_any_moment() {
    let (_dir, server) = pitr_server().await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE notes (body TEXT)")]).await;
    let insert = |body: &str| execute(&format!("INSERT INTO notes VALUES ('{body}')"));

    tokio::time::sleep(SETTLE).await;
    server.run(Some(&app), vec![insert("a")]).await;
    let after_a = mark().await;
    server.run(Some(&app), vec![insert("b")]).await;
    let after_b = mark().await;
    server.run(Some(&app), vec![insert("c"), execute("DELETE FROM notes WHERE body = 'a'")]).await;
    let after_c = mark().await;

    let (status, body) = restore_at(&server, "at-a", "app", json!(after_a)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(body["restored_to_ms"].as_u64().unwrap() <= after_a, "{body}");
    assert_eq!(notes(&server, "at-a").await, ["a"]);

    restore_at(&server, "at-b", "app", json!(after_b)).await;
    assert_eq!(notes(&server, "at-b").await, ["a", "b"]);

    // RFC 3339 works too.
    let secs = after_c / 1000;
    let (h, m, s) = (secs / 3600 % 24, secs / 60 % 60, secs % 60);
    let days = secs / 86_400;
    let (y, mo, d) = civil(days as i64);
    let stamp = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{:03}Z", after_c % 1000);
    let (status, body) = restore_at(&server, "at-c", "app", json!(stamp)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(notes(&server, "at-c").await, ["b", "c"]);
}

#[tokio::test]
async fn point_in_time_survives_wal_checkpoints() {
    let (_dir, server) = pitr_server().await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (i INTEGER)")]).await;
    tokio::time::sleep(SETTLE).await;

    // One commit per row: well past the shipper's checkpoint threshold.
    let batch = |from: usize| (from..from + 800).map(|i| execute(&format!("INSERT INTO t VALUES ({i})"))).collect();
    server.run(Some(&app), batch(0)).await;
    let middle = mark().await;
    server.run(Some(&app), batch(800)).await;
    tokio::time::sleep(SETTLE).await;
    let end = now_ms();

    let wal = std::fs::metadata(server.dir.path().join("app.db-wal")).map(|m| m.len()).unwrap_or(0);
    assert!(wal < 4 * 1024 * 1024, "the shipper checkpointed the WAL ({wal} bytes)");

    restore_at(&server, "middle", "app", json!(middle)).await;
    assert_eq!(rows(&server, "middle", "SELECT count(*) FROM t").await[0][0]["value"], "800");
    restore_at(&server, "end", "app", json!(end)).await;
    assert_eq!(rows(&server, "end", "SELECT count(*) FROM t").await[0][0]["value"], "1600");
}

#[tokio::test]
async fn every_restore_point_under_concurrent_writes_is_consistent() {
    let (_dir, server) = pitr_server().await;
    let server = Arc::new(server);
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (i INTEGER)")]).await;
    tokio::time::sleep(SETTLE).await;

    let writer = {
        let (server, app) = (server.clone(), app.clone());
        tokio::spawn(async move {
            for i in 0..1500 {
                let results = server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i})"))]).await;
                assert_eq!(results[0]["type"], "ok", "write {i}: {}", results[0]);
            }
        })
    };
    let mut points = Vec::new();
    while !writer.is_finished() {
        tokio::time::sleep(Duration::from_millis(120)).await;
        points.push(now_ms());
    }
    writer.await.unwrap();
    tokio::time::sleep(SETTLE).await;
    points.push(now_ms());

    let mut last = -1;
    for (n, at) in points.iter().enumerate() {
        let name = format!("p{n}");
        let (status, body) = restore_at(&server, &name, "app", json!(at)).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let row = rows(&server, &name, "SELECT count(*), coalesce(max(i), -1) FROM t").await[0].clone();
        let count = row[0]["value"].as_str().unwrap().parse::<i64>().unwrap();
        let max = row[1]["value"].as_str().unwrap().parse::<i64>().unwrap();
        assert_eq!(count, max + 1, "restore point {n} holds a whole prefix of the writes");
        assert!(count >= last, "restore points never go back in time");
        last = count;
    }
    assert_eq!(last, 1500, "the newest point has every write");
}

#[tokio::test]
async fn the_restore_window_is_reported_and_bad_times_are_refused() {
    let (_dir, server) = pitr_server().await;
    let app = server.database("app").await;
    let before_anything = now_ms() - 60_000;
    server.run(Some(&app), vec![execute("CREATE TABLE t (i)")]).await;
    tokio::time::sleep(SETTLE).await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (1)")]).await;
    let written = now_ms();
    tokio::time::sleep(SETTLE).await;

    let (status, body) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let window = &body["point_in_time"];
    let (earliest, latest) = (window["earliest_ms"].as_u64().unwrap(), window["latest_ms"].as_u64().unwrap());
    assert!(earliest <= written && latest >= written, "{body}");

    assert_eq!(restore_at(&server, "x", "app", json!(before_anything)).await.0, StatusCode::NOT_FOUND);
    assert_eq!(restore_at(&server, "x", "app", json!("yesterday")).await.0, StatusCode::BAD_REQUEST);
}

/// Days since 1970-01-01 to (year, month, day).
fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}
