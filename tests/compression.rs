//! Backups are stored compressed: snapshots as `<id>[.kind].db.zst`, WAL
//! segments as `<no>-<ms>.wal.zst`. Objects stored plain by earlier servers
//! still list, restore, download and prune.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};

fn store(wal: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().join("store"), keep: 20, every: None, wal };
    (dir, cfg)
}

/// Every file under `dir`, at any depth.
fn files(dir: &Path) -> Vec<PathBuf> {
    let (mut dirs, mut found) = (vec![dir.to_path_buf()], Vec::new());
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

async fn download(server: &TestServer, path: &str) -> (StatusCode, Bytes) {
    let client = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::get(format!("http://{}{path}", server.addr))
        .header("authorization", format!("Bearer {SECRET}"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    let status = resp.status();
    (status, resp.into_body().collect().await.unwrap().to_bytes())
}

async fn count(server: &TestServer, db: &str) -> Value {
    let token = server.token(db).await;
    scalar(&server.run(Some(&token), vec![execute("SELECT count(*) FROM t")]).await)
}

/// A database with `rows` rows of text, written one row per commit.
async fn filled(server: &TestServer, rows: usize) -> String {
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (n INTEGER, body TEXT)")]).await;
    for i in 0..rows {
        server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i}, 'row {i} of the same text')"))]).await;
    }
    app
}

#[tokio::test]
async fn snapshots_and_wal_segments_are_stored_compressed() {
    let (_dir, cfg) = store(Some(Duration::from_millis(50)));
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (n INTEGER, body TEXT)")]).await;
    for i in 0..200 {
        server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i}, 'row {i} of the same text')"))]).await;
    }
    server.settle_backups("app").await;
    let (status, manual) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{manual}");

    let stored = files(&cfg.dir);
    let snapshots = stored.iter().filter(|p| name(p).contains(".db")).collect::<Vec<_>>();
    let segments = stored.iter().filter(|p| name(p).contains(".wal")).collect::<Vec<_>>();
    assert!(!snapshots.is_empty() && !segments.is_empty(), "{stored:?}");
    assert!(stored.iter().all(|p| name(p).ends_with(".zst")), "everything is stored compressed: {stored:?}");
    // 200 commits of the same few pages: the frames repeat, and compress far.
    let wal_stored: u64 = segments.iter().map(|p| p.metadata().unwrap().len()).sum();
    let wal_plain: usize = segments.iter().map(|p| zstd::decode_all(std::fs::File::open(p).unwrap()).unwrap().len()).sum();
    assert!(wal_plain as u64 > 20 * wal_stored, "WAL {wal_plain} bytes stored as {wal_stored}");

    // The listing reports what is stored; a download is the database file itself.
    let (_, listing) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    let listed = &listing["backups"][0];
    assert_eq!(listed["id"], manual["id"]);
    assert_eq!(listed["size_bytes"], manual["size_bytes"]);
    let (status, file) = download(&server, &format!("/v1/databases/app/backups/{}", manual["id"].as_str().unwrap())).await;
    assert_eq!(status, StatusCode::OK);
    assert!(file.starts_with(b"SQLite format 3\0"));
    assert!(file.len() as u64 > manual["size_bytes"].as_u64().unwrap(), "the file is larger than what is stored");

    // Restoring replays compressed segments: to the last moment shipped, and from the manual snapshot.
    let latest = listing["point_in_time"]["latest_ms"].as_u64().unwrap();
    let (status, body) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "at", "restore": {"database": "app", "at": latest}}))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, "at").await, "200");
    let (status, body) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "copy", "restore": {"database": "app", "backup": manual["id"]}}))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, "copy").await, "200");
}

#[tokio::test]
async fn backups_stored_plain_by_earlier_servers_still_restore_and_prune() {
    let (_dir, cfg) = store(Some(Duration::from_millis(50)));
    let server = TestServer::with_backups(&cfg).await;
    filled(&server, 20).await;
    server.settle_backups("app").await;

    // Store everything as an earlier server did: plain, without `.zst`.
    for path in files(&cfg.dir) {
        let plain = zstd::decode_all(std::fs::File::open(&path).unwrap()).unwrap();
        std::fs::write(path.with_extension(""), plain).unwrap();
        std::fs::remove_file(&path).unwrap();
    }
    let (_, listing) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    let scheduled = listing["backups"][0].clone();
    assert_eq!(scheduled["kind"], "scheduled", "{listing}");
    let latest = listing["point_in_time"]["latest_ms"].as_u64().unwrap();
    let (status, body) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "at", "restore": {"database": "app", "at": latest}}))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(count(&server, "at").await, "20");
    let (status, file) = download(&server, &format!("/v1/databases/app/backups/{}", scheduled["id"].as_str().unwrap())).await;
    assert_eq!(status, StatusCode::OK);
    assert!(file.starts_with(b"SQLite format 3\0"));

    // Deleting the database with its backups removes the plain objects too.
    assert_eq!(server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None).await.0, StatusCode::NO_CONTENT);
    for _ in 0..250 {
        if files(&cfg.dir.join("app")).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(files(&cfg.dir.join("app")).is_empty(), "{:?}", files(&cfg.dir));
}
