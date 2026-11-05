//! Downloading a backup (`GET /v1/databases/{name}/backups/{id}`), and
//! deleting a database with its backups (`DELETE …?backups=delete`).

mod common;

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};

fn store(wal: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().join("store"), keep: 20, every: None, wal };
    (dir, cfg)
}

/// An admin GET answered with raw bytes.
async fn download(server: &TestServer, path: &str) -> (StatusCode, HeaderMap, Bytes) {
    let client = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::get(format!("http://{}{path}", server.addr))
        .header("authorization", format!("Bearer {SECRET}"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    let (status, headers) = (resp.status(), resp.headers().clone());
    (status, headers, resp.into_body().collect().await.unwrap().to_bytes())
}

async fn snapshot(server: &TestServer, db: &str) -> Value {
    let (status, body) = server.admin(Method::POST, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

/// Every file under `dir`, at any depth.
fn files(dir: &Path) -> Vec<String> {
    let (mut dirs, mut found) = (vec![dir.to_path_buf()], Vec::new());
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                found.push(path.to_string_lossy().into_owned());
            }
        }
    }
    found
}

#[tokio::test]
async fn a_backup_downloads_as_a_database_file() {
    let (_dir, cfg) = store(None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES ('kept')")]).await;
    let backup = snapshot(&server, "app").await;
    let id = backup["id"].as_str().unwrap();

    for which in [id, "latest"] {
        let (status, headers, bytes) = download(&server, &format!("/v1/databases/app/backups/{which}")).await;
        assert_eq!(status, StatusCode::OK, "{which}");
        assert_eq!(headers["content-type"], "application/vnd.sqlite3");
        assert_eq!(headers["content-disposition"], format!("attachment; filename=\"app-{id}.db\""));
        assert_eq!(bytes.len() as u64, backup["size_bytes"].as_u64().unwrap());
        assert!(bytes.starts_with(b"SQLite format 3\0"), "a database file");
        assert!(bytes.windows(4).any(|w| w == b"kept"), "with the data in it");
    }

    assert_eq!(download(&server, "/v1/databases/app/backups/1").await.0, StatusCode::NOT_FOUND);
    assert_eq!(download(&server, "/v1/databases/app/backups/x.db").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(download(&server, "/v1/databases/..%2Fother/backups/latest").await.0, StatusCode::BAD_REQUEST);
    let client = Client::builder(TokioExecutor::new()).build_http();
    let unauthorized = Request::get(format!("http://{}/v1/databases/app/backups/latest", server.addr)).body(Full::new(Bytes::new())).unwrap();
    assert_eq!(client.request(unauthorized).await.unwrap().status(), StatusCode::UNAUTHORIZED);

    let plain = TestServer::with_secret().await;
    plain.database("app").await;
    assert_eq!(download(&plain, "/v1/databases/app/backups/latest").await.0, StatusCode::BAD_REQUEST, "backups are not configured");
}

#[tokio::test]
async fn deleting_with_its_backups_removes_every_backup_and_wal_segment() {
    let (_dir, cfg) = store(Some(Duration::from_millis(50)));
    let server = TestServer::with_backups(&cfg).await;
    for name in ["app", "other"] {
        let token = server.database(name).await;
        server.run(Some(&token), vec![execute("CREATE TABLE t (x)")]).await;
        for i in 0..3 {
            server.run(Some(&token), vec![execute(&format!("INSERT INTO t VALUES ({i})"))]).await;
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        snapshot(&server, name).await;
    }
    assert!(files(&cfg.dir.join("app")).iter().any(|f| f.ends_with(".wal")), "WAL was shipped: {:?}", files(&cfg.dir.join("app")));

    let (status, body) = server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    for _ in 0..250 {
        if files(&cfg.dir.join("app")).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(files(&cfg.dir.join("app")), Vec::<String>::new(), "every backup and WAL segment went");
    let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(listed["backups"], json!([]), "{listed}");

    // A plain delete keeps them, as before.
    assert_eq!(server.admin(Method::DELETE, "/v1/databases/other", None).await.0, StatusCode::NO_CONTENT);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_, listed) = server.admin(Method::GET, "/v1/databases/other/backups", None).await;
    assert!(!listed["backups"].as_array().unwrap().is_empty(), "{listed}");
    assert_eq!(
        server.admin(Method::DELETE, "/v1/databases/missing?backups=delete", None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(server.admin(Method::DELETE, "/v1/databases/other?backups=maybe", None).await.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_database_created_again_keeps_its_own_new_backups() {
    let (_dir, cfg) = store(None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let old = snapshot(&server, "app").await;
    assert_eq!(server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None).await.0, StatusCode::NO_CONTENT);

    // Created again at once, while the old backups may still be going.
    server.database("app").await;
    let new = snapshot(&server, "app").await;
    for _ in 0..250 {
        let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
        if !listed["backups"].as_array().unwrap().iter().any(|b| b["id"] == old["id"]) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(listed["backups"], json!([new]), "{listed}");
}
