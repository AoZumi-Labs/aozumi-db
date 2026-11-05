//! A database's backup policy: how long its backups are kept, and how often
//! and how many backups may be asked for (`PATCH /v1/databases/{name}/backups`).

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

const HOUR_MS: u64 = 3_600_000;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn store(keep: usize, every: Option<Duration>, wal: Option<Duration>) -> (tempfile::TempDir, Backups) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Backups { dir: dir.path().join("store"), keep, every, wal };
    (dir, cfg)
}

async fn policy(server: &TestServer, db: &str, patch: Value) -> (StatusCode, Value) {
    server.admin(Method::PATCH, &format!("/v1/databases/{db}/backups"), Some(patch)).await
}

async fn snapshot(server: &TestServer, db: &str) -> (StatusCode, Value) {
    server.admin(Method::POST, &format!("/v1/databases/{db}/backups"), None).await
}

async fn listing(server: &TestServer, db: &str) -> Value {
    let (status, body) = server.admin(Method::GET, &format!("/v1/databases/{db}/backups"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// `(id, kind)` of each listed backup, newest first.
async fn kept(server: &TestServer, db: &str) -> Vec<(String, String)> {
    listing(server, db).await["backups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (b["id"].as_str().unwrap().to_string(), b["kind"].as_str().unwrap().to_string()))
        .collect()
}

/// A copy of a real backup in the store, as the schedule would have taken
/// it `ago_ms` ago.
fn plant(store: &Path, db: &str, from: &Path, ago_ms: u64) -> String {
    let id = format!("{:013}", now_ms() - ago_ms);
    std::fs::copy(from, store.join(db).join(format!("{id}.db"))).unwrap();
    id
}

fn file_of(store: &Path, db: &str, id: &str) -> PathBuf {
    let dir = store.join(db);
    std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.file_name().unwrap().to_string_lossy().starts_with(&format!("{id}.")))
        .unwrap_or_else(|| panic!("no file for {id} in {}", dir.display()))
}

fn ids(of: &[(String, String)]) -> Vec<&str> {
    of.iter().map(|(id, _)| id.as_str()).collect()
}

#[tokio::test]
async fn backups_are_kept_for_the_retention_window_and_one_from_before_it() {
    // Without a policy the server keeps 2 backups a database.
    let (_dir, cfg) = store(2, None, None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let (status, body) = policy(&server, "app", json!({"retention_hours": 1})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["policy"]["retention_hours"], 1, "{body}");

    let (_, first) = snapshot(&server, "app").await;
    let real = file_of(&cfg.dir, "app", first["id"].as_str().unwrap());
    let three_hours = plant(&cfg.dir, "app", &real, 3 * HOUR_MS);
    let two_hours = plant(&cfg.dir, "app", &real, 2 * HOUR_MS);
    let half_hour = plant(&cfg.dir, "app", &real, HOUR_MS / 2);
    let ten_minutes = plant(&cfg.dir, "app", &real, HOUR_MS / 6);

    // A new backup prunes: by age now, not by count.
    let (status, newest) = snapshot(&server, "app").await;
    assert_eq!(status, StatusCode::CREATED, "{newest}");
    let left = kept(&server, "app").await;
    assert_eq!(
        ids(&left),
        [newest["id"].as_str().unwrap(), first["id"].as_str().unwrap(), &ten_minutes, &half_hour, &two_hours],
        "everything inside the hour, and the newest from before it, where restoring to the hour's start begins"
    );
    assert!(!ids(&left).contains(&three_hours.as_str()));

    // The one from before the window still restores.
    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "copy", "restore": {"database": "app", "backup": two_hours}})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn an_unchanged_database_is_pruned_on_schedule_but_keeps_its_newest_backup() {
    let (_dir, cfg) = store(10, Some(Duration::from_millis(100)), None);
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)")]).await;
    assert_eq!(policy(&server, "app", json!({"retention_hours": 1})).await.0, StatusCode::OK);
    // Let the schedule back it up, then stop changing it.
    server.settle_backups("app").await;
    let scheduled = kept(&server, "app").await;
    let real = file_of(&cfg.dir, "app", &scheduled[0].0);

    let old = plant(&cfg.dir, "app", &real, 5 * HOUR_MS);
    let older = plant(&cfg.dir, "app", &real, 6 * HOUR_MS);
    // The next tick finds the database unchanged, then prunes; the tick
    // after it starts once that is done.
    let next = server.backed_up_past("app", now_ms()).await;
    server.backed_up_past("app", next).await;
    let left = kept(&server, "app").await;
    assert!(!ids(&left).contains(&older.as_str()), "pruned without a new backup: {left:?}");
    assert!(ids(&left).contains(&old.as_str()), "the newest from before the window stays: {left:?}");
    assert!(scheduled.iter().all(|b| left.contains(b)), "recent ones stay: {left:?}");
}

#[tokio::test]
async fn backups_asked_for_are_spaced_out_and_capped() {
    let (_dir, cfg) = store(50, None, None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let (status, body) = policy(&server, "app", json!({"manual_every_seconds": 3600, "manual_keep": 2})).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert_eq!(snapshot(&server, "app").await.0, StatusCode::CREATED);
    let (status, body) = snapshot(&server, "app").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(body.as_str().unwrap().contains("try again in"), "{body}");

    // Lifting the spacing; only the newest two asked-for backups stay.
    assert_eq!(policy(&server, "app", json!({"manual_every_seconds": null})).await.0, StatusCode::OK);
    let mut taken = Vec::new();
    for _ in 0..3 {
        let (status, body) = snapshot(&server, "app").await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        taken.push(body["id"].as_str().unwrap().to_string());
    }
    let left = kept(&server, "app").await;
    assert_eq!(ids(&left), [taken[2].as_str(), taken[1].as_str()], "{left:?}");
    assert!(left.iter().all(|(_, kind)| kind == "manual"));
}

#[tokio::test]
async fn the_cap_on_asked_for_backups_leaves_scheduled_ones_alone() {
    let (_dir, cfg) = store(50, None, None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let (_, first) = snapshot(&server, "app").await;
    let real = file_of(&cfg.dir, "app", first["id"].as_str().unwrap());
    let scheduled = plant(&cfg.dir, "app", &real, 60_000);
    assert_eq!(policy(&server, "app", json!({"manual_keep": 1})).await.0, StatusCode::OK);

    let (_, second) = snapshot(&server, "app").await;
    let left = kept(&server, "app").await;
    assert_eq!(ids(&left), [second["id"].as_str().unwrap(), scheduled.as_str()], "{left:?}");
}

#[tokio::test]
async fn the_policy_is_reported_checked_and_goes_with_its_database() {
    let (_dir, cfg) = store(10, None, None);
    let server = TestServer::with_backups(&cfg).await;
    server.database("app").await;
    let unset = json!({"retention_hours": null, "manual_every_seconds": null, "manual_keep": null});
    assert_eq!(listing(&server, "app").await["status"]["policy"], unset);

    let set = json!({"retention_hours": 168, "manual_every_seconds": 60, "manual_keep": 10});
    let (status, body) = policy(&server, "app", set.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"name": "app", "policy": set}));
    assert_eq!(listing(&server, "app").await["status"]["policy"], set);
    // A field left out keeps its value.
    let (_, body) = policy(&server, "app", json!({"manual_keep": 3})).await;
    assert_eq!(body["policy"], json!({"retention_hours": 168, "manual_every_seconds": 60, "manual_keep": 3}));

    for bad in [json!({"retention_hours": 0}), json!({"manual_keep": 0}), json!({"keep_forever": true})] {
        let (status, body) = policy(&server, "app", bad.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
    }
    assert_eq!(policy(&server, "missing", json!({"retention_hours": 1})).await.0, StatusCode::NOT_FOUND);

    // The policy is the database's: a database created again under the name starts without it.
    assert_eq!(server.admin(Method::DELETE, "/v1/databases/app", None).await.0, StatusCode::NO_CONTENT);
    server.database("app").await;
    assert_eq!(listing(&server, "app").await["status"]["policy"], unset);
}

#[tokio::test]
async fn backups_asked_for_do_not_split_the_point_in_time_history() {
    let (_dir, cfg) = store(10, None, Some(Duration::from_millis(50)));
    let server = TestServer::with_backups(&cfg).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    // WAL shipping starts its generation with a snapshot and ships the writes.
    server.backed_up_past("app", now_ms()).await;
    let (_, manual) = snapshot(&server, "app").await;
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;
    server.backed_up_past("app", now_ms()).await;

    // The schedule's generation carries on through the asked-for backup.
    let generations = std::fs::read_dir(cfg.dir.join("app").join("wal")).unwrap().count();
    assert_eq!(generations, 1, "no WAL generation starts at an asked-for backup");
    assert!(!cfg.dir.join("app").join("wal").join(manual["id"].as_str().unwrap()).exists());

    // Dropping the asked-for backup loses no moment of history.
    assert_eq!(policy(&server, "app", json!({"manual_keep": 1})).await.0, StatusCode::OK);
    let (_, second) = snapshot(&server, "app").await;
    assert!(!kept(&server, "app").await.iter().any(|(id, _)| id == manual["id"].as_str().unwrap()));
    let at = second["taken_ms"].as_u64().unwrap();
    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "at", "restore": {"database": "app", "at": at}})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let token = server.token("at").await;
    assert_eq!(scalar(&server.run(Some(&token), vec![execute("SELECT count(*) FROM t")]).await), "2");
}

#[tokio::test]
async fn a_server_wide_default_retention_covers_databases_without_their_own() {
    let dir = tempfile::tempdir().unwrap();
    let backup = aozumi_db::BackupConfig {
        target: aozumi_db::BackupTarget::directory(dir.path().join("store")),
        keep: 1,
        every: None,
        wal_every: None,
    };
    let server = TestServer::with_secret_and(move |c| {
        c.backup = Some(backup);
        c.default_backup_policy = aozumi_db::BackupPolicy { retention_hours: Some(1), ..Default::default() };
    })
    .await;
    server.database("app").await;
    assert_eq!(listing(&server, "app").await["status"]["policy"]["retention_hours"], 1, "the default, reported as the database's");

    // Kept by age, not by the server's count of 1.
    let (_, first) = snapshot(&server, "app").await;
    let (_, second) = snapshot(&server, "app").await;
    assert_eq!(ids(&kept(&server, "app").await), [second["id"].as_str().unwrap(), first["id"].as_str().unwrap()]);

    // A database's own policy wins, and keeps the default's other values.
    let (_, body) = policy(&server, "app", json!({"manual_keep": 5})).await;
    assert_eq!(body["policy"], json!({"retention_hours": 1, "manual_every_seconds": null, "manual_keep": 5}));
    let (_, body) = policy(&server, "app", json!({"retention_hours": 72})).await;
    assert_eq!(body["policy"]["retention_hours"], 72);
}
