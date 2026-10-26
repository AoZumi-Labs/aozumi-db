mod common;

use std::time::{Duration, Instant};

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

async fn create_with(server: &TestServer, name: &str, limits: Value) -> String {
    let (status, body) =
        server.admin(Method::POST, "/v1/databases", Some(json!({"name": name, "limits": limits}))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    server.token(name).await
}

async fn info(server: &TestServer, name: &str) -> Value {
    let (status, body) = server.admin(Method::GET, &format!("/v1/databases/{name}"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

fn insert_blob(bytes: usize) -> Value {
    execute(&format!("INSERT INTO t VALUES (randomblob({bytes}))"))
}

#[tokio::test]
async fn a_database_stops_growing_at_its_size_limit_until_the_limit_is_raised() {
    let server = TestServer::with_secret().await;
    let token = create_with(&server, "app", json!({"max_size_bytes": 64 * 1024})).await;
    server.run(Some(&token), vec![execute("CREATE TABLE t (b BLOB)")]).await;

    let results = server.run(Some(&token), (0..40).map(|_| insert_blob(4000)).collect()).await;
    let first_failure = results.iter().position(|r| r["type"] == "error").expect("the limit is reached");
    assert!(first_failure > 0, "small writes fit");
    assert_eq!(results[first_failure]["error"]["code"], "SQLITE_FULL", "{}", results[first_failure]);

    let body = info(&server, "app").await;
    assert_eq!(body["limits"]["max_size_bytes"], 64 * 1024);
    assert!(body["size_bytes"].as_u64().unwrap() <= 64 * 1024, "{body}");

    let (status, body) = server
        .admin(Method::PATCH, "/v1/databases/app/limits", Some(json!({"max_size_bytes": 1024 * 1024})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["limits"]["max_size_bytes"], 1024 * 1024);
    let results = server.run(Some(&token), vec![insert_blob(4000), insert_blob(4000)]).await;
    assert!(results[..2].iter().all(|r| r["type"] == "ok"), "{results:?}");
}

#[tokio::test]
async fn a_runaway_statement_is_stopped_at_its_time_limit() {
    let server = TestServer::with_secret().await;
    let token = create_with(&server, "app", json!({"max_statement_ms": 100})).await;

    let started = Instant::now();
    let results = server
        .run(Some(&token), vec![
            execute("WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r) SELECT count(*) FROM r"),
            execute("SELECT 1"),
        ])
        .await;
    assert!(started.elapsed() < Duration::from_secs(3), "stopped promptly: {:?}", started.elapsed());
    assert_eq!(results[0]["error"]["code"], "SQLITE_INTERRUPT", "{}", results[0]);
    assert_eq!(results[1]["type"], "ok", "the stream keeps working");
}

#[tokio::test]
async fn requests_over_the_rate_limit_get_429_and_recover() {
    let server = TestServer::with_secret().await;
    let token = create_with(&server, "app", json!({"max_requests_per_second": 5})).await;
    let request = json!({"baton": null, "requests": [execute("SELECT 1"), {"type": "close"}]});

    let mut statuses = Vec::new();
    for _ in 0..20 {
        statuses.push(server.pipeline(Some(&token), request.clone()).await.0);
    }
    let ok = statuses.iter().filter(|s| **s == StatusCode::OK).count();
    assert!((5..=7).contains(&ok), "about one second's worth passes: {statuses:?}");
    assert!(statuses.contains(&StatusCode::TOO_MANY_REQUESTS));

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(server.pipeline(Some(&token), request).await.0, StatusCode::OK);
}

#[tokio::test]
async fn server_defaults_apply_to_new_databases_and_null_means_unlimited() {
    let server = TestServer::with_secret_and(|c| {
        c.default_limits = aozumi_db::Limits {
            max_size_bytes: Some(10 * 1024 * 1024),
            max_statement_ms: Some(5000),
            max_requests_per_second: None,
        };
    })
    .await;

    server.database("plain").await;
    assert_eq!(
        info(&server, "plain").await["limits"],
        json!({"max_size_bytes": 10485760, "max_statement_ms": 5000, "max_requests_per_second": null})
    );

    create_with(&server, "custom", json!({"max_size_bytes": null, "max_requests_per_second": 50})).await;
    assert_eq!(
        info(&server, "custom").await["limits"],
        json!({"max_size_bytes": null, "max_statement_ms": 5000, "max_requests_per_second": 50.0})
    );

    let (status, _) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "bad", "limits": {"max_size_bytes": 0}})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = server.admin(Method::GET, "/v1/databases/missing", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
