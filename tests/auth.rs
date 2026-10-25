mod common;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

/// Minted outside this codebase (Python's hmac module) for database `notes`,
/// expiring in 2100, signed with `SECRET`. It pins the token format that the
/// AOZUMI control plane implements on its own.
const NOTES_TOKEN_FROM_PYTHON: &str =
    "adb1.eyJkYiI6Im5vdGVzIiwiZXhwIjo0MTAyNDQ0ODAwfQ.960Y4tkp2brGjhAEFMOsTQyTJjw77Hedx3nYTcB8N0Y";

async fn admin(server: &TestServer, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    server.request(method, path, Some(SECRET), body).await
}

async fn create(server: &TestServer, name: &str) -> StatusCode {
    admin(server, Method::POST, "/v1/databases", Some(json!({"name": name}))).await.0
}

async fn token(server: &TestServer, name: &str, ttl: Option<i64>) -> String {
    let body = match ttl {
        Some(ttl) => json!({"ttl_seconds": ttl}),
        None => json!({}),
    };
    let (status, body) = admin(server, Method::POST, &format!("/v1/databases/{name}/tokens"), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["token"].as_str().unwrap().to_string()
}

fn select_close(sql: &str) -> Value {
    json!({"baton": null, "requests": [execute(sql), {"type": "close"}]})
}

#[tokio::test]
async fn pipelines_and_admin_calls_need_credentials() {
    let server = TestServer::with_secret().await;

    let (status, _) = server.pipeline(None, select_close("SELECT 1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = server.request(Method::GET, "/v1/databases", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = server.request(Method::GET, "/v1/databases", Some("wrong"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A database token is not an admin credential.
    create(&server, "notes").await;
    let token = token(&server, "notes", None).await;
    let (status, _) = server.request(Method::GET, "/v1/databases", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_creates_lists_and_deletes_databases() {
    let server = TestServer::with_secret().await;

    assert_eq!(create(&server, "notes").await, StatusCode::CREATED);
    assert_eq!(create(&server, "notes").await, StatusCode::CONFLICT);
    for bad in ["Notes", "../etc", "-x", "", &"a".repeat(64)] {
        assert_eq!(create(&server, bad).await, StatusCode::BAD_REQUEST, "{bad:?}");
    }
    assert_eq!(create(&server, "app-2").await, StatusCode::CREATED);

    let (status, list) = admin(&server, Method::GET, "/v1/databases", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list, json!({"databases": [{"name": "app-2"}, {"name": "notes"}]}));

    let (status, _) = admin(&server, Method::DELETE, "/v1/databases/notes", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = admin(&server, Method::DELETE, "/v1/databases/notes", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, list) = admin(&server, Method::GET, "/v1/databases", None).await;
    assert_eq!(list, json!({"databases": [{"name": "app-2"}]}));
}

#[tokio::test]
async fn a_token_reaches_only_its_own_database() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;
    create(&server, "other").await;
    let notes = token(&server, "notes", Some(3600)).await;
    let other = token(&server, "other", None).await;

    let (status, _) = server.pipeline(Some(&notes), select_close("CREATE TABLE secret (x)")).await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = server.pipeline(Some(&other), select_close("SELECT * FROM secret")).await;
    assert_eq!(body["results"][0]["type"], "error", "{body}");

    let (_, body) = server.pipeline(Some(&notes), select_close("SELECT count(*) FROM secret")).await;
    assert_eq!(body["results"][0]["type"], "ok", "{body}");

    // A baton from one database cannot be used with another database's token.
    let (_, open) = server.pipeline(Some(&notes), json!({"baton": null, "requests": []})).await;
    let baton = open["baton"].as_str().unwrap();
    let (status, _) = server.pipeline(Some(&other), json!({"baton": baton, "requests": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_token_minted_elsewhere_with_the_same_secret_works() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;

    let (status, body) = server.pipeline(Some(NOTES_TOKEN_FROM_PYTHON), select_close("SELECT 1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["type"], "ok");
}

#[tokio::test]
async fn expired_tampered_and_orphaned_tokens_are_refused() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;

    let expired = token(&server, "notes", Some(0)).await;
    let (status, _) = server.pipeline(Some(&expired), select_close("SELECT 1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Claims rewritten to another database, signature kept.
    let mut parts = NOTES_TOKEN_FROM_PYTHON.split('.').collect::<Vec<_>>();
    parts[1] = "eyJkYiI6Im90aGVyIiwiZXhwIjo0MTAyNDQ0ODAwfQ";
    let (status, _) = server.pipeline(Some(&parts.join(".")), select_close("SELECT 1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A valid token for a database that was deleted must not recreate it.
    admin(&server, Method::DELETE, "/v1/databases/notes", None).await;
    let (status, _) = server.pipeline(Some(NOTES_TOKEN_FROM_PYTHON), select_close("SELECT 1")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, list) = admin(&server, Method::GET, "/v1/databases", None).await;
    assert_eq!(list, json!({"databases": []}));
}
