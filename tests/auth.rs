mod common;

use aozumi_db::{mint_token, Claims};
use base64::Engine;
use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

/// Minted outside this codebase (Python's hmac module) for database `notes`,
/// expiring in 2100, signed with `SECRET`. It pins the token format that the
/// AOZUMI control plane implements on its own.
const NOTES_TOKEN_FROM_PYTHON: &str =
    "adb1.eyJkYiI6Im5vdGVzIiwiZXhwIjo0MTAyNDQ0ODAwfQ.960Y4tkp2brGjhAEFMOsTQyTJjw77Hedx3nYTcB8N0Y";

/// The same, at token generation 1: `{"db":"notes","gen":1,"exp":4102444800}`.
const NOTES_GENERATION_1_TOKEN_FROM_PYTHON: &str =
    "adb1.eyJkYiI6Im5vdGVzIiwiZ2VuIjoxLCJleHAiOjQxMDI0NDQ4MDB9.XDION2F-8UHRQCzmSnKAyMj4KXYtP4zerZKP3dh-lNw";

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

async fn rotate(server: &TestServer, name: &str) -> (StatusCode, Value) {
    admin(server, Method::POST, &format!("/v1/databases/{name}/tokens/rotate"), None).await
}

fn select_close(sql: &str) -> Value {
    json!({"baton": null, "requests": [execute(sql), {"type": "close"}]})
}

async fn status_with(server: &TestServer, token: &str) -> StatusCode {
    server.pipeline(Some(token), select_close("SELECT 1")).await.0
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

#[tokio::test]
async fn rotating_revokes_every_older_token() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;
    let first = token(&server, "notes", None).await;
    let (_, read_only) = admin(&server, Method::POST, "/v1/databases/notes/tokens", Some(json!({"read_only": true}))).await;
    let read_only = read_only["token"].as_str().unwrap().to_string();
    assert_eq!(status_with(&server, &first).await, StatusCode::OK);

    let (status, body) = rotate(&server, "notes").await;
    assert_eq!((status, body), (StatusCode::OK, json!({"name": "notes", "token_generation": 1})));
    for old in [&first, &read_only] {
        let (status, body) = server.pipeline(Some(old), select_close("SELECT 1")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    }
    let (status, _) = server.request(Method::GET, "/v1/console/schema", Some(&first), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "the console checks the generation too");

    let second = token(&server, "notes", None).await;
    assert_ne!(second, first);
    assert_eq!(status_with(&server, &second).await, StatusCode::OK);

    // Each rotation revokes the generation before it.
    assert_eq!(rotate(&server, "notes").await.1["token_generation"], 2);
    assert_eq!(status_with(&server, &second).await, StatusCode::UNAUTHORIZED);
    assert_eq!(status_with(&server, &token(&server, "notes", None).await).await, StatusCode::OK);
    let (_, info) = admin(&server, Method::GET, "/v1/databases/notes", None).await;
    assert_eq!(info["token_generation"], 2);
}

#[tokio::test]
async fn tokens_without_a_generation_work_until_the_first_rotation() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;

    // Generation 0 mints the same tokens as before generations existed.
    let minted = token(&server, "notes", None).await;
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(minted.split('.').nth(1).unwrap()).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&claims).unwrap(), json!({"db": "notes"}));
    assert_eq!(status_with(&server, NOTES_TOKEN_FROM_PYTHON).await, StatusCode::OK);

    rotate(&server, "notes").await;
    assert_eq!(status_with(&server, NOTES_TOKEN_FROM_PYTHON).await, StatusCode::UNAUTHORIZED);
    assert_eq!(status_with(&server, &minted).await, StatusCode::UNAUTHORIZED);

    // A token minted elsewhere at the new generation works: the claim is `gen`.
    assert_eq!(status_with(&server, NOTES_GENERATION_1_TOKEN_FROM_PYTHON).await, StatusCode::OK);
    let claims = Claims { db: "notes".into(), generation: 1, exp: Some(4102444800), ..Default::default() };
    assert_eq!(mint_token(SECRET.as_bytes(), &claims), NOTES_GENERATION_1_TOKEN_FROM_PYTHON);
}

#[tokio::test]
async fn rotating_drops_the_databases_open_streams() {
    let server = TestServer::with_secret().await;
    let old = server.database("notes").await;
    let other = server.database("other").await;
    server.run(Some(&old), vec![execute("CREATE TABLE t (x)")]).await;

    // A transaction left open on a stream of each database.
    let begin = json!({"baton": null, "requests": [execute("BEGIN"), execute("INSERT INTO t VALUES (1)")]});
    let (_, open) = server.pipeline(Some(&old), begin).await;
    let baton = open["baton"].as_str().unwrap().to_string();
    let (_, other_open) = server.pipeline(Some(&other), json!({"baton": null, "requests": [execute("CREATE TABLE u (x)")]})).await;
    let other_baton = other_open["baton"].as_str().unwrap().to_string();

    rotate(&server, "notes").await;
    let new = server.token("notes").await;

    // The stream is gone, even for a current token, and its write was rolled back.
    let (status, _) = server.pipeline(Some(&new), json!({"baton": baton, "requests": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let results = server.run(Some(&new), vec![execute("SELECT count(*) FROM t"), execute("INSERT INTO t VALUES (2)")]).await;
    assert_eq!(scalar(&results), "0");
    assert_eq!(results[1]["type"], "ok", "writes are not stuck behind the dropped transaction: {}", results[1]);

    // Another database's stream carries on.
    let (status, body) = server.pipeline(Some(&other), json!({"baton": other_baton, "requests": [{"type": "close"}]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn revoked_tokens_do_not_use_up_the_request_rate() {
    let server = TestServer::with_secret().await;
    let limits = json!({"name": "notes", "limits": {"max_requests_per_second": 1}});
    assert_eq!(admin(&server, Method::POST, "/v1/databases", Some(limits)).await.0, StatusCode::CREATED);
    let old = token(&server, "notes", None).await;
    rotate(&server, "notes").await;
    for _ in 0..5 {
        assert_eq!(status_with(&server, &old).await, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(status_with(&server, &token(&server, "notes", None).await).await, StatusCode::OK);
}

#[tokio::test]
async fn only_the_admin_rotates_and_only_existing_databases() {
    let server = TestServer::with_secret().await;
    let token = server.database("notes").await;
    let (status, _) = server.request(Method::POST, "/v1/databases/notes/tokens/rotate", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a database token cannot rotate");
    assert_eq!(rotate(&server, "missing").await.0, StatusCode::NOT_FOUND);
    assert_eq!(rotate(&server, "Notes").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(status_with(&server, &token).await, StatusCode::OK, "nothing was rotated");

    let dev = TestServer::dev().await;
    let (status, _) = dev.request(Method::POST, "/v1/databases/default/tokens/rotate", None, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "dev mode has no tokens");
}

#[tokio::test]
async fn a_rotation_outlives_deleting_the_database() {
    let server = TestServer::with_secret().await;
    create(&server, "notes").await;
    rotate(&server, "notes").await;
    let current = token(&server, "notes", None).await;

    // Deleted and created again under the same name, as an in-place restore does.
    assert_eq!(admin(&server, Method::DELETE, "/v1/databases/notes", None).await.0, StatusCode::NO_CONTENT);
    assert_eq!(create(&server, "notes").await, StatusCode::CREATED);
    assert_eq!(status_with(&server, NOTES_TOKEN_FROM_PYTHON).await, StatusCode::UNAUTHORIZED, "revoked stays revoked");
    assert_eq!(status_with(&server, &current).await, StatusCode::OK, "the current generation carries over");

    // `aozumi-db token` reads the generation the same way, from the data directory.
    let generation = aozumi_db::token_generation(server.dir.path(), "notes").unwrap();
    assert_eq!(generation, 1);
    let offline = mint_token(SECRET.as_bytes(), &Claims { db: "notes".into(), generation, ..Default::default() });
    assert_eq!(status_with(&server, &offline).await, StatusCode::OK);
    assert!(aozumi_db::token_generation(server.dir.path(), "../notes").is_err());
}
