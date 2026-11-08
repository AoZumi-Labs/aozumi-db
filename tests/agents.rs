//! What makes the server easy for an agent to use safely: a branch of a live
//! database to try changes on, branches that expire on their own, a dry run
//! that shows what a script would change and keeps nothing, and the schema as
//! compact SQL.

mod common;

use std::time::Duration;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

async fn console(server: &TestServer, token: &str, body: Value) -> (StatusCode, Value) {
    server.request(Method::POST, "/v1/console/run", Some(token), Some(body)).await
}

async fn count(server: &TestServer, token: &str, table: &str) -> i64 {
    let results = server.run(Some(token), vec![execute(&format!("select count(*) from {table}"))]).await;
    results[0]["response"]["result"]["rows"][0][0]["value"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_branch_is_a_copy_of_the_live_database_that_changes_on_its_own() {
    let server = TestServer::with_secret().await;
    let main = server.database("app").await;
    server
        .run(Some(&main), vec![
            execute("create table notes (id integer primary key, body text)"),
            execute("insert into notes (body) values ('one'), ('two')"),
        ])
        .await;

    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "app-try", "branch_of": "app"})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["branch_of"], "app");
    assert!(body["branched_at_ms"].as_u64().unwrap() > 0);

    let branch = server.token("app-try").await;
    assert_eq!(count(&server, &branch, "notes").await, 2, "the branch starts with the source's rows");
    server.run(Some(&branch), vec![execute("delete from notes")]).await;
    assert_eq!(count(&server, &branch, "notes").await, 0);
    assert_eq!(count(&server, &main, "notes").await, 2, "the source is not changed by the branch");
    server.run(Some(&main), vec![execute("insert into notes (body) values ('three')")]).await;
    assert_eq!(count(&server, &branch, "notes").await, 0, "nor the branch by the source");
}

#[tokio::test]
async fn a_branch_keeps_its_sources_limits_and_refuses_bad_requests() {
    let server = TestServer::with_secret().await;
    let (status, _) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "src", "limits": {"max_statement_ms": 1234}})))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "b", "branch_of": "src"}))).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, info) = server.admin(Method::GET, "/v1/databases/b", None).await;
    assert_eq!(info["limits"]["max_statement_ms"], 1234);

    let (status, _) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "c", "branch_of": "missing"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "b", "branch_of": "src"}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "the name is taken");
    let (status, _) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "d", "branch_of": "src", "restore": {"database": "src", "backup": "latest"}})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "e", "expires_in_seconds": 0}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_database_with_an_expiry_is_deleted_when_it_passes() {
    let server = TestServer::start_with(Some(SECRET), |c| c.expiry_every = Duration::from_millis(200)).await;
    server.database("keep").await;
    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "brief", "branch_of": "keep", "expires_in_seconds": 1})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let expires_ms = body["expires_ms"].as_u64().unwrap();
    let (_, info) = server.admin(Method::GET, "/v1/databases/brief", None).await;
    assert_eq!(info["expires_ms"], expires_ms);

    let mut gone = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (status, _) = server.admin(Method::GET, "/v1/databases/brief", None).await;
        if status == StatusCode::NOT_FOUND {
            gone = true;
            break;
        }
    }
    assert!(gone, "the expired database was deleted");
    let (status, info) = server.admin(Method::GET, "/v1/databases/keep", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["expires_ms"], Value::Null, "a database without an expiry stays");
}

#[tokio::test]
async fn a_dry_run_reports_what_would_change_and_keeps_nothing() {
    let server = TestServer::with_secret().await;
    let token = server.database("app").await;
    server
        .run(Some(&token), vec![
            execute("create table notes (id integer primary key, body text)"),
            execute("insert into notes (body) values ('a'), ('b'), ('c')"),
        ])
        .await;

    // Destructive, yet no confirmation is asked: nothing is kept.
    let (status, body) = console(&server, &token, json!({
        "sql": "delete from notes where id > 1; update notes set body = 'x'; create table extra (id integer); insert into extra values (1); select count(*) from notes",
        "dry_run": true,
    }))
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["dryRun"], true);
    let statements = body["statements"].as_array().unwrap();
    assert_eq!(statements[0]["rowsAffected"], 2);
    assert_eq!(statements[1]["rowsAffected"], 1);
    assert_eq!(statements[4]["rows"][0][0]["value"], "1", "later statements see earlier ones");

    assert_eq!(count(&server, &token, "notes").await, 3, "every row is still there");
    let results = server.run(Some(&token), vec![execute("select count(*) from sqlite_schema where name = 'extra'")]).await;
    assert_eq!(results[0]["response"]["result"]["rows"][0][0]["value"], "0", "the table created in the dry run is gone");

    // A plain run still asks first.
    let (status, _) = console(&server, &token, json!({"sql": "delete from notes"})).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_dry_run_refuses_what_could_escape_it() {
    let server = TestServer::with_secret().await;
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (x)")]).await;
    for sql in ["commit", "begin; insert into t values (1); commit", "rollback", "savepoint s", "release s", "vacuum", "pragma user_version = 5"] {
        let (status, body) = console(&server, &token, json!({"sql": sql, "dry_run": true})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sql}: {body}");
        assert_eq!(body["code"], "dry_run_unsupported", "{sql}");
    }
    assert_eq!(count(&server, &token, "t").await, 0);
    let (status, body) = console(&server, &token, json!({"sql": "pragma table_info(t)", "dry_run": true})).await;
    assert_eq!(status, StatusCode::OK, "reading pragmas are fine: {body}");
}

#[tokio::test]
async fn the_compact_schema_is_sql_with_row_counts() {
    let server = TestServer::with_secret().await;
    let token = server.database("app").await;
    server
        .run(Some(&token), vec![
            execute("create table users (id integer primary key autoincrement, email text not null unique)"),
            execute("create table posts (id integer primary key, user_id integer references users(id), title text default 'untitled')"),
            execute("create index posts_user on posts(user_id)"),
            execute("insert into users (email) values ('a@x'), ('b@x')"),
            execute("insert into posts (user_id) values (1)"),
        ])
        .await;
    let (status, body) = server.request(Method::GET, "/v1/console/schema?format=sql", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let sql = body["sql"].as_str().unwrap();
    assert!(sql.contains("CREATE TABLE users"), "{sql}");
    assert!(sql.contains("-- 2 rows"), "{sql}");
    assert!(sql.contains("-- 1 row\n"), "{sql}");
    assert!(sql.contains("CREATE INDEX posts_user"), "{sql}");
    assert!(sql.contains("DEFAULT 'untitled'"), "defaults are kept: {sql}");
    assert!(!sql.contains("sqlite_sequence"), "SQLite's own tables are left out: {sql}");
    let posts = sql.find("CREATE TABLE posts").unwrap();
    assert!(sql.find("CREATE INDEX posts_user").unwrap() > posts, "an index follows its table");

    // The JSON schema is unchanged.
    let (status, body) = server.request(Method::GET, "/v1/console/schema", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["tables"].is_array());
}
