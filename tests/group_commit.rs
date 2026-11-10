//! Group commit (`synchronous = group`): writers share flushes, and every
//! answer, copy and branch still holds exactly what was committed.

mod common;

use aozumi_db::Synchronous;
use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

async fn group_server() -> TestServer {
    TestServer::start_with(Some(SECRET), |c| c.synchronous = Synchronous::Group).await
}

async fn count(server: &TestServer, token: &str) -> i64 {
    let results = server.run(Some(token), vec![execute("select count(*) from t")]).await;
    results[0]["response"]["result"]["rows"][0][0]["value"].as_str().unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_all_commit_and_each_sees_its_own_write() {
    let server = std::sync::Arc::new(group_server().await);
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (id integer primary key, writer integer, n integer)")]).await;

    let tasks = (0..16)
        .map(|writer| {
            let (server, token) = (server.clone(), token.clone());
            tokio::spawn(async move {
                for n in 0..20 {
                    let results = server
                        .run(Some(&token), vec![
                            execute_args("insert into t (writer, n) values (?, ?)", json!([{"type": "integer", "value": writer.to_string()}, {"type": "integer", "value": n.to_string()}])),
                            execute_args("select count(*) from t where writer = ?", json!([{"type": "integer", "value": writer.to_string()}])),
                        ])
                        .await;
                    assert_eq!(results[0]["type"], "ok", "{results:?}");
                    let seen: i64 = results[1]["response"]["result"]["rows"][0][0]["value"].as_str().unwrap().parse().unwrap();
                    assert_eq!(seen, n + 1, "a writer reads its own commits");
                }
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(count(&server, &token).await, 16 * 20);
}

#[tokio::test]
async fn an_interactive_transaction_commits_and_rolls_back() {
    let server = group_server().await;
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (x)")]).await;

    for (end, kept) in [("commit", 1), ("rollback", 1)] {
        let (status, body) = server.pipeline(Some(&token), json!({"baton": null, "requests": [execute("begin"), execute("insert into t values (1)")]})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let baton = body["baton"].clone();
        let (status, body) = server.pipeline(Some(&token), json!({"baton": baton, "requests": [execute(end), {"type": "close"}]})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(count(&server, &token).await, kept, "after {end}");
    }
}

#[tokio::test]
async fn a_branch_copies_every_acknowledged_commit() {
    let server = group_server().await;
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (x)")]).await;
    for _ in 0..10 {
        server.run(Some(&token), vec![execute("insert into t values (1)")]).await;
    }
    let (status, body) = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "copy", "branch_of": "app"}))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let branch = server.token("copy").await;
    assert_eq!(count(&server, &branch).await, 10);
}

#[tokio::test]
async fn the_console_answers_after_its_writes() {
    let server = group_server().await;
    let token = server.database("app").await;
    let (status, body): (StatusCode, Value) = server
        .request(Method::POST, "/v1/console/run", Some(&token), Some(json!({"sql": "create table t (x); insert into t values (1), (2); select count(*) from t"})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["statements"][2]["rows"][0][0]["value"], "2");
    assert_eq!(count(&server, &token).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_after_an_acknowledged_write_sees_it() {
    let server = std::sync::Arc::new(group_server().await);
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (x)")]).await;
    // Readers hammer the database while one client writes and reads back.
    let readers = (0..4)
        .map(|_| {
            let (server, token) = (server.clone(), token.clone());
            tokio::spawn(async move {
                for _ in 0..50 {
                    server.run(Some(&token), vec![execute("select count(*) from t")]).await;
                }
            })
        })
        .collect::<Vec<_>>();
    for n in 1..=50 {
        server.run(Some(&token), vec![execute("insert into t values (1)")]).await;
        assert_eq!(count(&server, &token).await, n, "a separate read request sees the acknowledged write");
    }
    for reader in readers {
        reader.await.unwrap();
    }
}

#[tokio::test]
async fn a_deferred_transaction_that_starts_by_reading_can_still_write() {
    let server = group_server().await;
    let token = server.database("app").await;
    server.run(Some(&token), vec![execute("create table t (x)"), execute("insert into t values (1)")]).await;
    let (status, body) = server.pipeline(Some(&token), json!({"baton": null, "requests": [execute("begin"), execute("select count(*) from t")]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let baton = body["baton"].clone();
    let (status, body) = server
        .pipeline(Some(&token), json!({"baton": baton, "requests": [execute("insert into t values (2)"), execute("commit"), {"type": "close"}]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["type"], "ok", "{body}");
    assert_eq!(count(&server, &token).await, 2);
}
