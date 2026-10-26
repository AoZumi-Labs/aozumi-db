mod common;

use std::time::Duration;

use common::*;
use hyper::StatusCode;
use serde_json::{json, Value};

fn count(results: &[Value]) -> Value {
    results[0]["response"]["result"]["rows"][0][0]["value"].clone()
}

async fn with_table(server: &TestServer) {
    server.run(None, vec![execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")]).await;
}

#[tokio::test]
async fn a_baton_carries_a_transaction_across_requests() {
    let server = TestServer::dev().await;
    with_table(&server).await;

    let (status, first) = server
        .pipeline(None, json!({"baton": null, "requests": [execute("BEGIN"), execute("INSERT INTO t VALUES (1)")]}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let baton = first["baton"].as_str().expect("an open stream returns a baton").to_string();

    // Other connections do not see the uncommitted row.
    assert_eq!(count(&server.run(None, vec![execute("SELECT count(*) FROM t")]).await), "0");

    let (status, second) = server
        .pipeline(None, json!({"baton": baton, "requests": [
            {"type": "get_autocommit"},
            execute("COMMIT"),
            {"type": "get_autocommit"},
            {"type": "close"}
        ]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["results"][0]["response"], json!({"type": "get_autocommit", "is_autocommit": false}));
    assert_eq!(second["results"][2]["response"], json!({"type": "get_autocommit", "is_autocommit": true}));
    assert_eq!(second["baton"], Value::Null, "a closed stream returns no baton");

    assert_eq!(count(&server.run(None, vec![execute("SELECT count(*) FROM t")]).await), "1");
}

#[tokio::test]
async fn batons_are_single_use_and_unknown_ones_are_rejected() {
    let server = TestServer::dev().await;

    let (_, first) = server.pipeline(None, json!({"baton": null, "requests": [execute("SELECT 1")]})).await;
    let baton = first["baton"].as_str().unwrap().to_string();

    let (status, second) = server.pipeline(None, json!({"baton": baton, "requests": [execute("SELECT 1")]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(second["baton"].as_str().unwrap(), baton, "the baton rotates");

    let (status, _) = server.pipeline(None, json!({"baton": baton, "requests": [execute("SELECT 1")]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "an old baton is rejected");

    let (status, _) = server.pipeline(None, json!({"baton": "made-up", "requests": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_idle_stream_expires_and_its_transaction_rolls_back() {
    let server = TestServer::dev_with_idle_timeout(Duration::from_millis(200)).await;
    with_table(&server).await;

    let (_, first) = server
        .pipeline(None, json!({"baton": null, "requests": [execute("BEGIN"), execute("INSERT INTO t VALUES (1)")]}))
        .await;
    let baton = first["baton"].as_str().unwrap().to_string();

    tokio::time::sleep(Duration::from_millis(600)).await;

    let (status, _) = server.pipeline(None, json!({"baton": baton, "requests": [execute("COMMIT")]})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(count(&server.run(None, vec![execute("SELECT count(*) FROM t")]).await), "0");
}

#[tokio::test]
async fn a_batch_runs_steps_by_condition() {
    let server = TestServer::dev().await;
    with_table(&server).await;
    let step = |sql: &str, condition: Value| json!({"condition": condition, "stmt": {"sql": sql}});

    // What @libsql/client sends for client.batch(): stop at the first failure
    // and roll back.
    let results = server
        .run(None, vec![json!({"type": "batch", "batch": {"steps": [
            step("BEGIN", Value::Null),
            step("INSERT INTO t VALUES (1)", json!({"type": "ok", "step": 0})),
            step("INSERT INTO t VALUES (1)", json!({"type": "ok", "step": 1})),
            step("COMMIT", json!({"type": "ok", "step": 2})),
            step("ROLLBACK", json!({"type": "and", "conds": [
                {"type": "not", "cond": {"type": "ok", "step": 3}},
                {"type": "not", "cond": {"type": "is_autocommit"}}
            ]})),
        ]}})])
        .await;

    let result = &results[0]["response"]["result"];
    let ran = |list: &str| result[list].as_array().unwrap().iter().map(|v| !v.is_null()).collect::<Vec<_>>();
    assert_eq!(ran("step_results"), [true, true, false, false, true]);
    assert_eq!(ran("step_errors"), [false, false, true, false, false]);
    assert_eq!(result["step_errors"][2]["code"], "SQLITE_CONSTRAINT");

    assert_eq!(count(&server.run(None, vec![execute("SELECT count(*) FROM t")]).await), "0");
}

#[tokio::test]
async fn sequence_runs_several_statements() {
    let server = TestServer::dev().await;

    let results = server
        .run(None, vec![
            json!({"type": "sequence", "sql": "CREATE TABLE a (x); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2);"}),
            execute("SELECT count(*) FROM a"),
        ])
        .await;

    assert_eq!(results[0], json!({"type": "ok", "response": {"type": "sequence"}}));
    assert_eq!(count(&results[1..]), "2");
}

#[tokio::test]
async fn stored_sql_can_be_executed_by_id_until_closed() {
    let server = TestServer::dev().await;

    let results = server
        .run(None, vec![
            json!({"type": "store_sql", "sql_id": 7, "sql": "SELECT 42"}),
            json!({"type": "execute", "stmt": {"sql_id": 7}}),
            json!({"type": "close_sql", "sql_id": 7}),
            json!({"type": "execute", "stmt": {"sql_id": 7}}),
        ])
        .await;

    assert_eq!(results[0]["response"]["type"], "store_sql");
    assert_eq!(count(&results[1..]), "42");
    assert_eq!(results[2]["response"]["type"], "close_sql");
    assert_eq!(results[3]["type"], "error");
}

#[tokio::test]
async fn connection_settings_and_temp_tables_do_not_leak_between_streams() {
    let server = TestServer::dev().await;

    for _ in 0..3 {
        let results = server
            .run(None, vec![
                execute("PRAGMA foreign_keys"),
                execute("SELECT count(*) FROM temp.sqlite_schema"),
                execute("PRAGMA foreign_keys = ON"),
                execute("CREATE TEMP TABLE scratch (x)"),
            ])
            .await;
        assert_eq!(results[0]["response"]["result"]["rows"][0][0]["value"], "0", "a fresh stream has default settings");
        assert_eq!(results[1]["response"]["result"]["rows"][0][0]["value"], "0", "and no temporary tables");
        assert_eq!(results[3]["type"], "ok", "{}", results[3]);
    }
}

#[tokio::test]
async fn the_wal_stays_bounded_without_any_backups() {
    let server = TestServer::dev().await;
    server.run(None, vec![execute("CREATE TABLE t (x)")]).await;
    // One commit per row, about 2,000 WAL frames.
    let inserts = (0..2000).map(|i| execute(&format!("INSERT INTO t VALUES ({i})"))).collect();
    server.run(None, inserts).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let wal = std::fs::metadata(server.dir.path().join("default.db-wal")).map(|m| m.len()).unwrap_or(0);
    assert!(wal < 4 * 1024 * 1024, "the WAL was checkpointed ({wal} bytes)");
    assert_eq!(count(&server.run(None, vec![execute("SELECT count(*) FROM t")]).await), "2000");
}
