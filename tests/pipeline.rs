mod common;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn select_one_returns_a_row_over_http() {
    let server = TestServer::dev().await;

    let results = server.run(None, vec![execute("SELECT 1 AS one")]).await;

    assert_eq!(results[0]["type"], "ok");
    let result = &results[0]["response"]["result"];
    assert_eq!(results[0]["response"]["type"], "execute");
    assert_eq!(result["cols"], json!([{"name": "one", "decltype": null}]));
    assert_eq!(result["rows"], json!([[{"type": "integer", "value": "1"}]]));
    assert_eq!(result["affected_row_count"], 0);
    assert_eq!(results[1], json!({"type": "ok", "response": {"type": "close"}}));
}

#[tokio::test]
async fn every_value_type_round_trips() {
    let server = TestServer::dev().await;
    let args = json!([
        {"type": "null"},
        {"type": "integer", "value": "9007199254740993"},
        {"type": "float", "value": 1.5},
        {"type": "text", "value": "héllo"},
        {"type": "blob", "base64": "AAH/"}
    ]);

    let results = server.run(None, vec![execute_args("SELECT ?, ?, ?, ?, ?", args.clone())]).await;

    assert_eq!(results[0]["response"]["result"]["rows"], json!([args]));
}

#[tokio::test]
async fn writes_persist_and_report_changes() {
    let server = TestServer::dev().await;
    server
        .run(None, vec![execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")])
        .await;

    let results = server
        .run(None, vec![execute("INSERT INTO notes (body) VALUES ('a'), ('b')")])
        .await;
    let insert = &results[0]["response"]["result"];
    assert_eq!(insert["affected_row_count"], 2);
    assert_eq!(insert["last_insert_rowid"], "2");

    // A later pipeline is a new connection; the rows must still be there.
    let results = server.run(None, vec![execute("SELECT body FROM notes ORDER BY id")]).await;
    assert_eq!(
        results[0]["response"]["result"]["rows"],
        json!([[{"type": "text", "value": "a"}], [{"type": "text", "value": "b"}]])
    );
}

#[tokio::test]
async fn want_rows_false_omits_rows() {
    let server = TestServer::dev().await;
    let results = server
        .run(None, vec![json!({"type": "execute", "stmt": {"sql": "SELECT 1", "want_rows": false}})])
        .await;
    assert_eq!(results[0]["response"]["result"]["rows"], json!([]));
}

#[tokio::test]
async fn named_args_match_with_or_without_prefix() {
    let server = TestServer::dev().await;
    let stmt = |sql: &str, name: &str| {
        json!({"type": "execute", "stmt": {
            "sql": sql,
            "named_args": [{"name": name, "value": {"type": "text", "value": "x"}}]
        }})
    };

    let results = server
        .run(None, vec![
            stmt("SELECT :v", ":v"),
            stmt("SELECT :v", "v"),
            stmt("SELECT @v", "v"),
            stmt("SELECT $v", "v"),
        ])
        .await;

    for result in &results[..4] {
        assert_eq!(result["response"]["result"]["rows"], json!([[{"type": "text", "value": "x"}]]), "{result}");
    }
}

#[tokio::test]
async fn a_failed_request_reports_a_sqlite_code_and_later_requests_still_run() {
    let server = TestServer::dev().await;

    let results = server
        .run(None, vec![
            execute("CREATE TABLE t (id INTEGER PRIMARY KEY)"),
            execute("INSERT INTO t VALUES (1)"),
            execute("INSERT INTO t VALUES (1)"),
            execute("SELEC nonsense"),
            execute("SELECT count(*) FROM t"),
        ])
        .await;

    assert_eq!(results[2]["type"], "error");
    assert_eq!(results[2]["error"]["code"], "SQLITE_CONSTRAINT");
    assert_eq!(results[3]["type"], "error");
    assert!(results[3]["error"]["message"].as_str().unwrap().len() > 0);
    assert_eq!(results[4]["response"]["result"]["rows"], json!([[{"type": "integer", "value": "1"}]]));
}

#[tokio::test]
async fn health_and_version_check_answer() {
    let server = TestServer::dev().await;

    let (status, body) = server.request(Method::GET, "/health", None, None).await;
    assert_eq!((status, body), (StatusCode::OK, json!("ok")));

    let (status, _) = server.request(Method::GET, "/v2", None, None).await;
    assert_eq!(status, StatusCode::OK);

    // @libsql/client probes v3 only when asked; a 404 makes it stay on v2.
    let (status, _) = server.request(Method::GET, "/v3-protobuf", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
