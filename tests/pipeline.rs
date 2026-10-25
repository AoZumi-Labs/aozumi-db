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
