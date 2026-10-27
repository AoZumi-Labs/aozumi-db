//! The console endpoints the AOZUMI DB console uses: a script run statement
//! by statement, the table list and a page of rows, all behind a database
//! token like the pipeline.

mod common;

use common::*;
use hyper::{Method, StatusCode};
use serde_json::{json, Value};

async fn read_only_token(server: &TestServer, db: &str) -> String {
    let (status, body) = server
        .admin(Method::POST, &format!("/v1/databases/{db}/tokens"), Some(json!({"read_only": true})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["token"].as_str().unwrap().to_string()
}

async fn run(server: &TestServer, token: &str, sql: &str) -> Value {
    let (status, body) = server.request(Method::POST, "/v1/console/run", Some(token), Some(json!({"sql": sql}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

fn states(result: &Value) -> Vec<&str> {
    result["statements"].as_array().unwrap().iter().map(|s| s["state"].as_str().unwrap()).collect()
}

#[tokio::test]
async fn a_script_runs_statement_by_statement() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    let result = run(&server, &rw, "create table notes (id integer primary key, body text, data blob, score real);
        insert into notes (body, data, score) values ('hello; world', x'00ff10', 1.5), ('second', null, null);
        -- the rows; with a comment
        select * from notes order by id;").await;

    assert_eq!(result["readOnly"], false);
    assert_eq!(states(&result), ["ok", "ok", "ok"]);
    let statements = result["statements"].as_array().unwrap();
    assert_eq!(statements[1]["rowsAffected"], 2);
    assert_eq!(statements[1]["lastInsertRowid"], "2");
    assert_eq!(statements[2]["sql"], "select * from notes order by id", "comments between statements are dropped");
    assert_eq!(statements[2]["columns"], json!(["id", "body", "data", "score"]));
    assert_eq!(statements[2]["rowCount"], 2);
    assert_eq!(statements[2]["truncated"], false);
    assert_eq!(statements[2]["rows"][0], json!([
        {"type": "integer", "value": "1"},
        {"type": "text", "value": "hello; world"},
        {"type": "blob", "bytes": 3, "hex": "00ff10"},
        {"type": "float", "value": 1.5},
    ]));
    assert_eq!(statements[2]["rows"][1], json!([{"type": "integer", "value": "2"}, {"type": "text", "value": "second"}, null, null]));
}

#[tokio::test]
async fn the_first_error_stops_the_rest() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    let result = run(&server, &rw, "create table t (x); insert into nope values (1); insert into t values (1)").await;
    assert_eq!(states(&result), ["ok", "error", "skipped"]);
    assert!(result["statements"][1]["error"]["message"].as_str().unwrap().contains("no such table"));
    assert_eq!(run(&server, &rw, "select count(*) from t").await["statements"][0]["rows"][0][0]["value"], "0");

    // A statement that does not parse: the ones before it run, the error is its own.
    let parsed = run(&server, &rw, "insert into t values (2); selec 1; insert into t values (3)").await;
    assert_eq!(states(&parsed), ["ok", "error"]);
    assert!(parsed["statements"][1]["sql"].as_str().unwrap().starts_with("selec 1"));
    assert_eq!(run(&server, &rw, "select count(*) from t").await["statements"][0]["rows"][0][0]["value"], "1");
}

#[tokio::test]
async fn a_read_only_token_reads_but_cannot_write() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    run(&server, &rw, "create table t (x); insert into t values (1)").await;
    let ro = read_only_token(&server, "app").await;

    let result = run(&server, &ro, "select count(*) from t; insert into t values (2)").await;
    assert_eq!(result["readOnly"], true);
    assert_eq!(states(&result), ["ok", "error"]);
    assert_eq!(result["statements"][1]["error"]["code"], "SQLITE_READONLY");
    let policy = run(&server, &ro, "pragma query_only = 0").await;
    assert_eq!(policy["statements"][0]["error"]["code"], "SQLITE_AUTH", "the SQL policy applies to the console too");
    assert_eq!(run(&server, &rw, "select count(*) from t").await["statements"][0]["rows"][0][0]["value"], "1");
}

#[tokio::test]
async fn a_transaction_left_open_is_rolled_back() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    run(&server, &rw, "create table t (x)").await;
    let result = run(&server, &rw, "begin; insert into t values (1); select count(*) from t").await;
    assert_eq!(states(&result), ["ok", "ok", "ok"]);
    assert_eq!(result["statements"][2]["rows"][0][0]["value"], "1", "visible inside the transaction");
    assert_eq!(run(&server, &rw, "select count(*) from t").await["statements"][0]["rows"][0][0]["value"], "0");
    // The app's own writes are not held up by it.
    server.run(Some(&rw), vec![execute("insert into t values (2)")]).await;
    assert_eq!(run(&server, &rw, "select count(*) from t").await["statements"][0]["rows"][0][0]["value"], "1");
}

#[tokio::test]
async fn results_are_capped_but_every_row_is_counted() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    let result = run(&server, &rw, "with recursive c(x) as (select 1 union all select x + 1 from c where x < 1500) select x, printf('%.20000c', 'a') from c limit 1500").await;
    let statement = &result["statements"][0];
    assert_eq!(statement["rowCount"], 1500);
    assert_eq!(statement["truncated"], true);
    let kept = statement["rows"].as_array().unwrap();
    assert!(kept.len() <= 1000 && !kept.is_empty(), "{} rows kept", kept.len());
    assert_eq!(kept[0][1]["value"].as_str().unwrap().len(), 10_000, "long text is cut");
    assert_eq!(kept[0][1]["truncated"], true);
}

#[tokio::test]
async fn tables_and_pages() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    run(&server, &rw, "create table notes (id integer primary key autoincrement, body text);
        create table \"we\"\"ird name\" (x);
        insert into notes (body) select 'note ' || value from generate_series(1, 120);").await;
    let ro = read_only_token(&server, "app").await;

    let (status, schema) = server.request(Method::GET, "/v1/console/schema", Some(&ro), None).await;
    assert_eq!(status, StatusCode::OK, "{schema}");
    let names = schema["tables"].as_array().unwrap().iter().map(|table| table["name"].as_str().unwrap()).collect::<Vec<_>>();
    assert_eq!(names, ["notes", "we\"ird name"], "SQLite's and the engine's own tables are left out");
    assert_eq!(schema["tables"][0]["columns"], json!([
        {"name": "id", "type": "integer", "notNull": false, "primaryKey": true},
        {"name": "body", "type": "text", "notNull": false, "primaryKey": false},
    ]));
    assert_eq!(schema["tables"][1]["columns"][0]["name"], "x");

    let (status, page) = server.request(Method::POST, "/v1/console/rows", Some(&ro), Some(json!({"table": "notes", "page": 3}))).await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!((page["total"].as_u64(), page["page"].as_u64(), page["pageSize"].as_u64()), (Some(120), Some(3), Some(50)));
    assert_eq!(page["columns"], json!(["id", "body"]));
    assert_eq!(page["rows"].as_array().unwrap().len(), 20);
    assert_eq!(page["rows"][0][1]["value"], "note 101");

    let (status, odd) = server.request(Method::POST, "/v1/console/rows", Some(&ro), Some(json!({"table": "we\"ird name"}))).await;
    assert_eq!((status, odd["total"].as_u64()), (StatusCode::OK, Some(0)));

    for (table, status, code) in [("nope", StatusCode::NOT_FOUND, "table_not_found"), ("", StatusCode::BAD_REQUEST, "table_invalid")] {
        let (got, body) = server.request(Method::POST, "/v1/console/rows", Some(&ro), Some(json!({"table": table}))).await;
        assert_eq!((got, body["code"].as_str()), (status, Some(code)), "{body}");
    }
    // A name only ever reaches SQL quoted.
    let (status, body) = server.request(Method::POST, "/v1/console/rows", Some(&ro), Some(json!({"table": "notes\"; drop table notes; --"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(run(&server, &rw, "select count(*) from notes").await["statements"][0]["rows"][0][0]["value"], "120");
}

#[tokio::test]
async fn the_console_needs_a_database_token() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    let other = server.database("other").await;
    run(&server, &other, "create table secret (x)").await;

    for (token, path) in [(None, "/v1/console/schema"), (Some(SECRET), "/v1/console/schema"), (Some("adb1.e30.AAAA"), "/v1/console/schema")] {
        let (status, _) = server.request(Method::GET, path, token, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}");
    }
    let (status, _) = server.request(Method::POST, "/v1/console/run", None, Some(json!({"sql": "select 1"}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // A token opens its own database and no other.
    let (_, schema) = server.request(Method::GET, "/v1/console/schema", Some(&rw), None).await;
    assert_eq!(schema["tables"], json!([]));
    // An expired token is refused.
    let expired = aozumi_db::mint_token(SECRET.as_bytes(), &aozumi_db::Claims { db: "app".into(), exp: Some(1), read_only: true });
    let (status, _) = server.request(Method::GET, "/v1/console/schema", Some(&expired), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn empty_and_oversized_scripts_are_refused() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    for (sql, status, code) in [
        ("  -- nothing to run\n", StatusCode::BAD_REQUEST, "empty"),
        (&"select 1;".repeat(51), StatusCode::BAD_REQUEST, "too_many_statements"),
        (&format!("select '{}'", "x".repeat(300 * 1024)), StatusCode::PAYLOAD_TOO_LARGE, "sql_too_long"),
    ] {
        let (got, body) = server.request(Method::POST, "/v1/console/run", Some(&rw), Some(json!({"sql": sql}))).await;
        assert_eq!((got, body["code"].as_str()), (status, Some(code)), "{body}");
    }
}

#[tokio::test]
async fn a_script_that_destroys_data_runs_only_when_confirmed() {
    let server = TestServer::with_secret().await;
    let rw = server.database("app").await;
    run(&server, &rw, "create table notes (x); insert into notes values (1)").await;
    let script = json!({"sql": "insert into notes values (2); drop table notes"});

    let (status, body) = server.request(Method::POST, "/v1/console/run", Some(&rw), Some(script.clone())).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "confirm_destructive");
    assert_eq!(body["destructive"], json!([{"sql": "drop table notes", "what": "drops the table notes"}]));
    assert_eq!(run(&server, &rw, "select count(*) from notes").await["statements"][0]["rows"][0][0]["value"], "1", "nothing ran");

    // Read-only, nothing can be destroyed, so nothing is asked.
    let ro = read_only_token(&server, "app").await;
    let (status, body) = server.request(Method::POST, "/v1/console/run", Some(&ro), Some(script)).await;
    assert_eq!((status, body["statements"][0]["error"]["code"].as_str()), (StatusCode::OK, Some("SQLITE_READONLY")));

    let (status, body) = server
        .request(Method::POST, "/v1/console/run", Some(&rw), Some(json!({"sql": "insert into notes values (2); drop table notes", "confirm_destructive": true})))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(states(&body), ["ok", "ok"]);
    let (_, schema) = server.request(Method::GET, "/v1/console/schema", Some(&rw), None).await;
    assert_eq!(schema["tables"], json!([]));
}
