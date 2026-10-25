#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use aozumi_db::Config;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use tempfile::TempDir;

pub const SECRET: &str = "test-secret-do-not-use";

pub struct TestServer {
    pub addr: SocketAddr,
    pub dir: TempDir,
}

impl TestServer {
    /// Dev mode: no secret, one database named `default`.
    pub async fn dev() -> Self {
        Self::start(None, None).await
    }

    pub async fn dev_with_idle_timeout(idle: Duration) -> Self {
        Self::start(None, Some(idle)).await
    }

    pub async fn with_secret() -> Self {
        Self::start(Some(SECRET), None).await
    }

    async fn start(secret: Option<&str>, idle: Option<Duration>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::new(dir.path().to_path_buf(), secret.map(|s| s.as_bytes().to_vec()));
        if let Some(idle) = idle {
            config.stream_idle_timeout = idle;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { aozumi_db::serve(listener, config).await.unwrap() });
        Self { addr, dir }
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let client = Client::builder(TokioExecutor::new()).build_http();
        let mut req = Request::builder()
            .method(method)
            .uri(format!("http://{}{}", self.addr, path));
        if let Some(token) = token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let body = body.map(|b| Bytes::from(b.to_string())).unwrap_or_default();
        let resp = client.request(req.body(Full::new(body)).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, value)
    }

    pub async fn pipeline(&self, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        self.request(Method::POST, "/v2/pipeline", token, Some(body)).await
    }

    /// Run requests in a fresh stream that is closed at the end, and return
    /// the `results` array.
    pub async fn run(&self, token: Option<&str>, mut requests: Vec<Value>) -> Vec<Value> {
        requests.push(json!({"type": "close"}));
        let (status, body) = self.pipeline(token, json!({"baton": null, "requests": requests})).await;
        assert_eq!(status, StatusCode::OK, "pipeline failed: {body}");
        body["results"].as_array().unwrap().clone()
    }
}

pub fn execute(sql: &str) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql, "want_rows": true}})
}

pub fn execute_args(sql: &str, args: Value) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql, "args": args, "want_rows": true}})
}
