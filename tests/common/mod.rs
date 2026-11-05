#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aozumi_db::{BackupConfig, BackupTarget, Config};
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

/// Backups go to a plain directory, so tests can also look at what was written.
pub struct Backups {
    pub dir: std::path::PathBuf,
    pub keep: usize,
    pub every: Option<Duration>,
    /// Ship the WAL this often for point-in-time restore. `None` = off.
    pub wal: Option<Duration>,
}

impl TestServer {
    /// Dev mode: no secret, one database named `default`.
    pub async fn dev() -> Self {
        Self::start(None, |_| {}).await
    }

    pub async fn dev_with_idle_timeout(idle: Duration) -> Self {
        Self::start(None, |c| c.stream_idle_timeout = idle).await
    }

    pub async fn with_secret() -> Self {
        Self::start(Some(SECRET), |_| {}).await
    }

    pub async fn with_secret_and(tweak: impl FnOnce(&mut Config)) -> Self {
        Self::start(Some(SECRET), tweak).await
    }

    pub async fn with_backups(backups: &Backups) -> Self {
        let target = BackupTarget::directory(backups.dir.clone());
        Self::with_backup(BackupConfig { target, keep: backups.keep, every: backups.every, wal_every: backups.wal }).await
    }

    pub async fn with_backup_target(target: BackupTarget, keep: usize, every: Option<Duration>) -> Self {
        Self::with_backup(BackupConfig { target, keep, every, wal_every: None }).await
    }

    pub async fn with_backup(backup: BackupConfig) -> Self {
        Self::start(Some(SECRET), move |c| c.backup = Some(backup)).await
    }

    pub async fn start_with(secret: Option<&str>, tweak: impl FnOnce(&mut Config)) -> Self {
        Self::start(secret, tweak).await
    }

    async fn start(secret: Option<&str>, tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::new(dir.path().to_path_buf(), secret.map(|s| s.as_bytes().to_vec()));
        tweak(&mut config);
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

    pub async fn admin(&self, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.request(method, path, Some(SECRET), body).await
    }

    /// Create a database and return a token for it.
    pub async fn database(&self, name: &str) -> String {
        let (status, body) = self.admin(Method::POST, "/v1/databases", Some(json!({"name": name}))).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        self.token(name).await
    }

    pub async fn token(&self, name: &str) -> String {
        let (status, body) = self.admin(Method::POST, &format!("/v1/databases/{name}/tokens"), Some(json!({}))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["token"].as_str().unwrap().to_string()
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

    /// The backup listing of `db`, polled until `done` holds, for at most 5 s.
    pub async fn backups_until(&self, db: &str, done: impl Fn(&Value) -> bool) -> Value {
        let path = format!("/v1/databases/{db}/backups");
        for _ in 0..250 {
            let (status, body) = self.admin(Method::GET, &path, None).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            if done(&body) {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("never true: {}", self.admin(Method::GET, &path, None).await.1);
    }

    /// Waits until every write to `db` committed before `ms` is in the backup
    /// store, and returns the moment the store is now up to date with. Each
    /// tick of the backup schedule, and of WAL shipping, moves that moment
    /// on, even one that finds nothing to store.
    pub async fn backed_up_past(&self, db: &str, ms: u64) -> u64 {
        let up_to_date = |body: &Value| body["status"]["up_to_date_ms"].as_u64();
        let body = self.backups_until(db, |body| up_to_date(body).is_some_and(|up| up > ms)).await;
        up_to_date(&body).unwrap()
    }

    /// Waits until the backup schedule holds every write to `db` so far and
    /// has ticked once more since: from then on it takes no backup until the
    /// database changes.
    pub async fn settle_backups(&self, db: &str) {
        self.backed_up_past(db, now_ms()).await;
        // A tick that looked at the database just before a write can still
        // catch the write in its backup, and then backs the database up again
        // on the next tick. Wait for that one.
        self.backed_up_past(db, now_ms()).await;
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn execute(sql: &str) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql, "want_rows": true}})
}

pub fn execute_args(sql: &str, args: Value) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql, "args": args, "want_rows": true}})
}

/// The single value of a one-row, one-column result.
pub fn scalar(results: &[Value]) -> Value {
    results[0]["response"]["result"]["rows"][0][0]["value"].clone()
}
