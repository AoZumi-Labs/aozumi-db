//! Backups through the S3 store, against a small fake S3 (and EC2 metadata
//! service) that checks what every request carries.

mod common;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aozumi_db::{BackupTarget, Credentials, S3Options};
use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Default)]
struct Fake {
    objects: BTreeMap<String, Vec<u8>>,
    /// Access key id and session token of every S3 request, in order.
    seen: Vec<(String, Option<String>)>,
    imds_calls: usize,
    /// Every upload fails, as when the bucket refuses writes.
    fail_puts: bool,
    /// Every listing fails, as when the bucket cannot be reached.
    fail_lists: bool,
    /// Requests with this method whose path and query contain the text wait
    /// at the gate until it opens.
    hold: Option<(Method, String, Arc<Gate>)>,
}

/// Where held requests wait: each says it arrived, then waits for `open`.
struct Gate {
    arrived: tokio::sync::Notify,
    opened: tokio::sync::Semaphore,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self { arrived: tokio::sync::Notify::new(), opened: tokio::sync::Semaphore::new(0) })
    }

    /// Let every held request through, and every later one.
    fn open(&self) {
        self.opened.close();
    }
}

async fn fake_s3() -> (SocketAddr, Arc<Mutex<Fake>>) {
    let state = Arc::new(Mutex::new(Fake::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shared = state.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let state = shared.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let state = state.clone();
                    async move { Ok::<_, Infallible>(handle(state, req).await) }
                });
                let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    });
    (addr, state)
}

fn reply(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder().status(status).body(Full::new(body.into())).unwrap()
}

async fn handle(state: Arc<Mutex<Fake>>, req: Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
    let (parts, body) = req.into_parts();
    let body = body.collect().await.unwrap().to_bytes().to_vec();
    let header = |name: &str| parts.headers.get(name).map(|v| v.to_str().unwrap().to_string());
    let path = parts.uri.path().to_string();

    // EC2 instance metadata (IMDSv2).
    if path.starts_with("/latest/") {
        state.lock().unwrap().imds_calls += 1;
        return match (parts.method.clone(), path.as_str()) {
            (Method::PUT, "/latest/api/token") if header("x-aws-ec2-metadata-token-ttl-seconds").is_some() => {
                reply(StatusCode::OK, "imds-token")
            }
            _ if header("x-aws-ec2-metadata-token").as_deref() != Some("imds-token") => reply(StatusCode::UNAUTHORIZED, ""),
            (Method::GET, "/latest/meta-data/iam/security-credentials/") => reply(StatusCode::OK, "db-role"),
            (Method::GET, "/latest/meta-data/iam/security-credentials/db-role") => reply(
                StatusCode::OK,
                json!({"Code": "Success", "AccessKeyId": "IMDSKEY", "SecretAccessKey": "imds-secret",
                       "Token": "imds-session", "Expiration": "2100-01-01T00:00:00Z"})
                .to_string(),
            ),
            _ => reply(StatusCode::NOT_FOUND, ""),
        };
    }

    // S3, path style: /<bucket>/<key>.
    let auth = header("authorization").unwrap_or_default();
    let Some(key_id) = auth
        .strip_prefix("AWS4-HMAC-SHA256 Credential=")
        .and_then(|rest| rest.split('/').next())
        .filter(|_| auth.contains("/us-east-1/s3/aws4_request,") && auth.contains("Signature="))
    else {
        return reply(StatusCode::FORBIDDEN, "<Error><Code>AccessDenied</Code></Error>");
    };
    let payload_hash = hex::encode(Sha256::digest(&body));
    if header("x-amz-content-sha256").as_deref() != Some(payload_hash.as_str()) || header("x-amz-date").is_none() {
        return reply(StatusCode::BAD_REQUEST, "<Error><Code>XAmzContentSHA256Mismatch</Code></Error>");
    }
    let gate = state.lock().unwrap().hold.as_ref().filter(|(method, part, _)| *method == parts.method && parts.uri.to_string().contains(part.as_str())).map(|(_, _, gate)| gate.clone());
    if let Some(gate) = gate {
        gate.arrived.notify_one();
        let _ = gate.opened.acquire().await;
    }
    let mut fake = state.lock().unwrap();
    fake.seen.push((key_id.to_string(), header("x-amz-security-token")));

    let Some(rest) = path.strip_prefix("/backups") else {
        return reply(StatusCode::NOT_FOUND, "<Error><Code>NoSuchBucket</Code></Error>");
    };
    let key = rest.trim_start_matches('/').to_string();
    match parts.method {
        Method::PUT if fake.fail_puts => reply(StatusCode::INTERNAL_SERVER_ERROR, "<Error><Code>InternalError</Code></Error>"),
        Method::PUT => {
            fake.objects.insert(key, body);
            reply(StatusCode::OK, "")
        }
        Method::GET if key.is_empty() && fake.fail_lists => reply(StatusCode::INTERNAL_SERVER_ERROR, "<Error><Code>InternalError</Code></Error>"),
        Method::GET if key.is_empty() => list(&fake.objects, parts.uri.query().unwrap_or("")),
        Method::GET => match fake.objects.get(&key) {
            Some(bytes) => reply(StatusCode::OK, bytes.clone()),
            None => reply(StatusCode::NOT_FOUND, "<Error><Code>NoSuchKey</Code></Error>"),
        },
        Method::DELETE => {
            fake.objects.remove(&key);
            reply(StatusCode::NO_CONTENT, "")
        }
        _ => reply(StatusCode::METHOD_NOT_ALLOWED, ""),
    }
}

/// ListObjectsV2, one key per page so every listing paginates.
fn list(objects: &BTreeMap<String, Vec<u8>>, query: &str) -> Response<Full<Bytes>> {
    let params = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_string(), percent_decode(v)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(params.get("list-type").map(String::as_str), Some("2"));
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let after = params.get("continuation-token").cloned().unwrap_or_default();
    // With a delimiter, keys in deeper "directories" are left out.
    let shallow = |k: &str| params.get("delimiter").is_none_or(|d| !k[prefix.len()..].contains(d.as_str()));
    let mut matching = objects.iter().filter(|(k, _)| k.starts_with(&prefix) && **k > after && shallow(k));
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    if let Some((key, bytes)) = matching.next() {
        xml += &format!("<Contents><Key>{key}</Key><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>", bytes.len());
        if matching.next().is_some() {
            xml += &format!("<IsTruncated>true</IsTruncated><NextContinuationToken>{key}</NextContinuationToken>");
        } else {
            xml += "<IsTruncated>false</IsTruncated>";
        }
    } else {
        xml += "<IsTruncated>false</IsTruncated>";
    }
    xml += "</ListBucketResult>";
    reply(StatusCode::OK, xml)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn options(addr: SocketAddr, bucket: &str, credentials: Option<Credentials>) -> S3Options {
    S3Options {
        bucket: bucket.to_string(),
        prefix: "team/prod".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some(format!("http://{addr}")),
        credentials,
        metadata_endpoint: Some(format!("http://{addr}")),
    }
}

fn static_key() -> Option<Credentials> {
    Some(Credentials { access_key_id: "TESTKEY".into(), secret_access_key: "test-secret".into(), session_token: None })
}

#[tokio::test]
async fn backups_go_to_s3_and_restore_from_it() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 2, None).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)")]).await;

    for i in 0..3 {
        server.run(Some(&app), vec![execute(&format!("INSERT INTO t VALUES ({i})"))]).await;
        let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let keys = fake.lock().unwrap().objects.keys().cloned().collect::<Vec<_>>();
    assert_eq!(keys.len(), 2, "retention deletes the oldest from the bucket: {keys:?}");
    assert!(keys.iter().all(|k| k.starts_with("team/prod/app/") && k.ends_with(".db")), "{keys:?}");

    let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(listed["backups"].as_array().unwrap().len(), 2, "{listed}");

    let (status, body) = server
        .admin(Method::POST, "/v1/databases", Some(json!({"name": "copy", "restore": {"database": "app", "backup": "latest"}})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let copy = server.token("copy").await;
    assert_eq!(scalar(&server.run(Some(&copy), vec![execute("SELECT count(*) FROM t")]).await), "3");
}

#[tokio::test]
async fn without_keys_the_instance_role_supplies_credentials() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", None)).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    server.database("app").await;

    for _ in 0..2 {
        let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let fake = fake.lock().unwrap();
    assert!(!fake.seen.is_empty());
    assert!(fake.seen.iter().all(|(key, token)| key == "IMDSKEY" && token.as_deref() == Some("imds-session")));
    assert_eq!(fake.imds_calls, 3, "credentials are fetched once (token, role, keys) and then cached");
}

#[tokio::test]
async fn s3_errors_are_reported() {
    let (addr, _fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "no-such-bucket", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    server.database("app").await;

    let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.as_str().unwrap().contains("NoSuchBucket"), "{body}");
}

#[tokio::test]
async fn a_failing_store_shows_in_the_status_until_a_backup_succeeds() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    server.database("app").await;

    fake.lock().unwrap().fail_puts = true;
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    let (status, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::OK, "listing still works while uploads fail: {listed}");
    let failure = &listed["status"]["last_failure"];
    assert!(failure["at_ms"].as_u64().unwrap() >= before, "{listed}");
    assert!(failure["message"].as_str().unwrap().contains("InternalError"), "{listed}");
    assert_eq!(listed["status"]["last_backup_ms"], serde_json::Value::Null, "{listed}");

    fake.lock().unwrap().fail_puts = false;
    let (status, backup) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{backup}");
    let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(listed["status"]["last_failure"], serde_json::Value::Null, "a success clears it: {listed}");
    assert_eq!(listed["status"]["last_backup_ms"], backup["taken_ms"], "{listed}");
}

#[tokio::test]
async fn a_restore_in_place_that_cannot_store_its_undo_backup_changes_nothing() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;

    fake.lock().unwrap().fail_puts = true;
    let (status, body) = server.admin(Method::POST, "/v1/databases/app/restore", Some(json!({"backup": "latest"}))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    // The database is as it was, and takes writes again.
    let results = server.run(Some(&app), vec![execute("SELECT count(*) FROM t"), execute("INSERT INTO t VALUES (3)")]).await;
    assert_eq!(scalar(&results), "2");
    assert_eq!(results[1]["type"], "ok", "{}", results[1]);
    let (_, listed) = server.admin(Method::GET, "/v1/databases/app/backups", None).await;
    assert_eq!(listed["backups"].as_array().unwrap().len(), 1, "{listed}");
    assert!(listed["status"]["last_failure"]["message"].as_str().unwrap().contains("InternalError"), "{listed}");
}

#[tokio::test]
async fn a_restore_in_place_runs_to_its_end_when_the_client_hangs_up() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;
    let (status, body) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    server.run(Some(&app), vec![execute("INSERT INTO t VALUES (2)")]).await;

    // The undo backup's upload waits at a gate.
    let gate = Gate::new();
    fake.lock().unwrap().hold = Some((Method::PUT, ".before-restore.db".to_string(), gate.clone()));
    let stream = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    let connection = tokio::spawn(connection);
    let request = Request::post("/v1/databases/app/restore")
        .header("host", "localhost")
        .header("authorization", format!("Bearer {SECRET}"))
        .body(Full::new(Bytes::from(json!({"backup": "latest"}).to_string())))
        .unwrap();
    let response = tokio::spawn(async move { sender.send_request(request).await });
    gate.arrived.notified().await;

    // The client hangs up while the undo backup uploads, and the upload goes on.
    response.abort();
    connection.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.open();

    // The restore still ends as one: the database is the backup again, and takes writes.
    let mut count = json!(null);
    for _ in 0..100 {
        count = scalar(&server.run(Some(&app), vec![execute("SELECT count(*) FROM t")]).await);
        if count == "1" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(count, "1", "restored although its client hung up");
    let results = server.run(Some(&app), vec![execute("INSERT INTO t VALUES (3)")]).await;
    assert_eq!(results[0]["type"], "ok", "{}", results[0]);
}

#[tokio::test]
async fn deleting_with_backups_lists_them_before_the_name_is_free() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    server.database("app").await;
    let (status, old) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{old}");
    let old_key = format!("team/prod/app/{}.manual.db", old["id"].as_str().unwrap());
    assert!(fake.lock().unwrap().objects.contains_key(&old_key));

    // Listing the database's backups waits at a gate, and meanwhile the name is asked for again.
    let gate = Gate::new();
    fake.lock().unwrap().hold = Some((Method::GET, "list-type=2".to_string(), gate.clone()));
    let delete = server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None);
    let meanwhile = async {
        gate.arrived.notified().await;
        let made = server.admin(Method::POST, "/v1/databases", Some(json!({"name": "app"}))).await;
        gate.open();
        made
    };
    let ((deleted, _), (made, body)) = tokio::join!(delete, meanwhile);
    assert_eq!(made, StatusCode::CONFLICT, "the name is free only once its backups are listed: {body}");
    assert_eq!(deleted, StatusCode::NO_CONTENT);

    // Made again, the database keeps its own backups; the old one's go.
    server.database("app").await;
    let (status, new) = server.admin(Method::POST, "/v1/databases/app/backups", None).await;
    assert_eq!(status, StatusCode::CREATED, "{new}");
    let new_key = format!("team/prod/app/{}.manual.db", new["id"].as_str().unwrap());
    for _ in 0..250 {
        if !fake.lock().unwrap().objects.contains_key(&old_key) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let objects = fake.lock().unwrap().objects.keys().cloned().collect::<Vec<_>>();
    assert!(!objects.contains(&old_key), "{objects:?}");
    assert!(objects.contains(&new_key), "{objects:?}");
}

#[tokio::test]
async fn a_delete_that_cannot_list_the_backups_deletes_nothing() {
    let (addr, fake) = fake_s3().await;
    let target = BackupTarget::s3(options(addr, "backups", static_key())).unwrap();
    let server = TestServer::with_backup_target(target, 5, None).await;
    let app = server.database("app").await;
    server.run(Some(&app), vec![execute("CREATE TABLE t (x)"), execute("INSERT INTO t VALUES (1)")]).await;

    fake.lock().unwrap().fail_lists = true;
    let (status, body) = server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None).await;
    assert!(status.is_server_error(), "{status}: {body}");
    assert_eq!(scalar(&server.run(Some(&app), vec![execute("SELECT count(*) FROM t")]).await), "1", "the database is still there");

    fake.lock().unwrap().fail_lists = false;
    let (status, body) = server.admin(Method::DELETE, "/v1/databases/app?backups=delete", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
}

#[test]
fn backup_urls_parse() {
    assert!(BackupTarget::parse("s3://bucket/some/prefix").is_ok());
    assert!(BackupTarget::parse("s3://bucket").is_ok());
    assert!(BackupTarget::parse("file:///var/backups").is_ok());
    assert!(BackupTarget::parse("s3://").is_err());
    assert!(BackupTarget::parse("gs://bucket").is_err());
}
