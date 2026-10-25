//! Backups through the S3 store, against a small fake S3 (and EC2 metadata
//! service) that checks what every request carries.

mod common;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

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
    let mut fake = state.lock().unwrap();
    fake.seen.push((key_id.to_string(), header("x-amz-security-token")));

    let Some(rest) = path.strip_prefix("/backups") else {
        return reply(StatusCode::NOT_FOUND, "<Error><Code>NoSuchBucket</Code></Error>");
    };
    let key = rest.trim_start_matches('/').to_string();
    match parts.method {
        Method::PUT => {
            fake.objects.insert(key, body);
            reply(StatusCode::OK, "")
        }
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
    let mut matching = objects.iter().filter(|(k, _)| k.starts_with(&prefix) && **k > after);
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

#[test]
fn backup_urls_parse() {
    assert!(BackupTarget::parse("s3://bucket/some/prefix").is_ok());
    assert!(BackupTarget::parse("s3://bucket").is_ok());
    assert!(BackupTarget::parse("file:///var/backups").is_ok());
    assert!(BackupTarget::parse("s3://").is_err());
    assert!(BackupTarget::parse("gs://bucket").is_err());
}
