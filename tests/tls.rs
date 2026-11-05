//! The TLS listener: HTTPS for clients that reach the server directly, with a
//! certificate another program renews, and the ACME challenge route that
//! lets a client such as certbot get one through the plain listener.

mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aozumi_db::Tls;
use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A self-signed certificate for `localhost` and its key, as PEM.
struct Pair {
    der: CertificateDer<'static>,
    cert: String,
    key: String,
}

fn pair() -> Pair {
    let rcgen::CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    Pair { der: cert.der().clone(), cert: cert.pem(), key: key_pair.serialize_pem() }
}

/// Replace a file in one step, as certbot's deploy hooks and `mv` do.
fn replace(path: &Path, contents: &str) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

struct Files {
    _dir: tempfile::TempDir,
    cert: PathBuf,
    key: PathBuf,
}

fn files() -> Files {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = (dir.path().join("fullchain.pem"), dir.path().join("privkey.pem"));
    Files { _dir: dir, cert, key }
}

fn install(files: &Files, pair: &Pair) {
    replace(&files.cert, &pair.cert);
    replace(&files.key, &pair.key);
}

/// A dev-mode server with a TLS listener on its own port.
async fn server(tls: impl FnOnce(TcpListener) -> Tls) -> (TestServer, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls = tls(listener);
    let server = TestServer::start_with(None, move |c| c.tls = Some(tls)).await;
    (server, addr)
}

/// A TLS connection to `addr` that trusts `roots`.
async fn connect(addr: SocketAddr, roots: &[&Pair]) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store.add(root.der.clone())?;
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(store)
        .with_no_client_auth();
    let tcp = TcpStream::connect(addr).await?;
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)).connect(ServerName::try_from("localhost")?, tcp).await?)
}

/// One HTTPS request on a fresh connection that trusts `roots`. Answers the
/// status, the body and the certificate the server presented.
async fn get(addr: SocketAddr, roots: &[&Pair], method: Method, path: &str, body: Option<serde_json::Value>) -> anyhow::Result<(StatusCode, String, CertificateDer<'static>)> {
    let tls = connect(addr, roots).await?;
    let presented = tls.get_ref().1.peer_certificates().unwrap()[0].clone().into_owned();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await?;
    tokio::spawn(conn);
    let body = body.map(|b| Bytes::from(b.to_string())).unwrap_or_default();
    let req = Request::builder().method(method).uri(path).header("host", "localhost").body(Full::new(body))?;
    let resp = sender.send_request(req).await?;
    let status = resp.status();
    let bytes = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned(), presented))
}

/// The server checks the files at most once a second, as connections arrive.
async fn after_a_check() {
    tokio::time::sleep(Duration::from_millis(1_100)).await;
}

/// Whether the server closes a connection on its own: a read that ends
/// (EOF or reset) rather than one that waits.
async fn closed_by_server(stream: &mut TcpStream, within: Duration) -> bool {
    let mut byte = [0u8; 1];
    matches!(tokio::time::timeout(within, stream.read(&mut byte)).await, Ok(Ok(0)) | Ok(Err(_)))
}

#[tokio::test]
async fn serves_the_api_over_tls_beside_the_plain_listener() {
    let (files, a) = (files(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (server, addr) = server(|l| Tls::new(l, cert, key)).await;

    let (status, body, presented) = get(addr, &[&a], Method::GET, "/health", None).await.unwrap();
    assert_eq!((status, body.as_str()), (StatusCode::OK, "\"ok\""));
    assert_eq!(presented, a.der);

    let pipeline = json!({"baton": null, "requests": [execute("SELECT 41 + 1"), {"type": "close"}]});
    let (status, body, _) = get(addr, &[&a], Method::POST, "/v2/pipeline", Some(pipeline)).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    let results: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(scalar(results["results"].as_array().unwrap()), json!("42"));

    // The plain listener is unchanged.
    let (status, _) = server.request(Method::GET, "/health", None, None).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn uses_a_renewed_certificate_without_a_restart() {
    let (files, a, b) = (files(), pair(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) = server(|l| Tls::new(l, cert, key)).await;
    assert_eq!(get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap().2, a.der);

    install(&files, &b);
    after_a_check().await;
    // The first connection after a change may still get the old pair; the
    // check it starts swaps it in.
    let _ = get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap();
    assert_eq!(get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap().2, b.der);
}

#[tokio::test]
async fn keeps_the_last_good_certificate_while_the_key_does_not_match() {
    let (files, a, b) = (files(), pair(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) = server(|l| Tls::new(l, cert, key)).await;
    assert_eq!(get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap().2, a.der);

    // Halfway through a renewal: the new certificate, the old key.
    replace(&files.cert, &b.cert);
    after_a_check().await;
    for _ in 0..2 {
        let (status, _, presented) = get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap();
        assert_eq!((status, presented), (StatusCode::OK, a.der.clone()));
    }

    // The renewal finishes.
    replace(&files.key, &b.key);
    after_a_check().await;
    let _ = get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap();
    assert_eq!(get(addr, &[&a, &b], Method::GET, "/health", None).await.unwrap().2, b.der);
}

#[tokio::test]
async fn starts_before_its_first_certificate_and_uses_it_once_it_is_there() {
    let (files, a) = (files(), pair());
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (server, addr) = server(|l| Tls::new(l, cert, key)).await;

    // No certificate yet: handshakes fail, the plain listener works (so an
    // ACME client can get the first certificate through it).
    assert!(get(addr, &[&a], Method::GET, "/health", None).await.is_err());
    assert_eq!(server.request(Method::GET, "/health", None, None).await.0, StatusCode::OK);

    install(&files, &a);
    after_a_check().await;
    let _ = get(addr, &[&a], Method::GET, "/health", None).await;
    let (status, _, presented) = get(addr, &[&a], Method::GET, "/health", None).await.unwrap();
    assert_eq!((status, presented), (StatusCode::OK, a.der));
}

#[tokio::test]
async fn closes_connections_past_the_limit_for_one_address() {
    let (files, a) = (files(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) = server(|l| Tls { max_connections_per_address: 2, ..Tls::new(l, cert, key) }).await;

    // Two connections that have not finished their handshake hold both places.
    let mut first = TcpStream::connect(addr).await.unwrap();
    let mut second = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut third = TcpStream::connect(addr).await.unwrap();
    assert!(closed_by_server(&mut third, Duration::from_secs(2)).await, "a third connection from the same address is closed");
    assert!(!closed_by_server(&mut first, Duration::from_millis(200)).await, "the first two stay open");
    assert!(!closed_by_server(&mut second, Duration::from_millis(200)).await);

    // A place frees up when a connection ends.
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (status, _, _) = get(addr, &[&a], Method::GET, "/health", None).await.unwrap();
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn closes_connections_past_the_total_limit() {
    let (files, a) = (files(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) = server(|l| Tls { max_connections: 1, ..Tls::new(l, cert, key) }).await;

    let mut held = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(get(addr, &[&a], Method::GET, "/health", None).await.is_err(), "no place left");
    assert!(!closed_by_server(&mut held, Duration::from_millis(200)).await);
}

#[tokio::test]
async fn closes_connections_that_never_finish_the_handshake() {
    let (files, a) = (files(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) = server(|l| Tls { handshake_timeout: Duration::from_millis(300), ..Tls::new(l, cert, key) }).await;

    let mut silent = TcpStream::connect(addr).await.unwrap();
    assert!(closed_by_server(&mut silent, Duration::from_secs(3)).await);
}

#[tokio::test]
async fn closes_requests_whose_body_never_arrives() {
    let (files, a) = (files(), pair());
    install(&files, &a);
    let (cert, key) = (files.cert.clone(), files.key.clone());
    let (_server, addr) =
        server(|l| Tls { body_timeout: Duration::from_millis(300), max_connections_per_address: 1, ..Tls::new(l, cert, key) }).await;

    // The headers in full, then a little of the body, then nothing.
    let mut stalled = connect(addr, &[&a]).await.unwrap();
    stalled.write_all(b"POST /v2/pipeline HTTP/1.1\r\nhost: localhost\r\ncontent-length: 100\r\n\r\n{\"baton\": ").await.unwrap();
    let mut answer = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(3), stalled.read_to_end(&mut answer)).await;
    let answer = String::from_utf8_lossy(&answer);
    assert!(read.is_ok(), "the server closes the connection");
    assert!(answer.starts_with("HTTP/1.1 408"), "{answer}");

    // Its place is free again.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _, _) = get(addr, &[&a], Method::GET, "/health", None).await.unwrap();
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn answers_acme_challenges_from_the_webroot() {
    let webroot = tempfile::tempdir().unwrap();
    let challenges = webroot.path().join(".well-known/acme-challenge");
    std::fs::create_dir_all(&challenges).unwrap();
    std::fs::write(challenges.join("Ab3_x-9Yz"), "Ab3_x-9Yz.thumbprint").unwrap();
    std::fs::write(webroot.path().join("outside"), "not a challenge").unwrap();
    let root = webroot.path().to_path_buf();
    let server = TestServer::start_with(None, move |c| c.acme_webroot = Some(root)).await;

    let (status, body) = server.request(Method::GET, "/.well-known/acme-challenge/Ab3_x-9Yz", None, None).await;
    assert_eq!((status, body), (StatusCode::OK, json!("Ab3_x-9Yz.thumbprint")));
    for path in [
        "/.well-known/acme-challenge/missing",
        "/.well-known/acme-challenge/..",
        "/.well-known/acme-challenge/../../outside",
        "/.well-known/acme-challenge/%2E%2E%2Foutside",
        "/.well-known/acme-challenge/",
    ] {
        assert_eq!(server.request(Method::GET, path, None, None).await.0, StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn has_no_acme_route_without_a_webroot() {
    let server = TestServer::dev().await;
    let (status, _) = server.request(Method::GET, "/.well-known/acme-challenge/Ab3_x-9Yz", None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
