//! HTTPS for clients that reach the server directly, such as apps in the
//! same network, without a proxy or tunnel in front of the plain listener.
//!
//! The certificate and key are PEM files that another program renews, such as
//! certbot answering its challenge through `Config::acme_webroot`. They are
//! read again when either file changes, checked at most once a second as
//! connections arrive, so a renewal needs no restart. A pair is used only when
//! the key matches the certificate; until then the last good pair stays, so a
//! renewal caught halfway (the new certificate, the old key) changes nothing.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use hyper_util::rt::TokioIo;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio::net::TcpListener;

/// How long a connection may take to send a request's headers, and how long
/// a kept-alive connection may sit idle between requests. Clients that keep
/// connections alive close idle ones sooner (Node's `fetch` after 4 s).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How often, at most, the certificate files are looked at.
const CHECK_EVERY: Duration = Duration::from_secs(1);

/// The TLS listener. `Tls::new` sets the limits to their defaults.
pub struct Tls {
    pub listener: TcpListener,
    /// The certificate chain, the server's own certificate first (certbot's
    /// `fullchain.pem`).
    pub cert: PathBuf,
    /// Its private key: PKCS#8, PKCS#1 or SEC1, as PEM.
    pub key: PathBuf,
    /// Connections at once; one past it is closed as it arrives. 2,048 by default.
    pub max_connections: usize,
    /// Connections at once from one address, so one client cannot take every
    /// place. 256 by default.
    pub max_connections_per_address: usize,
    /// How long a client has to finish the TLS handshake. 10 s by default.
    pub handshake_timeout: Duration,
    /// How long a request's body may take to arrive once its headers have.
    /// 30 s by default.
    pub body_timeout: Duration,
}

impl Tls {
    pub fn new(listener: TcpListener, cert: PathBuf, key: PathBuf) -> Self {
        Self {
            listener,
            cert,
            key,
            max_connections: 2_048,
            max_connections_per_address: 256,
            handshake_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(30),
        }
    }
}

pub(crate) async fn serve(tls: Tls, state: Arc<crate::State>) {
    let certificates = Arc::new(Certificates::new(tls.cert, tls.key));
    certificates.refresh();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports TLS 1.2 and 1.3")
        .with_no_client_auth()
        .with_cert_resolver(certificates.clone());
    // The server speaks HTTP/1.1 only.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let places = Arc::new(Places::new(tls.max_connections, tls.max_connections_per_address));
    loop {
        let (stream, peer) = match tls.listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                // Out of file descriptors, say: wait instead of spinning.
                eprintln!("aozumi-db: TLS: accepting a connection failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // No place left: dropping the stream closes it.
        let Some(place) = Places::claim(&places, peer.ip()) else { continue };
        certificates.refresh();
        let (acceptor, state, timeout) = (acceptor.clone(), state.clone(), tls.handshake_timeout);
        let timeouts = crate::Timeouts { header: HEADER_READ_TIMEOUT, body: tls.body_timeout };
        tokio::spawn(async move {
            let _place = place;
            let _ = stream.set_nodelay(true);
            let Ok(Ok(stream)) = tokio::time::timeout(timeout, acceptor.accept(stream)).await else { return };
            crate::serve_connection(TokioIo::new(stream), state, Some(timeouts)).await;
        });
    }
}

/// The pair in use, and what the files looked like when last read.
#[derive(Debug)]
struct Certificates {
    cert: PathBuf,
    key: PathBuf,
    current: Mutex<Option<Arc<CertifiedKey>>>,
    files: Mutex<Files>,
}

#[derive(Debug, Default)]
struct Files {
    /// The files as they were at the last read, good or not, so a broken
    /// pair is reported once and read again only after it changes.
    read: Option<(Option<Stamp>, Option<Stamp>)>,
    checked: Option<Instant>,
}

/// Enough to tell that a file was replaced or rewritten.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct Stamp {
    len: u64,
    modified: SystemTime,
    inode: u64,
}

impl Certificates {
    fn new(cert: PathBuf, key: PathBuf) -> Self {
        Self { cert, key, current: Mutex::new(None), files: Mutex::new(Files::default()) }
    }

    /// Read the pair again if either file changed since it was last read,
    /// looking at most once every `CHECK_EVERY`.
    fn refresh(&self) {
        let mut files = self.files.lock().unwrap();
        if files.checked.is_some_and(|at| at.elapsed() < CHECK_EVERY) {
            return;
        }
        files.checked = Some(Instant::now());
        let now = (stamp(&self.cert), stamp(&self.key));
        if files.read == Some(now) {
            return;
        }
        files.read = Some(now);
        match load(&self.cert, &self.key) {
            Ok(pair) => {
                *self.current.lock().unwrap() = Some(Arc::new(pair));
                eprintln!("aozumi-db: TLS: serving the certificate in {}", self.cert.display());
            }
            Err(err) => {
                let then = match self.current.lock().unwrap().is_some() {
                    true => "keeping the certificate already in use",
                    false => "TLS handshakes fail until they hold a certificate and its key",
                };
                eprintln!("aozumi-db: TLS: cannot use {} with {}: {err:#}; {then}", self.cert.display(), self.key.display());
            }
        }
    }
}

impl ResolvesServerCert for Certificates {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.current.lock().unwrap().clone()
    }
}

fn stamp(path: &Path) -> Option<Stamp> {
    let meta = std::fs::metadata(path).ok()?;
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(&meta);
    #[cfg(not(unix))]
    let inode = 0;
    Some(Stamp { len: meta.len(), modified: meta.modified().ok()?, inode })
}

/// The chain and its key, if the key is the certificate's.
fn load(cert: &Path, key: &Path) -> anyhow::Result<CertifiedKey> {
    let chain = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!chain.is_empty(), "{} holds no certificate", cert.display());
    let key = PrivateKeyDer::from_pem_file(key)?;
    Ok(CertifiedKey::from_der(chain, key, &rustls::crypto::ring::default_provider())?)
}

/// Connections being served: in all, and from each address.
struct Places {
    max: usize,
    max_per_address: usize,
    taken: Mutex<(usize, HashMap<IpAddr, usize>)>,
}

/// One connection's place, given back when it is dropped.
struct Place {
    places: Arc<Places>,
    address: IpAddr,
}

impl Places {
    fn new(max: usize, max_per_address: usize) -> Self {
        Self { max, max_per_address, taken: Mutex::new((0, HashMap::new())) }
    }

    fn claim(places: &Arc<Places>, address: IpAddr) -> Option<Place> {
        let mut taken = places.taken.lock().unwrap();
        let (total, per_address) = &mut *taken;
        let from_address = per_address.entry(address).or_insert(0);
        if *total >= places.max || *from_address >= places.max_per_address {
            if *from_address == 0 {
                per_address.remove(&address);
            }
            return None;
        }
        *total += 1;
        *from_address += 1;
        Some(Place { places: places.clone(), address })
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        let mut taken = self.places.taken.lock().unwrap();
        let (total, per_address) = &mut *taken;
        *total -= 1;
        if let Some(count) = per_address.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                per_address.remove(&self.address);
            }
        }
    }
}
