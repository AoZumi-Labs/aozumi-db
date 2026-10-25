//! A minimal S3 client for backups: PutObject, GetObject, ListObjectsV2 and
//! DeleteObject, signed with AWS Signature Version 4. Works with AWS S3 and
//! S3-compatible stores (Cloudflare R2, MinIO) through a custom endpoint.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Instance-role keys stay valid well past this; refetching early is cheap.
const ROLE_CREDENTIALS_TTL: Duration = Duration::from_secs(600);
const DEFAULT_METADATA_ENDPOINT: &str = "http://169.254.169.254";

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub struct S3Options {
    pub bucket: String,
    /// Key prefix inside the bucket, e.g. `prod/db`. May be empty.
    pub prefix: String,
    pub region: String,
    /// For S3-compatible stores, e.g. `https://<account>.r2.cloudflarestorage.com`.
    /// Uses path-style URLs. `None` = AWS S3 with virtual-hosted URLs.
    pub endpoint: Option<String>,
    /// `None` = the EC2 instance role, through the instance metadata service.
    pub credentials: Option<Credentials>,
    /// Instance metadata service; `None` = 169.254.169.254.
    pub metadata_endpoint: Option<String>,
}

impl S3Options {
    /// `s3://bucket/prefix`, with the rest from the standard AWS environment
    /// variables and `AOZUMI_DB_S3_ENDPOINT`.
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let rest = url.strip_prefix("s3://").ok_or_else(|| anyhow!("not an s3:// URL: {url:?}"))?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            bail!("the backup URL {url:?} has no bucket");
        }
        let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let credentials = match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
            (Some(access_key_id), Some(secret_access_key)) => {
                Some(Credentials { access_key_id, secret_access_key, session_token: env("AWS_SESSION_TOKEN") })
            }
            _ => None,
        };
        Ok(Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            region: env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")).unwrap_or_else(|| "us-east-1".into()),
            endpoint: env("AOZUMI_DB_S3_ENDPOINT"),
            credentials,
            metadata_endpoint: env("AWS_EC2_METADATA_SERVICE_ENDPOINT"),
        })
    }
}

pub struct S3 {
    bucket: String,
    prefix: String,
    region: String,
    scheme: String,
    host: String,
    path_style: bool,
    credentials: Option<Credentials>,
    metadata_endpoint: String,
    role_credentials: Mutex<Option<(Credentials, Instant)>>,
    http: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl S3 {
    pub fn new(options: S3Options) -> anyhow::Result<Self> {
        let (scheme, host, path_style) = match &options.endpoint {
            Some(endpoint) => {
                let (scheme, host) = endpoint
                    .split_once("://")
                    .ok_or_else(|| anyhow!("the S3 endpoint {endpoint:?} needs http:// or https://"))?;
                (scheme.to_string(), host.trim_end_matches('/').to_string(), true)
            }
            None => ("https".to_string(), format!("{}.s3.{}.amazonaws.com", options.bucket, options.region), false),
        };
        let prefix = options.prefix.trim_matches('/');
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            bucket: options.bucket,
            prefix: if prefix.is_empty() { String::new() } else { format!("{prefix}/") },
            region: options.region,
            scheme,
            host,
            path_style,
            credentials: options.credentials,
            metadata_endpoint: options.metadata_endpoint.unwrap_or_else(|| DEFAULT_METADATA_ENDPOINT.into()),
            role_credentials: Mutex::new(None),
            http: Client::builder(TokioExecutor::new()).build(https),
        })
    }

    pub async fn put(&self, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
        self.send(Method::PUT, key, Vec::new(), body).await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match self.send(Method::GET, key, Vec::new(), Vec::new()).await {
            Ok(body) => Ok(Some(body.to_vec())),
            Err(err) if err.downcast_ref::<NotFound>().is_some() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.send(Method::DELETE, key, Vec::new(), Vec::new()).await?;
        Ok(())
    }

    /// Keys under `prefix` (relative to the store's own prefix) with sizes.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![("list-type".to_string(), "2".to_string()), ("prefix".to_string(), format!("{}{prefix}", self.prefix))];
            if let Some(token) = &token {
                query.push(("continuation-token".to_string(), token.clone()));
            }
            let body = self.send(Method::GET, "", query, Vec::new()).await?;
            let xml = String::from_utf8_lossy(&body);
            for item in elements(&xml, "Contents") {
                let key = elements(item, "Key").first().map(|k| unescape(k)).context("listing entry without a Key")?;
                let size = elements(item, "Size").first().and_then(|s| s.parse().ok()).unwrap_or(0);
                if let Some(relative) = key.strip_prefix(&self.prefix) {
                    out.push((relative.to_string(), size));
                }
            }
            token = match elements(&xml, "IsTruncated").first() {
                Some(&"true") => Some(unescape(elements(&xml, "NextContinuationToken").first().context("truncated listing without a token")?)),
                _ => None,
            };
            if token.is_none() {
                return Ok(out);
            }
        }
    }

    async fn send(&self, method: Method, key: &str, query: Vec<(String, String)>, body: Vec<u8>) -> anyhow::Result<Bytes> {
        let credentials = self.credentials().await?;
        let object = if key.is_empty() { String::new() } else { format!("{}{key}", self.prefix) };
        let path = match self.path_style {
            true => format!("/{}/{}", self.bucket, uri_encode(&object, false)),
            false => format!("/{}", uri_encode(&object, false)),
        };
        let path = if key.is_empty() && self.path_style { path.trim_end_matches('/').to_string() } else { path };
        let query = canonical_query(&query);
        let payload_hash = hex::encode(Sha256::digest(&body));
        let mut headers = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date(SystemTime::now())),
        ];
        if let Some(token) = &credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        let auth = authorization(&credentials, &self.region, method.as_str(), &path, &query, &headers, &payload_hash);

        let uri = match query.is_empty() {
            true => format!("{}://{}{path}", self.scheme, self.host),
            false => format!("{}://{}{path}?{query}", self.scheme, self.host),
        };
        let mut request = Request::builder().method(method.clone()).uri(uri).header("authorization", auth);
        for (name, value) in headers.iter().filter(|(name, _)| name != "host") {
            request = request.header(name, value);
        }
        let response = tokio::time::timeout(REQUEST_TIMEOUT, self.http.request(request.body(Full::new(Bytes::from(body)))?))
            .await
            .map_err(|_| anyhow!("S3 {method} {key:?} timed out"))??;
        let status = response.status();
        let bytes = tokio::time::timeout(REQUEST_TIMEOUT, response.into_body().collect())
            .await
            .map_err(|_| anyhow!("S3 {method} {key:?} timed out"))??
            .to_bytes();
        if status.is_success() {
            return Ok(bytes);
        }
        let text = String::from_utf8_lossy(&bytes);
        let code = elements(&text, "Code").first().map(|c| c.to_string());
        // A missing object, not a missing bucket (which is also a 404).
        if method == Method::GET && status == StatusCode::NOT_FOUND && matches!(code.as_deref(), None | Some("NoSuchKey")) {
            return Err(NotFound.into());
        }
        if matches!(status, StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN) && self.credentials.is_none() {
            // Possibly rotated role credentials; fetch fresh ones next time.
            *self.role_credentials.lock().await = None;
        }
        let code = code.unwrap_or_else(|| text.chars().take(200).collect());
        bail!("S3 {method} {key:?} returned {status}: {code}")
    }

    async fn credentials(&self) -> anyhow::Result<Credentials> {
        if let Some(credentials) = &self.credentials {
            return Ok(credentials.clone());
        }
        let mut cached = self.role_credentials.lock().await;
        if let Some((credentials, at)) = cached.as_ref() {
            if at.elapsed() < ROLE_CREDENTIALS_TTL {
                return Ok(credentials.clone());
            }
        }
        let credentials = self.fetch_role_credentials().await.context("no AWS keys set and no instance role credentials")?;
        *cached = Some((credentials.clone(), Instant::now()));
        Ok(credentials)
    }

    /// IMDSv2: a session token, then the role name, then its keys.
    async fn fetch_role_credentials(&self) -> anyhow::Result<Credentials> {
        let base = self.metadata_endpoint.trim_end_matches('/');
        let token = self
            .metadata(Method::PUT, &format!("{base}/latest/api/token"), ("x-aws-ec2-metadata-token-ttl-seconds", "300"))
            .await?;
        let token_header = ("x-aws-ec2-metadata-token", token.as_str());
        let roles = self.metadata(Method::GET, &format!("{base}/latest/meta-data/iam/security-credentials/"), token_header).await?;
        let role = roles.lines().next().filter(|r| !r.is_empty()).context("the instance has no IAM role")?;
        let keys = self
            .metadata(Method::GET, &format!("{base}/latest/meta-data/iam/security-credentials/{role}"), token_header)
            .await?;
        let keys: serde_json::Value = serde_json::from_str(&keys)?;
        let field = |name: &str| keys[name].as_str().map(str::to_string).with_context(|| format!("role credentials without {name}"));
        Ok(Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("Token")?),
        })
    }

    async fn metadata(&self, method: Method, uri: &str, header: (&str, &str)) -> anyhow::Result<String> {
        let request = Request::builder().method(method).uri(uri).header(header.0, header.1).body(Full::default())?;
        let response = tokio::time::timeout(Duration::from_secs(2), self.http.request(request))
            .await
            .map_err(|_| anyhow!("the instance metadata service did not answer"))??;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        if !status.is_success() {
            bail!("instance metadata {uri} returned {status}");
        }
        Ok(String::from_utf8(body.to_vec())?)
    }
}

#[derive(Debug)]
struct NotFound;

impl std::fmt::Display for NotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("not found")
    }
}

impl std::error::Error for NotFound {}

/// The `Authorization` header for a request. `headers` must include `host`,
/// `x-amz-content-sha256` and `x-amz-date`, with lowercase names.
fn authorization(
    credentials: &Credentials,
    region: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let mut headers = headers.iter().map(|(n, v)| (n.to_lowercase(), v.trim().to_string())).collect::<Vec<_>>();
    headers.sort();
    let amz_date = headers.iter().find(|(n, _)| n == "x-amz-date").map(|(_, v)| v.clone()).unwrap_or_default();
    let signed = headers.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(";");
    let canonical_headers = headers.iter().map(|(n, v)| format!("{n}:{v}\n")).collect::<String>();
    let canonical_request = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed}\n{payload_hash}");

    let scope = format!("{}/{region}/s3/aws4_request", &amz_date[..8]);
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex::encode(Sha256::digest(canonical_request)));
    let key = [&amz_date[..8], region, "s3", "aws4_request"]
        .iter()
        .fold(format!("AWS4{}", credentials.secret_access_key).into_bytes(), |key, part| hmac(&key, part.as_bytes()));
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}", credentials.access_key_id)
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn canonical_query(params: &[(String, String)]) -> String {
    let mut encoded = params.iter().map(|(k, v)| (uri_encode(k, true), uri_encode(v, true))).collect::<Vec<_>>();
    encoded.sort();
    encoded.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

/// SigV4 URI encoding: everything but unreserved characters, and `/` too
/// unless it separates path segments.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `YYYYMMDDTHHMMSSZ` in UTC.
fn amz_date(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z", rem / 3_600, rem % 3_600 / 60, rem % 60)
}

/// The text inside each `<tag>…</tag>`, outermost first. Enough for the
/// flat XML S3 returns for listings and errors.
fn elements<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(&after[..end]);
        rest = &after[end + close.len()..];
    }
    out
}

fn unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The four worked examples from AWS's "Signature Calculations for the
    // Authorization Header" page for S3 (also used by many other SigV4
    // implementations' test suites).
    fn example_credentials() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        }
    }

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn sign(method: &str, path: &str, query: &[(&str, &str)], extra: &[(&str, &str)], payload_hash: &str) -> String {
        let mut headers = vec![
            ("host".to_string(), "examplebucket.s3.amazonaws.com".to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        headers.extend(extra.iter().map(|(n, v)| (n.to_string(), v.to_string())));
        let query = canonical_query(&query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>());
        authorization(&example_credentials(), "us-east-1", method, &uri_encode(path, false), &query, &headers, payload_hash)
    }

    fn signature(auth: &str) -> &str {
        auth.rsplit("Signature=").next().unwrap()
    }

    #[test]
    fn get_object_matches_the_aws_example() {
        let auth = sign("GET", "/test.txt", &[], &[("Range", "bytes=0-9")], EMPTY_SHA256);
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "));
        assert_eq!(signature(&auth), "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
    }

    #[test]
    fn put_object_matches_the_aws_example() {
        let payload_hash = hex::encode(Sha256::digest(b"Welcome to Amazon S3."));
        let extra = [("Date", "Fri, 24 May 2013 00:00:00 GMT"), ("x-amz-storage-class", "REDUCED_REDUNDANCY")];
        let auth = sign("PUT", "/test$file.text", &[], &extra, &payload_hash);
        assert_eq!(signature(&auth), "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd");
    }

    #[test]
    fn get_bucket_lifecycle_matches_the_aws_example() {
        let auth = sign("GET", "/", &[("lifecycle", "")], &[], EMPTY_SHA256);
        assert_eq!(signature(&auth), "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543");
    }

    #[test]
    fn list_objects_matches_the_aws_example() {
        let auth = sign("GET", "/", &[("max-keys", "2"), ("prefix", "J")], &[], EMPTY_SHA256);
        assert_eq!(signature(&auth), "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7");
    }

    #[test]
    fn amz_date_formats_utc() {
        // 2013-05-24T00:00:00Z and 2024-02-29T23:59:59Z (a leap day).
        assert_eq!(amz_date(UNIX_EPOCH + Duration::from_secs(1_369_353_600)), "20130524T000000Z");
        assert_eq!(amz_date(UNIX_EPOCH + Duration::from_secs(1_709_251_199)), "20240229T235959Z");
    }
}
