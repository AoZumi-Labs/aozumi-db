//! Benchmarks for aozumi-db, over HTTP as clients use it.
//!
//!     cargo run --release --example bench -- ycsb [A-F...] [--records N] [--clients N] [--seconds N]
//!     cargo run --release --example bench -- latency [--rate N] [--clients N] [--seconds N]
//!     cargo run --release --example bench -- micro [--clients N] [--seconds N]
//!
//! Add `--url http://host:port` to benchmark a server running elsewhere
//! instead of one in this process: another aozumi-db, or any server that
//! speaks libSQL's HTTP protocol (such as libSQL's own server), against an
//! empty database; add `--token <token>` when it needs one.
//!
//! Add `--prod` to run the in-process server as production runs it: a
//! secret, so every request's token is checked; backups to a directory, a
//! snapshot an hour and the WAL shipped every 5 s (the shipper is then the
//! only checkpointer); the default limits on size and statement time. The
//! benchmark's database is created through the admin API, without a request
//! rate limit, and reached with a token, as an app reaches its own.
//!
//! The server runs in this process on its own 4-thread runtime; clients run
//! on another 2-thread runtime over keep-alive HTTP/1.1, as `@libsql/client`
//! does. Clients and server share the machine, so compare runs made on the
//! same machine only.
//!
//! - `ycsb`: the YCSB core workloads (see ycsb.rs), closed loop: every
//!   client sends its next request as soon as the last one returns. Requests
//!   the server refuses as busy count as failed, not as throughput.
//! - `latency`: single-row write transactions arriving on a fixed schedule
//!   (open loop), timed from when each was due, so time spent queued
//!   behind a slow writer counts (no coordinated omission). The same
//!   method as the storage engine's own latency suite.
//! - `micro`: point reads, single-row inserts and 100-row transactions,
//!   plus the bare engine's point-read rate for comparison.

mod ycsb;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

struct Args {
    mode: String,
    workloads: Vec<char>,
    records: u64,
    clients: usize,
    seconds: u64,
    rate: u64,
    url: Option<String>,
    token: Option<String>,
    prod: bool,
}

/// `Bearer <token>` for every pipeline request, when the server checks tokens.
static AUTH: OnceLock<String> = OnceLock::new();
/// The secret of the in-process server under `--prod`.
const PROD_SECRET: &str = "bench-secret-not-for-production-0123456789abcdef";

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args {
        mode: args.next().unwrap_or_else(|| "micro".into()),
        workloads: vec![],
        records: 100_000,
        clients: 32,
        seconds: 10,
        rate: 1000,
        url: None,
        token: None,
        prod: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => {
                parsed.url = Some(args.next().expect("a URL after --url").trim_end_matches('/').to_string());
                continue;
            }
            "--token" => {
                parsed.token = Some(args.next().expect("a token after --token"));
                continue;
            }
            "--prod" => {
                parsed.prod = true;
                continue;
            }
            _ => {}
        }
        let mut value = || args.next().and_then(|v| v.parse().ok()).expect("a number after the flag");
        match arg.as_str() {
            "--records" => parsed.records = value(),
            "--clients" => parsed.clients = value() as usize,
            "--seconds" => parsed.seconds = value(),
            "--rate" => parsed.rate = value(),
            w => parsed.workloads.extend(w.chars().filter(|c| c.is_ascii_alphabetic())),
        }
    }
    if parsed.workloads.is_empty() {
        parsed.workloads = "ABCDEF".chars().collect();
    }
    parsed
}

fn main() {
    let args = parse_args();
    let dir = tempfile::tempdir().unwrap();
    let server = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    // The database's file in the in-process server's data directory.
    let database = if args.prod { "bench" } else { "default" };
    let url = match &args.url {
        Some(url) => {
            if let Some(token) = &args.token {
                AUTH.set(format!("Bearer {token}")).unwrap();
            }
            println!("server: {url}");
            format!("{url}/v2/pipeline")
        }
        None => {
            let prod = args.prod;
            let data = dir.path().to_path_buf();
            let addr = server.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let mut config = match prod {
                    false => aozumi_db::Config::new(data.join("data"), None),
                    true => production_config(&data),
                };
                // As the server reads it, so one binary measures both ways.
                config.sync_method = match std::env::var("AOZUMI_DB_SYNC_METHOD").as_deref() {
                    Ok("fsync") => aozumi_db::SyncMethod::Fsync,
                    _ => aozumi_db::SyncMethod::DataSync,
                };
                tokio::spawn(async move { aozumi_db::serve(listener, config).await.unwrap() });
                addr
            });
            println!("sync method: {}", std::env::var("AOZUMI_DB_SYNC_METHOD").unwrap_or_else(|_| "fdatasync".into()));
            match args.prod {
                false => println!("server: in this process, dev mode (no token check, no backups)"),
                true => {
                    let token = server.block_on(bench_database(addr));
                    AUTH.set(format!("Bearer {token}")).unwrap();
                    println!("server: in this process, as production runs it (tokens checked, backups with the WAL shipped every 5 s)");
                }
            }
            format!("http://{addr}/v2/pipeline")
        }
    };
    let local = args.url.is_none();
    let data = dir.path().join("data");
    let clients = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let duration = Duration::from_secs(args.seconds);
    let machine = format!("{} cores", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));

    match args.mode.as_str() {
        "ycsb" => clients.block_on(async {
            println!("YCSB, {} records, {} clients, {}s per workload, {machine}\n", args.records, args.clients, args.seconds);
            ycsb_load(&url, args.records).await;
            println!("| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |");
            println!("|---|---:|---:|---:|---:|---|---:|");
            // One keyspace for all workloads: inserts in D and E add records.
            let keys = ycsb::Keyspace::new(args.records);
            for name in args.workloads {
                let Some(workload) = ycsb::workload(name) else { continue };
                let workload = Arc::new(workload);
                let keys = keys.clone();
                let label = format!("{}", workload.name);
                closed_loop(&label, &url, args.clients, duration, move |rng| {
                    let (op, requests) = workload.next(&keys, rng);
                    (op.name(), requests)
                })
                .await;
            }
        }),
        "latency" => clients.block_on(async {
            println!("Open-loop write latency, {} tx/s, {} connections, {}s, {machine}\n", args.rate, args.clients, args.seconds);
            assert!(send(&new_client(), &url, vec![execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")]).await);
            open_loop(&url, args.clients, args.rate, duration).await;
            if local {
                let wal = std::fs::metadata(data.join(format!("{database}.db-wal"))).map(|m| m.len()).unwrap_or(0);
                println!("\nWAL at the end: {:.1} MB", wal as f64 / 1e6);
            }
        }),
        _ => {
            clients.block_on(micro(&url, args.clients, duration));
            if local {
                engine_baseline(&data.join(format!("{database}.db")), duration);
            }
        }
    }
}

/// The in-process server with production's settings (hosting/install.sh in
/// AOZUMI's hosting): checked tokens, backups with the WAL shipped every
/// 5 s, and the default size and statement limits.
fn production_config(dir: &std::path::Path) -> aozumi_db::Config {
    let mut config = aozumi_db::Config::new(dir.join("data"), Some(PROD_SECRET.as_bytes().to_vec()));
    config.backup = Some(aozumi_db::BackupConfig {
        target: aozumi_db::BackupTarget::directory(dir.join("backups")),
        keep: 48,
        every: Some(Duration::from_secs(3600)),
        wal_every: Some(Duration::from_secs(5)),
    });
    config.default_limits = aozumi_db::Limits { max_size_bytes: Some(268_435_456), max_statement_ms: Some(5000), max_requests_per_second: None };
    config
}

/// Create the benchmark's database through the admin API and mint its token.
async fn bench_database(addr: std::net::SocketAddr) -> String {
    let client = new_client();
    let admin = |path: &str, body: Value| {
        Request::post(format!("http://{addr}{path}"))
            .header("authorization", format!("Bearer {PROD_SECRET}"))
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap()
    };
    let created = client.request(admin("/v1/databases", json!({"name": "bench"}))).await.unwrap();
    assert!(created.status().is_success(), "creating the benchmark's database: HTTP {}", created.status());
    let minted = client.request(admin("/v1/databases/bench/tokens", json!({}))).await.unwrap();
    let body: Value = serde_json::from_slice(&minted.into_body().collect().await.unwrap().to_bytes()).unwrap();
    body["token"].as_str().expect("a token").to_string()
}

pub fn int(n: u64) -> Value {
    json!({"type": "integer", "value": n.to_string()})
}

pub fn text(s: String) -> Value {
    json!({"type": "text", "value": s})
}

pub fn execute(sql: &str) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql}})
}

pub fn execute_args(sql: &str, args: Value) -> Value {
    json!({"type": "execute", "stmt": {"sql": sql, "args": args}})
}

/// "0", or "n (x%)" plus how many of those timed out rather than being
/// refused as busy.
fn failed_cell(failed: u64, succeeded: usize) -> String {
    let timed_out = TIMED_OUT.swap(0, Ordering::Relaxed);
    match failed {
        0 => "0".to_string(),
        n if timed_out == 0 => format!("{n} ({:.1}%)", 100.0 * n as f64 / (n as f64 + succeeded as f64)),
        n => format!("{n} ({:.1}%), {timed_out} timed out", 100.0 * n as f64 / (n as f64 + succeeded as f64)),
    }
}

fn new_client() -> HttpClient {
    Client::builder(TokioExecutor::new()).pool_max_idle_per_host(1).build_http()
}

/// A request with no answer by then counts as failed, so a server that
/// stalls shows up in the results instead of hanging the benchmark.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
static TIMED_OUT: AtomicU64 = AtomicU64::new(0);

/// Send one pipeline. Returns false if the server refused it as busy
/// (`SQLITE_BUSY`) or did not answer within `REQUEST_TIMEOUT`: YCSB counts
/// those as failed operations. Any other error stops the benchmark, since
/// it would make the numbers meaningless.
async fn send(client: &HttpClient, url: &str, mut requests: Vec<Value>) -> bool {
    requests.push(json!({"type": "close"}));
    let body = json!({"baton": null, "requests": requests}).to_string();
    let mut request = Request::post(url);
    if let Some(auth) = AUTH.get() {
        request = request.header("authorization", auth);
    }
    let request = request.body(Full::new(Bytes::from(body))).unwrap();
    let exchange = async {
        let response = client.request(request).await.unwrap();
        let status = response.status();
        (status, response.into_body().collect().await.unwrap().to_bytes())
    };
    let Ok((status, body)) = tokio::time::timeout(REQUEST_TIMEOUT, exchange).await else {
        TIMED_OUT.fetch_add(1, Ordering::Relaxed);
        return false;
    };
    assert!(status.is_success(), "HTTP {status}: {}", String::from_utf8_lossy(&body));
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    let mut ok = true;
    for result in parsed["results"].as_array().unwrap() {
        if result["type"] == "ok" {
            continue;
        }
        let busy = result["error"]["code"].as_str().is_some_and(|c| c.starts_with("SQLITE_BUSY"))
            || result["error"]["message"].as_str().is_some_and(|m| m.contains("database is locked"));
        assert!(busy, "{result}");
        ok = false;
    }
    ok
}

async fn ycsb_load(url: &str, records: u64) {
    let client = new_client();
    assert!(send(&client, url, vec![ycsb::schema()]).await);
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let started = Instant::now();
    for chunk in (0..records).collect::<Vec<_>>().chunks(1000) {
        let mut requests = vec![execute("BEGIN")];
        requests.extend(chunk.iter().map(|n| ycsb::insert(*n, &mut rng)));
        requests.push(execute("COMMIT"));
        assert!(send(&client, url, requests).await, "loading refused as busy");
    }
    let secs = started.elapsed().as_secs_f64();
    println!("load: {records} records in {secs:.1}s ({:.0} records/s, 1000 per transaction)\n", records as f64 / secs);
}

/// Clients send back to back for `duration`; prints one table row.
async fn closed_loop(
    label: &str,
    url: &str,
    clients: usize,
    duration: Duration,
    next: impl Fn(&mut rand::rngs::StdRng) -> (&'static str, Vec<Value>) + Send + Sync + 'static,
) {
    let next = Arc::new(next);
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let tasks = (0..clients)
        .map(|c| {
            let (next, stop, url) = (next.clone(), stop.clone(), url.to_string());
            tokio::spawn(async move {
                let client = new_client();
                let mut rng = rand::rngs::StdRng::seed_from_u64(c as u64);
                let mut latencies: Vec<(&'static str, Duration)> = Vec::new();
                let mut failed = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let (op, requests) = next(&mut rng);
                    let t = Instant::now();
                    match send(&client, &url, requests).await {
                        true => latencies.push((op, t.elapsed())),
                        false => failed += 1,
                    }
                }
                (latencies, failed)
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    let (mut all, mut failed) = (Vec::new(), 0u64);
    for task in tasks {
        let (latencies, errors) = task.await.unwrap();
        all.extend(latencies);
        failed += errors;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let errors = failed_cell(failed, all.len());
    let mut by_op: HashMap<&str, Vec<Duration>> = HashMap::new();
    for (op, latency) in &all {
        by_op.entry(op).or_default().push(*latency);
    }
    let mut overall = all.iter().map(|(_, l)| *l).collect::<Vec<_>>();
    overall.sort();
    let mut ops = by_op.into_iter().collect::<Vec<_>>();
    ops.sort_by_key(|(op, _)| *op);
    let detail = ops
        .iter_mut()
        .map(|(op, l)| {
            l.sort();
            format!("{op} {:.2} / {:.2} ms", ms(pct(l, 0.5)), ms(pct(l, 0.99)))
        })
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "| {label} | {:.0} | {:.2} ms | {:.2} ms | {:.2} ms | {detail} | {errors} |",
        overall.len() as f64 / elapsed,
        ms(pct(&overall, 0.5)),
        ms(pct(&overall, 0.99)),
        ms(pct(&overall, 0.999))
    );
}

/// Transactions due every 1/rate seconds, spread over `connections`; each
/// is timed from when it was due.
async fn open_loop(url: &str, connections: usize, rate: u64, duration: Duration) {
    let interval = Duration::from_secs_f64(connections as f64 / rate as f64);
    let start = Instant::now() + Duration::from_millis(100);
    let tasks = (0..connections)
        .map(|c| {
            let url = url.to_string();
            tokio::spawn(async move {
                let client = new_client();
                let mut rng = rand::rngs::StdRng::seed_from_u64(c as u64);
                let offset = interval.mul_f64(c as f64 / connections as f64);
                let (mut latencies, mut failed) = (Vec::new(), 0u64);
                let mut n = 0u32;
                loop {
                    let due = start + offset + interval * n;
                    if due > start + duration {
                        break;
                    }
                    tokio::time::sleep_until(due.into()).await;
                    let data: String = (0..100).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
                    match send(&client, &url, vec![execute_args("INSERT INTO test_table (data) VALUES (?)", json!([text(data)]))]).await {
                        true => latencies.push(due.elapsed()),
                        false => failed += 1,
                    }
                    n += 1;
                }
                (latencies, failed)
            })
        })
        .collect::<Vec<_>>();
    let (mut all, mut failed) = (Vec::new(), 0u64);
    for task in tasks {
        let (latencies, errors) = task.await.unwrap();
        all.extend(latencies);
        failed += errors;
    }
    all.sort();
    let failed = failed_cell(failed, all.len());
    if all.is_empty() {
        all.push(REQUEST_TIMEOUT);
    }
    println!("| offered | committed | p50 | p90 | p99 | p99.9 | max | failed: busy or timed out |");
    println!("|---:|---:|---:|---:|---:|---:|---:|---:|");
    println!(
        "| {rate}/s | {:.0}/s | {:.2} ms | {:.2} ms | {:.2} ms | {:.2} ms | {:.2} ms | {failed} |",
        all.len() as f64 / duration.as_secs_f64(),
        ms(pct(&all, 0.5)),
        ms(pct(&all, 0.9)),
        ms(pct(&all, 0.99)),
        ms(pct(&all, 0.999)),
        ms(*all.last().unwrap())
    );
}

async fn micro(url: &str, clients: usize, duration: Duration) {
    const ROWS: u64 = 10_000;
    let client = new_client();
    let mut requests = vec![
        execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)"),
        execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)"),
        execute("BEGIN"),
    ];
    requests.extend((0..ROWS).map(|k| execute_args("INSERT INTO kv VALUES (?, ?)", json!([int(k), text(format!("value-{k}"))]))));
    requests.push(execute("COMMIT"));
    assert!(send(&client, url, requests).await);

    println!("micro, {clients} clients, {}s each\n", duration.as_secs());
    println!("| scenario | ops/s | p50 | p99 | p99.9 | | failed: busy or timed out |");
    println!("|---|---:|---:|---:|---:|---|---:|");
    closed_loop("point read", url, clients, duration, |rng| {
        ("read", vec![execute_args("SELECT v FROM kv WHERE k = ?", json!([int(rng.gen_range(0..ROWS))]))])
    })
    .await;
    closed_loop("single-row insert", url, clients, duration, |rng| {
        ("insert", vec![execute_args("INSERT INTO log (n, body) VALUES (?, 'hello world')", json!([int(rng.gen::<u32>() as u64)]))])
    })
    .await;
    closed_loop("100-row transaction", url, clients.min(8), duration, |rng| {
        let mut requests = vec![execute("BEGIN")];
        requests.extend((0..100).map(|_| execute_args("INSERT INTO log (n, body) VALUES (?, 'batch row')", json!([int(rng.gen::<u32>() as u64)]))));
        requests.push(execute("COMMIT"));
        ("txn", requests)
    })
    .await;
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// The same point reads straight on the engine, one thread, no server.
fn engine_baseline(file: &std::path::Path, duration: Duration) {
    use turso_core::{Database, SqliteDialect, Value as Core};
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = Database::open_file(io, file.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let started = Instant::now();
    let mut n = 0u64;
    while started.elapsed() < duration {
        let mut stmt = conn.prepare("SELECT v FROM kv WHERE k = ?").unwrap();
        stmt.bind_at(std::num::NonZero::new(1).unwrap(), Core::from_i64((n % 10_000) as i64)).unwrap();
        while stmt.run_one_step_blocking(|| Ok(()), || Ok(())).unwrap().is_some() {}
        n += 1;
    }
    println!(
        "\nbare engine, one thread, same point read (prepare + run): {:.0} ops/s",
        n as f64 / started.elapsed().as_secs_f64()
    );
}
