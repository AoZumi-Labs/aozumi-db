//! Benchmarks for aozumi-db, over HTTP as clients use it.
//!
//!     cargo run --release --example bench -- ycsb [A-F...] [--records N] [--clients N] [--seconds N]
//!     cargo run --release --example bench -- latency [--rate N] [--clients N] [--seconds N]
//!     cargo run --release --example bench -- micro [--clients N] [--seconds N]
//!
//! The server runs in this process on its own 4-thread runtime; clients run
//! on another 2-thread runtime over keep-alive HTTP/1.1, as `@libsql/client`
//! does. Clients and server share the machine, so compare runs made on the
//! same machine only.
//!
//! - `ycsb`: the YCSB core workloads (see ycsb.rs), closed loop: every
//!   client sends its next request as soon as the last one returns.
//! - `latency`: single-row write transactions arriving on a fixed schedule
//!   (open loop), timed from when each was due, so time spent queued
//!   behind a slow writer counts (no coordinated omission). The same
//!   method as the storage engine's own latency suite.
//! - `micro`: point reads, single-row inserts and 100-row transactions,
//!   plus the bare engine's point-read rate for comparison.

mod ycsb;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args { mode: args.next().unwrap_or_else(|| "micro".into()), workloads: vec![], records: 100_000, clients: 32, seconds: 10, rate: 1000 };
    while let Some(arg) = args.next() {
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
    let addr = server.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = aozumi_db::Config::new(dir.path().to_path_buf(), None);
        tokio::spawn(async move { aozumi_db::serve(listener, config).await.unwrap() });
        addr
    });
    let url = format!("http://{addr}/v2/pipeline");
    let clients = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let duration = Duration::from_secs(args.seconds);
    let machine = format!("{} cores", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));

    match args.mode.as_str() {
        "ycsb" => clients.block_on(async {
            println!("YCSB, {} records, {} clients, {}s per workload, {machine}\n", args.records, args.clients, args.seconds);
            ycsb_load(&url, args.records).await;
            println!("| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) |");
            println!("|---|---:|---:|---:|---:|---|");
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
            send(&new_client(), &url, vec![execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")]).await;
            open_loop(&url, args.clients, args.rate, duration).await;
        }),
        _ => {
            clients.block_on(micro(&url, args.clients, duration));
            engine_baseline(dir.path(), duration);
        }
    }
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

fn new_client() -> HttpClient {
    Client::builder(TokioExecutor::new()).pool_max_idle_per_host(1).build_http()
}

async fn send(client: &HttpClient, url: &str, mut requests: Vec<Value>) {
    requests.push(json!({"type": "close"}));
    let body = json!({"baton": null, "requests": requests}).to_string();
    let request = Request::post(url).body(Full::new(Bytes::from(body))).unwrap();
    let response = client.request(request).await.unwrap();
    assert!(response.status().is_success(), "HTTP {}", response.status());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    // An error inside the pipeline would make the numbers meaningless.
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    for result in parsed["results"].as_array().unwrap() {
        assert_eq!(result["type"], "ok", "{result}");
    }
}

async fn ycsb_load(url: &str, records: u64) {
    let client = new_client();
    send(&client, url, vec![ycsb::schema()]).await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let started = Instant::now();
    for chunk in (0..records).collect::<Vec<_>>().chunks(1000) {
        let mut requests = vec![execute("BEGIN")];
        requests.extend(chunk.iter().map(|n| ycsb::insert(*n, &mut rng)));
        requests.push(execute("COMMIT"));
        send(&client, url, requests).await;
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
                while !stop.load(Ordering::Relaxed) {
                    let (op, requests) = next(&mut rng);
                    let t = Instant::now();
                    send(&client, &url, requests).await;
                    latencies.push((op, t.elapsed()));
                }
                latencies
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    let elapsed = started.elapsed().as_secs_f64();
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
        "| {label} | {:.0} | {:.2} ms | {:.2} ms | {:.2} ms | {detail} |",
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
                let mut latencies = Vec::new();
                let mut n = 0u32;
                loop {
                    let due = start + offset + interval * n;
                    if due > start + duration {
                        break;
                    }
                    tokio::time::sleep_until(due.into()).await;
                    let data: String = (0..100).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
                    send(&client, &url, vec![execute_args("INSERT INTO test_table (data) VALUES (?)", json!([text(data)]))]).await;
                    latencies.push(due.elapsed());
                    n += 1;
                }
                latencies
            })
        })
        .collect::<Vec<_>>();
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    all.sort();
    println!("| offered | completed | p50 | p90 | p99 | p99.9 | max |");
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    println!(
        "| {rate}/s | {:.0}/s | {:.2} ms | {:.2} ms | {:.2} ms | {:.2} ms | {:.2} ms |",
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
    send(&client, url, requests).await;

    println!("micro, {clients} clients, {}s each\n", duration.as_secs());
    println!("| scenario | ops/s | p50 | p99 | p99.9 | |");
    println!("|---|---:|---:|---:|---:|---|");
    closed_loop("point read", url, clients, duration, |rng| {
        ("read", vec![execute_args("SELECT v FROM kv WHERE k = ?", json!([int(rng.gen_range(0..ROWS))]))])
    })
    .await;
    closed_loop("single-row insert", url, clients, duration, |rng| {
        ("insert", vec![execute_args("INSERT INTO log (n, body) VALUES (?, 'hello world')", json!([int(rng.gen())]))])
    })
    .await;
    closed_loop("100-row transaction", url, clients.min(8), duration, |rng| {
        let mut requests = vec![execute("BEGIN")];
        requests.extend((0..100).map(|_| execute_args("INSERT INTO log (n, body) VALUES (?, 'batch row')", json!([int(rng.gen())]))));
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
fn engine_baseline(dir: &std::path::Path, duration: Duration) {
    use turso_core::{Database, SqliteDialect, Value as Core};
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = Database::open_file(io, dir.join("default.db").to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
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
