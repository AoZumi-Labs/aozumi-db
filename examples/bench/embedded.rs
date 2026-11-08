//! The same YCSB workloads straight on SQLite's C library, in this process:
//! one connection per client thread, WAL, the given `synchronous` level, a
//! 5 s busy timeout. No network, protocol or server, so this is the ceiling
//! an embedded app gets, for comparison with the servers. The statements,
//! arguments and key popularity are the ones the servers get (`ycsb.rs`),
//! read from the same pipeline requests.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::SeedableRng;
use rusqlite::types::Value as Sql;
use rusqlite::{params_from_iter, Connection, ErrorCode};
use serde_json::Value;

use crate::{ms, pct, ycsb};

fn open(path: &Path, synchronous: &str) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    conn.execute_batch(&format!("PRAGMA journal_mode = WAL; PRAGMA synchronous = {synchronous};")).unwrap();
    conn
}

/// A pipeline `execute` request's SQL and arguments.
fn statement(request: &Value) -> (String, Vec<Sql>) {
    let stmt = &request["stmt"];
    let args = stmt["args"]
        .as_array()
        .map(|args| {
            args.iter()
                .map(|arg| match arg["type"].as_str() {
                    Some("integer") => Sql::Integer(arg["value"].as_str().unwrap().parse().unwrap()),
                    Some("text") => Sql::Text(arg["value"].as_str().unwrap().to_string()),
                    other => panic!("argument type {other:?}"),
                })
                .collect()
        })
        .unwrap_or_default();
    (stmt["sql"].as_str().unwrap().to_string(), args)
}

/// Runs one request; false if SQLite answered busy (YCSB counts it failed).
fn run(conn: &Connection, request: &Value) -> bool {
    let (sql, args) = statement(request);
    let mut stmt = conn.prepare_cached(&sql).unwrap();
    let result = match stmt.readonly() {
        // Read every row and column, as a server reads them to answer.
        true => stmt.query(params_from_iter(args)).and_then(|mut rows| {
            while let Some(row) = rows.next()? {
                for i in 0..row.as_ref().column_count() {
                    let _: Sql = row.get(i)?;
                }
            }
            Ok(())
        }),
        false => stmt.execute(params_from_iter(args)).map(|_| ()),
    };
    match result {
        Ok(()) => true,
        Err(rusqlite::Error::SqliteFailure(e, _)) if matches!(e.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => false,
        Err(err) => panic!("{sql}: {err}"),
    }
}

pub fn ycsb(dir: &Path, records: u64, clients: usize, duration: Duration, workloads: &[char], synchronous: &str) {
    let path = dir.join("embedded.db");
    {
        let conn = open(&path, synchronous);
        run(&conn, &ycsb::schema());
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        conn.execute_batch("BEGIN").unwrap();
        for n in 0..records {
            run(&conn, &ycsb::insert(n, &mut rng));
        }
        conn.execute_batch("COMMIT").unwrap();
    }
    println!("| workload | ops/s | p50 | p99 | p99.9 | by operation (p50 / p99) | failed: busy or timed out |");
    println!("|---|---:|---:|---:|---:|---|---:|");
    let keys = ycsb::Keyspace::new(records);
    for name in workloads {
        let Some(workload) = ycsb::workload(*name) else { continue };
        let workload = Arc::new(workload);
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let threads = (0..clients)
            .map(|c| {
                let (workload, keys, stop, path, synchronous) = (workload.clone(), keys.clone(), stop.clone(), path.clone(), synchronous.to_string());
                std::thread::spawn(move || {
                    let conn = open(&path, &synchronous);
                    let mut rng = rand::rngs::StdRng::seed_from_u64(c as u64);
                    let (mut latencies, mut failed) = (Vec::new(), 0u64);
                    while !stop.load(Ordering::Relaxed) {
                        let (op, requests) = workload.next(&keys, &mut rng);
                        let t = Instant::now();
                        match requests.iter().all(|request| run(&conn, request)) {
                            true => latencies.push((op.name(), t.elapsed())),
                            false => failed += 1,
                        }
                    }
                    (latencies, failed)
                })
            })
            .collect::<Vec<_>>();
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
        let (mut all, mut failed) = (Vec::new(), 0u64);
        for thread in threads {
            let (latencies, errors) = thread.join().unwrap();
            all.extend(latencies);
            failed += errors;
        }
        let elapsed = started.elapsed().as_secs_f64();
        let mut by_op: HashMap<&str, Vec<Duration>> = HashMap::new();
        for (op, latency) in &all {
            by_op.entry(*op).or_default().push(*latency);
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
        let failed = match failed {
            0 => "0".to_string(),
            n => format!("{n} ({:.1}%)", 100.0 * n as f64 / (n as f64 + all.len() as f64)),
        };
        println!(
            "| {} | {:.0} | {:.2} ms | {:.2} ms | {:.2} ms | {detail} | {failed} |",
            workload.name,
            overall.len() as f64 / elapsed,
            ms(pct(&overall, 0.5)),
            ms(pct(&overall, 0.99)),
            ms(pct(&overall, 0.999)),
        );
    }
}
