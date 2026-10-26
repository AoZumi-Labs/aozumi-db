//! YCSB core workloads A–F over the libSQL HTTP protocol, following the
//! YCSB core workload definitions: a `usertable` of records with ten
//! 100-byte fields, keys `user<hash>`, Zipfian request popularity
//! (constant 0.99, scrambled), and these mixes:
//!
//! | workload | mix | key choice |
//! |---|---|---|
//! | A | 50% read, 50% update | zipfian |
//! | B | 95% read, 5% update | zipfian |
//! | C | 100% read | zipfian |
//! | D | 95% read, 5% insert | latest |
//! | E | 95% scan (1–100 rows), 5% insert | zipfian |
//! | F | 50% read, 50% read-modify-write | zipfian |

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rand::Rng;
use serde_json::{json, Value};

use crate::{execute, execute_args, int, text};

pub const FIELDS: usize = 10;
pub const FIELD_LEN: usize = 100;

#[derive(Clone, Copy, Debug)]
pub enum Op {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

impl Op {
    pub fn name(self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Update => "update",
            Op::Insert => "insert",
            Op::Scan => "scan",
            Op::ReadModifyWrite => "rmw",
        }
    }
}

pub struct Workload {
    pub name: char,
    mix: &'static [(Op, f64)],
    latest: bool,
}

pub fn workload(name: char) -> Option<Workload> {
    let (mix, latest): (&'static [(Op, f64)], bool) = match name.to_ascii_uppercase() {
        'A' => (&[(Op::Read, 0.5), (Op::Update, 0.5)], false),
        'B' => (&[(Op::Read, 0.95), (Op::Update, 0.05)], false),
        'C' => (&[(Op::Read, 1.0)], false),
        'D' => (&[(Op::Read, 0.95), (Op::Insert, 0.05)], true),
        'E' => (&[(Op::Scan, 0.95), (Op::Insert, 0.05)], false),
        'F' => (&[(Op::Read, 0.5), (Op::ReadModifyWrite, 0.5)], false),
        _ => return None,
    };
    Some(Workload { name: name.to_ascii_uppercase(), mix, latest })
}

/// Shared state: how many records exist (inserts add more) and the key
/// popularity distribution.
pub struct Keyspace {
    inserted: AtomicU64,
    zipf: Zipfian,
}

impl Keyspace {
    pub fn new(records: u64) -> Arc<Self> {
        Arc::new(Self { inserted: AtomicU64::new(records), zipf: Zipfian::new(records, 0.99) })
    }
}

/// YCSB's key naming: `user` and the FNV hash of the insertion number.
pub fn key(n: u64) -> String {
    format!("user{}", fnv64(n))
}

pub fn schema() -> Value {
    let fields = (0..FIELDS).map(|i| format!("field{i} TEXT")).collect::<Vec<_>>().join(", ");
    execute(&format!("CREATE TABLE usertable (ycsb_key TEXT PRIMARY KEY, {fields})"))
}

pub fn insert(n: u64, rng: &mut impl Rng) -> Value {
    let marks = vec!["?"; FIELDS + 1].join(", ");
    let mut args = vec![text(key(n))];
    args.extend((0..FIELDS).map(|_| text(field(rng))));
    execute_args(&format!("INSERT INTO usertable VALUES ({marks})"), Value::Array(args))
}

fn field(rng: &mut impl Rng) -> String {
    (0..FIELD_LEN).map(|_| rng.gen_range(b'a'..=b'z') as char).collect()
}

impl Workload {
    /// The next operation, as pipeline requests.
    pub fn next(&self, keys: &Keyspace, rng: &mut impl Rng) -> (Op, Vec<Value>) {
        let mut roll = rng.gen::<f64>();
        let op = self
            .mix
            .iter()
            .find(|(_, share)| {
                roll -= share;
                roll < 0.0
            })
            .map(|(op, _)| *op)
            .unwrap_or(self.mix[0].0);
        let existing = keys.inserted.load(Ordering::Relaxed);
        let pick = |rng: &mut _| match self.latest {
            // "latest": the most recently inserted records are hottest.
            true => existing - 1 - keys.zipf.next(rng).min(existing - 1),
            false => fnv64(keys.zipf.next(rng)) % existing,
        };
        let read = |n| execute_args("SELECT * FROM usertable WHERE ycsb_key = ?", json!([text(key(n))]));
        let update = |n, rng: &mut _| {
            let f = rand::Rng::gen_range(rng, 0..FIELDS);
            execute_args(&format!("UPDATE usertable SET field{f} = ? WHERE ycsb_key = ?"), json!([text(field(rng)), text(key(n))]))
        };
        let requests = match op {
            Op::Read => vec![read(pick(rng))],
            Op::Update => {
                let n = pick(rng);
                vec![update(n, rng)]
            }
            Op::Insert => vec![insert(keys.inserted.fetch_add(1, Ordering::Relaxed), rng)],
            Op::Scan => {
                let (n, len) = (pick(rng), rng.gen_range(1..=100u64));
                vec![execute_args(
                    "SELECT * FROM usertable WHERE ycsb_key >= ? ORDER BY ycsb_key LIMIT ?",
                    json!([text(key(n)), int(len)]),
                )]
            }
            Op::ReadModifyWrite => {
                let n = pick(rng);
                vec![read(n), update(n, rng)]
            }
        };
        (op, requests)
    }
}

/// YCSB's ZipfianGenerator (Gray et al., "Quickly generating billion-record
/// synthetic databases"): item 0 is the most popular.
pub struct Zipfian {
    items: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipfian {
    pub fn new(items: u64, theta: f64) -> Self {
        let zeta = |n: u64| (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let (zetan, zeta2) = (zeta(items), zeta(2));
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self { items, theta, alpha, zetan, eta }
    }

    pub fn next(&self, rng: &mut impl Rng) -> u64 {
        let u = rng.gen::<f64>();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64 % self.items
    }
}

/// FNV-1a over the 8 bytes of `n`, as YCSB's `Utils.fnvhash64`.
pub fn fnv64(n: u64) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in n.to_le_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0100_0000_01B3);
    }
    hash
}
