//! Per-database limits and the request-rate limiter.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Deserializer, Serialize};

/// `None` means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// The database file may not grow past this.
    pub max_size_bytes: Option<u64>,
    /// A statement running longer than this is interrupted.
    pub max_statement_ms: Option<u64>,
    /// Pipeline requests per second, with bursts of up to one second's worth.
    pub max_requests_per_second: Option<f64>,
}

/// A change to some limits: a missing field keeps its value, `null` lifts it.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsPatch {
    #[serde(default, deserialize_with = "present")]
    max_size_bytes: Option<Option<u64>>,
    #[serde(default, deserialize_with = "present")]
    max_statement_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "present")]
    max_requests_per_second: Option<Option<f64>>,
}

fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(d).map(Some)
}

impl Limits {
    pub fn apply(mut self, patch: &LimitsPatch) -> Result<Self, String> {
        if let Some(value) = patch.max_size_bytes {
            self.max_size_bytes = value;
        }
        if let Some(value) = patch.max_statement_ms {
            self.max_statement_ms = value;
        }
        if let Some(value) = patch.max_requests_per_second {
            self.max_requests_per_second = value;
        }
        if self.max_size_bytes == Some(0) || self.max_statement_ms == Some(0) {
            return Err("limits must be positive, or null for unlimited".into());
        }
        if self.max_requests_per_second.is_some_and(|rate| !(rate.is_finite() && rate > 0.0)) {
            return Err("max_requests_per_second must be a positive number, or null".into());
        }
        Ok(self)
    }
}

/// GCRA: each database's requests are spaced 1/rate seconds apart on
/// average, with a burst of up to `rate` requests at once.
#[derive(Default)]
pub struct RateLimiter {
    next_allowed: Mutex<HashMap<String, Instant>>,
}

impl RateLimiter {
    /// `Err` carries how long to wait before retrying.
    pub fn check(&self, db: &str, rate: f64) -> Result<(), Duration> {
        let now = Instant::now();
        let interval = Duration::from_secs_f64(1.0 / rate);
        let burst = Duration::from_secs_f64((rate.max(1.0) - 1.0) / rate);
        let mut next = self.next_allowed.lock().unwrap();
        let theoretical = next.get(db).copied().unwrap_or(now).max(now);
        let ahead = theoretical - now;
        if ahead > burst {
            return Err(ahead - burst);
        }
        next.insert(db.to_string(), theoretical + interval);
        Ok(())
    }

    pub fn forget(&self, db: &str) {
        self.next_allowed.lock().unwrap().remove(db);
    }
}
