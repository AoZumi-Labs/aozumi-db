//! A stream is one database connection plus its stored SQL, kept open across
//! HTTP requests by a baton. Batons are random, single-use and rotated on
//! every request; a stream idle longer than the timeout is dropped, which
//! rolls back any open transaction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngCore;
use tokio::sync::Mutex;

use crate::console;
use crate::engine::{Conn, Db};
use crate::registry::Pool;
use crate::policy;
use crate::proto::{
    BatchCond, BatchResult, Error, Stmt, StmtResult, StreamRequest, StreamResponse, StreamResult,
};

pub struct Stream {
    pub db: String,
    pub read_only: bool,
    /// The database's token generation when the stream opened. Rotating the
    /// database's tokens past it ends the stream, whatever token comes next.
    pub token_generation: u64,
    /// The open database file the stream's connection is on. A database
    /// deleted and created again under the same name, as an in-place restore
    /// does, has another one, so the stream ends with the file it opened on.
    pub file: Db,
    pool: Arc<Pool>,
    /// The stream ran SQL that may leave state on the connection (a pragma
    /// setting, a temporary table), so it is not reused.
    dirty: bool,
    conn: Conn,
    sqls: HashMap<i32, String>,
    closed: bool,
}

impl Stream {
    pub fn new(db: String, read_only: bool, token_generation: u64, file: Db, conn: Conn, pool: Arc<Pool>) -> Self {
        Self { db, read_only, token_generation, file, pool, dirty: false, conn, sqls: HashMap::new(), closed: false }
    }

    /// A closed stream hands its connection back for reuse when it is clean.
    pub fn release(self) {
        if !self.dirty && self.conn.autocommit() {
            self.pool.put(self.conn, self.read_only);
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Whether running `requests` may write. Errs toward yes: a wrong yes
    /// costs a short wait in the writer queue, never correctness.
    pub fn may_write(&self, requests: &[StreamRequest]) -> bool {
        let stmt_may_write = |stmt: &Stmt| match (&stmt.sql, stmt.sql_id) {
            (Some(sql), _) => sql_may_write(sql),
            (None, Some(id)) => self.sqls.get(&id).is_none_or(|sql| sql_may_write(sql)),
            (None, None) => false,
        };
        !self.read_only
            && requests.iter().any(|request| match request {
                StreamRequest::Execute { stmt } => stmt_may_write(stmt),
                StreamRequest::Batch { batch } => batch.steps.iter().any(|step| stmt_may_write(&step.stmt)),
                StreamRequest::Sequence { sql, sql_id } => match (sql, sql_id) {
                    (Some(sql), _) => sql_may_write(sql),
                    (None, Some(id)) => self.sqls.get(id).is_none_or(|sql| sql_may_write(sql)),
                    (None, None) => false,
                },
                _ => false,
            })
    }

    /// Runs a console script (src/console.rs) on this stream's connection,
    /// then closes the stream, rolling back any transaction the script left
    /// open: a console request never holds one past its response.
    pub fn run_console(&mut self, split: console::Split) -> Vec<console::Outcome> {
        self.dirty |= split.texts().any(changes_connection);
        let outcomes = console::run(&self.conn, split);
        // A failed rollback leaves the transaction open, and `release` then drops the connection.
        if !self.conn.autocommit() {
            let _ = self.conn.execute_script("ROLLBACK");
        }
        self.closed = true;
        outcomes
    }

    /// Inside a transaction, the stream may hold the engine's write lock
    /// across requests, so it must not queue behind writers waiting for it.
    pub fn in_transaction(&self) -> bool {
        !self.conn.autocommit()
    }

    /// Blocks: call from the blocking pool.
    pub fn handle(&mut self, request: &StreamRequest) -> StreamResult {
        let outcome = if self.closed {
            Err(Error::new("the stream is closed", "STREAM_CLOSED"))
        } else {
            self.run(request)
        };
        match outcome {
            Ok(response) => StreamResult::Ok { response },
            Err(error) => StreamResult::Error { error },
        }
    }

    fn run(&mut self, request: &StreamRequest) -> Result<StreamResponse, Error> {
        Ok(match request {
            StreamRequest::Close => {
                self.closed = true;
                StreamResponse::Close
            }
            StreamRequest::Execute { stmt } => StreamResponse::Execute { result: self.execute(stmt)? },
            StreamRequest::Batch { batch } => {
                let mut result = BatchResult { step_results: Vec::new(), step_errors: Vec::new() };
                for step in &batch.steps {
                    let run = step.condition.as_ref().is_none_or(|c| self.holds(c, &result));
                    let (ok, err) = match run {
                        false => (None, None),
                        true => match self.execute(&step.stmt) {
                            Ok(r) => (Some(r), None),
                            Err(e) => (None, Some(e)),
                        },
                    };
                    result.step_results.push(ok);
                    result.step_errors.push(err);
                }
                StreamResponse::Batch { result }
            }
            StreamRequest::Sequence { sql, sql_id } => {
                let sql = self.sql(sql.as_deref(), *sql_id)?;
                policy::check(&sql)?;
                self.dirty |= changes_connection(&sql);
                self.conn.execute_script(&sql)?;
                StreamResponse::Sequence
            }
            StreamRequest::StoreSql { sql_id, sql } => {
                self.sqls.insert(*sql_id, sql.clone());
                StreamResponse::StoreSql
            }
            StreamRequest::CloseSql { sql_id } => {
                self.sqls.remove(sql_id);
                StreamResponse::CloseSql
            }
            StreamRequest::GetAutocommit => StreamResponse::GetAutocommit { is_autocommit: self.autocommit() },
        })
    }

    fn execute(&mut self, stmt: &Stmt) -> Result<StmtResult, Error> {
        let sql = self.sql(stmt.sql.as_deref(), stmt.sql_id)?;
        policy::check(&sql)?;
        self.dirty |= changes_connection(&sql);
        let named = stmt.named_args.iter().map(|a| (a.name.clone(), a.value.clone())).collect::<Vec<_>>();
        self.conn.execute(&sql, &stmt.args, &named, stmt.want_rows.unwrap_or(true))
    }

    fn sql(&self, sql: Option<&str>, sql_id: Option<i32>) -> Result<String, Error> {
        match (sql, sql_id) {
            (Some(sql), None) => Ok(sql.to_string()),
            (None, Some(id)) => self
                .sqls
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::new(format!("no stored SQL with id {id}"), "SQL_NOT_FOUND")),
            _ => Err(Error::new("give exactly one of sql and sql_id", "SQL_INVALID")),
        }
    }

    fn holds(&self, cond: &BatchCond, so_far: &BatchResult) -> bool {
        match cond {
            BatchCond::Ok { step } => so_far.step_results.get(*step).is_some_and(Option::is_some),
            BatchCond::Error { step } => so_far.step_errors.get(*step).is_some_and(Option::is_some),
            BatchCond::Not { cond } => !self.holds(cond, so_far),
            BatchCond::And { conds } => conds.iter().all(|c| self.holds(c, so_far)),
            BatchCond::Or { conds } => conds.iter().any(|c| self.holds(c, so_far)),
            BatchCond::IsAutocommit => self.autocommit(),
        }
    }

    fn autocommit(&self) -> bool {
        self.conn.autocommit()
    }
}

pub struct Streams {
    idle: Duration,
    parked: Mutex<HashMap<String, (Stream, Instant)>>,
}

impl Streams {
    pub fn new(idle: Duration) -> Arc<Self> {
        let streams = Arc::new(Self { idle, parked: Mutex::new(HashMap::new()) });
        let weak = Arc::downgrade(&streams);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(idle.max(Duration::from_millis(50)) / 2);
            loop {
                tick.tick().await;
                let Some(streams) = weak.upgrade() else { return };
                let now = Instant::now();
                streams.parked.lock().await.retain(|_, (_, at)| now.duration_since(*at) < streams.idle);
            }
        });
        streams
    }

    /// Take the stream for a baton out of the parked set. A stream is driven
    /// by one request at a time, so it stays out until `park` returns it.
    pub async fn take(&self, baton: &str) -> Option<Stream> {
        let (stream, at) = self.parked.lock().await.remove(baton)?;
        (at.elapsed() < self.idle).then_some(stream)
    }

    /// Whether the stream for a baton is still parked and inside its idle
    /// timeout, as `take` judges it. A request that waits after parking its
    /// stream hands out the baton only then (`pipeline`).
    pub async fn is_parked(&self, baton: &str) -> bool {
        self.parked.lock().await.get(baton).is_some_and(|(_, at)| at.elapsed() < self.idle)
    }

    /// Drop every parked stream of a database that is being deleted or whose
    /// tokens were rotated. A stream busy with a request is not parked, so
    /// the request checks again once it has parked its stream (`pipeline`).
    pub async fn close_database(&self, db: &str) {
        self.parked.lock().await.retain(|_, (stream, _)| stream.db != db);
    }

    /// Park a stream and return the baton for the next request.
    pub async fn park(&self, stream: Stream) -> String {
        let mut bytes = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut bytes);
        let baton = hex::encode(bytes);
        self.parked.lock().await.insert(baton.clone(), (stream, Instant::now()));
        baton
    }
}

/// Whether a statement may write, judged from its first keyword.
fn sql_may_write(sql: &str) -> bool {
    let mut rest = sql.trim_start();
    // Skip leading comments.
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, tail)| tail).trim_start();
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map_or("", |(_, tail)| tail).trim_start();
        } else {
            break;
        }
    }
    let word = rest.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_ascii_uppercase();
    match word.as_str() {
        "SELECT" | "VALUES" | "EXPLAIN" | "PRAGMA" | "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => false,
        "WITH" => {
            let upper = rest.to_ascii_uppercase();
            ["INSERT", "UPDATE", "DELETE", "REPLACE"].iter().any(|w| upper.contains(w))
        }
        _ => true,
    }
}

/// Whether SQL may leave state on its connection: pragma settings and
/// temporary objects. Errs toward yes; a yes only skips reusing the
/// connection.
fn changes_connection(sql: &str) -> bool {
    let has = |word: &[u8]| sql.as_bytes().windows(word.len()).any(|w| w.eq_ignore_ascii_case(word));
    has(b"pragma") || has(b"temp")
}
