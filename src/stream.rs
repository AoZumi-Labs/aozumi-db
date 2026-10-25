//! A stream is one database connection plus its stored SQL, kept open across
//! HTTP requests by a baton. Batons are random, single-use and rotated on
//! every request; a stream idle longer than the timeout is dropped, which
//! rolls back any open transaction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngCore;
use tokio::sync::Mutex;

use crate::engine::Conn;
use crate::proto::{
    BatchCond, BatchResult, Error, Stmt, StmtResult, StreamRequest, StreamResponse, StreamResult,
};

pub struct Stream {
    pub db: String,
    conn: Conn,
    sqls: HashMap<i32, String>,
    closed: bool,
}

impl Stream {
    pub fn new(db: String, conn: Conn) -> Self {
        Self { db, conn, sqls: HashMap::new(), closed: false }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
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

    fn execute(&self, stmt: &Stmt) -> Result<StmtResult, Error> {
        let sql = self.sql(stmt.sql.as_deref(), stmt.sql_id)?;
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

    /// Drop every parked stream of a database that is being deleted.
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
