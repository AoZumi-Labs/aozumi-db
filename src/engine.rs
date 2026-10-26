//! The storage engine, driven through its core API: database files,
//! connections, and statements run to completion on the calling thread.
//! Everything here blocks, so callers run it on tokio's blocking pool.

use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZero;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use turso_core::Numeric;
use turso_core::{CheckpointMode, Database, LimboError, SqliteDialect, Value as Core, IO};

use crate::proto::{Col, Error, StmtResult, Value};

pub type Db = Arc<Database>;

pub fn open(path: &Path) -> Result<Db, LimboError> {
    let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new()?);
    let path = path.to_str().ok_or_else(|| LimboError::InvalidArgument("database path is not UTF-8".into()))?;
    Database::open_file(io, path, Arc::new(SqliteDialect))
}

#[derive(Debug, Clone, Default)]
pub struct ConnOptions {
    /// How long a statement waits for another writer before failing.
    pub busy_timeout: Duration,
    /// The engine refuses every write on this connection.
    pub read_only: bool,
    /// The database may not grow past this many pages.
    pub max_pages: Option<u64>,
    /// Statements running longer than this are interrupted.
    pub statement_timeout: Option<Duration>,
}

/// Where a database's WAL is: which incarnation (bumped each time the WAL
/// restarts) and the last committed frame in it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WalPosition {
    pub seq: u32,
    pub frame: u64,
}

/// Bytes of one WAL frame header.
pub const FRAME_HEADER: usize = 24;

/// Compiled statements kept per connection, by SQL text.
const STATEMENT_CACHE: usize = 64;

pub struct Conn {
    inner: Arc<turso_core::Connection>,
    options: ConnOptions,
    /// Parsing and planning dominate short queries, so compiled programs
    /// are kept and wrapped in a fresh statement on each use. (Keeping the
    /// statements themselves pins an old pager and stalls other writers.)
    programs: RefCell<HashMap<String, (Arc<turso_core::PreparedProgram>, turso_core::QueryMode)>>,
}

impl Conn {
    pub fn open(db: &Db, options: &ConnOptions) -> Result<Self, LimboError> {
        let inner = db.connect()?;
        // Commits never checkpoint the WAL themselves: a checkpoint inside a
        // commit stalls every writer queued behind it. The server checkpoints
        // in the background instead (the WAL shipper does when it is on, so
        // no frame is folded in before it is backed up).
        inner.wal_auto_actions_disable();
        inner.set_busy_timeout(options.busy_timeout);
        inner.set_query_only(options.read_only);
        if let Some(timeout) = options.statement_timeout {
            inner.set_query_timeout(timeout);
        }
        if let Some(pages) = options.max_pages {
            // Set by the server, not tenant SQL, which may not change it.
            inner.execute(format!("PRAGMA max_page_count = {pages}"))?;
        }
        Ok(Self { inner, options: options.clone(), programs: RefCell::new(HashMap::new()) })
    }

    pub fn autocommit(&self) -> bool {
        self.inner.get_auto_commit()
    }

    /// Run one statement to completion.
    pub fn execute(
        &self,
        sql: &str,
        positional: &[Value],
        named: &[(String, Value)],
        want_rows: bool,
    ) -> Result<StmtResult, Error> {
        if !positional.is_empty() && !named.is_empty() {
            return Err(Error::new("mixing positional and named arguments is not supported", "ARGS_INVALID"));
        }
        let mut stmt = self.prepare(sql)?;
        self.run(&mut stmt, sql, positional, named, want_rows)
    }

    fn prepare(&self, sql: &str) -> Result<turso_core::Statement, Error> {
        if let Some((program, mode)) = self.programs.borrow().get(sql) {
            // A schema change since compiling makes the program stale.
            if program.is_compatible_with(&self.inner) {
                let program = turso_core::Program::from_prepared(program.clone(), self.inner.clone());
                return Ok(turso_core::Statement::new(program, self.inner.get_pager(), *mode, 0));
            }
        }
        let stmt = self.inner.prepare(sql).map_err(|err| self.error(err))?;
        let mut programs = self.programs.borrow_mut();
        if programs.len() >= STATEMENT_CACHE {
            programs.clear();
        }
        programs.insert(sql.to_string(), (stmt.get_program().prepared().clone(), stmt.get_query_mode()));
        Ok(stmt)
    }

    fn run(
        &self,
        stmt: &mut turso_core::Statement,
        sql: &str,
        positional: &[Value],
        named: &[(String, Value)],
        want_rows: bool,
    ) -> Result<StmtResult, Error> {
        for (i, value) in positional.iter().enumerate() {
            let index = NonZero::new(i + 1).expect("i + 1 > 0");
            stmt.bind_at(index, to_core(value)).map_err(sql_error)?;
        }
        for (name, value) in named {
            let name = resolve_name(sql, name);
            let index = stmt
                .parameter_index(&name)
                .ok_or_else(|| Error::new(format!("named parameter {name} not found"), "ARGS_INVALID"))?;
            stmt.bind_at(index, to_core(value)).map_err(sql_error)?;
        }

        let cols = (0..stmt.num_columns())
            .map(|i| Col { name: Some(stmt.get_column_name(i).into_owned()), decltype: stmt.get_column_decltype(i) })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        while let Some(row) = stmt.run_one_step_blocking(|| Ok(()), || Ok(())).map_err(|err| self.error(err))? {
            if want_rows {
                rows.push(row.get_values().map(from_core).collect());
            }
        }

        let changes = stmt.n_change().max(0) as u64;
        Ok(StmtResult {
            cols,
            rows,
            affected_row_count: changes,
            // Only meaningful when this statement changed rows; otherwise it
            // would be whatever the connection last inserted.
            last_insert_rowid: (changes > 0).then(|| self.inner.last_insert_rowid().to_string()),
        })
    }

    /// Run every statement in `sql`, ignoring rows.
    pub fn execute_script(&self, sql: &str) -> Result<(), Error> {
        self.inner.execute(sql).map_err(|err| self.error(err))
    }

    /// The WAL's current position, as of now rather than this connection's
    /// last read.
    pub fn wal_position(&self) -> anyhow::Result<WalPosition> {
        // Reading the schema starts a fresh read, which refreshes the view.
        self.execute("SELECT count(*) FROM sqlite_schema", &[], &[], false)?;
        let state = self.inner.wal_state()?;
        Ok(WalPosition { seq: state.checkpoint_seq_no, frame: state.max_frame })
    }

    /// Frames `from..=to`, header included, cut after the last commit frame.
    /// Returns the bytes and the number of the last frame included.
    pub fn committed_frames(&self, from: u64, to: u64, page_size: usize) -> anyhow::Result<(Vec<u8>, u64)> {
        let frame_size = FRAME_HEADER + page_size;
        let mut out = Vec::with_capacity((to + 1 - from) as usize * frame_size);
        let mut frame = vec![0u8; frame_size];
        let (mut kept, mut last) = (0, from - 1);
        for number in from..=to {
            let info = self.inner.wal_get_frame(number, &mut frame)?;
            out.extend_from_slice(&frame);
            if info.is_commit_frame() {
                kept = out.len();
                last = number;
            }
        }
        out.truncate(kept);
        Ok((out, last))
    }

    /// Copy WAL frames up to `upper_bound` into the database file without
    /// blocking readers or writers.
    pub fn checkpoint_passive(&self, upper_bound: Option<u64>) -> anyhow::Result<()> {
        self.inner.checkpoint(CheckpointMode::Passive { upper_bound_inclusive: upper_bound })?;
        Ok(())
    }

    /// Copy every remaining WAL frame into the database file and empty the
    /// WAL, holding writers off meanwhile. Returns how many frames this
    /// copied, so the caller can tell whether any arrived that it had not
    /// seen. Run [`Self::checkpoint_passive`] first to keep this short.
    pub fn checkpoint_truncate(&self) -> anyhow::Result<u64> {
        let result = self.inner.checkpoint(CheckpointMode::Truncate { upper_bound_inclusive: None })?;
        Ok(result.wal_checkpoint_backfilled)
    }

    fn error(&self, err: LimboError) -> Error {
        match &err {
            // The engine reports a write on a query-only connection as a
            // parse error; clients expect SQLite's READONLY code.
            LimboError::ParseError(message) if self.options.read_only && message.contains("query_only") => {
                Error::new("this token is read-only", "SQLITE_READONLY")
            }
            LimboError::DatabaseFull if self.options.max_pages.is_some() => {
                Error::new("the database has reached its size limit", "SQLITE_FULL")
            }
            LimboError::Interrupt if self.options.statement_timeout.is_some() => {
                Error::new("the statement ran past its time limit and was stopped", "SQLITE_INTERRUPT")
            }
            _ => sql_error(err),
        }
    }
}

fn to_core(value: &Value) -> Core {
    match value {
        Value::Null => Core::Null,
        Value::Integer(i) => Core::from_i64(*i),
        Value::Float(f) => Core::from_f64(*f),
        Value::Text(t) => Core::from_text(t.clone()),
        Value::Blob(b) => Core::from_blob(b.clone()),
    }
}

fn from_core(value: &Core) -> Value {
    match value {
        Core::Null => Value::Null,
        Core::Numeric(Numeric::Integer(i)) => Value::Integer(*i),
        Core::Numeric(Numeric::Float(f)) => Value::Float(f64::from(*f)),
        Core::Text(_) => Value::Text(value.to_text().unwrap_or_default().to_string()),
        Core::Blob(_) => Value::Blob(value.to_blob().unwrap_or_default().to_vec()),
    }
}

/// `@libsql/client` sends object keys without the `:`, `@` or `$` prefix, so
/// find which prefixed form the SQL actually uses.
fn resolve_name(sql: &str, name: &str) -> String {
    if name.starts_with([':', '@', '$', '?']) {
        return name.to_string();
    }
    ['@', '$']
        .iter()
        .map(|p| format!("{p}{name}"))
        .find(|candidate| uses_param(sql, candidate))
        .unwrap_or_else(|| format!(":{name}"))
}

fn uses_param(sql: &str, param: &str) -> bool {
    sql.match_indices(param).any(|(at, _)| {
        sql[at + param.len()..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
    })
}

/// Map an engine error to a Hrana error with SQLite's code names, which
/// `@libsql/client` exposes as `LibsqlError.code`.
pub fn sql_error(err: LimboError) -> Error {
    use LimboError as E;
    let code = match &err {
        E::Constraint(_) | E::ForeignKeyConstraint(_) => "SQLITE_CONSTRAINT",
        E::Busy | E::BusySnapshot | E::TableLocked => "SQLITE_BUSY",
        E::ReadOnly => "SQLITE_READONLY",
        E::Corrupt(_) => "SQLITE_CORRUPT",
        E::NotADB => "SQLITE_NOTADB",
        E::DatabaseFull => "SQLITE_FULL",
        E::Interrupt => "SQLITE_INTERRUPT",
        E::TooBig => "SQLITE_TOOBIG",
        E::CompletionError(_) => "SQLITE_IOERR",
        _ => "SQLITE_ERROR",
    };
    Error::new(err.to_string(), code)
}
