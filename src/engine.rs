//! The storage engine, driven through its core API: database files,
//! connections, and statements run to completion on the calling thread.
//! Everything here blocks, so callers run it on tokio's blocking pool.

use std::num::NonZero;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use turso_core::Numeric;
use turso_core::{Database, LimboError, SqliteDialect, Value as Core, IO};

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

pub struct Conn {
    inner: Arc<turso_core::Connection>,
    options: ConnOptions,
}

impl Conn {
    pub fn open(db: &Db, options: &ConnOptions) -> Result<Self, LimboError> {
        let inner = db.connect()?;
        inner.set_busy_timeout(options.busy_timeout);
        inner.set_query_only(options.read_only);
        if let Some(timeout) = options.statement_timeout {
            inner.set_query_timeout(timeout);
        }
        if let Some(pages) = options.max_pages {
            // Set by the server, not tenant SQL, which may not change it.
            inner.execute(format!("PRAGMA max_page_count = {pages}"))?;
        }
        Ok(Self { inner, options: options.clone() })
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
        let mut stmt = self.inner.prepare(sql).map_err(|err| self.error(err))?;
        if !positional.is_empty() && !named.is_empty() {
            return Err(Error::new("mixing positional and named arguments is not supported", "ARGS_INVALID"));
        }
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

        Ok(StmtResult {
            cols,
            rows,
            affected_row_count: stmt.n_change().max(0) as u64,
            last_insert_rowid: Some(self.inner.last_insert_rowid().to_string()),
        })
    }

    /// Run every statement in `sql`, ignoring rows.
    pub fn execute_script(&self, sql: &str) -> Result<(), Error> {
        self.inner.execute(sql).map_err(|err| self.error(err))
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
