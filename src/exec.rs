//! Running Hrana statements on a database connection.

use turso::Connection;

use crate::proto::{Col, Error, Stmt, StmtResult, Value};

pub async fn execute(conn: &Connection, sql: &str, stmt: &Stmt) -> Result<StmtResult, Error> {
    let mut prepared = conn.prepare(sql).await.map_err(sql_error)?;
    let cols = prepared
        .columns()
        .into_iter()
        .map(|c| Col { name: Some(c.name().to_string()), decltype: c.decl_type().map(str::to_string) })
        .collect::<Vec<_>>();

    let mut rows = match (stmt.args.is_empty(), stmt.named_args.is_empty()) {
        (_, true) => {
            let args = stmt.args.iter().map(|v| v.0.clone()).collect::<Vec<_>>();
            prepared.query(args).await
        }
        (true, false) => {
            let args = stmt
                .named_args
                .iter()
                .map(|a| (resolve_name(sql, &a.name), a.value.0.clone()))
                .collect::<Vec<_>>();
            prepared.query(args).await
        }
        (false, false) => {
            return Err(Error::new("mixing positional and named arguments is not supported", "ARGS_INVALID"))
        }
    }
    .map_err(sql_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(sql_error)? {
        let values = (0..cols.len())
            .map(|i| row.get_value(i).map(Value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        out.push(values);
    }
    drop(rows);

    Ok(StmtResult {
        cols,
        rows: if stmt.want_rows.unwrap_or(true) { out } else { Vec::new() },
        affected_row_count: prepared.n_change(),
        last_insert_rowid: Some(conn.last_insert_rowid().to_string()),
    })
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
pub fn sql_error(err: turso::Error) -> Error {
    use turso::Error as E;
    let code = match &err {
        E::Constraint(_) => "SQLITE_CONSTRAINT",
        E::Busy(_) | E::BusySnapshot(_) => "SQLITE_BUSY",
        E::Readonly(_) => "SQLITE_READONLY",
        E::Corrupt(_) => "SQLITE_CORRUPT",
        E::NotAdb(_) => "SQLITE_NOTADB",
        E::DatabaseFull(_) => "SQLITE_FULL",
        E::Misuse(_) => "SQLITE_MISUSE",
        E::Interrupt(_) => "SQLITE_INTERRUPT",
        E::IoError(..) => "SQLITE_IOERR",
        _ => "SQLITE_ERROR",
    };
    Error::new(err.to_string(), code)
}
