//! What tenant SQL may run. The engine itself enforces read-only
//! connections; this keeps tenants inside their own database file and away
//! from the settings the server owns: limits, durability and the WAL.

use turso_parser::ast::{Cmd, PragmaBody, Stmt};
use turso_parser::parser::Parser;

use crate::proto::Error;

/// Pragmas a tenant may read (`PRAGMA x`, or `PRAGMA x(arg)` for lookups).
const READABLE: &[&str] = &[
    "application_id", "busy_timeout", "case_sensitive_like", "collation_list", "compile_options",
    "data_version", "defer_foreign_keys", "encoding", "foreign_key_check", "foreign_key_list",
    "foreign_keys", "freelist_count", "function_list", "index_info", "index_list", "index_xinfo",
    "integrity_check", "journal_mode", "max_page_count", "page_count", "page_size", "pragma_list",
    "query_only", "quick_check", "recursive_triggers", "schema_version", "synchronous", "table_info",
    "table_list", "table_xinfo", "user_version",
];

/// Pragmas whose argument names what to look at rather than a new value.
const LOOKUPS: &[&str] = &[
    "foreign_key_check", "foreign_key_list", "index_info", "index_list", "index_xinfo",
    "integrity_check", "quick_check", "table_info", "table_list", "table_xinfo",
];

/// Pragmas a tenant may set: they only change its own data or connection.
const SETTABLE: &[&str] =
    &["application_id", "case_sensitive_like", "defer_foreign_keys", "foreign_keys", "recursive_triggers", "user_version"];

/// Check every statement in `sql`.
pub fn check(sql: &str) -> Result<(), Error> {
    // Every statement this refuses names one of these words, so most SQL
    // skips parsing entirely.
    let lower = sql.to_ascii_lowercase();
    if !["pragma", "vacuum", "attach", "detach"].iter().any(|word| lower.contains(word)) {
        return Ok(());
    }
    if lower.contains("pragma_database_list") {
        return Err(denied("pragma_database_list is not allowed"));
    }
    for cmd in Parser::new(sql.as_bytes()) {
        // The engine uses the same parser and reports syntax errors itself.
        let Ok(cmd) = cmd else { return Ok(()) };
        let stmt = match &cmd {
            Cmd::Stmt(stmt) | Cmd::Explain(stmt) => stmt,
            Cmd::ExplainQueryPlan { stmt, .. } => stmt,
        };
        check_stmt(stmt)?;
    }
    Ok(())
}

fn check_stmt(stmt: &Stmt) -> Result<(), Error> {
    match stmt {
        Stmt::Attach { .. } | Stmt::Detach { .. } => Err(denied("ATTACH and DETACH are not allowed")),
        Stmt::Vacuum { into: Some(_), .. } => Err(denied("VACUUM INTO is not allowed")),
        Stmt::Pragma { name, body } => {
            let pragma = name.name.as_str().to_ascii_lowercase();
            let allowed = match body {
                None => READABLE.contains(&pragma.as_str()),
                Some(PragmaBody::Call(_)) => LOOKUPS.contains(&pragma.as_str()) || SETTABLE.contains(&pragma.as_str()),
                Some(PragmaBody::Equals(_)) => SETTABLE.contains(&pragma.as_str()),
            };
            match allowed {
                true => Ok(()),
                false => Err(denied(&format!("PRAGMA {pragma} is not allowed here"))),
            }
        }
        _ => Ok(()),
    }
}

fn denied(message: &str) -> Error {
    Error::new(message, "SQLITE_AUTH")
}
