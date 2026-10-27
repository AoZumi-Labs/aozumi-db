//! The AOZUMI DB console's view of one database: run a script and see what
//! each statement did, list the tables, page through one. It is made for
//! people reading results, not for programs: rows and bytes are capped and
//! values are shaped for display. Apps keep using libSQL's protocol
//! (`/v2/pipeline`).
//!
//! Every endpoint takes a database token, as the pipeline does. The AOZUMI
//! control plane mints a short-lived one for each request, read-only unless
//! the person asked to write, so it never reaches their browser. Scripts are
//! split by the engine's own parser and run in order on one connection; the
//! first error stops the rest, and a transaction left open is rolled back
//! before the response. The JSON is camelCase, the shape the console renders.

use serde::Serialize;
use turso_core::{Dialect, Numeric, SqliteDialect, Value as Core};
use turso_parser::ast::{AlterTableBody, Cmd, Stmt};
use turso_parser::lexer::Lexer;
use turso_parser::parser::Parser;
use turso_parser::token::TokenType;

use crate::engine::{sql_error, Conn};
use crate::policy;
use crate::proto::Error;

/// Rows of one statement kept for the console; the rest are only counted.
pub const MAX_ROWS: usize = 1_000;
/// Bytes of values kept across one request, about as the JSON spells them.
pub const MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_STATEMENTS: usize = 50;
pub const MAX_SCRIPT_BYTES: usize = 256 * 1024;
/// Rows in one page of the table browser.
pub const PAGE_SIZE: u64 = 50;
/// Tables described by the schema endpoint; a database with more lists these.
pub const MAX_SCHEMA_TABLES: usize = 500;
/// Pages past this are refused, so an offset can never overflow.
pub const MAX_PAGE: u64 = 1_000_000;
const MAX_TEXT_CHARS: usize = 10_000;
const BLOB_PREVIEW_BYTES: usize = 24;

/// The tables a person made, without SQLite's own (`sqlite_sequence` and the
/// like) or the engine's (its AUTOINCREMENT counters).
pub const LIST_TABLES: &str = "SELECT name FROM sqlite_schema WHERE type = 'table' \
    AND substr(name, 1, 7) <> 'sqlite_' AND substr(name, 1, 16) <> '__turso_internal' ORDER BY name";

/// A value as the console shows it; SQL NULL is JSON null. Integers stay
/// decimal strings so 64-bit values survive JavaScript; long text and blobs
/// arrive cut down (a blob as its size and first bytes in hex).
#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Cell {
    Integer {
        value: String,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        truncated: bool,
    },
    Blob {
        bytes: usize,
        hex: String,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Ok,
    Error,
    /// Not run, because a statement before it failed.
    Skipped,
}

/// What one statement of a script did.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    pub sql: String,
    pub state: State,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Cell>>>,
    /// Rows the statement returned; more than `rows` holds when truncated.
    pub row_count: u64,
    pub truncated: bool,
    pub rows_affected: u64,
    pub last_insert_rowid: Option<String>,
    pub error: Option<Error>,
}

impl Outcome {
    fn new(sql: String, state: State, error: Option<Error>) -> Self {
        Self {
            sql,
            state,
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            truncated: false,
            rows_affected: 0,
            last_insert_rowid: None,
            error,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunResult {
    pub read_only: bool,
    pub duration_ms: u64,
    pub statements: Vec<Outcome>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TablePage {
    pub table: String,
    pub page: u64,
    pub page_size: u64,
    pub total: u64,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Cell>>>,
}

/// A column as the console's schema panel shows it (`PRAGMA table_info`).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub decl_type: String,
    pub not_null: bool,
    pub primary_key: bool,
}

#[derive(Debug, Serialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

/// A statement that destroys data, with what it does in words. With writes
/// allowed, the console runs a script holding one only once it is confirmed.
#[derive(Debug, Serialize, PartialEq)]
pub struct Destructive {
    pub sql: String,
    pub what: String,
}

/// A script cut into statements. When one does not parse, the statements
/// before it are kept and the rest of the script carries the error, so those
/// still run and the error lands where it belongs.
#[derive(Debug, Default)]
pub struct Split {
    pub statements: Vec<String>,
    pub unparsed: Option<(String, Error)>,
}

impl Split {
    pub fn len(&self) -> usize {
        self.statements.len() + usize::from(self.unparsed.is_some())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every piece of SQL in the script, parsed or not.
    pub fn texts(&self) -> impl Iterator<Item = &str> {
        self.statements.iter().map(String::as_str).chain(self.unparsed.iter().map(|(sql, _)| sql.as_str()))
    }
}

/// Splits a script with the engine's parser, so a semicolon inside a string,
/// a comment or a trigger body never ends a statement. Each statement runs
/// from its first token to its last one: comments and semicolons between
/// statements are dropped, and so are empty statements.
pub fn split(sql: &str) -> Split {
    let mut split = Split::default();
    let mut rest = sql;
    while !rest.trim().is_empty() {
        match SqliteDialect.parse(rest) {
            Ok((Some(_), consumed)) if consumed > 0 => {
                let text = statement_text(&rest[..consumed]);
                if !text.is_empty() {
                    split.statements.push(text.to_string());
                }
                rest = &rest[consumed..];
            }
            // Only comments or semicolons are left.
            Ok(_) => break,
            Err(err) => {
                split.unparsed = Some((statement_text(rest).to_string(), sql_error(err)));
                break;
            }
        }
    }
    split
}

/// The statement in what the parser consumed for it, which reaches up to the
/// next statement's first token: from its first token to its last one that is
/// not a semicolon. The lexer tells comments and strings apart.
fn statement_text(piece: &str) -> &str {
    let bytes = piece.as_bytes();
    let mut lexer = Lexer::new(bytes);
    let (mut start, mut end) = (None, 0);
    loop {
        // Where each token lies, from how much input is left before and after it.
        let before = bytes.len() - lexer.remaining().len();
        let Some(Ok(token)) = lexer.next() else { break };
        if !matches!(token.token_type, TokenType::TK_NONE | TokenType::TK_SEMI) {
            start.get_or_insert(before);
            end = bytes.len() - lexer.remaining().len();
        }
    }
    start.map_or("", |start| &piece[start..end])
}

/// The statements of a script that drop a table, index, view, trigger or
/// column, or delete or change every row of a table (no `WHERE`), read from
/// the parsed statement rather than guessed from its text. `EXPLAIN` of one
/// runs nothing, so it is not one.
pub fn destructive(split: &Split) -> Vec<Destructive> {
    split
        .statements
        .iter()
        .filter_map(|sql| {
            let Some(Ok(Cmd::Stmt(stmt))) = Parser::new(sql.as_bytes()).next() else { return None };
            let what = match &stmt {
                Stmt::DropTable { tbl_name, .. } => format!("drops the table {}", tbl_name.name.as_str()),
                Stmt::DropIndex { idx_name, .. } => format!("drops the index {}", idx_name.name.as_str()),
                Stmt::DropView { view_name, .. } => format!("drops the view {}", view_name.name.as_str()),
                Stmt::DropTrigger { trigger_name, .. } => format!("drops the trigger {}", trigger_name.name.as_str()),
                Stmt::Delete { tbl_name, where_clause: None, .. } => format!("deletes every row of {}", tbl_name.name.as_str()),
                Stmt::Update(update) if update.where_clause.is_none() => format!("changes every row of {}", update.tbl_name.name.as_str()),
                Stmt::AlterTable(alter) => match &alter.body {
                    AlterTableBody::DropColumn(column) => format!("drops the column {} of {}", column.as_str(), alter.name.name.as_str()),
                    _ => return None,
                },
                _ => return None,
            };
            Some(Destructive { sql: sql.clone(), what })
        })
        .collect()
}

/// Runs a script's statements in order on `conn`; the first error stops the
/// rest. The caller rolls back a transaction the script left open.
pub fn run(conn: &Conn, split: Split) -> Vec<Outcome> {
    let mut outcomes = Vec::with_capacity(split.len());
    let mut budget = MAX_RESULT_BYTES;
    let mut failed = false;
    for sql in split.statements {
        let outcome = if failed { Outcome::new(sql, State::Skipped, None) } else { run_one(conn, sql, &mut budget) };
        failed |= outcome.state == State::Error;
        outcomes.push(outcome);
    }
    if let Some((sql, error)) = split.unparsed {
        outcomes.push(match failed {
            true => Outcome::new(sql, State::Skipped, None),
            false => Outcome::new(sql, State::Error, Some(error)),
        });
    }
    outcomes
}

fn run_one(conn: &Conn, sql: String, budget: &mut usize) -> Outcome {
    if let Err(error) = policy::check(&sql) {
        return Outcome::new(sql, State::Error, Some(error));
    }
    let mut kept = Kept::new(budget);
    match conn.execute_each(&sql, |values| kept.row(values)) {
        Ok(stepped) => Outcome {
            state: State::Ok,
            columns: stepped.columns,
            row_count: kept.count,
            truncated: kept.truncated,
            rows: kept.rows,
            rows_affected: stepped.affected_row_count,
            last_insert_rowid: stepped.last_insert_rowid,
            error: None,
            sql,
        },
        Err(error) => Outcome::new(sql, State::Error, Some(error)),
    }
}

/// The rows a statement keeps: at most MAX_ROWS, within the request's byte
/// budget. Every row is counted.
struct Kept<'a> {
    rows: Vec<Vec<Option<Cell>>>,
    count: u64,
    truncated: bool,
    budget: &'a mut usize,
}

impl<'a> Kept<'a> {
    fn new(budget: &'a mut usize) -> Self {
        Self { rows: Vec::new(), count: 0, truncated: false, budget }
    }

    fn row(&mut self, values: &[&Core]) {
        self.count += 1;
        if self.truncated || self.rows.len() >= MAX_ROWS {
            self.truncated = true;
            return;
        }
        let row = values.iter().map(|value| cell(value)).collect::<Vec<_>>();
        let size = row.iter().map(cell_size).sum::<usize>() + 4;
        if size > *self.budget {
            // Nothing more is kept in this request, in this statement or later ones.
            *self.budget = 0;
            self.truncated = true;
            return;
        }
        *self.budget -= size;
        self.rows.push(row);
    }
}

fn cell(value: &Core) -> Option<Cell> {
    Some(match value {
        Core::Null => return None,
        Core::Numeric(Numeric::Integer(i)) => Cell::Integer { value: i.to_string() },
        Core::Numeric(Numeric::Float(f)) => Cell::Float { value: f64::from(*f) },
        Core::Text(_) => {
            let text = value.to_text().unwrap_or_default();
            match text.char_indices().nth(MAX_TEXT_CHARS) {
                Some((at, _)) => Cell::Text { value: text[..at].to_string(), truncated: true },
                None => Cell::Text { value: text.to_string(), truncated: false },
            }
        }
        Core::Blob(_) => {
            let blob = value.to_blob().unwrap_or_default();
            Cell::Blob { bytes: blob.len(), hex: hex::encode(&blob[..blob.len().min(BLOB_PREVIEW_BYTES)]) }
        }
    })
}

/// About how many bytes a cell takes in the JSON.
fn cell_size(cell: &Option<Cell>) -> usize {
    match cell {
        None => 5,
        Some(Cell::Integer { value }) => value.len() + 30,
        Some(Cell::Float { .. }) => 50,
        Some(Cell::Text { value, .. }) => value.len() + 40,
        Some(Cell::Blob { hex, .. }) => hex.len() + 50,
    }
}

/// The database's tables, by name.
pub fn tables(conn: &Conn) -> Result<Vec<String>, Error> {
    let mut names = Vec::new();
    conn.execute_each(LIST_TABLES, |values| {
        if let Some(name) = values.first().and_then(|value| value.to_text()) {
            names.push(name.to_string());
        }
    })?;
    Ok(names)
}

/// Every table (up to MAX_SCHEMA_TABLES) with its columns.
pub fn schema(conn: &Conn) -> Result<Vec<TableSchema>, Error> {
    tables(conn)?
        .into_iter()
        .take(MAX_SCHEMA_TABLES)
        .map(|name| {
            let mut columns = Vec::new();
            // Rows are cid, name, type, notnull, dflt_value, pk.
            conn.execute_each(&format!("PRAGMA table_info({})", quote_identifier(&name)), |values| {
                let text = |i: usize| values.get(i).and_then(|value| value.to_text()).unwrap_or_default().to_string();
                let set = |i: usize| matches!(values.get(i), Some(Core::Numeric(Numeric::Integer(n))) if *n != 0);
                columns.push(Column { name: text(1), decl_type: text(2), not_null: set(3), primary_key: set(5) });
            })?;
            Ok(TableSchema { name, columns })
        })
        .collect()
}

/// A table name the browser may ask for: 1 to 256 characters, no control characters.
pub fn valid_table_name(name: &str) -> bool {
    (1..=256).contains(&name.chars().count()) && !name.chars().any(char::is_control)
}

/// `"name"`, with every `"` doubled: names a table and nothing else.
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Whether an error means the table is not there.
pub fn is_missing_table(error: &Error) -> bool {
    error.message.contains("no such table")
}

/// One page of a table, 1-based, and how many rows it has. Only a table the
/// database lists opens, never a table-valued function or SQLite's own
/// tables; names match as SQL matches them, regardless of ASCII case.
pub fn page(conn: &Conn, table: &str, page: u64) -> Result<TablePage, Error> {
    let Some(listed) = tables(conn)?.into_iter().find(|name| name.eq_ignore_ascii_case(table)) else {
        return Err(Error::new(format!("no such table: {table}"), "SQLITE_ERROR"));
    };
    let name = quote_identifier(&listed);
    let mut total = 0;
    conn.execute_each(&format!("SELECT count(*) FROM {name}"), |values| {
        if let Some(Core::Numeric(Numeric::Integer(n))) = values.first() {
            total = u64::try_from(*n).unwrap_or(0);
        }
    })?;
    let mut budget = MAX_RESULT_BYTES;
    let mut kept = Kept::new(&mut budget);
    let offset = (page - 1) * PAGE_SIZE;
    let stepped = conn.execute_each(&format!("SELECT * FROM {name} LIMIT {PAGE_SIZE} OFFSET {offset}"), |values| kept.row(values))?;
    Ok(TablePage { table: table.to_string(), page, page_size: PAGE_SIZE, total, columns: stepped.columns, rows: kept.rows })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statements(sql: &str) -> Vec<String> {
        let split = split(sql);
        assert!(split.unparsed.is_none(), "{sql:?} did not parse: {:?}", split.unparsed);
        split.statements
    }

    #[test]
    fn splits_where_sqlite_does() {
        assert_eq!(statements("select 1; select 2"), ["select 1", "select 2"]);
        assert_eq!(statements("insert into t values ('a; b', \"c;d\");\n-- a comment; with one\nselect 1; /* done; */"), [
            "insert into t values ('a; b', \"c;d\")",
            "select 1",
        ]);
        assert_eq!(statements("create trigger t after insert on x begin select 1; end"), ["create trigger t after insert on x begin select 1; end"]);
        assert_eq!(
            statements("create trigger t after insert on x begin update y set a = case when 1 then 2 end; end; select 3"),
            ["create trigger t after insert on x begin update y set a = case when 1 then 2 end; end", "select 3"],
        );
        assert_eq!(statements(" ;; select 1 ;; "), ["select 1"]);
        assert!(statements("-- only a comment").is_empty());
        assert!(statements("").is_empty());
    }

    #[test]
    fn a_statement_that_does_not_parse_keeps_the_ones_before_it() {
        let split = split("select 1; selec 2; select 3");
        assert_eq!(split.statements, ["select 1"]);
        let (rest, error) = split.unparsed.unwrap();
        assert!(rest.starts_with("selec 2"), "{rest}");
        assert_eq!(error.code.as_deref(), Some("SQLITE_ERROR"));
    }

    #[test]
    fn destructive_statements_are_read_from_the_parse() {
        let found = destructive(&split("select 1; drop table notes; delete from people; delete from people where id = 1;
            update people set name = 'x'; update people set name = 'x' where id = 2; alter table people drop column email;
            alter table people add column age integer; drop index if exists people_name; explain drop table notes"));
        let what = found.iter().map(|item| item.what.as_str()).collect::<Vec<_>>();
        assert_eq!(what, [
            "drops the table notes",
            "deletes every row of people",
            "changes every row of people",
            "drops the column email of people",
            "drops the index people_name",
        ]);
        assert_eq!(found[1].sql, "delete from people");
    }

    #[test]
    fn identifiers_are_quoted_and_names_checked() {
        assert_eq!(quote_identifier("notes"), "\"notes\"");
        assert_eq!(quote_identifier("we\"ird"), "\"we\"\"ird\"");
        assert!(valid_table_name("my table"));
        assert!(!valid_table_name(""));
        assert!(!valid_table_name("a\u{0}b"));
        assert!(!valid_table_name(&"x".repeat(257)));
    }
}
