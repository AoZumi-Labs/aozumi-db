//! libSQL's Hrana-over-HTTP v2 messages, JSON encoding.
//!
//! Field names and shapes follow what `@libsql/hrana-client` sends and parses,
//! because that client is the compatibility target.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Deserialize)]
pub struct PipelineReq {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Debug, Serialize)]
pub struct PipelineResp {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Close,
    Execute { stmt: Stmt },
    Batch { batch: Batch },
    Sequence { sql: Option<String>, sql_id: Option<i32> },
    StoreSql { sql_id: i32, sql: String },
    CloseSql { sql_id: i32 },
    GetAutocommit,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: Error },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Close,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
}

#[derive(Debug, Deserialize)]
pub struct Stmt {
    pub sql: Option<String>,
    pub sql_id: Option<i32>,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    pub want_rows: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Deserialize)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
}

#[derive(Debug, Deserialize)]
pub struct BatchStep {
    pub condition: Option<BatchCond>,
    pub stmt: Stmt,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    Ok { step: usize },
    Error { step: usize },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

#[derive(Debug, Serialize)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<Error>>,
}

#[derive(Debug, Serialize)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: u64,
    pub last_insert_rowid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Col {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Error {
    pub message: String,
    pub code: Option<String>,
}

impl Error {
    pub fn new(message: impl Into<String>, code: &str) -> Self {
        Self { message: message.into(), code: Some(code.to_string()) }
    }
}

/// A SQL value. Integers travel as decimal strings so 64-bit values survive
/// JavaScript's number type; blobs travel as base64.
#[derive(Debug, Clone, PartialEq)]
pub struct Value(pub turso::Value);

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Wire {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let wire = match &self.0 {
            turso::Value::Null => Wire::Null,
            turso::Value::Integer(i) => Wire::Integer { value: i.to_string() },
            turso::Value::Real(f) => Wire::Float { value: *f },
            turso::Value::Text(t) => Wire::Text { value: t.clone() },
            turso::Value::Blob(b) => Wire::Blob { base64: BASE64.encode(b) },
        };
        wire.serialize(s)
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Value(match Wire::deserialize(d)? {
            Wire::Null => turso::Value::Null,
            Wire::Integer { value } => turso::Value::Integer(value.parse().map_err(D::Error::custom)?),
            Wire::Float { value } => turso::Value::Real(value),
            Wire::Text { value } => turso::Value::Text(value),
            Wire::Blob { base64 } => turso::Value::Blob(BASE64.decode(base64).map_err(D::Error::custom)?),
        }))
    }
}
