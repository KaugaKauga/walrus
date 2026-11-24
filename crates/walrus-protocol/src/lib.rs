//! Walrus wire protocol v0 (PLAN.md §6): offsets, event payloads and SSE framing.
//!
//! Shared by the engine (encoding) and clients (decoding). Serde only, so it also builds for WASM.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A column value in Postgres text format; `None` is SQL NULL.
pub type Value = Option<String>;
/// A row: values in the table's column order.
pub type Row = Vec<Value>;
/// A primary key: the key columns' text values, in key-column order. Never NULL.
pub type Key = Vec<String>;

/// SSE event names.
pub mod event {
    pub const TABLE_RESET: &str = "table-reset";
    pub const ROWS: &str = "rows";
    pub const COMMIT: &str = "commit";
}

/// Sent first on every stream: reconnect after 1 s.
pub const SSE_RETRY: &str = "retry: 1000\n\n";
/// Sent every 15 s on an idle stream.
pub const SSE_KEEPALIVE: &str = ": keepalive\n\n";

/// `epoch` + commit LSN: "I am up to date as of here" (PLAN.md §6.2).
///
/// Text form: `<epoch as 16 lower-case hex digits>-<lsn as 16 lower-case hex digits>`,
/// e.g. `9f2c4e1a7b3d5f60-00000000016b3740`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Offset {
    pub epoch: u64,
    pub lsn: u64,
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("format as {{:016x}}-{{:016x}}")
    }
}

/// Parse error for [`Offset`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetError;

impl fmt::Display for OffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("malformed offset")
    }
}

impl std::error::Error for OffsetError {}

impl FromStr for Offset {
    type Err = OffsetError;

    /// Accepts exactly the text form produced by `Display` (hex digits in either case).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

/// One row operation inside a `rows` event (PLAN.md §6.3). Serialized as a JSON array:
///
/// - `Upsert(row)`                         → `["u", row]`
/// - `Delete(key)`                         → `["d", key]`
/// - `Patch { row, keep, from: None }`     → `["p", row, keep]`
/// - `Patch { row, keep, from: Some(k) }`  → `["p", row, keep, k]`
///
/// In a patch, `row` holds `null` at the indexes listed in `keep`; the client keeps its current
/// values for those columns, taken from the row at `from` if present, else from the patched key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Upsert(Row),
    Delete(Key),
    Patch {
        row: Row,
        keep: Vec<usize>,
        from: Option<Key>,
    },
}

impl Serialize for Op {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        todo!()
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        todo!()
    }
}

/// Column description in a `table-reset` event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub pk: bool,
}

/// `table-reset` event data: drop all local rows of `table`; here is its schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableReset {
    pub table: String,
    pub columns: Vec<ColumnInfo>,
}

/// `rows` event data: operations to apply in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rows {
    pub table: String,
    pub ops: Vec<Op>,
}

/// `commit` event data. The same offset is also sent as the SSE `id:` field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub offset: String,
}

/// Frame one SSE event. `data` must be a single line (compact JSON always is).
///
/// Output: `event: <event>\n` + (`id: <id>\n` if any) + `data: <data>\n\n`.
pub fn sse_event(event: &str, id: Option<&str>, data: &str) -> String {
    todo!()
}
