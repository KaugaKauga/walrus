//! Minimal native walrus client (PLAN.md §6.5): connects to `/v1/stream`, parses SSE, applies
//! events to in-memory tables and tracks the offset. Used by tests and benchmarks; it is the seed
//! of the Phase 2 client, not a product SDK.

use std::collections::BTreeMap;

use walrus_protocol::{ColumnInfo, Key, Offset, Row};

/// One table's local copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Table {
    pub columns: Vec<ColumnInfo>,
    /// Rows by primary key (key values in key-column order, i.e. the order of `pk == true`
    /// columns in `columns`).
    pub rows: BTreeMap<Key, Row>,
}

/// Everything a client holds: its tables and the offset of the last applied commit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub tables: BTreeMap<String, Table>,
    pub offset: Option<Offset>,
}

/// Why a stream ended.
#[derive(Debug)]
pub enum End {
    /// The server closed the stream (reconnect with `State::offset`).
    Closed,
    /// The server sent something that violates the protocol (e.g. a patch for a row the client
    /// doesn't hold). The client must drop its offset and reconnect fresh (PLAN.md §6.5).
    Broken(String),
}

/// An open `/v1/stream` connection.
pub struct Stream {
    // Implementation-defined: the reqwest byte stream plus an SSE line buffer and the events
    // buffered since the last commit.
}

impl State {
    /// Apply one committed batch atomically (PLAN.md §6.5): `table-reset` and `rows` events, in
    /// order, followed by the `commit` that ends them.
    ///
    /// Op semantics (§6.3): upsert = insert or replace; delete of an unknown key = no-op; patch =
    /// replace the row except the `keep` columns, which keep the values of the row at `from` (or
    /// at the patched key when `from` is absent) — that row must exist, otherwise `Err(reason)`.
    pub fn apply(&mut self, events: &[Event], offset: Offset) -> Result<(), String> {
        todo!()
    }
}

/// A parsed, not yet applied event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    TableReset(walrus_protocol::TableReset),
    Rows(walrus_protocol::Rows),
}

impl Stream {
    /// Connect to `{base_url}/v1/stream?tables=<comma-joined>` sending `offset` (if any) as the
    /// `Last-Event-ID` header. Fails if the response status is not 200.
    pub async fn connect(
        base_url: &str,
        tables: &[&str],
        offset: Option<Offset>,
    ) -> anyhow::Result<Stream> {
        todo!()
    }

    /// Read until the next `commit` event and apply everything since the previous commit to
    /// `state` atomically. Returns the new offset, or why the stream ended. Ignores comments
    /// (keepalives) and the `retry:` field.
    pub async fn next_commit(&mut self, state: &mut State) -> Result<Offset, End> {
        todo!()
    }
}
