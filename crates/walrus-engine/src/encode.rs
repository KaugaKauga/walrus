//! Committed transactions → shared SSE frames (PLAN.md §6.3, §7.3). Each transaction is encoded
//! once; every session writes the same bytes. Sessions re-encode only during the join, when they
//! drop some of a transaction's items (`encode_items`).

use std::sync::Arc;

use bytes::Bytes;
use walrus_protocol::Op;

use crate::pg::decode::Change;
use crate::schema::{Key, TableId, TableSchema};

/// Ops per `rows` event (PLAN.md §6.3).
pub const ROWS_PER_EVENT: usize = 1000;
/// Above this many encoded bytes a transaction is published as oversized (PLAN.md §5.3).
pub const MAX_TXN_BYTES: usize = 8 << 20;

/// One item of a table's part of a transaction, in commit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    /// The table was truncated: `table-reset` with no rows.
    Reset,
    Op(Op),
}

/// A transaction's items and frames for one table it touched.
#[derive(Clone, Debug)]
pub struct TxnTable {
    pub table: TableId,
    pub items: Vec<Item>,
    /// `encode_items(table, &items)`.
    pub frames: Bytes,
}

/// One committed transaction, shared by every session through the live ring.
#[derive(Clone, Debug)]
pub struct EncodedTxn {
    /// Commit LSN.
    pub lsn: u64,
    pub xid: u32,
    /// Too big to fan out (encoded size above `MAX_TXN_BYTES`): `tables` lists the touched
    /// tables with empty items/frames; sessions subscribed to any of them close (§5.3).
    pub oversized: bool,
    /// Only tables the transaction touched, in first-touch order.
    pub tables: Vec<TxnTable>,
    /// `commit_frame(epoch, lsn)`.
    pub commit_frame: Bytes,
}

impl EncodedTxn {
    pub fn table(&self, id: TableId) -> Option<&TxnTable> {
        self.tables.iter().find(|t| t.table == id)
    }
}

/// Builds one `EncodedTxn` from its changes, in order.
///
/// Change → items for the touched table (PLAN.md §4.8):
/// - Insert → `Upsert(row)`.
/// - Update, key unchanged → `Upsert(row)`, or `Patch { row, keep: unchanged, from: None }` if
///   any column is unchanged.
/// - Update, key changed → (`Upsert(row)` | `Patch { .., from: Some(old_key) }`) then
///   `Delete(old_key)`.
/// - Delete → `Delete(key)`.
/// - Truncate → `Reset` for each listed table.
pub struct TxnBuilder {
    // Implementation-defined: tables, xid, lsn, per-table items, running size estimate.
    _tables: Arc<Vec<TableSchema>>,
}

impl TxnBuilder {
    pub fn new(tables: Arc<Vec<TableSchema>>, xid: u32, lsn: u64) -> Self {
        let _ = (xid, lsn);
        TxnBuilder { _tables: tables }
    }

    /// Add one change. Once the running size estimate passes `MAX_TXN_BYTES` the builder drops
    /// items and only remembers which tables were touched.
    pub fn push(&mut self, change: &Change) {
        let _ = change;
        todo!()
    }

    pub fn finish(self, epoch: u64) -> EncodedTxn {
        let _ = epoch;
        todo!()
    }
}

/// Frames for a table's items, in order: consecutive ops become `rows` events of up to
/// `ROWS_PER_EVENT` ops; each `Reset` becomes a `table_reset_frame`.
pub fn encode_items(table: &TableSchema, items: &[Item]) -> Bytes {
    let _ = (table, items);
    todo!()
}

/// `event: table-reset` with the table's name and `column_info()`.
pub fn table_reset_frame(table: &TableSchema) -> Bytes {
    let _ = table;
    todo!()
}

/// `rows` events for `ops`, chunked by `ROWS_PER_EVENT`. Empty input → empty bytes.
pub fn rows_frames(table: &TableSchema, ops: &[Op]) -> Bytes {
    let _ = (table, ops);
    todo!()
}

/// `event: commit` with `id: <offset>` and `data: {"offset":"<offset>"}`.
pub fn commit_frame(epoch: u64, lsn: u64) -> Bytes {
    let _ = (epoch, lsn);
    todo!()
}

/// The key an op targets: the row's key for `Upsert`/`Patch`, the key for `Delete`.
pub fn op_key(table: &TableSchema, op: &Op) -> Key {
    let _ = (table, op);
    todo!()
}
