//! Committed transactions → shared SSE frames (PLAN.md §6.3, §7.3). Each transaction is encoded
//! once; every session writes the same bytes. Sessions re-encode only during the join, when they
//! drop some of a transaction's items (`encode_items`).

use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use walrus_protocol::{Commit, Offset, Op, TableReset, event, sse_event};

use crate::pg::decode::Change;
use crate::schema::{Key, TableId, TableSchema};

/// Ops per `rows` event (PLAN.md §6.3).
pub const ROWS_PER_EVENT: usize = 1000;
/// Above this many encoded bytes a transaction is published as oversized (PLAN.md §5.3).
pub const MAX_TXN_BYTES: usize = 8 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    /// The table was truncated: `table-reset` with no rows.
    Reset,
    Op(Op),
}

#[derive(Clone, Debug)]
pub struct TxnTable {
    pub table: TableId,
    pub items: Vec<Item>,
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
    pub tables: Vec<TxnTable>,
    pub commit_frame: Bytes,
}

impl EncodedTxn {
    pub fn table(&self, id: TableId) -> Option<&TxnTable> {
        self.tables.iter().find(|t| t.table == id)
    }
}

/// Change → items for the touched table (PLAN.md §4.8):
/// - Insert → `Upsert(row)`.
/// - Update, key unchanged → `Upsert(row)`, or `Patch { row, keep: unchanged, from: None }` if
///   any column is unchanged.
/// - Update, key changed → (`Upsert(row)` | `Patch { .., from: Some(old_key) }`) then
///   `Delete(old_key)`.
/// - Delete → `Delete(key)`.
/// - Truncate → `Reset` for each listed table.
pub struct TxnBuilder {
    tables: Arc<Vec<TableSchema>>,
    xid: u32,
    lsn: u64,
    /// Touched tables in first-touch order, with their items (empty once oversized).
    per_table: Vec<(TableId, Vec<Item>)>,
    /// Rough encoded size so far.
    size: usize,
    oversized: bool,
}

impl TxnBuilder {
    pub fn new(tables: Arc<Vec<TableSchema>>, xid: u32, lsn: u64) -> Self {
        TxnBuilder {
            tables,
            xid,
            lsn,
            per_table: Vec::new(),
            size: 0,
            oversized: false,
        }
    }

    /// Add one change. Once the running size estimate passes `MAX_TXN_BYTES` the builder drops
    /// items and only remembers which tables were touched.
    pub fn push(&mut self, change: &Change) {
        for (table, item) in change_items(change) {
            let idx = match self.per_table.iter().position(|(t, _)| *t == table) {
                Some(idx) => idx,
                None => {
                    self.per_table.push((table, Vec::new()));
                    self.per_table.len() - 1
                }
            };
            if self.oversized {
                continue;
            }
            self.size += item_size(&item);
            self.per_table[idx].1.push(item);
        }
        if !self.oversized && self.size > MAX_TXN_BYTES {
            // Sessions will close and catch up from Postgres instead (§5.3); free the memory now.
            self.oversized = true;
            for (_, items) in &mut self.per_table {
                *items = Vec::new();
            }
        }
    }

    pub fn finish(self, epoch: u64) -> EncodedTxn {
        let tables = &self.tables;
        let oversized = self.oversized;
        let per_table = self
            .per_table
            .into_iter()
            .map(|(table, items)| {
                let frames = if oversized {
                    Bytes::new()
                } else {
                    encode_items(&tables[table], &items)
                };
                TxnTable {
                    table,
                    items,
                    frames,
                }
            })
            .collect();
        EncodedTxn {
            lsn: self.lsn,
            xid: self.xid,
            oversized,
            tables: per_table,
            commit_frame: commit_frame(epoch, self.lsn),
        }
    }
}

fn change_items(change: &Change) -> Vec<(TableId, Item)> {
    match change {
        Change::Insert { table, row } => vec![(*table, Item::Op(Op::Upsert(row.clone())))],
        Change::Update {
            table,
            old_key,
            row,
            unchanged,
        } => {
            let row = row.clone();
            let op = if unchanged.is_empty() {
                Op::Upsert(row)
            } else {
                Op::Patch {
                    row,
                    keep: unchanged.clone(),
                    from: old_key.clone(),
                }
            };
            let mut items = vec![(*table, Item::Op(op))];
            // The old key goes after the new row, because a patch reads kept values from it.
            if let Some(old) = old_key {
                items.push((*table, Item::Op(Op::Delete(old.clone()))));
            }
            items
        }
        Change::Delete { table, key } => vec![(*table, Item::Op(Op::Delete(key.clone())))],
        Change::Truncate { tables } => tables.iter().map(|t| (*t, Item::Reset)).collect(),
    }
}

fn item_size(item: &Item) -> usize {
    // Per-op overhead covers brackets, quotes and commas.
    const OVERHEAD: usize = 16;
    let values = |row: &[Option<String>]| {
        row.iter()
            .map(|v| v.as_ref().map_or(4, |s| s.len() + 3))
            .sum::<usize>()
    };
    let key = |k: &[String]| k.iter().map(|s| s.len() + 3).sum::<usize>();
    OVERHEAD
        + match item {
            Item::Reset => 0,
            Item::Op(Op::Upsert(row)) => values(row),
            Item::Op(Op::Delete(k)) => key(k),
            Item::Op(Op::Patch { row, keep, from }) => {
                values(row) + keep.len() * 4 + from.as_ref().map_or(0, |k| key(k))
            }
        }
}

/// Frames for a table's items, in order: consecutive ops become `rows` events of up to
/// `ROWS_PER_EVENT` ops; each `Reset` becomes a `table_reset_frame`.
pub fn encode_items(table: &TableSchema, items: &[Item]) -> Bytes {
    let mut out = Vec::new();
    let mut ops: Vec<Op> = Vec::new();
    for item in items {
        match item {
            Item::Op(op) => ops.push(op.clone()),
            Item::Reset => {
                // Ops before a reset must stay before it.
                out.extend_from_slice(&rows_frames(table, &ops));
                ops.clear();
                out.extend_from_slice(&table_reset_frame(table));
            }
        }
    }
    out.extend_from_slice(&rows_frames(table, &ops));
    Bytes::from(out)
}

/// `event: table-reset` with the table's name and `column_info()`.
pub fn table_reset_frame(table: &TableSchema) -> Bytes {
    let data = TableReset {
        table: table.name.clone(),
        columns: table.column_info(),
    };
    frame(event::TABLE_RESET, None, &data)
}

/// `rows` events for `ops`, chunked by `ROWS_PER_EVENT`. Empty input → empty bytes.
pub fn rows_frames(table: &TableSchema, ops: &[Op]) -> Bytes {
    // Borrowing twin of `walrus_protocol::Rows`, to avoid cloning every op to serialize it.
    #[derive(Serialize)]
    struct RowsRef<'a> {
        table: &'a str,
        ops: &'a [Op],
    }
    let mut out = Vec::new();
    for chunk in ops.chunks(ROWS_PER_EVENT) {
        let data = RowsRef {
            table: &table.name,
            ops: chunk,
        };
        out.extend_from_slice(&frame(event::ROWS, None, &data));
    }
    Bytes::from(out)
}

/// `event: commit` with `id: <offset>` and `data: {"offset":"<offset>"}`.
pub fn commit_frame(epoch: u64, lsn: u64) -> Bytes {
    let offset = Offset { epoch, lsn }.to_string();
    let data = Commit {
        offset: offset.clone(),
    };
    frame(event::COMMIT, Some(&offset), &data)
}

fn frame(event: &str, id: Option<&str>, data: &impl Serialize) -> Bytes {
    let json = serde_json::to_string(data).expect("plain strings and arrays always serialize");
    Bytes::from(sse_event(event, id, &json))
}

pub fn op_key(table: &TableSchema, op: &Op) -> Key {
    match op {
        Op::Upsert(row) | Op::Patch { row, .. } => table
            .key_of(row)
            .expect("key columns are never NULL or unchanged (decoder invariant)"),
        Op::Delete(key) => key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn schema() -> TableSchema {
        let col = |name: &str, ty: &str| ColumnSchema {
            name: name.into(),
            type_name: ty.into(),
            type_oid: 0,
        };
        TableSchema {
            name: "public.t".into(),
            columns: vec![
                col("id", "bigint"),
                col("title", "text"),
                col("done", "boolean"),
            ],
            key: vec![0],
        }
    }

    fn row(vals: &[Option<&str>]) -> Vec<Option<String>> {
        vals.iter().map(|v| v.map(String::from)).collect()
    }

    #[test]
    fn insert_is_upsert() {
        let r = row(&[Some("1"), Some("a"), Some("f")]);
        let items = change_items(&Change::Insert {
            table: 0,
            row: r.clone(),
        });
        assert_eq!(items, vec![(0, Item::Op(Op::Upsert(r)))]);
    }

    #[test]
    fn update_without_unchanged_is_upsert() {
        let r = row(&[Some("1"), Some("a"), Some("t")]);
        let items = change_items(&Change::Update {
            table: 2,
            old_key: None,
            row: r.clone(),
            unchanged: vec![],
        });
        assert_eq!(items, vec![(2, Item::Op(Op::Upsert(r)))]);
    }

    #[test]
    fn update_with_unchanged_is_patch() {
        let r = row(&[Some("1"), None, Some("t")]);
        let items = change_items(&Change::Update {
            table: 0,
            old_key: None,
            row: r.clone(),
            unchanged: vec![1],
        });
        assert_eq!(
            items,
            vec![(
                0,
                Item::Op(Op::Patch {
                    row: r,
                    keep: vec![1],
                    from: None
                })
            )]
        );
    }

    #[test]
    fn key_change_upserts_new_then_deletes_old() {
        let r = row(&[Some("2"), Some("a"), Some("t")]);
        let items = change_items(&Change::Update {
            table: 0,
            old_key: Some(vec!["1".into()]),
            row: r.clone(),
            unchanged: vec![],
        });
        assert_eq!(
            items,
            vec![
                (0, Item::Op(Op::Upsert(r))),
                (0, Item::Op(Op::Delete(vec!["1".into()]))),
            ]
        );
    }

    #[test]
    fn key_change_with_unchanged_patches_from_old_key() {
        let r = row(&[Some("2"), None, Some("t")]);
        let items = change_items(&Change::Update {
            table: 0,
            old_key: Some(vec!["1".into()]),
            row: r.clone(),
            unchanged: vec![1],
        });
        assert_eq!(
            items,
            vec![
                (
                    0,
                    Item::Op(Op::Patch {
                        row: r,
                        keep: vec![1],
                        from: Some(vec!["1".into()])
                    })
                ),
                (0, Item::Op(Op::Delete(vec!["1".into()]))),
            ]
        );
    }

    #[test]
    fn delete_and_truncate() {
        assert_eq!(
            change_items(&Change::Delete {
                table: 1,
                key: vec!["9".into()]
            }),
            vec![(1, Item::Op(Op::Delete(vec!["9".into()])))]
        );
        assert_eq!(
            change_items(&Change::Truncate { tables: vec![0, 2] }),
            vec![(0, Item::Reset), (2, Item::Reset)]
        );
    }

    #[test]
    fn builder_groups_by_table_in_first_touch_order() {
        let mut b = TxnBuilder::new(Arc::new(vec![schema(), schema(), schema()]), 7, 100);
        let r = row(&[Some("1"), Some("a"), Some("f")]);
        b.push(&Change::Insert {
            table: 2,
            row: r.clone(),
        });
        b.push(&Change::Delete {
            table: 0,
            key: vec!["1".into()],
        });
        b.push(&Change::Insert { table: 2, row: r });
        let order: Vec<_> = b
            .per_table
            .iter()
            .map(|(t, items)| (*t, items.len()))
            .collect();
        assert_eq!(order, vec![(2, 2), (0, 1)]);
        assert!(!b.oversized);
    }

    #[test]
    fn builder_drops_items_when_oversized_but_remembers_tables() {
        let mut b = TxnBuilder::new(Arc::new(vec![schema(), schema()]), 7, 100);
        let big = "x".repeat(MAX_TXN_BYTES / 2 + 1);
        let r = row(&[Some("1"), Some(&big), Some("f")]);
        b.push(&Change::Insert {
            table: 0,
            row: r.clone(),
        });
        assert!(!b.oversized);
        b.push(&Change::Insert {
            table: 0,
            row: r.clone(),
        });
        assert!(b.oversized);
        b.push(&Change::Insert { table: 1, row: r });
        let tables: Vec<_> = b
            .per_table
            .iter()
            .map(|(t, items)| (*t, items.len()))
            .collect();
        assert_eq!(tables, vec![(0, 0), (1, 0)]);
    }

    #[test]
    fn op_key_uses_key_columns() {
        let t = schema();
        let r = row(&[Some("5"), None, Some("t")]);
        assert_eq!(op_key(&t, &Op::Upsert(r.clone())), vec!["5".to_string()]);
        assert_eq!(
            op_key(
                &t,
                &Op::Patch {
                    row: r,
                    keep: vec![1],
                    from: None
                }
            ),
            vec!["5".to_string()]
        );
        assert_eq!(
            op_key(&t, &Op::Delete(vec!["8".into()])),
            vec!["8".to_string()]
        );
    }

    #[test]
    fn frames_chunk_and_interleave_resets() {
        let t = schema();
        let ops: Vec<Op> = (0..ROWS_PER_EVENT + 1)
            .map(|i| Op::Upsert(row(&[Some(&i.to_string()), None, None])))
            .collect();
        let frames = rows_frames(&t, &ops);
        let text = std::str::from_utf8(&frames).expect("utf8");
        assert_eq!(text.matches("event: rows\n").count(), 2);
        assert!(rows_frames(&t, &[]).is_empty());

        let items = vec![
            Item::Op(Op::Delete(vec!["1".into()])),
            Item::Reset,
            Item::Op(Op::Delete(vec!["2".into()])),
        ];
        let text = String::from_utf8(encode_items(&t, &items).to_vec()).expect("utf8");
        let kinds: Vec<_> = text
            .lines()
            .filter_map(|l| l.strip_prefix("event: "))
            .collect();
        assert_eq!(kinds, vec!["rows", "table-reset", "rows"]);
    }

    #[test]
    fn finish_builds_frames_and_commit() {
        let mut b = TxnBuilder::new(Arc::new(vec![schema()]), 7, 0x16b3740);
        b.push(&Change::Delete {
            table: 0,
            key: vec!["1".into()],
        });
        let txn = b.finish(0x9f2c4e1a7b3d5f60);
        assert!(!txn.oversized);
        assert_eq!(txn.tables.len(), 1);
        assert_eq!(
            std::str::from_utf8(&txn.tables[0].frames).expect("utf8"),
            "event: rows\ndata: {\"table\":\"public.t\",\"ops\":[[\"d\",[\"1\"]]]}\n\n"
        );
        assert_eq!(
            std::str::from_utf8(&txn.commit_frame).expect("utf8"),
            "event: commit\nid: 9f2c4e1a7b3d5f60-00000000016b3740\n\
             data: {\"offset\":\"9f2c4e1a7b3d5f60-00000000016b3740\"}\n\n"
        );
    }
}
