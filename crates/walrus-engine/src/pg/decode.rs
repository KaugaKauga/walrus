//! pgoutput (protocol v1, text format) → row changes (PLAN.md §4.8). Pure: bytes in, changes out.
//!
//! `pgwire-replication` already parses `Begin`, `Commit` and `Message`; this decoder handles the
//! raw `XLogData` payloads: `R` Relation, `I` Insert, `U` Update, `D` Delete, `T` Truncate, and
//! ignores `Y` Type and `O` Origin.
//!
//! Wire format (all integers big-endian; String = NUL-terminated UTF-8):
//! - `R`: Int32 relid, String namespace, String relname, Int8 replica identity, Int16 ncols,
//!   then per column: Int8 flags (1 = part of key), String name, Int32 type oid, Int32 typmod.
//! - `I`: Int32 relid, Byte1 'N', TupleData.
//! - `U`: Int32 relid, optionally (Byte1 'K' | 'O', TupleData), then Byte1 'N', TupleData.
//! - `D`: Int32 relid, Byte1 'K' | 'O', TupleData.
//! - `T`: Int32 nrels, Int8 options, nrels × Int32 relid.
//! - TupleData: Int16 ncols, then per column Byte1 kind: 'n' NULL, 'u' unchanged TOAST value
//!   (no data), 't' text (Int32 len + bytes). 'b' (binary) is never requested → error.
//!   'K' tuples (old key, REPLICA IDENTITY DEFAULT) carry all columns, non-key ones as 'n'.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use crate::schema::{Key, Row, TableId, TableSchema};

/// One decoded row change. Table ids index the engine's `Vec<TableSchema>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Insert {
        table: TableId,
        row: Row,
    },
    Update {
        table: TableId,
        /// The old primary key, only when the key changed (an old `K`/`O` tuple whose key
        /// differs from the new row's key). `None` when the key is unchanged.
        old_key: Option<Key>,
        /// New row; `None` at the indexes in `unchanged`.
        row: Row,
        /// Columns pgoutput marked `u` (unchanged large value), ascending.
        unchanged: Vec<usize>,
    },
    Delete {
        table: TableId,
        key: Key,
    },
    /// One `T` message: every listed table was truncated.
    Truncate {
        tables: Vec<TableId>,
    },
}

/// Stateful decoder: remembers relation ids announced by `R` messages.
pub struct Decoder {
    tables: Arc<Vec<TableSchema>>,
    relations: HashMap<u32, TableId>,
}

impl Decoder {
    pub fn new(tables: Arc<Vec<TableSchema>>) -> Self {
        Decoder {
            tables,
            relations: HashMap::new(),
        }
    }

    /// Decode one pgoutput message, appending any resulting change to `out`.
    ///
    /// Errors (all fatal for the engine, PLAN.md §4.8):
    /// - malformed or truncated message, unknown tag, binary tuple values;
    /// - `R` for a relation that is not a configured table (matched by `namespace.relname`);
    /// - `R` whose columns (names and type oids, in order) differ from the stored schema:
    ///   message "schema of <table> changed; restart walrus to start a new epoch";
    /// - a change for a relation id not announced by an `R` message;
    /// - a key column that is NULL or `u`, or a tuple whose column count differs from the schema.
    pub fn decode(&mut self, msg: &[u8], out: &mut Vec<Change>) -> Result<()> {
        let _ = (&self.tables, &mut self.relations, msg, out);
        todo!()
    }
}
