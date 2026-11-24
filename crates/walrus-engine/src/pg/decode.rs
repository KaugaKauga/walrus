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

use anyhow::{Context, Result, bail, ensure};

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
        let mut cur = Cursor { buf: msg };
        let tag = cur.u8().context("empty pgoutput message")?;
        match tag {
            b'R' => self.relation(&mut cur),
            b'I' => {
                let (table, schema) = self.table(&mut cur)?;
                let kind = cur.u8()?;
                ensure!(
                    kind == b'N',
                    "insert: expected 'N' tuple, got {:?}",
                    kind as char
                );
                let (row, unchanged) = tuple(&mut cur, schema)?;
                cur.finish()?;
                // A new row always carries every column; 'u' only appears in updates.
                ensure!(
                    unchanged.is_empty(),
                    "insert into {} has unchanged ('u') columns",
                    schema.name
                );
                key_of(schema, &row)?;
                out.push(Change::Insert { table, row });
                Ok(())
            }
            b'U' => {
                let (table, schema) = self.table(&mut cur)?;
                let mut kind = cur.u8()?;
                let mut old = None;
                if kind == b'K' || kind == b'O' {
                    let (old_row, _) = tuple(&mut cur, schema)?;
                    old = Some(key_of(schema, &old_row)?);
                    kind = cur.u8()?;
                }
                ensure!(
                    kind == b'N',
                    "update: expected 'N' tuple, got {:?}",
                    kind as char
                );
                let (row, unchanged) = tuple(&mut cur, schema)?;
                cur.finish()?;
                let new_key = key_of(schema, &row)?;
                let old_key = old.filter(|k| *k != new_key);
                out.push(Change::Update {
                    table,
                    old_key,
                    row,
                    unchanged,
                });
                Ok(())
            }
            b'D' => {
                let (table, schema) = self.table(&mut cur)?;
                let kind = cur.u8()?;
                ensure!(
                    kind == b'K' || kind == b'O',
                    "delete: expected 'K' or 'O' tuple, got {:?}",
                    kind as char
                );
                let (row, _) = tuple(&mut cur, schema)?;
                cur.finish()?;
                let key = key_of(schema, &row)?;
                out.push(Change::Delete { table, key });
                Ok(())
            }
            b'T' => {
                let n = cur.u32()? as usize;
                let _options = cur.u8()?;
                // Each relid takes 4 bytes; checking first stops a bogus count from
                // reserving a huge Vec.
                ensure!(
                    cur.buf.len() == n.saturating_mul(4),
                    "truncate: bad length for {n} relations"
                );
                let mut tables = Vec::with_capacity(n);
                for _ in 0..n {
                    let relid = cur.u32()?;
                    tables.push(self.lookup(relid)?);
                }
                out.push(Change::Truncate { tables });
                Ok(())
            }
            // Type and Origin carry nothing we index.
            b'Y' | b'O' => Ok(()),
            other => bail!(
                "unknown pgoutput message tag {:?} (0x{other:02x})",
                other as char
            ),
        }
    }

    fn lookup(&self, relid: u32) -> Result<TableId> {
        self.relations.get(&relid).copied().with_context(|| {
            format!("change for relation id {relid} with no preceding Relation message")
        })
    }

    /// Read the relation id of a change message and resolve its table.
    fn table(&self, cur: &mut Cursor) -> Result<(TableId, &TableSchema)> {
        let id = self.lookup(cur.u32()?)?;
        Ok((id, &self.tables[id]))
    }

    fn relation(&mut self, cur: &mut Cursor) -> Result<()> {
        let relid = cur.u32()?;
        let namespace = cur.cstr()?;
        let relname = cur.cstr()?;
        let _replica_identity = cur.u8()?;
        let ncols = cur.u16()? as usize;
        let name = format!("{namespace}.{relname}");
        let id = self
            .tables
            .iter()
            .position(|t| t.name == name)
            .with_context(|| format!("relation {name} is not a configured table"))?;
        let schema = &self.tables[id];
        let changed = || format!("schema of {name} changed; restart walrus to start a new epoch");
        ensure!(ncols == schema.columns.len(), "{}", changed());
        for col in &schema.columns {
            let _flags = cur.u8()?;
            let col_name = cur.cstr()?;
            let oid = cur.u32()?;
            let _typmod = cur.u32()?;
            ensure!(col_name == col.name && oid == col.type_oid, "{}", changed());
        }
        cur.finish()?;
        self.relations.insert(relid, id);
        Ok(())
    }
}

/// The key of `row`; errors if a key column is NULL or unchanged.
fn key_of(schema: &TableSchema, row: &Row) -> Result<Key> {
    schema
        .key_of(row)
        .with_context(|| format!("{}: key column is NULL or unchanged ('u')", schema.name))
}

/// Parse TupleData. Returns the row (`None` for NULL and `u`) and the indexes of `u` columns.
fn tuple(cur: &mut Cursor, schema: &TableSchema) -> Result<(Row, Vec<usize>)> {
    let n = cur.u16()? as usize;
    ensure!(
        n == schema.columns.len(),
        "{}: tuple has {n} columns, schema has {}",
        schema.name,
        schema.columns.len()
    );
    let mut row = Vec::with_capacity(n);
    let mut unchanged = Vec::new();
    for i in 0..n {
        match cur.u8()? {
            b'n' => row.push(None),
            b'u' => {
                unchanged.push(i);
                row.push(None);
            }
            b't' => {
                let len = cur.u32()? as usize;
                let bytes = cur.take(len)?;
                let text = std::str::from_utf8(bytes)
                    .with_context(|| format!("{}: column {i} is not valid UTF-8", schema.name))?;
                row.push(Some(text.to_owned()));
            }
            b'b' => bail!(
                "{}: column {i} is binary; only text is requested",
                schema.name
            ),
            other => bail!(
                "{}: unknown tuple column kind {:?}",
                schema.name,
                other as char
            ),
        }
    }
    Ok((row, unchanged))
}

/// Bounds-checked reader over a message.
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(self.buf.len() >= n, "truncated pgoutput message");
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// NUL-terminated UTF-8 string.
    fn cstr(&mut self) -> Result<&'a str> {
        let end = self
            .buf
            .iter()
            .position(|&b| b == 0)
            .context("unterminated string in pgoutput message")?;
        let s =
            std::str::from_utf8(&self.buf[..end]).context("invalid UTF-8 in pgoutput string")?;
        self.buf = &self.buf[end + 1..];
        Ok(s)
    }

    fn finish(&self) -> Result<()> {
        ensure!(
            self.buf.is_empty(),
            "{} trailing bytes in pgoutput message",
            self.buf.len()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn col(name: &str, oid: u32) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            type_name: "t".into(),
            type_oid: oid,
        }
    }

    /// 0: public.todos (id int8 pk, title text, done bool)
    /// 1: public.tags (note text, a int8 pk, b text pk)
    fn schemas() -> Arc<Vec<TableSchema>> {
        Arc::new(vec![
            TableSchema {
                name: "public.todos".into(),
                columns: vec![col("id", 20), col("title", 25), col("done", 16)],
                key: vec![0],
            },
            TableSchema {
                name: "public.tags".into(),
                columns: vec![col("note", 25), col("a", 20), col("b", 25)],
                key: vec![1, 2],
            },
        ])
    }

    fn cstr(v: &mut Vec<u8>, s: &str) {
        v.extend_from_slice(s.as_bytes());
        v.push(0);
    }

    fn relation(relid: u32, ns: &str, name: &str, cols: &[(&str, u32)]) -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend(relid.to_be_bytes());
        cstr(&mut v, ns);
        cstr(&mut v, name);
        v.push(b'd');
        v.extend((cols.len() as u16).to_be_bytes());
        for (n, oid) in cols {
            v.push(0);
            cstr(&mut v, n);
            v.extend(oid.to_be_bytes());
            v.extend((-1i32).to_be_bytes());
        }
        v
    }

    fn todos_rel() -> Vec<u8> {
        relation(
            1,
            "public",
            "todos",
            &[("id", 20), ("title", 25), ("done", 16)],
        )
    }

    fn tags_rel() -> Vec<u8> {
        relation(2, "public", "tags", &[("note", 25), ("a", 20), ("b", 25)])
    }

    /// `Ok(Some("x"))` = text, `Ok(None)` = NULL, `Err(())` = unchanged.
    type C = Result<Option<&'static str>, ()>;

    fn tup(cols: &[C]) -> Vec<u8> {
        let mut v = (cols.len() as u16).to_be_bytes().to_vec();
        for c in cols {
            match c {
                Ok(None) => v.push(b'n'),
                Err(()) => v.push(b'u'),
                Ok(Some(s)) => {
                    v.push(b't');
                    v.extend((s.len() as u32).to_be_bytes());
                    v.extend(s.as_bytes());
                }
            }
        }
        v
    }

    fn t(s: &'static str) -> C {
        Ok(Some(s))
    }
    const N: C = Ok(None);
    const U: C = Err(());

    fn dml(tag: u8, relid: u32, parts: &[(u8, Vec<u8>)]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend(relid.to_be_bytes());
        for (k, body) in parts {
            v.push(*k);
            v.extend(body);
        }
        v
    }

    fn truncate(relids: &[u32]) -> Vec<u8> {
        let mut v = vec![b'T'];
        v.extend((relids.len() as u32).to_be_bytes());
        v.push(0);
        for r in relids {
            v.extend(r.to_be_bytes());
        }
        v
    }

    fn ready() -> Decoder {
        let mut d = Decoder::new(schemas());
        let mut out = vec![];
        d.decode(&todos_rel(), &mut out).unwrap();
        d.decode(&tags_rel(), &mut out).unwrap();
        assert!(out.is_empty());
        d
    }

    fn one(d: &mut Decoder, msg: &[u8]) -> Change {
        let mut out = vec![];
        d.decode(msg, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        out.remove(0)
    }

    fn err(d: &mut Decoder, msg: &[u8]) -> String {
        let mut out = vec![];
        let e = d.decode(msg, &mut out).unwrap_err();
        assert!(out.is_empty());
        format!("{e:#}")
    }

    fn row(v: &[Option<&str>]) -> Row {
        v.iter().map(|s| s.map(String::from)).collect()
    }

    #[test]
    fn relation_ok_and_repeat() {
        let mut d = ready();
        let mut out = vec![];
        d.decode(&todos_rel(), &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn relation_name_mismatch() {
        let mut d = Decoder::new(schemas());
        let msg = relation(
            1,
            "public",
            "todos",
            &[("id", 20), ("name", 25), ("done", 16)],
        );
        assert_eq!(
            err(&mut d, &msg),
            "schema of public.todos changed; restart walrus to start a new epoch"
        );
    }

    #[test]
    fn relation_type_mismatch() {
        let mut d = Decoder::new(schemas());
        let msg = relation(
            1,
            "public",
            "todos",
            &[("id", 23), ("title", 25), ("done", 16)],
        );
        assert!(err(&mut d, &msg).contains("schema of public.todos changed"));
    }

    #[test]
    fn relation_column_count_mismatch() {
        let mut d = Decoder::new(schemas());
        let msg = relation(1, "public", "todos", &[("id", 20), ("title", 25)]);
        assert!(err(&mut d, &msg).contains("schema of public.todos changed"));
    }

    #[test]
    fn relation_unknown_table() {
        let mut d = Decoder::new(schemas());
        let msg = relation(9, "public", "other", &[("id", 20)]);
        assert!(err(&mut d, &msg).contains("not a configured table"));
    }

    #[test]
    fn insert_nulls_unicode() {
        let mut d = ready();
        let msg = dml(b'I', 1, &[(b'N', tup(&[t("1"), t("héllo ✓"), N]))]);
        assert_eq!(
            one(&mut d, &msg),
            Change::Insert {
                table: 0,
                row: row(&[Some("1"), Some("héllo ✓"), None])
            }
        );
    }

    #[test]
    fn update_plain() {
        let mut d = ready();
        let msg = dml(b'U', 1, &[(b'N', tup(&[t("1"), t("a"), t("t")]))]);
        assert_eq!(
            one(&mut d, &msg),
            Change::Update {
                table: 0,
                old_key: None,
                row: row(&[Some("1"), Some("a"), Some("t")]),
                unchanged: vec![]
            }
        );
    }

    #[test]
    fn update_unchanged_columns() {
        let mut d = ready();
        let msg = dml(b'U', 1, &[(b'N', tup(&[t("1"), U, N]))]);
        assert_eq!(
            one(&mut d, &msg),
            Change::Update {
                table: 0,
                old_key: None,
                row: row(&[Some("1"), None, None]),
                unchanged: vec![1]
            }
        );
    }

    #[test]
    fn update_key_changed_k() {
        let mut d = ready();
        let msg = dml(
            b'U',
            1,
            &[
                (b'K', tup(&[t("1"), N, N])),
                (b'N', tup(&[t("2"), t("a"), t("t")])),
            ],
        );
        match one(&mut d, &msg) {
            Change::Update { old_key, .. } => assert_eq!(old_key, Some(vec!["1".to_string()])),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn update_o_key_unchanged() {
        let mut d = ready();
        let msg = dml(
            b'U',
            1,
            &[
                (b'O', tup(&[t("1"), t("old"), t("f")])),
                (b'N', tup(&[t("1"), t("new"), t("t")])),
            ],
        );
        match one(&mut d, &msg) {
            Change::Update { old_key, .. } => assert_eq!(old_key, None),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn delete_k_and_o() {
        let mut d = ready();
        let k = dml(b'D', 1, &[(b'K', tup(&[t("5"), N, N]))]);
        let o = dml(b'D', 1, &[(b'O', tup(&[t("5"), t("x"), t("f")]))]);
        let want = Change::Delete {
            table: 0,
            key: vec!["5".into()],
        };
        assert_eq!(one(&mut d, &k), want);
        assert_eq!(one(&mut d, &o), want);
    }

    #[test]
    fn truncate_two_tables() {
        let mut d = ready();
        assert_eq!(
            one(&mut d, &truncate(&[2, 1])),
            Change::Truncate { tables: vec![1, 0] }
        );
        assert!(err(&mut d, &truncate(&[1, 7])).contains("relation id 7"));
    }

    #[test]
    fn type_and_origin_ignored() {
        let mut d = ready();
        let mut out = vec![];
        d.decode(b"Y\0\0\0\x01public\0mytype\0", &mut out).unwrap();
        d.decode(b"O\0\0\0\0\0\0\0\x01origin\0", &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn change_before_relation() {
        let mut d = Decoder::new(schemas());
        let msg = dml(b'I', 1, &[(b'N', tup(&[t("1"), N, N]))]);
        assert!(err(&mut d, &msg).contains("no preceding Relation"));
    }

    #[test]
    fn unknown_tag_and_empty() {
        let mut d = ready();
        assert!(err(&mut d, b"Z").contains("unknown pgoutput message tag"));
        assert!(err(&mut d, b"").contains("empty"));
    }

    #[test]
    fn truncated_at_every_length() {
        let msgs = [
            todos_rel(),
            dml(b'I', 1, &[(b'N', tup(&[t("1"), t("héllo"), N]))]),
            dml(
                b'U',
                1,
                &[
                    (b'K', tup(&[t("1"), N, N])),
                    (b'N', tup(&[t("2"), U, t("t")])),
                ],
            ),
            dml(b'D', 1, &[(b'K', tup(&[t("5"), N, N]))]),
            truncate(&[1, 2]),
        ];
        for msg in &msgs {
            for len in 0..msg.len() {
                let mut d = ready();
                let mut out = vec![];
                assert!(
                    d.decode(&msg[..len], &mut out).is_err(),
                    "tag {} len {len}",
                    msg[0] as char
                );
                assert!(out.is_empty());
            }
            let mut d = ready();
            d.decode(msg, &mut vec![]).unwrap();
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut d = ready();
        let mut msg = dml(b'D', 1, &[(b'K', tup(&[t("5"), N, N]))]);
        msg.push(0);
        assert!(err(&mut d, &msg).contains("trailing"));
    }

    #[test]
    fn binary_value_rejected() {
        let mut d = ready();
        let mut body = 3u16.to_be_bytes().to_vec();
        body.extend([b'b', 0, 0, 0, 1, 7]);
        let msg = dml(b'I', 1, &[(b'N', body)]);
        assert!(err(&mut d, &msg).contains("binary"));
    }

    #[test]
    fn null_or_unchanged_key_rejected() {
        let mut d = ready();
        let ins = dml(b'I', 1, &[(b'N', tup(&[N, t("a"), t("t")]))]);
        assert!(err(&mut d, &ins).contains("key column"));
        let upd = dml(b'U', 1, &[(b'N', tup(&[U, t("a"), t("t")]))]);
        assert!(err(&mut d, &upd).contains("key column"));
        let del = dml(b'D', 1, &[(b'K', tup(&[N, N, N]))]);
        assert!(err(&mut d, &del).contains("key column"));
    }

    #[test]
    fn tuple_column_count_mismatch() {
        let mut d = ready();
        let msg = dml(b'I', 1, &[(b'N', tup(&[t("1"), t("a")]))]);
        assert!(err(&mut d, &msg).contains("tuple has 2 columns"));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut d = ready();
        let mut body = 3u16.to_be_bytes().to_vec();
        body.extend([b't', 0, 0, 0, 1, 0xff, b'n', b'n']);
        let msg = dml(b'I', 1, &[(b'N', body)]);
        assert!(err(&mut d, &msg).contains("UTF-8"));
    }

    #[test]
    fn composite_key_not_first_columns() {
        let mut d = ready();
        let ins = dml(b'I', 2, &[(b'N', tup(&[t("n"), t("7"), t("x")]))]);
        assert_eq!(
            one(&mut d, &ins),
            Change::Insert {
                table: 1,
                row: row(&[Some("n"), Some("7"), Some("x")])
            }
        );
        let del = dml(b'D', 2, &[(b'K', tup(&[N, t("7"), t("x")]))]);
        assert_eq!(
            one(&mut d, &del),
            Change::Delete {
                table: 1,
                key: vec!["7".into(), "x".into()]
            }
        );
        let upd = dml(
            b'U',
            2,
            &[
                (b'K', tup(&[N, t("7"), t("x")])),
                (b'N', tup(&[t("n"), t("7"), t("y")])),
            ],
        );
        match one(&mut d, &upd) {
            Change::Update { old_key, .. } => {
                assert_eq!(old_key, Some(vec!["7".to_string(), "x".to_string()]))
            }
            c => panic!("{c:?}"),
        }
    }
}
