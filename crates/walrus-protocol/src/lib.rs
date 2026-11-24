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
        write!(f, "{:016x}-{:016x}", self.epoch, self.lsn)
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
        // Checked by hand: `u64::from_str_radix` would also accept a leading `+`.
        let b = s.as_bytes();
        if b.len() != 33 || b[16] != b'-' {
            return Err(OffsetError);
        }
        let hex = |digits: &[u8]| -> Result<u64, OffsetError> {
            digits.iter().try_fold(0u64, |acc, &c| {
                let d = (c as char).to_digit(16).ok_or(OffsetError)?;
                Ok(acc << 4 | u64::from(d))
            })
        };
        Ok(Offset {
            epoch: hex(&b[..16])?,
            lsn: hex(&b[17..])?,
        })
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
        use serde::ser::SerializeSeq;
        match self {
            Op::Upsert(row) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("u")?;
                seq.serialize_element(row)?;
                seq.end()
            }
            Op::Delete(key) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("d")?;
                seq.serialize_element(key)?;
                seq.end()
            }
            Op::Patch { row, keep, from } => {
                let mut seq = serializer.serialize_seq(Some(3 + usize::from(from.is_some())))?;
                seq.serialize_element("p")?;
                seq.serialize_element(row)?;
                seq.serialize_element(keep)?;
                if let Some(from) = from {
                    seq.serialize_element(from)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, IgnoredAny, SeqAccess, Visitor};

        struct OpVisitor;

        impl<'de> Visitor<'de> for OpVisitor {
            type Value = Op;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an op array: [\"u\",row], [\"d\",key] or [\"p\",row,keep(,from)?]")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Op, A::Error> {
                let tag: String = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let op = match tag.as_str() {
                    "u" => Op::Upsert(
                        seq.next_element()?
                            .ok_or_else(|| A::Error::invalid_length(1, &self))?,
                    ),
                    "d" => Op::Delete(
                        seq.next_element()?
                            .ok_or_else(|| A::Error::invalid_length(1, &self))?,
                    ),
                    "p" => {
                        let row = seq
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
                        let keep = seq
                            .next_element()?
                            .ok_or_else(|| A::Error::invalid_length(2, &self))?;
                        let from = seq.next_element()?;
                        Op::Patch { row, keep, from }
                    }
                    other => return Err(A::Error::unknown_variant(other, &["u", "d", "p"])),
                };
                // Too many elements is a wrong arity too, not something to ignore silently.
                if seq.next_element::<IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("too many elements in op"));
                }
                Ok(op)
            }
        }

        deserializer.deserialize_seq(OpVisitor)
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
    let mut out = String::with_capacity(event.len() + data.len() + 64);
    out.push_str("event: ");
    out.push_str(event);
    out.push('\n');
    if let Some(id) = id {
        out.push_str("id: ");
        out.push_str(id);
        out.push('\n');
    }
    out.push_str("data: ");
    out.push_str(data);
    out.push_str("\n\n");
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    fn roundtrip(op: Op, expected: &str) {
        assert_eq!(serde_json::to_string(&op).unwrap(), expected);
        assert_eq!(serde_json::from_str::<Op>(expected).unwrap(), op);
    }

    #[test]
    fn op_upsert() {
        roundtrip(
            Op::Upsert(vec![Some(s("1")), Some(s("milk")), Some(s("f"))]),
            r#"["u",["1","milk","f"]]"#,
        );
        roundtrip(Op::Upsert(vec![]), r#"["u",[]]"#);
    }

    #[test]
    fn op_nulls_and_unicode() {
        roundtrip(
            Op::Upsert(vec![Some(s("1")), None, Some(s("héllo 🦭 \"q\" \\ \n"))]),
            r#"["u",["1",null,"héllo 🦭 \"q\" \\ \n"]]"#,
        );
    }

    #[test]
    fn op_delete() {
        roundtrip(Op::Delete(vec![s("1")]), r#"["d",["1"]]"#);
        roundtrip(Op::Delete(vec![s("1"), s("é")]), r#"["d",["1","é"]]"#);
    }

    #[test]
    fn op_patch() {
        roundtrip(
            Op::Patch {
                row: vec![Some(s("2")), None, Some(s("t"))],
                keep: vec![1],
                from: None,
            },
            r#"["p",["2",null,"t"],[1]]"#,
        );
        roundtrip(
            Op::Patch {
                row: vec![Some(s("3")), None],
                keep: vec![1],
                from: Some(vec![s("2")]),
            },
            r#"["p",["3",null],[1],["2"]]"#,
        );
        roundtrip(
            Op::Patch {
                row: vec![],
                keep: vec![],
                from: None,
            },
            r#"["p",[],[]]"#,
        );
    }

    #[test]
    fn op_rejects_bad_input() {
        for bad in [
            r#"[]"#,
            r#"["x",[]]"#,
            r#"["u"]"#,
            r#"["d"]"#,
            r#"["p",[]]"#,
            r#"["u",[],[]]"#,
            r#"["d",["1"],["2"]]"#,
            r#"["p",[],[],["1"],["2"]]"#,
            r#"["d",[null]]"#,
            r#"["p",[],[-1]]"#,
            r#"{"u":[]}"#,
            r#""u""#,
            r#"[1,[]]"#,
        ] {
            assert!(serde_json::from_str::<Op>(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn rows_event_json() {
        let rows = Rows {
            table: s("public.todos"),
            ops: vec![
                Op::Upsert(vec![Some(s("2")), Some(s("eggs")), Some(s("f"))]),
                Op::Delete(vec![s("1")]),
            ],
        };
        let text = serde_json::to_string(&rows).unwrap();
        assert_eq!(
            text,
            r#"{"table":"public.todos","ops":[["u",["2","eggs","f"]],["d",["1"]]]}"#
        );
        assert_eq!(serde_json::from_str::<Rows>(&text).unwrap(), rows);
    }

    #[test]
    fn table_reset_json() {
        let reset = TableReset {
            table: s("public.todos"),
            columns: vec![ColumnInfo {
                name: s("id"),
                type_name: s("bigint"),
                pk: true,
            }],
        };
        assert_eq!(
            serde_json::to_string(&reset).unwrap(),
            r#"{"table":"public.todos","columns":[{"name":"id","type":"bigint","pk":true}]}"#
        );
    }

    #[test]
    fn offset_format() {
        let o = Offset {
            epoch: 0x9f2c4e1a7b3d5f60,
            lsn: 0x16b3740,
        };
        assert_eq!(o.to_string(), "9f2c4e1a7b3d5f60-00000000016b3740");
        assert_eq!(
            Offset { epoch: 0, lsn: 0 }.to_string(),
            "0000000000000000-0000000000000000"
        );
        let max = Offset {
            epoch: u64::MAX,
            lsn: u64::MAX,
        };
        assert_eq!(max.to_string(), "ffffffffffffffff-ffffffffffffffff");
    }

    #[test]
    fn offset_parse() {
        let o: Offset = "9f2c4e1a7b3d5f60-00000000016b3740".parse().unwrap();
        assert_eq!(o.epoch, 0x9f2c4e1a7b3d5f60);
        assert_eq!(o.lsn, 0x16b3740);
        let upper: Offset = "9F2C4E1A7B3D5F60-00000000016B3740".parse().unwrap();
        assert_eq!(upper, o);
        assert_eq!(o.to_string().parse::<Offset>().unwrap(), o);
        let max: Offset = "ffffffffffffffff-ffffffffffffffff".parse().unwrap();
        assert_eq!(max.lsn, u64::MAX);
    }

    #[test]
    fn offset_parse_rejects() {
        for bad in [
            "",
            "-",
            "0-0",
            "9f2c4e1a7b3d5f60",
            "9f2c4e1a7b3d5f60-",
            "9f2c4e1a7b3d5f60-00000000016b374",
            "9f2c4e1a7b3d5f60-00000000016b37400",
            "9f2c4e1a7b3d5f6-000000000016b3740",
            "9f2c4e1a7b3d5f60_00000000016b3740",
            "9f2c4e1a7b3d5f60-00000000016b374g",
            "+f2c4e1a7b3d5f60-00000000016b3740",
            " 9f2c4e1a7b3d5f60-00000000016b3740",
            "9f2c4e1a7b3d5f60-00000000016b3740 ",
            "9f2c4e1a7b3d5f60-00000000016b3740\n",
            "9f2c4e1a7b3d5f6é-00000000016b3740",
            "0x2c4e1a7b3d5f60-00000000016b3740",
        ] {
            assert_eq!(bad.parse::<Offset>(), Err(OffsetError), "accepted {bad:?}");
        }
    }

    #[test]
    fn sse_framing() {
        assert_eq!(
            sse_event("rows", None, r#"{"a":1}"#),
            "event: rows\ndata: {\"a\":1}\n\n"
        );
        assert_eq!(
            sse_event("commit", Some("abc-def"), r#"{"offset":"abc-def"}"#),
            "event: commit\nid: abc-def\ndata: {\"offset\":\"abc-def\"}\n\n"
        );
        assert_eq!(SSE_RETRY, "retry: 1000\n\n");
        assert_eq!(SSE_KEEPALIVE, ": keepalive\n\n");
    }
}
