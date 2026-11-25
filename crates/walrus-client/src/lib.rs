//! Minimal native walrus client (PLAN.md §6.5): connects to `/v1/stream`, parses SSE, applies
//! events to in-memory tables and tracks the offset. Used by tests and benchmarks; it is the seed
//! of the Phase 2 client, not a product SDK.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use bytes::Bytes;
use futures_util::{Stream as FuturesStream, StreamExt};
use walrus_protocol::{ColumnInfo, Commit, Key, Offset, Op, Row, Rows, TableReset, event};

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

pub struct Stream {
    body: Pin<Box<dyn FuturesStream<Item = reqwest::Result<Bytes>> + Send>>,
    parser: SseParser,
    ready: VecDeque<SseEvent>,
    /// `table-reset` / `rows` events since the last commit.
    pending: Vec<Event>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SseEvent {
    /// `message` when no `event:` field was given.
    name: String,
    data: String,
    id: Option<String>,
}

/// Incremental SSE parser: feed byte chunks split anywhere, get complete events back.
#[derive(Default)]
struct SseParser {
    /// Bytes of the current, not yet terminated line.
    line: Vec<u8>,
    name: Option<String>,
    data: Vec<String>,
    id: Option<String>,
}

impl SseParser {
    fn feed(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        let mut out = Vec::new();
        for &b in chunk {
            if b == b'\n' {
                let mut line = std::mem::take(&mut self.line);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                let line = String::from_utf8_lossy(&line).into_owned();
                if let Some(ev) = self.line_done(&line) {
                    out.push(ev);
                }
            } else {
                self.line.push(b);
            }
        }
        out
    }

    fn line_done(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            let name = self.name.take();
            let id = self.id.take();
            let data = std::mem::take(&mut self.data);
            if name.is_none() && data.is_empty() {
                return None;
            }
            return Some(SseEvent {
                name: name.unwrap_or_else(|| "message".to_string()),
                data: data.join("\n"),
                id,
            });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => self.name = Some(value.to_string()),
            "data" => self.data.push(value.to_string()),
            "id" => self.id = Some(value.to_string()),
            _ => {} // `retry` and unknown fields are ignored
        }
        None
    }
}

impl State {
    /// Apply one committed batch atomically (PLAN.md §6.5): `table-reset` and `rows` events, in
    /// order, followed by the `commit` that ends them.
    ///
    /// Op semantics (§6.3): upsert = insert or replace; delete of an unknown key = no-op; patch =
    /// replace the row except the `keep` columns, which keep the values of the row at `from` (or
    /// at the patched key when `from` is absent) — that row must exist, otherwise `Err(reason)`.
    pub fn apply(&mut self, events: &[Event], offset: Offset) -> Result<(), String> {
        let mut next = self.clone();
        for ev in events {
            match ev {
                Event::TableReset(reset) => {
                    next.tables.insert(
                        reset.table.clone(),
                        Table {
                            columns: reset.columns.clone(),
                            rows: BTreeMap::new(),
                        },
                    );
                }
                Event::Rows(rows) => {
                    let table = next
                        .tables
                        .get_mut(&rows.table)
                        .ok_or_else(|| format!("rows for unknown table {}", rows.table))?;
                    for op in &rows.ops {
                        table
                            .apply_op(op)
                            .map_err(|e| format!("{}: {e}", rows.table))?;
                    }
                }
            }
        }
        next.offset = Some(offset);
        *self = next;
        Ok(())
    }
}

impl Table {
    fn key_of(&self, row: &Row) -> Result<Key, String> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "row has {} values, table has {} columns",
                row.len(),
                self.columns.len()
            ));
        }
        self.columns
            .iter()
            .zip(row)
            .filter(|(c, _)| c.pk)
            .map(|(c, v)| {
                v.clone()
                    .ok_or_else(|| format!("NULL in key column {}", c.name))
            })
            .collect()
    }

    fn apply_op(&mut self, op: &Op) -> Result<(), String> {
        match op {
            Op::Upsert(row) => {
                let key = self.key_of(row)?;
                self.rows.insert(key, row.clone());
            }
            Op::Delete(key) => {
                self.rows.remove(key);
            }
            Op::Patch { row, keep, from } => {
                let key = self.key_of(row)?;
                let source = from.as_ref().unwrap_or(&key);
                let old = self
                    .rows
                    .get(source)
                    .ok_or_else(|| format!("patch for missing row {source:?}"))?;
                let mut new = row.clone();
                for &i in keep {
                    let kept = old
                        .get(i)
                        .ok_or_else(|| format!("keep index {i} out of range"))?;
                    new[i] = kept.clone();
                }
                self.rows.insert(key, new);
            }
        }
        Ok(())
    }
}

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
        let url = format!(
            "{}/v1/stream?tables={}",
            base_url.trim_end_matches('/'),
            tables.join(",")
        );
        let mut req = reqwest::Client::new().get(url);
        if let Some(offset) = offset {
            req = req.header("Last-Event-ID", offset.to_string());
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status != reqwest::StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("stream request failed: {status}: {body}");
        }
        Ok(Stream {
            body: Box::pin(resp.bytes_stream()),
            parser: SseParser::default(),
            ready: VecDeque::new(),
            pending: Vec::new(),
        })
    }

    /// Read until the next `commit` event and apply everything since the previous commit to
    /// `state` atomically. Returns the new offset, or why the stream ended. Ignores comments
    /// (keepalives) and the `retry:` field.
    pub async fn next_commit(&mut self, state: &mut State) -> Result<Offset, End> {
        loop {
            let Some(ev) = self.ready.pop_front() else {
                // A transport error loses nothing applied, so resuming from `state.offset` is
                // safe: treat it like a close.
                match self.body.next().await {
                    Some(Ok(chunk)) => self.ready.extend(self.parser.feed(&chunk)),
                    Some(Err(_)) | None => return Err(End::Closed),
                }
                continue;
            };
            match ev.name.as_str() {
                event::TABLE_RESET => {
                    let reset: TableReset =
                        serde_json::from_str(&ev.data).map_err(|e| bad_json(&ev, e))?;
                    self.pending.push(Event::TableReset(reset));
                }
                event::ROWS => {
                    let rows: Rows =
                        serde_json::from_str(&ev.data).map_err(|e| bad_json(&ev, e))?;
                    self.pending.push(Event::Rows(rows));
                }
                event::COMMIT => {
                    let commit: Commit =
                        serde_json::from_str(&ev.data).map_err(|e| bad_json(&ev, e))?;
                    let offset: Offset = commit
                        .offset
                        .parse()
                        .map_err(|_| End::Broken(format!("bad commit offset {}", commit.offset)))?;
                    let events = std::mem::take(&mut self.pending);
                    state.apply(&events, offset).map_err(End::Broken)?;
                    return Ok(offset);
                }
                _ => {}
            }
        }
    }
}

fn bad_json(ev: &SseEvent, e: serde_json::Error) -> End {
    End::Broken(format!("bad {} event: {e}", ev.name))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const SAMPLE: &str = "retry: 1000\n\n: keepalive\n\nevent: table-reset\r\ndata: {\"a\":1}\r\n\n\
event: rows\ndata: line1\ndata:line2\ndata\n\n\
event: commit\nid: 0-1\ndata: {\"offset\":\"0-1\"}\n\n: bye\n\n";

    fn expected() -> Vec<SseEvent> {
        vec![
            SseEvent {
                name: "table-reset".into(),
                data: "{\"a\":1}".into(),
                id: None,
            },
            SseEvent {
                name: "rows".into(),
                data: "line1\nline2\n".into(),
                id: None,
            },
            SseEvent {
                name: "commit".into(),
                data: "{\"offset\":\"0-1\"}".into(),
                id: Some("0-1".into()),
            },
        ]
    }

    #[test]
    fn sse_whole_and_every_split() {
        let bytes = SAMPLE.as_bytes();
        assert_eq!(SseParser::default().feed(bytes), expected());
        for cut in 0..=bytes.len() {
            let mut p = SseParser::default();
            let mut got = p.feed(&bytes[..cut]);
            got.extend(p.feed(&bytes[cut..]));
            assert_eq!(got, expected(), "cut at {cut}");
        }
        let mut p = SseParser::default();
        let got: Vec<_> = bytes.chunks(1).flat_map(|c| p.feed(c)).collect();
        assert_eq!(got, expected());
    }

    fn col(name: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            type_name: "text".into(),
            pk,
        }
    }

    fn row(vals: &[&str]) -> Row {
        vals.iter().map(|v| Some(v.to_string())).collect()
    }

    fn key(vals: &[&str]) -> Key {
        vals.iter().map(|v| v.to_string()).collect()
    }

    fn off(lsn: u64) -> Offset {
        Offset { epoch: 1, lsn }
    }

    fn reset(table: &str, cols: Vec<ColumnInfo>) -> Event {
        Event::TableReset(TableReset {
            table: table.into(),
            columns: cols,
        })
    }

    fn ops(table: &str, ops: Vec<Op>) -> Event {
        Event::Rows(Rows {
            table: table.into(),
            ops,
        })
    }

    fn base() -> State {
        let mut s = State::default();
        s.apply(
            &[
                reset("t", vec![col("id", true), col("title", false)]),
                ops(
                    "t",
                    vec![Op::Upsert(row(&["1", "a"])), Op::Upsert(row(&["2", "b"]))],
                ),
            ],
            off(1),
        )
        .unwrap();
        s
    }

    #[test]
    fn upsert_replaces_and_sets_offset() {
        let mut s = base();
        s.apply(&[ops("t", vec![Op::Upsert(row(&["1", "z"]))])], off(2))
            .unwrap();
        assert_eq!(s.offset, Some(off(2)));
        assert_eq!(s.tables["t"].rows.len(), 2);
        assert_eq!(s.tables["t"].rows[&key(&["1"])], row(&["1", "z"]));
    }

    #[test]
    fn delete_known_and_unknown() {
        let mut s = base();
        s.apply(
            &[ops(
                "t",
                vec![Op::Delete(key(&["1"])), Op::Delete(key(&["9"]))],
            )],
            off(2),
        )
        .unwrap();
        assert_eq!(s.tables["t"].rows.len(), 1);
        assert!(s.tables["t"].rows.contains_key(&key(&["2"])));
    }

    #[test]
    fn patch_without_from() {
        let mut s = base();
        let patch = Op::Patch {
            row: vec![Some("2".into()), None],
            keep: vec![1],
            from: None,
        };
        s.apply(&[ops("t", vec![patch])], off(2)).unwrap();
        assert_eq!(s.tables["t"].rows[&key(&["2"])], row(&["2", "b"]));
    }

    #[test]
    fn patch_with_from_then_delete_old() {
        let mut s = base();
        let patch = Op::Patch {
            row: vec![Some("3".into()), None],
            keep: vec![1],
            from: Some(key(&["2"])),
        };
        s.apply(&[ops("t", vec![patch, Op::Delete(key(&["2"]))])], off(2))
            .unwrap();
        assert_eq!(s.tables["t"].rows.len(), 2);
        assert_eq!(s.tables["t"].rows[&key(&["3"])], row(&["3", "b"]));
        assert!(!s.tables["t"].rows.contains_key(&key(&["2"])));
    }

    #[test]
    fn patch_on_missing_row_is_err_and_atomic() {
        let mut s = base();
        let before = s.clone();
        let patch = Op::Patch {
            row: vec![Some("9".into()), None],
            keep: vec![1],
            from: None,
        };
        let res = s.apply(&[ops("t", vec![Op::Delete(key(&["1"])), patch])], off(2));
        assert!(res.is_err());
        assert_eq!(s, before);
    }

    #[test]
    fn table_reset_clears_rows() {
        let mut s = base();
        s.apply(&[reset("t", vec![col("k", true), col("v", false)])], off(2))
            .unwrap();
        assert!(s.tables["t"].rows.is_empty());
        assert_eq!(s.tables["t"].columns[0].name, "k");
    }

    #[test]
    fn rows_for_unknown_table_is_err() {
        let mut s = State::default();
        assert!(
            s.apply(&[ops("nope", vec![Op::Upsert(row(&["1"]))])], off(1))
                .is_err()
        );
        assert_eq!(s, State::default());
    }

    #[test]
    fn composite_keys_with_pk_not_first() {
        let mut s = State::default();
        s.apply(
            &[
                reset("c", vec![col("a", true), col("v", false), col("b", true)]),
                ops(
                    "c",
                    vec![
                        Op::Upsert(row(&["1", "x", "2"])),
                        Op::Upsert(row(&["1", "y", "3"])),
                    ],
                ),
            ],
            off(1),
        )
        .unwrap();
        assert_eq!(s.tables["c"].rows.len(), 2);
        assert_eq!(s.tables["c"].rows[&key(&["1", "3"])], row(&["1", "y", "3"]));
        s.apply(&[ops("c", vec![Op::Delete(key(&["1", "2"]))])], off(2))
            .unwrap();
        assert_eq!(s.tables["c"].rows.len(), 1);
    }
}
