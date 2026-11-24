//! The change index (PLAN.md §5): a durable map `(table, primary key) → commit LSN of the key's
//! latest change`, per-table floors, `last_lsn` and the epoch. SQLite (rusqlite, bundled), WAL
//! mode, `synchronous=FULL`. Schema: PLAN.md §5.2.
//!
//! One writer connection (owned by the writer thread) and a small pool of read-only connections
//! for sessions. Blocking code: call readers from `tokio::task::spawn_blocking`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

use crate::schema::{Key, TableId, TableSchema};

/// File name inside the data directory.
pub const FILE_NAME: &str = "walrus.sqlite";
/// Store format version, checked on open.
pub const FORMAT_VERSION: u32 = 1;

/// How long a connection waits for a lock before failing. Readers never block the writer in WAL
/// mode; this only covers checkpoint and open races.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const SCHEMA_SQL: &str = "
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE synced_tables (
  id        INTEGER PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE,
  columns   TEXT NOT NULL,
  floor_lsn INTEGER NOT NULL
);
CREATE TABLE changes (
  table_id INTEGER NOT NULL,
  pk       BLOB    NOT NULL,
  lsn      INTEGER NOT NULL,
  PRIMARY KEY (table_id, pk)
) WITHOUT ROWID;
CREATE INDEX changes_by_lsn ON changes (table_id, lsn);
CREATE TABLE lsn_clock (
  time_us INTEGER PRIMARY KEY,
  lsn     INTEGER NOT NULL
);
";

/// What an initialized store holds besides the index entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub epoch: u64,
    pub last_lsn: u64,
    /// Table schemas; a table's id is its index here.
    pub tables: Vec<TableSchema>,
    /// Per-table floor LSN, same order as `tables`.
    pub floors: Vec<u64>,
}

/// LSNs are u64 in Rust but SQLite integers are i64. Real LSNs are far below 2^63.
fn to_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| anyhow!("value {v} does not fit in a SQLite integer"))
}

fn from_i64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| anyhow!("negative value {v} in the change index"))
}

/// Primary key as stored: for each part, u32 big-endian byte length + UTF-8 bytes.
fn encode_key(key: &Key) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(key.iter().map(|p| p.len() + 4).sum());
    for part in key {
        let len = u32::try_from(part.len()).context("key part longer than 4 GiB")?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(part.as_bytes());
    }
    Ok(out)
}

fn decode_key(mut bytes: &[u8]) -> Result<Key> {
    let mut key = Vec::new();
    while !bytes.is_empty() {
        let (len, rest) = bytes
            .split_first_chunk::<4>()
            .context("truncated key length in change index")?;
        let len = u32::from_be_bytes(*len) as usize;
        if rest.len() < len {
            bail!("truncated key part in change index");
        }
        let (part, rest) = rest.split_at(len);
        key.push(String::from_utf8(part.to_vec()).context("key part is not UTF-8")?);
        bytes = rest;
    }
    Ok(key)
}

fn db_path(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

/// Open the store in `dir`. `Ok(None)` if there is no store file or it is not `initialized`
/// (a crash during new-epoch setup). Errors on a different `FORMAT_VERSION`.
pub fn open(dir: &Path) -> Result<Option<(IndexWriter, IndexReader, Meta)>> {
    let path = db_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {}", path.display()))?;
    configure_writer(&conn)?;

    // A file without `meta` is a store whose creation never committed.
    let has_meta: bool = conn.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
        [],
        |r| r.get(0),
    )?;
    if !has_meta {
        return Ok(None);
    }
    if meta_int(&conn, "initialized")? != Some(1) {
        return Ok(None);
    }
    let version = meta_int(&conn, "format_version")?;
    if version != Some(i64::from(FORMAT_VERSION)) {
        bail!(
            "store format version {version:?} is not supported (expected {FORMAT_VERSION}); \
             remove the data directory to start over"
        );
    }
    let meta = read_meta(&conn)?;
    let reader = IndexReader::new(path);
    Ok(Some((IndexWriter { conn }, reader, meta)))
}

fn meta_int(conn: &Connection, key: &str) -> Result<Option<i64>> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| {
        r.get::<_, i64>(0)
    })
    .optional()
    .with_context(|| format!("reading meta.{key}"))
}

fn read_meta(conn: &Connection) -> Result<Meta> {
    let epoch = meta_int(conn, "epoch")?.context("meta.epoch missing")?;
    let last_lsn = meta_int(conn, "last_lsn")?.context("meta.last_lsn missing")?;
    let mut stmt = conn.prepare("SELECT columns, floor_lsn FROM synced_tables ORDER BY id")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
    let mut tables = Vec::new();
    let mut floors = Vec::new();
    for row in rows {
        let (json, floor) = row?;
        tables.push(serde_json::from_str(&json).context("parsing stored table schema")?);
        floors.push(from_i64(floor)?);
    }
    Ok(Meta {
        epoch: from_i64(epoch)?,
        last_lsn: from_i64(last_lsn)?,
        tables,
        floors,
    })
}

fn configure_writer(conn: &Connection) -> Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let mode: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        bail!("could not enable WAL mode (got {mode})");
    }
    // Durability is an invariant (I1): we acknowledge Postgres only after the commit is on disk.
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

/// Create a fresh store in `dir` (created if missing), replacing any existing one. In ONE
/// transaction (I10): meta (epoch, last_lsn = floor_lsn, format_version, initialized = 1), every
/// table with `floor_lsn`.
pub fn create(
    dir: &Path,
    epoch: u64,
    tables: &[TableSchema],
    floor_lsn: u64,
) -> Result<(IndexWriter, IndexReader, Meta)> {
    remove(dir)?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = db_path(dir);
    let mut conn =
        Connection::open(&path).with_context(|| format!("creating {}", path.display()))?;
    configure_writer(&conn)?;

    let floor = to_i64(floor_lsn)?;
    let tx = conn.transaction()?;
    tx.execute_batch(SCHEMA_SQL)?;
    {
        let mut put = tx.prepare("INSERT INTO meta (key, value) VALUES (?1, ?2)")?;
        put.execute(params!["epoch", to_i64(epoch)?])?;
        put.execute(params!["last_lsn", floor])?;
        put.execute(params!["format_version", i64::from(FORMAT_VERSION)])?;
        put.execute(params!["initialized", 1_i64])?;
        let mut add = tx.prepare(
            "INSERT INTO synced_tables (id, name, columns, floor_lsn) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (id, table) in tables.iter().enumerate() {
            let json = serde_json::to_string(table).context("serializing table schema")?;
            add.execute(params![to_i64(id as u64)?, table.name, json, floor])?;
        }
    }
    tx.commit()?;

    let meta = Meta {
        epoch,
        last_lsn: floor_lsn,
        tables: tables.to_vec(),
        floors: vec![floor_lsn; tables.len()],
    };
    Ok((IndexWriter { conn }, IndexReader::new(path), meta))
}

/// Delete the store files in `dir` (including `-wal`/`-shm`). Ok if they don't exist.
pub fn remove(dir: &Path) -> Result<()> {
    let base = db_path(dir);
    for suffix in ["", "-wal", "-shm"] {
        let mut name = base.clone().into_os_string();
        name.push(suffix);
        let file = PathBuf::from(name);
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing {}", file.display())),
        }
    }
    Ok(())
}

/// The writer's connection. Not `Sync`; lives on the writer thread.
pub struct IndexWriter {
    conn: Connection,
}

impl IndexWriter {
    /// Begin a store transaction for a group commit.
    pub fn batch(&mut self) -> Result<Batch<'_>> {
        // Immediate: we are the only writer, so take the write lock up front.
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(Batch { tx })
    }

    /// Record a `(time, last_lsn)` sample for time-based retention (§5.5).
    pub fn record_clock(&mut self, time_us: i64, lsn: u64) -> Result<()> {
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO lsn_clock (time_us, lsn) VALUES (?1, ?2)")?
            .execute(params![time_us, to_i64(lsn)?])?;
        Ok(())
    }

    /// One bounded retention slice (§5.5): R = LSN of the newest clock sample with
    /// `time_us <= cutoff_time_us` (none → return 0). Raise every table's floor to at least R,
    /// then delete up to `limit` entries with `lsn <= floor` and clock samples older than that
    /// sample. Floors are raised before entries are deleted (I5/I6). Returns entries deleted.
    pub fn gc_slice(&mut self, cutoff_time_us: i64, limit: usize) -> Result<usize> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sample: Option<(i64, i64)> = tx
            .query_row(
                "SELECT time_us, lsn FROM lsn_clock WHERE time_us <= ?1
                 ORDER BY time_us DESC LIMIT 1",
                [cutoff_time_us],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((sample_time, r)) = sample else {
            return Ok(0);
        };
        // Floors first: once raised, the entries below them are no longer promised (I5), so
        // deleting them in any number of slices keeps the invariant true.
        tx.execute(
            "UPDATE synced_tables SET floor_lsn = max(floor_lsn, ?1)",
            [r],
        )?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let deleted = tx.execute(
            "DELETE FROM changes WHERE (table_id, pk) IN (
               SELECT c.table_id, c.pk FROM changes c
               JOIN synced_tables t ON t.id = c.table_id
               WHERE c.lsn <= t.floor_lsn LIMIT ?1)",
            [limit],
        )?;
        tx.execute("DELETE FROM lsn_clock WHERE time_us < ?1", [sample_time])?;
        tx.commit()?;
        Ok(deleted)
    }
}

/// One open store transaction. Dropping it without `commit` rolls everything back.
pub struct Batch<'a> {
    tx: rusqlite::Transaction<'a>,
}

impl Batch<'_> {
    /// Upsert `(table, key) → lsn`.
    pub fn record(&mut self, table: TableId, key: &Key, lsn: u64) -> Result<()> {
        let pk = encode_key(key)?;
        self.tx
            .prepare_cached(
                "INSERT INTO changes (table_id, pk, lsn) VALUES (?1, ?2, ?3)
                 ON CONFLICT (table_id, pk) DO UPDATE SET lsn = excluded.lsn",
            )?
            .execute(params![to_i64(table as u64)?, pk, to_i64(lsn)?])?;
        Ok(())
    }

    /// Truncate: raise the table's floor to `lsn` (if higher) and delete its entries (I6).
    pub fn truncate(&mut self, table: TableId, lsn: u64) -> Result<()> {
        let id = to_i64(table as u64)?;
        self.tx.execute(
            "UPDATE synced_tables SET floor_lsn = max(floor_lsn, ?2) WHERE id = ?1",
            params![id, to_i64(lsn)?],
        )?;
        self.tx
            .execute("DELETE FROM changes WHERE table_id = ?1", [id])?;
        Ok(())
    }

    /// Write `last_lsn` and commit (I2): durable when this returns (`synchronous=FULL`).
    pub fn commit(self, last_lsn: u64) -> Result<()> {
        self.tx.execute(
            "UPDATE meta SET value = ?1 WHERE key = 'last_lsn'",
            [to_i64(last_lsn)?],
        )?;
        self.tx.commit().context("committing change index batch")?;
        Ok(())
    }
}

/// Result of one table's delta lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// `since` is below the table's floor or above `last_lsn`: send the table in full.
    Full,
    /// More than `cap` keys changed: send the table in full.
    OverCap,
    /// The keys whose latest change is after `since` (possibly empty), in any order.
    Keys(Vec<Key>),
}

/// A consistent read of the index (one read transaction).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSnapshot {
    /// P0 in PLAN.md §7.1.
    pub last_lsn: u64,
    /// One per request, same order.
    pub lookups: Vec<Lookup>,
}

struct ReaderShared {
    path: PathBuf,
    /// Idle read-only connections; opened on demand, returned after a successful use.
    pool: Mutex<Vec<Connection>>,
}

/// Read-only access for sessions; cheap to clone, `Send + Sync`.
#[derive(Clone)]
pub struct IndexReader {
    shared: Arc<ReaderShared>,
}

impl IndexReader {
    fn new(path: PathBuf) -> Self {
        IndexReader {
            shared: Arc::new(ReaderShared {
                path,
                pool: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Run `f` with a pooled connection. A connection that saw an error is dropped, not reused.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let pooled = self
            .shared
            .pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let mut conn = match pooled {
            Some(c) => c,
            None => {
                let c = Connection::open_with_flags(
                    &self.shared.path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .with_context(|| format!("opening {} read-only", self.shared.path.display()))?;
                c.busy_timeout(BUSY_TIMEOUT)?;
                c
            }
        };
        let out = f(&mut conn)?;
        self.shared
            .pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(conn);
        Ok(out)
    }

    /// In ONE read transaction: `last_lsn`, and for each `(table, since)` request a `Lookup`
    /// (range scan on `changes_by_lsn`: `lsn > since`), stopping at `cap + 1` keys.
    pub fn snapshot(&self, requests: &[(TableId, u64)], cap: usize) -> Result<ReadSnapshot> {
        self.with_conn(|conn| {
            // Deferred read transaction: in WAL mode the snapshot is fixed by the first read,
            // so last_lsn, floors and keys all come from the same commit.
            let tx = conn.transaction()?;
            let last_lsn = from_i64(
                tx.query_row("SELECT value FROM meta WHERE key = 'last_lsn'", [], |r| {
                    r.get::<_, i64>(0)
                })
                .context("reading last_lsn")?,
            )?;
            let mut lookups = Vec::with_capacity(requests.len());
            for &(table, since) in requests {
                let id = to_i64(table as u64)?;
                let floor = from_i64(
                    tx.prepare_cached("SELECT floor_lsn FROM synced_tables WHERE id = ?1")?
                        .query_row([id], |r| r.get::<_, i64>(0))
                        .with_context(|| format!("no table with id {table}"))?,
                )?;
                if since < floor || since > last_lsn {
                    lookups.push(Lookup::Full);
                    continue;
                }
                // `since <= last_lsn < 2^63`, so the conversion cannot fail.
                let since = to_i64(since)?;
                let limit = i64::try_from(cap.saturating_add(1)).unwrap_or(i64::MAX);
                let mut stmt = tx.prepare_cached(
                    "SELECT pk FROM changes WHERE table_id = ?1 AND lsn > ?2 LIMIT ?3",
                )?;
                let mut rows = stmt.query(params![id, since, limit])?;
                let mut keys = Vec::new();
                while let Some(row) = rows.next()? {
                    keys.push(decode_key(&row.get::<_, Vec<u8>>(0)?)?);
                }
                if keys.len() > cap {
                    lookups.push(Lookup::OverCap);
                } else {
                    lookups.push(Lookup::Keys(keys));
                }
            }
            Ok(ReadSnapshot { last_lsn, lookups })
        })
    }

    /// Current floors, one per table (for tests and metrics).
    pub fn floors(&self) -> Result<Vec<u64>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT floor_lsn FROM synced_tables ORDER BY id")?;
            let rows = stmt.query_map([], |r| r.get::<_, i64>(0))?;
            rows.map(|v| from_i64(v?)).collect()
        })
    }

    /// Number of index entries per table (for tests and metrics).
    pub fn entry_counts(&self) -> Result<Vec<u64>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT (SELECT count(*) FROM changes WHERE table_id = t.id)
                 FROM synced_tables t ORDER BY t.id",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, i64>(0))?;
            rows.map(|v| from_i64(v?)).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn table(name: &str) -> TableSchema {
        TableSchema {
            name: name.into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                type_name: "text".into(),
                type_oid: 25,
            }],
            key: vec![0],
        }
    }

    fn k(s: &str) -> Key {
        vec![s.to_string()]
    }

    fn sorted(lookup: Lookup) -> Vec<Key> {
        match lookup {
            Lookup::Keys(mut v) => {
                v.sort();
                v
            }
            other => panic!("expected keys, got {other:?}"),
        }
    }

    fn fresh(dir: &Path) -> (IndexWriter, IndexReader) {
        let tables = [table("public.a"), table("public.b")];
        let (w, r, _) = create(dir, 7, &tables, 100).unwrap();
        (w, r)
    }

    #[test]
    fn create_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let tables = vec![table("public.a"), table("public.b")];
        let (w, _r, meta) = create(dir.path(), 3, &tables, 42).unwrap();
        assert_eq!(meta.last_lsn, 42);
        assert_eq!(meta.floors, vec![42, 42]);
        drop(w);
        let (_w, r, opened) = open(dir.path()).unwrap().unwrap();
        assert_eq!(opened, meta);
        assert_eq!(r.floors().unwrap(), vec![42, 42]);
    }

    #[test]
    fn create_replaces_existing_and_makes_dir() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("nested/data");
        let (mut w, _r) = fresh(&sub);
        let mut b = w.batch().unwrap();
        b.record(0, &k("x"), 150).unwrap();
        b.commit(150).unwrap();
        drop(w);
        let (_w, r, meta) = create(&sub, 8, &[table("public.c")], 5).unwrap();
        assert_eq!(meta.epoch, 8);
        assert_eq!(r.entry_counts().unwrap(), vec![0]);
    }

    #[test]
    fn open_missing_and_uninitialized() {
        let dir = tempfile::tempdir().unwrap();
        assert!(open(dir.path()).unwrap().is_none());

        // An empty file (creation never committed).
        std::fs::write(dir.path().join(FILE_NAME), b"").unwrap();
        assert!(open(dir.path()).unwrap().is_none());

        let (w, _r) = fresh(dir.path());
        w.conn
            .execute("UPDATE meta SET value = 0 WHERE key = 'initialized'", [])
            .unwrap();
        drop(w);
        assert!(open(dir.path()).unwrap().is_none());
    }

    #[test]
    fn open_rejects_other_format_version() {
        let dir = tempfile::tempdir().unwrap();
        let (w, _r) = fresh(dir.path());
        w.conn
            .execute(
                "UPDATE meta SET value = 99 WHERE key = 'format_version'",
                [],
            )
            .unwrap();
        drop(w);
        assert!(open(dir.path()).is_err());
    }

    #[test]
    fn record_commit_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        b.record(0, &k("a"), 110).unwrap();
        b.record(0, &k("b"), 120).unwrap();
        b.record(0, &k("c"), 130).unwrap();
        b.record(1, &k("z"), 125).unwrap();
        b.commit(130).unwrap();

        let snap = r
            .snapshot(
                &[(0, 100), (0, 110), (0, 130), (1, 100), (0, 50), (0, 131)],
                10,
            )
            .unwrap();
        assert_eq!(snap.last_lsn, 130);
        let mut it = snap.lookups.into_iter();
        assert_eq!(sorted(it.next().unwrap()), vec![k("a"), k("b"), k("c")]);
        assert_eq!(sorted(it.next().unwrap()), vec![k("b"), k("c")]);
        assert_eq!(it.next().unwrap(), Lookup::Keys(vec![]));
        assert_eq!(sorted(it.next().unwrap()), vec![k("z")]);
        assert_eq!(it.next().unwrap(), Lookup::Full); // below floor
        assert_eq!(it.next().unwrap(), Lookup::Full); // above last_lsn
    }

    #[test]
    fn over_cap() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        for i in 0..5 {
            b.record(0, &k(&i.to_string()), 101 + i).unwrap();
        }
        b.commit(110).unwrap();
        let at_cap = r.snapshot(&[(0, 100)], 5).unwrap().lookups.remove(0);
        assert_eq!(sorted(at_cap).len(), 5);
        assert_eq!(
            r.snapshot(&[(0, 100)], 4).unwrap().lookups[0],
            Lookup::OverCap
        );
        assert_eq!(
            r.snapshot(&[(0, 100)], 0).unwrap().lookups[0],
            Lookup::OverCap
        );
    }

    #[test]
    fn upsert_keeps_latest_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        b.record(0, &k("a"), 110).unwrap();
        b.commit(110).unwrap();
        let mut b = w.batch().unwrap();
        b.record(0, &k("a"), 120).unwrap();
        b.commit(120).unwrap();
        assert_eq!(r.entry_counts().unwrap(), vec![1, 0]);
        let snap = r.snapshot(&[(0, 110), (0, 120)], 10).unwrap();
        assert_eq!(snap.lookups[0], Lookup::Keys(vec![k("a")]));
        assert_eq!(snap.lookups[1], Lookup::Keys(vec![]));
    }

    #[test]
    fn truncate_raises_floor_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        b.record(0, &k("a"), 110).unwrap();
        b.record(1, &k("b"), 111).unwrap();
        b.commit(111).unwrap();
        let mut b = w.batch().unwrap();
        b.truncate(0, 120).unwrap();
        b.commit(120).unwrap();
        assert_eq!(r.floors().unwrap(), vec![120, 100]);
        assert_eq!(r.entry_counts().unwrap(), vec![0, 1]);
        let snap = r.snapshot(&[(0, 110), (0, 120), (1, 100)], 10).unwrap();
        assert_eq!(snap.lookups[0], Lookup::Full);
        assert_eq!(snap.lookups[1], Lookup::Keys(vec![]));
        assert_eq!(snap.lookups[2], Lookup::Keys(vec![k("b")]));
        // A lower truncate lsn never lowers the floor.
        let mut b = w.batch().unwrap();
        b.truncate(0, 105).unwrap();
        b.commit(120).unwrap();
        assert_eq!(r.floors().unwrap(), vec![120, 100]);
    }

    #[test]
    fn gc_slice_raises_floors_then_deletes_within_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        for i in 0..6 {
            b.record(0, &k(&format!("a{i}")), 101 + i).unwrap(); // 101..=106
        }
        b.record(1, &k("b"), 107).unwrap();
        b.commit(107).unwrap();
        w.record_clock(1_000, 103).unwrap();
        w.record_clock(2_000, 106).unwrap();
        w.record_clock(3_000, 107).unwrap();

        // No sample at or before the cutoff: nothing happens.
        assert_eq!(w.gc_slice(500, 10).unwrap(), 0);
        assert_eq!(r.floors().unwrap(), vec![100, 100]);

        // R = 106 (sample at 2000): floors rise even though the limit stops deletion short.
        assert_eq!(w.gc_slice(2_500, 2).unwrap(), 2);
        assert_eq!(r.floors().unwrap(), vec![106, 106]);
        assert_eq!(r.entry_counts().unwrap(), vec![4, 1]);
        assert_eq!(w.gc_slice(2_500, 100).unwrap(), 4);
        assert_eq!(r.entry_counts().unwrap(), vec![0, 1]); // lsn 107 > floor survives
        assert_eq!(w.gc_slice(2_500, 100).unwrap(), 0);
        // The older sample (1000) was pruned; the ones at 2000 and 3000 remain.
        let samples: i64 = w
            .conn
            .query_row("SELECT count(*) FROM lsn_clock", [], |r| r.get(0))
            .unwrap();
        assert_eq!(samples, 2);
    }

    #[test]
    fn uncommitted_batch_is_not_visible_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, _r) = fresh(dir.path());
        let mut b = w.batch().unwrap();
        b.record(0, &k("kept"), 110).unwrap();
        b.commit(110).unwrap();
        let mut b = w.batch().unwrap();
        b.record(0, &k("lost"), 120).unwrap();
        drop(b);
        drop(w);
        let (_w, r, meta) = open(dir.path()).unwrap().unwrap();
        assert_eq!(meta.last_lsn, 110);
        let snap = r.snapshot(&[(0, 100)], 10).unwrap();
        assert_eq!(snap.lookups[0], Lookup::Keys(vec![k("kept")]));
    }

    #[test]
    fn composite_empty_and_unicode_keys_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let keys: Vec<Key> = vec![
            vec!["".into()],
            vec!["".into(), "".into()],
            vec!["a".into(), "".into()],
            vec!["".into(), "a".into()],
            vec!["héllo wörld ✓".into(), "日本語".into(), "🦭".into()],
            vec![],
        ];
        let mut b = w.batch().unwrap();
        for (i, key) in keys.iter().enumerate() {
            b.record(0, key, 101 + i as u64).unwrap();
        }
        b.commit(200).unwrap();
        let snap = r.snapshot(&[(0, 100)], 100).unwrap();
        let Lookup::Keys(mut got) = snap.lookups.into_iter().next().unwrap() else {
            panic!("expected keys");
        };
        let mut want = keys;
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn snapshot_is_consistent_with_concurrent_commits() {
        let dir = tempfile::tempdir().unwrap();
        let (mut w, r) = fresh(dir.path());
        let writer = std::thread::spawn(move || {
            for lsn in 101..=400_u64 {
                let mut b = w.batch().unwrap();
                b.record(0, &k(&lsn.to_string()), lsn).unwrap();
                b.commit(lsn).unwrap();
            }
        });
        let mut seen = 0;
        while seen < 400 {
            let snap = r.snapshot(&[(0, 100)], 10_000).unwrap();
            let Lookup::Keys(keys) = &snap.lookups[0] else {
                panic!("unexpected {:?}", snap.lookups[0]);
            };
            assert_eq!(keys.len() as u64, snap.last_lsn - 100);
            for key in keys {
                assert!(key[0].parse::<u64>().unwrap() <= snap.last_lsn);
            }
            seen = snap.last_lsn;
        }
        writer.join().unwrap();
    }

    #[test]
    fn key_codec() {
        let key: Key = vec!["ab".into(), "".into()];
        assert_eq!(
            encode_key(&key).unwrap(),
            [0, 0, 0, 2, b'a', b'b', 0, 0, 0, 0]
        );
        assert_eq!(decode_key(&encode_key(&key).unwrap()).unwrap(), key);
        assert!(decode_key(&[0, 0, 0, 5, b'a']).is_err());
        assert!(decode_key(&[0, 0]).is_err());
    }
}
