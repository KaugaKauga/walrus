//! The change index (PLAN.md §5): a durable map `(table, primary key) → commit LSN of the key's
//! latest change`, per-table floors, `last_lsn` and the epoch. SQLite (rusqlite, bundled), WAL
//! mode, `synchronous=FULL`. Schema: PLAN.md §5.2.
//!
//! One writer connection (owned by the writer thread) and a small pool of read-only connections
//! for sessions. Blocking code: call readers from `tokio::task::spawn_blocking`.

use std::path::Path;

use anyhow::Result;

use crate::schema::{Key, TableId, TableSchema};

/// File name inside the data directory.
pub const FILE_NAME: &str = "walrus.sqlite";
/// Store format version, checked on open.
pub const FORMAT_VERSION: u32 = 1;

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

/// Open the store in `dir`. `Ok(None)` if there is no store file or it is not `initialized`
/// (a crash during new-epoch setup). Errors on a different `FORMAT_VERSION`.
pub fn open(dir: &Path) -> Result<Option<(IndexWriter, IndexReader, Meta)>> {
    let _ = dir;
    todo!()
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
    let _ = (dir, epoch, tables, floor_lsn);
    todo!()
}

/// Delete the store files in `dir` (including `-wal`/`-shm`). Ok if they don't exist.
pub fn remove(dir: &Path) -> Result<()> {
    let _ = dir;
    todo!()
}

/// The writer's connection. Not `Sync`; lives on the writer thread.
pub struct IndexWriter {
    // Implementation-defined (a rusqlite::Connection).
}

impl IndexWriter {
    /// Begin a store transaction for a group commit.
    pub fn batch(&mut self) -> Result<Batch<'_>> {
        todo!()
    }

    /// Record a `(time, last_lsn)` sample for time-based retention (§5.5).
    pub fn record_clock(&mut self, time_us: i64, lsn: u64) -> Result<()> {
        let _ = (time_us, lsn);
        todo!()
    }

    /// One bounded retention slice (§5.5): R = LSN of the newest clock sample with
    /// `time_us <= cutoff_time_us` (none → return 0). Raise every table's floor to at least R,
    /// then delete up to `limit` entries with `lsn <= floor` and clock samples older than that
    /// sample. Floors are raised before entries are deleted (I5/I6). Returns entries deleted.
    pub fn gc_slice(&mut self, cutoff_time_us: i64, limit: usize) -> Result<usize> {
        let _ = (cutoff_time_us, limit);
        todo!()
    }
}

/// One open store transaction.
pub struct Batch<'a> {
    // Implementation-defined (a rusqlite::Transaction<'a>).
    _conn: std::marker::PhantomData<&'a mut IndexWriter>,
}

impl Batch<'_> {
    /// Upsert `(table, key) → lsn`.
    pub fn record(&mut self, table: TableId, key: &Key, lsn: u64) -> Result<()> {
        let _ = (table, key, lsn);
        todo!()
    }

    /// Truncate: raise the table's floor to `lsn` (if higher) and delete its entries (I6).
    pub fn truncate(&mut self, table: TableId, lsn: u64) -> Result<()> {
        let _ = (table, lsn);
        todo!()
    }

    /// Write `last_lsn` and commit (I2): durable when this returns (`synchronous=FULL`).
    pub fn commit(self, last_lsn: u64) -> Result<()> {
        let _ = last_lsn;
        todo!()
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

/// Read-only access for sessions; cheap to clone, `Send + Sync`.
#[derive(Clone)]
pub struct IndexReader {
    // Implementation-defined: e.g. Arc<Mutex<Vec<rusqlite::Connection>>> of read-only
    // connections, opened on demand and returned after use.
}

impl IndexReader {
    /// In ONE read transaction: `last_lsn`, and for each `(table, since)` request a `Lookup`
    /// (range scan on `changes_by_lsn`: `lsn > since`), stopping at `cap + 1` keys.
    pub fn snapshot(&self, requests: &[(TableId, u64)], cap: usize) -> Result<ReadSnapshot> {
        let _ = (requests, cap);
        todo!()
    }

    /// Current floors, one per table (for tests and metrics).
    pub fn floors(&self) -> Result<Vec<u64>> {
        todo!()
    }

    /// Number of index entries per table (for tests and metrics).
    pub fn entry_counts(&self) -> Result<Vec<u64>> {
        todo!()
    }
}
