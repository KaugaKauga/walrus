//! The single writer (PLAN.md §5.3): applies replication events to the change index in group
//! commits, acknowledges durable positions, and publishes encoded transactions to sessions.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{broadcast, mpsc, watch};

use crate::change_index::IndexWriter;
use crate::encode::EncodedTxn;
use crate::metrics::Metrics;
use crate::pg::decode::Change;
use crate::schema::TableSchema;

/// Max transactions per store commit (PLAN.md §5.3).
pub const MAX_BATCH_TXNS: usize = 1000;
/// How often the writer records a clock sample and runs a retention slice (§5.5).
pub const CLOCK_INTERVAL: Duration = Duration::from_secs(60);
/// Index entries deleted per retention slice.
pub const GC_SLICE: usize = 10_000;

/// What the replication task sends the writer, in stream order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalEvent {
    /// `commit_lsn` is the transaction's commit LSN (`Begin.final_lsn` = `Commit.lsn`).
    Begin {
        xid: u32,
        commit_lsn: u64,
    },
    Change(Change),
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
    },
}

pub struct WriterCtx {
    pub index: IndexWriter,
    /// `Meta::last_lsn` at startup.
    pub last_lsn: u64,
    pub epoch: u64,
    pub tables: Arc<Vec<TableSchema>>,
    pub events: mpsc::Receiver<WalEvent>,
    pub live: broadcast::Sender<Arc<EncodedTxn>>,
    /// End LSN of the last durably committed transaction (I1).
    pub acks: watch::Sender<u64>,
    pub retention: Duration,
    pub metrics: Arc<Metrics>,
}

/// Blocking loop; run it with `tokio::task::spawn_blocking`. Returns Ok when `events` is closed
/// (after committing what it has), Err on store failures or protocol violations (a change
/// outside Begin/Commit, mismatched commit LSN).
///
/// ```text
/// loop:
///   wait for the next event (blocking_recv); None → finish
///   open a store batch
///   repeat:
///     Begin: skip the whole transaction if commit_lsn <= last_lsn (I3, metrics.txns_skipped);
///            else start a TxnBuilder
///     Change: index it (record each affected key → commit_lsn: insert/update new key, old key on
///             key change, delete key; truncate → Batch::truncate per table), push to builder
///     Commit: last_lsn = commit_lsn; finish the builder; keep (EncodedTxn, end_lsn)
///     take the next event with try_recv; stop at a commit boundary when the channel is empty
///     or MAX_BATCH_TXNS transactions are in the batch
///   commit(last_lsn)                                        -- I1, I2 (fsync)
///   acks.send(end_lsn of the last committed transaction)
///   publish each EncodedTxn in order: live.send(Arc::new(txn)) (no receivers is fine)  -- I4
///   metrics: applied_lsn, txns_applied, changes_applied, index_commits
///   every CLOCK_INTERVAL: record_clock(now, last_lsn); gc_slice(now - retention, GC_SLICE)
/// ```
///
/// A transaction may span batches only in the sense that a batch is never committed in the
/// middle of one: the inner loop keeps blocking for events until it reaches a commit boundary.
pub fn run(ctx: WriterCtx) -> Result<()> {
    let _ = ctx;
    todo!()
}
