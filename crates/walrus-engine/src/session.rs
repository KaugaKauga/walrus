//! One SSE session: catch-up, join, live (PLAN.md §7.1, I7, I8).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::Pool;
use tokio::sync::{broadcast, mpsc, watch};
use walrus_protocol::Offset;

use crate::change_index::IndexReader;
use crate::encode::EncodedTxn;
use crate::metrics::Metrics;
use crate::schema::{TableId, TableSchema};

/// Delta keys per table above which the table is sent in full (PLAN.md §5.4).
pub const DELTA_CAP: usize = 10_000;
/// Snapshot attempts before giving up on a catch-up (§7.2).
pub const SNAPSHOT_ATTEMPTS: usize = 20;
/// Upper bound for one catch-up (§7.4).
pub const CATCHUP_TIME_LIMIT: Duration = Duration::from_secs(300);
/// Idle interval between keepalive comments.
pub const KEEPALIVE: Duration = Duration::from_secs(15);
/// Frames buffered between a session and its HTTP response.
pub const SOCKET_BUFFER: usize = 64;

/// Everything sessions share.
pub struct Shared {
    pub epoch: u64,
    pub tables: Arc<Vec<TableSchema>>,
    pub reader: IndexReader,
    pub live: broadcast::Sender<Arc<EncodedTxn>>,
    pub pool: Pool,
    pub metrics: Arc<Metrics>,
    pub ready: watch::Receiver<bool>,
    pub shutdown: watch::Receiver<bool>,
}

/// A validated `/v1/stream` request.
#[derive(Clone, Debug)]
pub struct Request {
    /// Subscribed tables, deduplicated.
    pub tables: Vec<TableId>,
    pub offset: Option<Offset>,
}

/// Spawn the session task; returns the receiving end of its frame stream (the HTTP body). The
/// task ends when the client goes away (send fails), on shutdown, or when it closes the stream on
/// purpose (lag, oversized transaction, join conflict, catch-up failure).
///
/// ```text
/// send SSE_RETRY
/// rx = live.subscribe()                                               -- first (I7)
/// requests: per table, (table, offset.lsn) if offset.epoch == epoch, else the table is Full
/// snapshot = reader.snapshot(requests, DELTA_CAP) on spawn_blocking   -- P0 = last_lsn
/// modes: Full (Lookup::Full / OverCap / other epoch) | Delta(keys) | None (no keys)
/// if any Full or non-empty Delta:                                     -- else catchups_none
///   conn = pool.get()                                                 -- admission control
///   for attempt in 0..SNAPSHOT_ATTEMPTS:
///     tx = REPEATABLE READ READ ONLY; S = read::take_snapshot(&tx)
///     None → rollback, metrics.join_retries, sleep 10ms·2^attempt (max 1s), retry
///   all attempts failed → close
///   Full tables: table_reset_frame, then COPY full_query → Upsert ops → rows_frames
///   Delta tables: COPY delta_query per DELTA_BATCH keys → Upsert ops; afterwards Delete ops
///     for the table's keys Postgres did not return
///   commit tx (on any error: drop the connection instead of returning it to the pool)
///   the whole catch-up runs under CATCHUP_TIME_LIMIT
/// send commit_frame(epoch, P0)
/// join = Some((S, per table: DropAll for Full, DropKeys(HashSet) for Delta, Nothing for None))
///        if the catch-up read Postgres, else None
/// live loop (select on rx, shutdown, KEEPALIVE timer → SSE_KEEPALIVE):
///   Lagged → close (closes_lagged); Closed → close
///   txn.lsn <= P0 → skip
///   txn touches no subscribed table → skip (but still run the join's began_after check)
///   txn.oversized → close (closes_oversized)
///   if join active and S.began_after(txn.xid) → join = None
///   if join active and S.visible(txn.xid):
///     per subscribed table in txn: DropAll → drop everything; DropKeys(K) → drop ops whose
///       op_key ∈ K, keep the rest — but close (closes_join) if this table has a Reset, or a
///       kept Patch whose `from` ∈ K (PLAN.md §7.1); Nothing → keep all
///     send encode_items(kept) per table; if anything was sent, send txn.commit_frame
///   else: send txn.table(t).frames for each subscribed table, then txn.commit_frame
/// ```
pub fn spawn(shared: Arc<Shared>, request: Request) -> mpsc::Receiver<Bytes> {
    let _ = (shared, request);
    todo!()
}
