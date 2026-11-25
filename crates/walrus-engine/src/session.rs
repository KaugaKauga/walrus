//! One SSE session: catch-up, join, live (PLAN.md §7.1, I7, I8).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use deadpool_postgres::{Object, Pool, Transaction};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_postgres::IsolationLevel;
use walrus_protocol::{Offset, Op, SSE_KEEPALIVE, SSE_RETRY};

use crate::change_index::{IndexReader, Lookup};
use crate::encode::{EncodedTxn, Item, commit_frame, encode_items, rows_frames, table_reset_frame};
use crate::metrics::Metrics;
use crate::pg::read;
use crate::pg::xid::RecentXids;
use crate::pg::xid::Snapshot;
use crate::schema::{Key, TableId, TableSchema};

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
/// Live ring capacity (transactions); sessions further behind are closed (PLAN.md §7.3).
pub const LIVE_RING: usize = 1024;
/// How often a session blocked on a full socket buffer checks whether it fell behind the ring.
const LAG_POLL: Duration = Duration::from_millis(100);

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
    /// Transactions applied by the writer, for the snapshot check (PLAN.md §7.2).
    pub recent_xids: Arc<RecentXids>,
}

/// A validated `/v1/stream` request.
#[derive(Clone, Debug)]
pub struct Request {
    /// Subscribed tables, deduplicated.
    pub tables: Vec<TableId>,
    pub offset: Option<Offset>,
}

/// How a catch-up table is treated while the join filter is on (I8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableJoin {
    /// Nothing was read for this table: live transactions are sent whole.
    Nothing,
    /// The whole table was read from the snapshot: drop all of a visible transaction's items.
    DropAll,
    /// Only these keys were read: drop the items for exactly these keys.
    DropKeys(HashSet<Key>),
}

/// The join filter's state: the catch-up snapshot S and what the catch-up read per table.
#[derive(Clone, Debug)]
pub struct Join {
    pub snapshot: Snapshot,
    /// Per subscribed table; a table missing here is `Nothing`.
    pub tables: HashMap<TableId, TableJoin>,
}

/// One table's share of a visible transaction after filtering.
#[derive(Debug, PartialEq, Eq)]
pub enum TableOut {
    /// Nothing was dropped: send the transaction's shared frames.
    Whole(TableId),
    /// Some items were dropped: re-encode these.
    Items(TableId, Vec<Item>),
}

/// What the join does with one live transaction.
#[derive(Debug, PartialEq, Eq)]
pub enum JoinDecision {
    /// The transaction began after S: the join is over, handle this and later transactions
    /// as plain live ones.
    EndJoin,
    /// Not visible in S: send whole.
    Send,
    /// Visible in S and every item was read by the catch-up (or no subscribed table is touched).
    Skip,
    /// Visible in S: send these parts (non-empty), then the commit frame.
    Filtered(Vec<TableOut>),
    /// The join cannot be resolved per key; close the session.
    Close(&'static str),
}

enum Visibility {
    BeganAfter,
    Visible,
    Invisible,
}

/// Pure join logic (I8, PLAN.md §7.1): decide what to do with a live transaction while the join
/// filter is on. No encoding happens here.
pub fn join_filter(
    join: &Join,
    subscribed: &[TableId],
    txn: &EncodedTxn,
    tables: &[TableSchema],
) -> JoinDecision {
    let visibility = if join.snapshot.began_after(txn.xid) {
        Visibility::BeganAfter
    } else if join.snapshot.visible(txn.xid) {
        Visibility::Visible
    } else {
        Visibility::Invisible
    };
    decide(visibility, &join.tables, subscribed, txn, tables)
}

fn decide(
    visibility: Visibility,
    modes: &HashMap<TableId, TableJoin>,
    subscribed: &[TableId],
    txn: &EncodedTxn,
    tables: &[TableSchema],
) -> JoinDecision {
    match visibility {
        Visibility::BeganAfter => return JoinDecision::EndJoin,
        Visibility::Invisible => return JoinDecision::Send,
        Visibility::Visible => {}
    }
    // S already holds this transaction's effect on the rows the catch-up read; those ops must
    // not be applied a second time. Everything else was not read and must be sent.
    let mut out = Vec::new();
    for t in txn.tables.iter().filter(|t| subscribed.contains(&t.table)) {
        match modes.get(&t.table).unwrap_or(&TableJoin::Nothing) {
            TableJoin::Nothing => out.push(TableOut::Whole(t.table)),
            TableJoin::DropAll => {}
            TableJoin::DropKeys(keys) => {
                let schema = &tables[t.table];
                let mut kept = Vec::with_capacity(t.items.len());
                for item in &t.items {
                    match item {
                        // A reset wipes rows we cannot tell apart per key.
                        Item::Reset => return JoinDecision::Close("truncate during join"),
                        Item::Op(op) => {
                            if keys.contains(&op_key(schema, op)) {
                                continue;
                            }
                            // A kept patch takes unchanged values from the row at `from`; if the
                            // catch-up already replaced or removed that row, they are gone.
                            if let Op::Patch {
                                from: Some(from), ..
                            } = op
                                && keys.contains(from)
                            {
                                return JoinDecision::Close(
                                    "patch from a key read by the catch-up",
                                );
                            }
                            kept.push(item.clone());
                        }
                    }
                }
                if kept.len() == t.items.len() {
                    out.push(TableOut::Whole(t.table));
                } else if !kept.is_empty() {
                    out.push(TableOut::Items(t.table, kept));
                }
            }
        }
    }
    if out.is_empty() {
        JoinDecision::Skip
    } else {
        JoinDecision::Filtered(out)
    }
}

/// The key an op targets. Same contract as `encode::op_key`; kept local so the join logic is
/// self-contained and testable. Key columns are never NULL or unchanged in pgoutput, so a
/// patch's row always holds its key.
fn op_key(table: &TableSchema, op: &Op) -> Key {
    match op {
        Op::Delete(key) => key.clone(),
        Op::Upsert(row) | Op::Patch { row, .. } => table.key_of(row).unwrap_or_default(),
    }
}

/// The client went away: end quietly.
#[derive(Debug)]
struct ClientGone;

impl std::fmt::Display for ClientGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("client gone")
    }
}

impl std::error::Error for ClientGone {}

/// Why a session ended.
enum Stop {
    ClientGone,
    /// Closed on purpose; the reason is logged.
    Close(&'static str),
    Failed(anyhow::Error),
}

impl From<ClientGone> for Stop {
    fn from(_: ClientGone) -> Self {
        Stop::ClientGone
    }
}

impl From<anyhow::Error> for Stop {
    fn from(e: anyhow::Error) -> Self {
        // `ClientGone` travels through `copy_rows` callbacks as an anyhow error.
        if e.downcast_ref::<ClientGone>().is_some() {
            Stop::ClientGone
        } else {
            Stop::Failed(e)
        }
    }
}

type Out = mpsc::Sender<Bytes>;

async fn send(out: &Out, bytes: Bytes) -> Result<(), ClientGone> {
    if bytes.is_empty() {
        return Ok(());
    }
    out.send(bytes).await.map_err(|_| ClientGone)
}

/// `send` for the live loop. While the client's socket is full this session is not receiving
/// from the ring, so `Lagged` would never surface: poll the ring and close once the session is
/// further behind than the ring holds (`closes_lagged`), instead of waiting on a client that
/// may never read. Shutdown also ends the wait, so a stuck client can't hold shutdown up.
async fn send_live(
    shared: &Shared,
    out: &Out,
    rx: &broadcast::Receiver<Arc<EncodedTxn>>,
    bytes: Bytes,
) -> Result<(), Stop> {
    if bytes.is_empty() {
        return Ok(());
    }
    let send = out.send(bytes);
    tokio::pin!(send);
    let mut shutdown = shared.shutdown.clone();
    loop {
        tokio::select! {
            r = &mut send => return r.map_err(|_| Stop::ClientGone),
            _ = shutdown_requested(&mut shutdown) => return Err(Stop::Close("shutdown")),
            _ = tokio::time::sleep(LAG_POLL) => {
                if rx.len() > LIVE_RING {
                    Metrics::inc(&shared.metrics.closes_lagged);
                    return Err(Stop::Close("lagged while blocked on the socket"));
                }
            }
        }
    }
}

/// Resolves when shutdown is requested. A helper so the watch guard is never held across an
/// await in a caller.
async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|s| *s).await;
}

/// What the catch-up does for one table.
enum Mode {
    Full,
    Delta(Vec<Key>),
    None,
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
    let (out, rx) = mpsc::channel(SOCKET_BUFFER);
    tokio::spawn(async move {
        match run(&shared, &request, &out).await {
            Ok(()) => tracing::debug!("session ended"),
            Err(Stop::ClientGone) => tracing::debug!("session ended: client left"),
            Err(Stop::Close(reason)) => tracing::debug!(reason, "session closed"),
            Err(Stop::Failed(e)) => tracing::warn!(error = format!("{e:#}"), "session failed"),
        }
    });
    rx
}

async fn run(shared: &Shared, request: &Request, out: &Out) -> Result<(), Stop> {
    let metrics = &shared.metrics;
    send(out, Bytes::from_static(SSE_RETRY.as_bytes())).await?;

    // I7: subscribe before reading the index and before taking the Postgres snapshot, so no
    // transaction committed after P0 can be missed.
    let mut rx = shared.live.subscribe();

    let since = request
        .offset
        .filter(|o| o.epoch == shared.epoch)
        .map(|o| o.lsn);
    let requests: Vec<(TableId, u64)> = match since {
        Some(lsn) => request.tables.iter().map(|&t| (t, lsn)).collect(),
        None => Vec::new(),
    };
    let reader = shared.reader.clone();
    let index = tokio::task::spawn_blocking(move || reader.snapshot(&requests, DELTA_CAP))
        .await
        .context("index read task failed")??;
    let p0 = index.last_lsn;

    let mut modes: Vec<(TableId, Mode)> = Vec::with_capacity(request.tables.len());
    if since.is_some() {
        for (&t, lookup) in request.tables.iter().zip(index.lookups) {
            modes.push((
                t,
                match lookup {
                    Lookup::Full => Mode::Full,
                    Lookup::OverCap => {
                        Metrics::inc(&metrics.delta_over_cap);
                        Mode::Full
                    }
                    Lookup::Keys(keys) if keys.is_empty() => Mode::None,
                    Lookup::Keys(keys) => Mode::Delta(keys),
                },
            ));
        }
    } else {
        modes.extend(request.tables.iter().map(|&t| (t, Mode::Full)));
    }

    let reads_postgres = modes.iter().any(|(_, m)| !matches!(m, Mode::None));
    let mut join = if reads_postgres {
        if modes.iter().any(|(_, m)| matches!(m, Mode::Full)) {
            Metrics::inc(&metrics.catchups_full);
        } else {
            Metrics::inc(&metrics.catchups_delta);
        }
        let snapshot = catch_up(shared, out, &modes, p0).await?;
        send(out, commit_frame(shared.epoch, p0)).await?;
        let tables = modes
            .into_iter()
            .map(|(t, m)| {
                let j = match m {
                    Mode::Full => TableJoin::DropAll,
                    Mode::Delta(keys) => TableJoin::DropKeys(keys.into_iter().collect()),
                    Mode::None => TableJoin::Nothing,
                };
                (t, j)
            })
            .collect();
        Some(Join { snapshot, tables })
    } else {
        // Nothing changed for this client: Postgres is not queried.
        Metrics::inc(&metrics.catchups_none);
        send(out, commit_frame(shared.epoch, p0)).await?;
        None
    };

    let mut shutdown = shared.shutdown.clone();
    // Keepalives are due after KEEPALIVE without sending this client anything; transactions it
    // isn't subscribed to don't count.
    let mut keepalive_at = tokio::time::Instant::now() + KEEPALIVE;
    loop {
        let txn = tokio::select! {
            r = rx.recv() => match r {
                Ok(txn) => txn,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    Metrics::inc(&metrics.closes_lagged);
                    tracing::debug!(skipped = n, "session lagged behind the live ring");
                    return Err(Stop::Close("lagged"));
                }
                Err(broadcast::error::RecvError::Closed) => return Err(Stop::Close("live ring closed")),
            },
            _ = shutdown_requested(&mut shutdown) => return Ok(()),
            _ = out.closed() => return Err(Stop::ClientGone),
            _ = tokio::time::sleep_until(keepalive_at) => {
                send(out, Bytes::from_static(SSE_KEEPALIVE.as_bytes())).await?;
                keepalive_at = tokio::time::Instant::now() + KEEPALIVE;
                continue;
            }
        };

        // Covered by the catch-up (or the client's offset) already.
        if txn.lsn <= p0 {
            continue;
        }
        let touches = txn.tables.iter().any(|t| request.tables.contains(&t.table));
        if touches && txn.oversized {
            Metrics::inc(&metrics.closes_oversized);
            return Err(Stop::Close("oversized transaction"));
        }

        let decision = match &join {
            Some(j) => join_filter(j, &request.tables, &txn, &shared.tables),
            None => JoinDecision::Send,
        };
        let sent = match decision {
            JoinDecision::EndJoin => {
                // Every later transaction began after S too, so none can be visible in it.
                join = None;
                send_whole(shared, out, request, &txn, &rx).await?
            }
            JoinDecision::Send => send_whole(shared, out, request, &txn, &rx).await?,
            JoinDecision::Skip => false,
            JoinDecision::Filtered(parts) => {
                for part in parts {
                    match part {
                        TableOut::Whole(t) => {
                            if let Some(tt) = txn.table(t) {
                                send_live(shared, out, &rx, tt.frames.clone()).await?;
                            }
                        }
                        TableOut::Items(t, items) => {
                            send_live(shared, out, &rx, encode_items(&shared.tables[t], &items))
                                .await?;
                        }
                    }
                }
                send_live(shared, out, &rx, txn.commit_frame.clone()).await?;
                true
            }
            JoinDecision::Close(reason) => {
                Metrics::inc(&metrics.closes_join);
                return Err(Stop::Close(reason));
            }
        };
        if sent {
            keepalive_at = tokio::time::Instant::now() + KEEPALIVE;
        }
    }
}

/// Send a transaction's frames for the subscribed tables it touched, then its commit frame.
/// Returns whether anything was sent.
async fn send_whole(
    shared: &Shared,
    out: &Out,
    request: &Request,
    txn: &EncodedTxn,
    rx: &broadcast::Receiver<Arc<EncodedTxn>>,
) -> Result<bool, Stop> {
    let mut any = false;
    for t in txn
        .tables
        .iter()
        .filter(|t| request.tables.contains(&t.table))
    {
        any = true;
        send_live(shared, out, rx, t.frames.clone()).await?;
    }
    if any {
        send_live(shared, out, rx, txn.commit_frame.clone()).await?;
    }
    Ok(any)
}

/// Read what the client is missing from one Postgres snapshot and send it; returns the
/// snapshot S for the join. Waits for a pooled connection (admission control).
async fn catch_up(
    shared: &Shared,
    out: &Out,
    modes: &[(TableId, Mode)],
    p0: u64,
) -> Result<Snapshot, Stop> {
    let mut shutdown = shared.shutdown.clone();
    let mut conn = tokio::select! {
        c = shared.pool.get() => c.map_err(|e| anyhow!(e)).context("getting a catch-up connection")?,
        _ = shutdown_requested(&mut shutdown) => return Err(Stop::Close("shutdown")),
    };
    let result = tokio::select! {
        r = tokio::time::timeout(CATCHUP_TIME_LIMIT, read_all(shared, out, &mut conn, modes, p0)) => {
            r.unwrap_or(Err(Stop::Close("catch-up time limit")))
        }
        _ = shutdown_requested(&mut shutdown) => Err(Stop::Close("shutdown")),
    };
    if result.is_err() {
        // The connection may be mid-COPY or mid-transaction: never return it to the pool.
        drop(Object::take(conn));
    }
    result
}

async fn read_all(
    shared: &Shared,
    out: &Out,
    conn: &mut Object,
    modes: &[(TableId, Mode)],
    p0: u64,
) -> Result<Snapshot, Stop> {
    let metrics = &shared.metrics;
    for attempt in 0..SNAPSHOT_ATTEMPTS {
        let tx = conn
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .context("starting the catch-up transaction")?;
        let snapshot = read::take_snapshot(&tx).await?;
        if !shared.recent_xids.misses_applied(&snapshot, p0) {
            copy_tables(shared, out, &tx, modes).await?;
            tx.commit()
                .await
                .context("ending the catch-up transaction")?;
            return Ok(snapshot);
        }
        // I8: the engine already applied (and will never resend) a transaction S can't see yet.
        // Take a later snapshot once it is visible.
        tx.rollback()
            .await
            .context("rolling back a rejected snapshot")?;
        Metrics::inc(&metrics.join_retries);
        let backoff = Duration::from_millis((10u64 << attempt.min(7)).min(1000));
        tokio::time::sleep(backoff).await;
    }
    Err(Stop::Close("no usable snapshot"))
}

/// Send every table's catch-up rows, read inside the snapshot transaction `tx`.
async fn copy_tables(
    shared: &Shared,
    out: &Out,
    tx: &Transaction<'_>,
    modes: &[(TableId, Mode)],
) -> Result<(), Stop> {
    let metrics = &shared.metrics;
    for (t, mode) in modes {
        let table = &shared.tables[*t];
        let ncols = table.columns.len();
        match mode {
            Mode::None => {}
            Mode::Full => {
                send(out, table_reset_frame(table)).await?;
                let n = read::copy_rows(tx, &read::full_query(table), ncols, |rows| async move {
                    let ops: Vec<Op> = rows.into_iter().map(Op::Upsert).collect();
                    send(out, rows_frames(table, &ops)).await?;
                    Ok(())
                })
                .await?;
                Metrics::add(&metrics.rows_read_full, n);
            }
            Mode::Delta(keys) => {
                Metrics::add(&metrics.delta_keys, keys.len() as u64);
                for batch in keys.chunks(read::DELTA_BATCH) {
                    // Keys of the rows Postgres returned; the rest of the batch was deleted.
                    // A mutex (not a RefCell) keeps the callback's future `Send`.
                    let returned: Mutex<HashSet<Key>> = Mutex::new(HashSet::new());
                    let returned_ref = &returned;
                    let n = read::copy_rows(
                        tx,
                        &read::delta_query(table, batch),
                        ncols,
                        |rows| async move {
                            let mut keys = Vec::with_capacity(rows.len());
                            for row in &rows {
                                keys.push(
                                    table
                                        .key_of(row)
                                        .context("catch-up row with a NULL key column")?,
                                );
                            }
                            returned_ref
                                .lock()
                                .expect("no code panics while holding this lock")
                                .extend(keys);
                            let ops: Vec<Op> = rows.into_iter().map(Op::Upsert).collect();
                            send(out, rows_frames(table, &ops)).await?;
                            Ok(())
                        },
                    )
                    .await?;
                    Metrics::add(&metrics.rows_read_delta, n);
                    let returned = returned
                        .into_inner()
                        .expect("no code panics while holding this lock");
                    let deletes: Vec<Op> = batch
                        .iter()
                        .filter(|k| !returned.contains(*k))
                        .map(|k| Op::Delete(k.clone()))
                        .collect();
                    send(out, rows_frames(table, &deletes)).await?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::TxnTable;
    use crate::schema::ColumnSchema;

    fn schema(name: &str) -> TableSchema {
        TableSchema {
            name: format!("public.{name}"),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    type_oid: 20,
                },
                ColumnSchema {
                    name: "v".into(),
                    type_name: "text".into(),
                    type_oid: 25,
                },
            ],
            key: vec![0],
        }
    }

    fn tables() -> Vec<TableSchema> {
        vec![schema("a"), schema("b"), schema("c")]
    }

    fn row(id: &str, v: &str) -> Vec<Option<String>> {
        vec![Some(id.into()), Some(v.into())]
    }

    fn up(id: &str) -> Item {
        Item::Op(Op::Upsert(row(id, "x")))
    }

    fn del(id: &str) -> Item {
        Item::Op(Op::Delete(vec![id.into()]))
    }

    fn txn(parts: Vec<(TableId, Vec<Item>)>) -> EncodedTxn {
        EncodedTxn {
            lsn: 100,
            xid: 7,
            oversized: false,
            tables: parts
                .into_iter()
                .map(|(table, items)| TxnTable {
                    table,
                    items,
                    frames: Bytes::from_static(b"frames"),
                })
                .collect(),
            commit_frame: Bytes::from_static(b"commit"),
        }
    }

    fn keys(ids: &[&str]) -> TableJoin {
        TableJoin::DropKeys(ids.iter().map(|i| vec![i.to_string()]).collect())
    }

    fn modes(v: Vec<(TableId, TableJoin)>) -> HashMap<TableId, TableJoin> {
        v.into_iter().collect()
    }

    fn run_decide(
        vis: Visibility,
        m: &HashMap<TableId, TableJoin>,
        subscribed: &[TableId],
        t: &EncodedTxn,
    ) -> JoinDecision {
        decide(vis, m, subscribed, t, &tables())
    }

    #[test]
    fn invisible_txn_is_sent_whole() {
        let m = modes(vec![(0, TableJoin::DropAll)]);
        let t = txn(vec![(0, vec![up("1")])]);
        assert_eq!(
            run_decide(Visibility::Invisible, &m, &[0], &t),
            JoinDecision::Send
        );
    }

    #[test]
    fn visible_txn_on_full_table_is_dropped() {
        let m = modes(vec![(0, TableJoin::DropAll)]);
        let t = txn(vec![(0, vec![up("1"), del("2")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Skip
        );
    }

    #[test]
    fn visible_txn_drops_exactly_the_delta_keys() {
        let m = modes(vec![(0, keys(&["1", "3"]))]);
        let t = txn(vec![(0, vec![up("1"), up("2"), del("3"), del("4")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Filtered(vec![TableOut::Items(0, vec![up("2"), del("4")])])
        );
    }

    #[test]
    fn visible_txn_without_overlap_keeps_shared_frames() {
        let m = modes(vec![(0, keys(&["1"]))]);
        let t = txn(vec![(0, vec![up("2")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Filtered(vec![TableOut::Whole(0)])
        );
    }

    #[test]
    fn visible_txn_mixes_table_modes() {
        let m = modes(vec![
            (0, TableJoin::DropAll),
            (1, keys(&["1"])),
            (2, TableJoin::Nothing),
        ]);
        let t = txn(vec![
            (0, vec![up("1")]),
            (1, vec![up("1"), up("2")]),
            (2, vec![up("1")]),
        ]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0, 1, 2], &t),
            JoinDecision::Filtered(vec![TableOut::Items(1, vec![up("2")]), TableOut::Whole(2)])
        );
    }

    #[test]
    fn visible_reset_on_delta_table_closes() {
        let m = modes(vec![(0, keys(&["1"]))]);
        let t = txn(vec![(0, vec![Item::Reset, up("2")])]);
        assert!(matches!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Close(_)
        ));
    }

    #[test]
    fn visible_reset_on_full_table_is_dropped() {
        let m = modes(vec![(0, TableJoin::DropAll)]);
        let t = txn(vec![(0, vec![Item::Reset, up("2")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Skip
        );
    }

    #[test]
    fn kept_patch_from_a_delta_key_closes() {
        let m = modes(vec![(0, keys(&["1"]))]);
        let patch = Item::Op(Op::Patch {
            row: vec![Some("2".into()), None],
            keep: vec![1],
            from: Some(vec!["1".into()]),
        });
        let t = txn(vec![(0, vec![patch, del("1")])]);
        assert!(matches!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Close(_)
        ));
    }

    #[test]
    fn dropped_patch_from_a_delta_key_does_not_close() {
        // The patch's own key was read, so the patch is dropped and its `from` is moot.
        let m = modes(vec![(0, keys(&["1", "2"]))]);
        let patch = Item::Op(Op::Patch {
            row: vec![Some("2".into()), None],
            keep: vec![1],
            from: Some(vec!["1".into()]),
        });
        let t = txn(vec![(0, vec![patch, del("1")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Skip
        );
    }

    #[test]
    fn kept_patch_from_other_key_is_kept() {
        let m = modes(vec![(0, keys(&["9"]))]);
        let patch = Item::Op(Op::Patch {
            row: vec![Some("2".into()), None],
            keep: vec![1],
            from: None,
        });
        let t = txn(vec![(0, vec![patch])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Filtered(vec![TableOut::Whole(0)])
        );
    }

    #[test]
    fn began_after_ends_the_join() {
        let m = modes(vec![(0, TableJoin::DropAll)]);
        let t = txn(vec![(0, vec![up("1")])]);
        assert_eq!(
            run_decide(Visibility::BeganAfter, &m, &[0], &t),
            JoinDecision::EndJoin
        );
    }

    #[test]
    fn txn_touching_no_subscribed_table() {
        let m = modes(vec![(0, TableJoin::DropAll)]);
        let t = txn(vec![(1, vec![up("1")])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Skip
        );
        // Invisible: `Send`, which writes nothing for a transaction touching no subscribed table.
        assert_eq!(
            run_decide(Visibility::Invisible, &m, &[0], &t),
            JoinDecision::Send
        );
    }

    #[test]
    fn unsubscribed_table_with_reset_does_not_close() {
        let m = modes(vec![(0, keys(&["1"]))]);
        let t = txn(vec![(1, vec![Item::Reset])]);
        assert_eq!(
            run_decide(Visibility::Visible, &m, &[0], &t),
            JoinDecision::Skip
        );
    }

    #[test]
    fn join_filter_uses_the_snapshot() {
        let join = Join {
            snapshot: Snapshot {
                xmin: 5,
                xmax: 10,
                xip: vec![7],
            },
            tables: modes(vec![(0, TableJoin::DropAll)]),
        };
        let mut t = txn(vec![(0, vec![up("1")])]);
        // xid 7 is in progress in S: not visible.
        assert_eq!(join_filter(&join, &[0], &t, &tables()), JoinDecision::Send);
        t.xid = 6;
        assert_eq!(join_filter(&join, &[0], &t, &tables()), JoinDecision::Skip);
        t.xid = 10;
        assert_eq!(
            join_filter(&join, &[0], &t, &tables()),
            JoinDecision::EndJoin
        );
    }
}
