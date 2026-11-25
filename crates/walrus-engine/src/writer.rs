//! The single writer (PLAN.md §5.3): applies replication events to the change index in group
//! commits, acknowledges durable positions, and publishes encoded transactions to sessions.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tokio::sync::{broadcast, mpsc, watch};

use crate::change_index::{Batch, IndexWriter};
use crate::encode::{EncodedTxn, TxnBuilder};
use crate::metrics::Metrics;
use crate::pg::decode::Change;
use crate::pg::xid::RecentXids;
use crate::schema::TableSchema;

/// Max transactions per store commit (PLAN.md §5.3).
pub const MAX_BATCH_TXNS: usize = 1000;
/// How often the writer records a clock sample and runs a retention slice (§5.5).
pub const CLOCK_INTERVAL: Duration = Duration::from_secs(60);
/// Index entries deleted per retention slice.
pub const GC_SLICE: usize = 10_000;

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
    /// Every applied transaction's xid, for the catch-up snapshot check (PLAN.md §7.2).
    pub recent_xids: Arc<RecentXids>,
    /// A 64-bit xid assigned at startup: the reference for widening streamed 32-bit xids.
    pub xid_reference: u64,
}

/// Blocking loop; run it with `tokio::task::spawn_blocking`. Returns Ok when `events` is closed
/// (after committing what it has), Err on store failures or protocol violations (a change
/// outside Begin/Commit, mismatched commit LSN). Group commit (PLAN.md §5.3):
/// a batch is never committed in the middle of a transaction.
pub fn run(mut ctx: WriterCtx) -> Result<()> {
    let mut last_lsn = ctx.last_lsn;
    let mut xid_reference = ctx.xid_reference;
    let mut next_clock = Instant::now() + CLOCK_INTERVAL;
    // A previous GC slice deleted a full slice: more is waiting, so run another after the
    // next batch instead of waiting a whole interval.
    let mut gc_backlog = false;

    while let Some(first) = ctx.events.blocking_recv() {
        let mut batch = ctx.index.batch().context("opening a store transaction")?;
        let mut done: Vec<(EncodedTxn, u64)> = Vec::new();
        // Highest end LSN that is durable once this batch commits: committed transactions
        // and replayed ones we skipped (already durable, I3).
        let mut ack_lsn = 0u64;
        let mut changes_applied = 0u64;
        let mut open: Option<OpenTxn> = None;
        let mut closed = false;
        let mut event = first;

        loop {
            match event {
                WalEvent::Begin { xid, commit_lsn } => {
                    if open.is_some() {
                        bail!("protocol violation: Begin inside a transaction (xid {xid})");
                    }
                    // I3: replay after a restart is at-least-once; skip what is already indexed.
                    let skip = commit_lsn <= last_lsn;
                    if skip {
                        Metrics::inc(&ctx.metrics.txns_skipped);
                    } else {
                        // I8: recorded before the index commit that covers it, so a session
                        // that reads P0 >= commit_lsn always checks its snapshot against it.
                        let xid64 = crate::pg::xid::extend(xid_reference, xid);
                        xid_reference = xid_reference.max(xid64);
                        ctx.recent_xids.push(xid64, commit_lsn);
                    }
                    open = Some(OpenTxn {
                        commit_lsn,
                        builder: (!skip)
                            .then(|| TxnBuilder::new(ctx.tables.clone(), xid, commit_lsn)),
                    });
                }
                WalEvent::Change(change) => {
                    let Some(txn) = open.as_mut() else {
                        bail!("protocol violation: change outside a transaction");
                    };
                    if let Some(builder) = txn.builder.as_mut() {
                        index_change(&mut batch, &ctx.tables, &change, txn.commit_lsn)?;
                        builder.push(&change);
                        changes_applied += 1;
                    }
                }
                WalEvent::Commit {
                    commit_lsn,
                    end_lsn,
                } => {
                    let Some(txn) = open.take() else {
                        bail!("protocol violation: Commit outside a transaction");
                    };
                    if txn.commit_lsn != commit_lsn {
                        bail!(
                            "protocol violation: Begin said commit LSN {} but Commit says {commit_lsn}",
                            txn.commit_lsn
                        );
                    }
                    ack_lsn = ack_lsn.max(end_lsn);
                    if let Some(builder) = txn.builder {
                        last_lsn = commit_lsn;
                        done.push((builder.finish(ctx.epoch), end_lsn));
                    }
                }
            }

            // Never end a batch mid-transaction: block for the rest of it. At a boundary, take
            // only what is already queued (opportunistic group commit, no linger).
            let next = if open.is_some() {
                ctx.events.blocking_recv()
            } else if done.len() >= MAX_BATCH_TXNS {
                None
            } else {
                match ctx.events.try_recv() {
                    Ok(e) => Some(e),
                    Err(mpsc::error::TryRecvError::Empty) => None,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        closed = true;
                        None
                    }
                }
            };
            match next {
                Some(e) => event = e,
                None => {
                    // `blocking_recv` returning None means the sender is gone mid-transaction
                    // (shutdown): that transaction was never acknowledged, so Postgres will
                    // resend it and we drop it here.
                    closed |= open.is_some();
                    break;
                }
            }
        }

        if done.is_empty() {
            // Nothing new to persist (everything was a replay): no fsync needed.
            drop(batch);
        } else {
            // I1, I2: `last_lsn` is written in the same store transaction as the entries.
            // Timed so the benchmark can tell when the store becomes the bottleneck.
            let commit_started = Instant::now();
            batch
                .commit(last_lsn)
                .context("committing the change index")?;
            Metrics::add(
                &ctx.metrics.index_commit_micros,
                commit_started.elapsed().as_micros() as u64,
            );
            Metrics::inc(&ctx.metrics.index_commits);
            Metrics::add(&ctx.metrics.txns_applied, done.len() as u64);
            Metrics::add(&ctx.metrics.changes_applied, changes_applied);
            ctx.metrics.applied_lsn.store(last_lsn, Relaxed);
        }

        // I1: acknowledge only after the commit above is durable.
        if ack_lsn > *ctx.acks.borrow() {
            ctx.acks.send_replace(ack_lsn);
        }
        // I4: sessions see a transaction only after it is durable. No receivers is fine.
        for (txn, _) in done {
            let _ = ctx.live.send(Arc::new(txn));
        }

        let now = Instant::now();
        if now >= next_clock {
            next_clock = now + CLOCK_INTERVAL;
            let now_us = now_micros();
            ctx.index
                .record_clock(now_us, last_lsn)
                .context("recording the clock sample")?;
            gc_backlog = true;
        }
        if gc_backlog {
            let cutoff = now_micros().saturating_sub(ctx.retention.as_micros() as i64);
            let deleted = ctx
                .index
                .gc_slice(cutoff, GC_SLICE)
                .context("retention GC")?;
            gc_backlog = deleted >= GC_SLICE;
        }

        if closed {
            return Ok(());
        }
    }
    Ok(())
}

struct OpenTxn {
    commit_lsn: u64,
    /// `None` when the transaction is a replay we skip (I3).
    builder: Option<TxnBuilder>,
}

/// Record every key the change affects at the transaction's commit LSN (I5/I6).
fn index_change(
    batch: &mut Batch<'_>,
    tables: &[TableSchema],
    change: &Change,
    lsn: u64,
) -> Result<()> {
    let table_of = |id: usize| {
        tables
            .get(id)
            .with_context(|| format!("change for unknown table id {id}"))
    };
    match change {
        Change::Insert { table, row } => {
            let key = table_of(*table)?
                .key_of(row)
                .context("inserted row has a NULL key column")?;
            batch.record(*table, &key, lsn)?;
        }
        Change::Update {
            table,
            old_key,
            row,
            ..
        } => {
            let key = table_of(*table)?
                .key_of(row)
                .context("updated row has a NULL key column")?;
            batch.record(*table, &key, lsn)?;
            // The old key stopped existing: clients must learn it was deleted.
            if let Some(old) = old_key {
                batch.record(*table, old, lsn)?;
            }
        }
        Change::Delete { table, key } => {
            table_of(*table)?;
            batch.record(*table, key, lsn)?;
        }
        Change::Truncate { tables: truncated } => {
            for &table in truncated {
                table_of(table)?;
                // I6: we cannot list the removed keys, so raise the floor instead.
                batch.truncate(table, lsn)?;
            }
        }
    }
    Ok(())
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
