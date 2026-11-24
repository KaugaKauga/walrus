//! The replication stream: pgoutput → `WalEvent`s for the writer, acknowledgements back to
//! Postgres (PLAN.md §4.2, §4.6, §7.4).

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::{Context, Result, anyhow, bail};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};
use tokio::sync::{mpsc, watch};

use crate::metrics::Metrics;
use crate::pg::decode::{Change, Decoder};
use crate::pg::setup::PgParams;
use crate::schema::TableSchema;
use crate::writer::WalEvent;

pub struct ReplicationCtx {
    pub params: PgParams,
    pub slot: String,
    pub publication: String,
    pub tables: Arc<Vec<TableSchema>>,
    /// Bounded: when the writer is behind, sending waits (backpressure, §7.4).
    pub events: mpsc::Sender<WalEvent>,
    /// End LSN of the last durably indexed transaction (I1); forwarded to
    /// `ReplicationClient::update_applied_lsn`.
    pub acks: watch::Receiver<u64>,
    /// Set to true once START_REPLICATION is accepted.
    pub ready: watch::Sender<bool>,
    pub shutdown: watch::Receiver<bool>,
    pub metrics: Arc<Metrics>,
}

/// Stream until shutdown or error. Connects with `PgParams::replication_config` (START at 0/0:
/// resume from the slot), then:
/// - `Begin { final_lsn, xid }` → `WalEvent::Begin { xid, commit_lsn: final_lsn }`;
/// - `XLogData { data }` → `Decoder::decode` → one `WalEvent::Change` per change;
/// - `Commit { lsn, end_lsn }` → `WalEvent::Commit { commit_lsn: lsn, end_lsn }`;
/// - `KeepAlive { wal_end }` → `metrics.server_wal_end`; `Message`s are ignored;
/// - every change of `acks` → `update_applied_lsn(end_lsn)`.
///
/// On shutdown: stop the client and return Ok (dropping `events` lets the writer finish).
pub async fn run(ctx: ReplicationCtx) -> Result<()> {
    let cfg = ctx.params.replication_config(&ctx.slot, &ctx.publication);
    // Local receivers, so waiting on them does not borrow `ctx` while `forward` uses it.
    let mut shutdown = ctx.shutdown.clone();
    let mut acks = ctx.acks.clone();
    let mut client = ReplicationClient::connect(cfg)
        .await
        .context("opening the replication connection")?;

    // Connecting returns before the server accepted START_REPLICATION; wait for it so `ready`
    // means the slot is held and the stream is up.
    tokio::select! {
        r = client.wait_started() => {
            r.with_context(|| format!(
                "starting replication on slot {:?} (is another walrus using it?)", ctx.slot
            ))?;
        }
        _ = shutdown_requested(&mut shutdown) => return stop(client).await,
    }
    let _ = ctx.ready.send(true);

    let mut decoder = Decoder::new(ctx.tables.clone());
    let mut changes: Vec<Change> = Vec::new();
    loop {
        tokio::select! {
            event = client.recv() => {
                let Some(event) = event.context("replication stream failed")? else {
                    // Only our own stop() ends the stream normally, and that path returns below.
                    bail!("the replication stream ended unexpectedly");
                };
                if !forward(&ctx, &mut decoder, &mut changes, event).await? {
                    return stop(client).await;
                }
            }
            r = acks.changed() => {
                // Sender dropped: the writer is gone; its task reports why.
                r.map_err(|_| anyhow!("the writer stopped"))?;
                let lsn = *acks.borrow_and_update();
                // I1: the writer only publishes LSNs whose index commit is durable.
                client.update_applied_lsn(Lsn(lsn));
            }
            _ = shutdown_requested(&mut shutdown) => return stop(client).await,
        }
    }
}

/// Turn one replication event into writer events. `false` if shutdown arrived while waiting
/// for room in the writer queue.
async fn forward(
    ctx: &ReplicationCtx,
    decoder: &mut Decoder,
    changes: &mut Vec<Change>,
    event: ReplicationEvent,
) -> Result<bool> {
    match event {
        ReplicationEvent::Begin { final_lsn, xid, .. } => {
            send(
                ctx,
                WalEvent::Begin {
                    xid,
                    commit_lsn: final_lsn.as_u64(),
                },
            )
            .await
        }
        ReplicationEvent::XLogData { data, .. } => {
            changes.clear();
            decoder.decode(&data, changes)?;
            for change in changes.drain(..) {
                if !send(ctx, WalEvent::Change(change)).await? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        ReplicationEvent::Commit { lsn, end_lsn, .. } => {
            send(
                ctx,
                WalEvent::Commit {
                    commit_lsn: lsn.as_u64(),
                    end_lsn: end_lsn.as_u64(),
                },
            )
            .await
        }
        ReplicationEvent::KeepAlive { wal_end, .. } => {
            ctx.metrics.server_wal_end.store(wal_end.as_u64(), Relaxed);
            Ok(true)
        }
        // Not ours (`messages 'true'` is on for everyone) or only sent with a stop LSN.
        ReplicationEvent::Message { .. } | ReplicationEvent::StoppedAt { .. } => Ok(true),
    }
}

/// Queue an event for the writer; waits when the queue is full (backpressure, §7.4).
async fn send(ctx: &ReplicationCtx, event: WalEvent) -> Result<bool> {
    let mut shutdown = ctx.shutdown.clone();
    tokio::select! {
        r = ctx.events.send(event) => {
            r.map_err(|_| anyhow!("the writer stopped"))?;
            Ok(true)
        }
        _ = shutdown_requested(&mut shutdown) => Ok(false),
    }
}

/// Graceful stop. Anything buffered but not yet sent is simply replayed on the next start (I3).
async fn stop(mut client: ReplicationClient) -> Result<()> {
    if let Err(e) = client.shutdown().await {
        tracing::warn!("replication stream did not stop cleanly: {e}");
    }
    Ok(())
}

/// Resolves when shutdown is requested (or its sender is gone). A function of its own so the
/// `watch::Ref` guard is dropped before any `.await` in the caller (it is not `Send`).
async fn shutdown_requested(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}
