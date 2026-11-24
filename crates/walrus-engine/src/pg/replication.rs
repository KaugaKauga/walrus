//! The replication stream: pgoutput → `WalEvent`s for the writer, acknowledgements back to
//! Postgres (PLAN.md §4.2, §4.6, §7.4).

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, watch};

use crate::metrics::Metrics;
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
    let _ = ctx;
    todo!()
}
