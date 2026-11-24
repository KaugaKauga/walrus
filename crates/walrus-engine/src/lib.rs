//! Walrus engine: Postgres logical replication → change index → SSE. See PLAN.md.

pub mod change_index;
pub mod config;
pub mod encode;
pub mod http;
pub mod metrics;
pub mod pg;
pub mod schema;
pub mod session;
pub mod writer;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::pg::setup::{self, PgParams, SlotState};
use crate::pg::{read, replication};
use crate::session::Shared;

/// Replication → writer queue (events).
const EVENT_QUEUE: usize = 4096;
/// Live ring capacity (transactions); sessions further behind are closed (§7.3).
const LIVE_RING: usize = 1024;

/// A running engine.
pub struct Engine {
    /// The HTTP address actually bound.
    pub addr: SocketAddr,
    pub epoch: u64,
    pub metrics: Arc<Metrics>,
    pub shared: Arc<Shared>,
    ready: watch::Receiver<bool>,
    shutdown: watch::Sender<bool>,
    http: JoinHandle<Result<()>>,
    replication: JoinHandle<Result<()>>,
    writer: JoinHandle<Result<()>>,
}

/// Check Postgres, open or create the change index, and start the writer, the replication
/// stream and the HTTP server on `listener` (PLAN.md §4.6). Returns once everything is spawned;
/// use `Engine::wait_ready` to wait for the stream.
pub async fn start(config: Config, listener: TcpListener) -> Result<Engine> {
    config.validate()?;
    let params = PgParams::from_url(&config.database_url)?;
    let client = setup::connect(&params).await?;
    setup::check_server(&client, config.catchup_connections).await?;
    let tables = setup::read_schemas(&client, &config.tables).await?;

    // Reuse the store only if it matches Postgres; anything else starts a new epoch (§4.1).
    let existing = match change_index::open(&config.data_dir)? {
        Some((w, r, meta)) if meta.tables != tables => {
            tracing::warn!("table set or schema changed; starting a new epoch");
            drop((w, r));
            None
        }
        Some(store) => match setup::slot_state(&client, &config.slot).await? {
            SlotState::Ready => Some(store),
            state => {
                tracing::warn!(?state, slot = %config.slot, "slot unusable; starting a new epoch");
                None
            }
        },
        None => None,
    };
    let (index, reader, meta) = match existing {
        Some(store) => store,
        None => setup::new_epoch(&params, &client, &config, &tables).await?,
    };
    drop(client);
    tracing::info!(
        epoch = format!("{:016x}", meta.epoch),
        last_lsn = meta.last_lsn,
        "store open"
    );

    let tables = Arc::new(meta.tables.clone());
    let metrics = Arc::new(Metrics::default());
    let (events_tx, events_rx) = mpsc::channel(EVENT_QUEUE);
    let (acks_tx, acks_rx) = watch::channel(0u64);
    let (live_tx, _) = broadcast::channel(LIVE_RING);
    let (ready_tx, ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let writer = tokio::task::spawn_blocking({
        let ctx = writer::WriterCtx {
            index,
            last_lsn: meta.last_lsn,
            epoch: meta.epoch,
            tables: tables.clone(),
            events: events_rx,
            live: live_tx.clone(),
            acks: acks_tx,
            retention: config.retention,
            metrics: metrics.clone(),
        };
        move || writer::run(ctx)
    });

    let replication = tokio::spawn(replication::run(replication::ReplicationCtx {
        params: params.clone(),
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        tables: tables.clone(),
        events: events_tx,
        acks: acks_rx,
        ready: ready_tx,
        shutdown: shutdown_rx.clone(),
        metrics: metrics.clone(),
    }));

    let shared = Arc::new(Shared {
        epoch: meta.epoch,
        tables,
        reader,
        live: live_tx,
        pool: read::create_pool(&params, config.catchup_connections)?,
        metrics: metrics.clone(),
        ready: ready_rx.clone(),
        shutdown: shutdown_rx,
    });
    let addr = listener.local_addr()?;
    let http = tokio::spawn(http::serve(listener, shared.clone()));

    Ok(Engine {
        addr,
        epoch: meta.epoch,
        metrics,
        shared,
        ready: ready_rx,
        shutdown: shutdown_tx,
        http,
        replication,
        writer,
    })
}

impl Engine {
    /// Wait until the replication stream is up (sessions are served from then on).
    pub async fn wait_ready(&mut self) -> Result<()> {
        tokio::select! {
            r = self.ready.wait_for(|r| *r) => { r.context("engine stopped before ready")?; Ok(()) }
            r = &mut self.replication => Err(task_end("replication", r)),
            r = &mut self.writer => Err(task_end("writer", r)),
        }
    }

    /// Run until a component fails or ctrl-c, then shut down.
    pub async fn run_until_stopped(mut self) -> Result<()> {
        let failure = tokio::select! {
            _ = tokio::signal::ctrl_c() => None,
            r = &mut self.http => Some(task_end("http", r)),
            r = &mut self.replication => Some(task_end("replication", r)),
            r = &mut self.writer => Some(task_end("writer", r)),
        };
        let _ = self.shutdown.send(true);
        match failure {
            Some(e) => Err(e),
            None => self.shutdown().await,
        }
    }

    /// Graceful shutdown (PLAN.md §7.6): stop HTTP and sessions, stop replication, let the
    /// writer commit what it has.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(true);
        flatten("http", self.http.await)?;
        flatten("replication", self.replication.await)?;
        flatten("writer", self.writer.await)?;
        Ok(())
    }
}

fn flatten(name: &str, r: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    r.map_err(|e| anyhow!("{name} task panicked: {e}"))?
        .with_context(|| format!("{name} failed"))
}

fn task_end(
    name: &str,
    r: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> anyhow::Error {
    match flatten(name, r) {
        Ok(()) => anyhow!("{name} stopped unexpectedly"),
        Err(e) => e,
    }
}
