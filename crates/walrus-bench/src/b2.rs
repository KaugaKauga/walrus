//! B2: sustained write throughput, replication lag and group commit.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::{mpsc, watch};
use tokio_postgres::Client;
use walrus_engine::metrics::Metrics;

use crate::env::{ms, percentile, table, with_engine};
use crate::sse::Sse;

const WRITERS: usize = 8;
const SECONDS: u64 = 30;
const KEY_SPACE: i64 = 50_000;

const SETUP: &str =
    "CREATE TABLE bench_w (id bigint PRIMARY KEY, v bigint NOT NULL, payload text NOT NULL);";

/// What the one subscribed client saw.
#[derive(Default)]
struct Seen {
    commits: AtomicU64,
    /// LSN of the latest commit offset received.
    lsn: AtomicU64,
    /// Times the server closed the stream and the client resumed.
    reconnects: AtomicU64,
}

pub async fn run() -> Result<String> {
    with_engine(SETUP, &["public.bench_w"], async |env, engine| {
        let m = &engine.metrics;
        let seen = Arc::new(Seen::default());
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        let (stop_tx, stop_rx) = watch::channel(false);
        let reader = tokio::spawn(client(engine.addr, seen.clone(), ready_tx, stop_rx));
        tokio::time::timeout(Duration::from_secs(30), ready_rx.recv())
            .await?
            .context("client died before its first commit")?;

        let admin = env.client().await?;
        let before = Snapshot::take(m);
        let deadline = Instant::now() + Duration::from_secs(SECONDS);
        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            writers.push(tokio::spawn(write(env.client().await?, deadline)));
        }

        // Sample lag once a second while the writers run.
        let (mut lag_metric, mut lag_pg) = (Vec::new(), Vec::new());
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            lag_metric
                .push(Metrics::get(&m.server_wal_end).saturating_sub(Metrics::get(&m.applied_lsn)));
            lag_pg.push(
                current_wal(&admin)
                    .await?
                    .saturating_sub(Metrics::get(&m.applied_lsn)),
            );
        }
        let in_window = Snapshot::take(m).since(&before);
        let mut commit_us = Vec::new();
        let mut txns = 0u64;
        for w in writers {
            let (n, lat) = w.await??;
            txns += n;
            commit_us.extend(lat);
        }
        commit_us.sort_unstable();
        let drain_start = Instant::now();

        // Drain: wait until the engine applied every transaction and the client has seen them.
        let applied_total = loop {
            let applied = Metrics::get(&m.txns_applied) - before.txns;
            if applied >= txns {
                break applied;
            }
            if drain_start.elapsed() > Duration::from_secs(120) {
                bail!("engine applied {applied} of {txns} txns after 120 s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let engine_drain = drain_start.elapsed();
        while seen.lsn.load(Relaxed) < Metrics::get(&m.applied_lsn)
            && drain_start.elapsed() < Duration::from_secs(120)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let client_drain = drain_start.elapsed();
        let total = Snapshot::take(m).since(&before);
        let _ = stop_tx.send(true);
        let bytes = reader.await??;

        let secs = SECONDS as f64;
        let rate = |n: u64| format!("{:.0}", n as f64 / secs);
        let t = table(
            &["measure", "value"],
            &[
                vec!["writer txns committed".into(), txns.to_string()],
                vec!["writer txns/s".into(), rate(txns)],
                vec!["writer row changes/s".into(), rate(txns)],
                vec![
                    "writer commit round trip p50 / p99 ms".into(),
                    format!(
                        "{} / {}",
                        ms(percentile(&commit_us, 0.5)),
                        ms(percentile(&commit_us, 0.99))
                    ),
                ],
                vec![
                    "engine applied txns/s (during the 30 s window)".into(),
                    rate(in_window.txns),
                ],
                vec![
                    "engine applied changes/s (during the 30 s window)".into(),
                    rate(in_window.changes),
                ],
                vec![
                    "engine applied txns / changes (total, after drain)".into(),
                    format!("{applied_total} / {}", total.changes),
                ],
                vec![
                    "time to drain after writers stopped: engine / client".into(),
                    format!(
                        "{:.2} s / {:.2} s",
                        engine_drain.as_secs_f64(),
                        client_drain.as_secs_f64()
                    ),
                ],
                vec![
                    "lag, `server_wal_end − applied_lsn`: max / final".into(),
                    format!("{} / {}", kib(max(&lag_metric)), kib(last(&lag_metric))),
                ],
                vec![
                    "lag, `pg_current_wal_lsn − applied_lsn`: max / final".into(),
                    format!("{} / {}", kib(max(&lag_pg)), kib(last(&lag_pg))),
                ],
                vec![
                    "store commits (`index_commits`)".into(),
                    total.commits.to_string(),
                ],
                vec![
                    "txns per store commit".into(),
                    format!("{:.1}", total.txns as f64 / total.commits.max(1) as f64),
                ],
                vec![
                    "sessions closed as lagged (`closes_lagged`) / client reconnects".into(),
                    format!(
                        "{} / {}",
                        Metrics::get(&m.closes_lagged) - before.lagged,
                        seen.reconnects.load(Relaxed)
                    ),
                ],
                vec![
                    "client: commit events / bytes".into(),
                    format!(
                        "{} / {:.1} MiB",
                        seen.commits.load(Relaxed),
                        bytes as f64 / 1_048_576.0
                    ),
                ],
            ],
        );
        Ok(format!(
            "## B2: sustained write throughput\n\n\
             Table `bench_w (id bigint pk, v bigint, payload text)`, initially empty. {WRITERS} \
             writer connections run autocommit single-row `INSERT ... ON CONFLICT DO UPDATE` over \
             {KEY_SPACE} random keys (so it starts as inserts and turns into updates) for \
             {SECONDS} s, as fast as they can. One client subscribed. The load generator, the \
             engine and the client share one process and one machine.\n\n{t}\n\
             Lag per second while writing, KiB (`server_wal_end − applied_lsn`): {}\n\n\
             Lag per second while writing, KiB (`pg_current_wal_lsn − applied_lsn`): {}\n",
            series(&lag_metric),
            series(&lag_pg),
        ))
    })
    .await
}

/// One writer: returns its txn count and each commit's round trip (µs).
async fn write(pg: Client, deadline: Instant) -> Result<(u64, Vec<u64>)> {
    let stmt = pg
        .prepare(
            "INSERT INTO bench_w (id, v, payload) VALUES ($1, $2, 'xxxxxxxxxxxxxxxxxxxx') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        )
        .await?;
    let mut latencies = Vec::new();
    let mut n = 0u64;
    while Instant::now() < deadline {
        let id = rand::random_range(1..=KEY_SPACE);
        let t = Instant::now();
        pg.execute(&stmt, &[&id, &(n as i64)]).await?;
        latencies.push(t.elapsed().as_micros() as u64);
        n += 1;
    }
    Ok((n, latencies))
}

/// Reads the stream until stopped, resuming with its offset whenever the server closes the
/// stream (as a real client does, PLAN.md §6.5). Returns the bytes consumed.
async fn client(
    addr: SocketAddr,
    seen: Arc<Seen>,
    ready: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
) -> Result<u64> {
    let mut sse = Sse::open(addr, "public.bench_w", None).await?;
    let (mut first, mut offset, mut bytes) = (true, None::<String>, 0);
    loop {
        let event = tokio::select! {
            _ = stop.changed() => break,
            e = sse.next() => e?,
        };
        let Some(event) = event else {
            bytes += sse.bytes;
            seen.reconnects.fetch_add(1, Relaxed);
            sse = Sse::open(addr, "public.bench_w", offset.as_deref()).await?;
            continue;
        };
        if event.name == "commit" {
            let commit: walrus_protocol::Commit = serde_json::from_str(&event.data)?;
            let lsn = commit.offset.rsplit('-').next().unwrap_or("0");
            seen.lsn.store(u64::from_str_radix(lsn, 16)?, Relaxed);
            seen.commits.fetch_add(1, Relaxed);
            offset = Some(commit.offset);
            if first {
                first = false;
                ready.send(()).await?;
            }
        }
    }
    Ok(bytes + sse.bytes)
}

async fn current_wal(pg: &Client) -> Result<u64> {
    let row = pg
        .query_one("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint", &[])
        .await?;
    Ok(row.get::<_, i64>(0) as u64)
}

/// The engine counters this scenario reports on.
struct Snapshot {
    txns: u64,
    changes: u64,
    commits: u64,
    lagged: u64,
}

impl Snapshot {
    fn take(m: &Metrics) -> Snapshot {
        Snapshot {
            txns: Metrics::get(&m.txns_applied),
            changes: Metrics::get(&m.changes_applied),
            commits: Metrics::get(&m.index_commits),
            lagged: Metrics::get(&m.closes_lagged),
        }
    }

    fn since(&self, earlier: &Snapshot) -> Snapshot {
        Snapshot {
            txns: self.txns - earlier.txns,
            changes: self.changes - earlier.changes,
            commits: self.commits - earlier.commits,
            lagged: self.lagged - earlier.lagged,
        }
    }
}

fn max(v: &[u64]) -> u64 {
    v.iter().copied().max().unwrap_or(0)
}

fn last(v: &[u64]) -> u64 {
    v.last().copied().unwrap_or(0)
}

fn kib(bytes: u64) -> String {
    format!("{:.1} KiB", bytes as f64 / 1024.0)
}

fn series(v: &[u64]) -> String {
    let items: Vec<String> = v
        .iter()
        .map(|b| format!("{:.0}", *b as f64 / 1024.0))
        .collect();
    items.join(", ")
}
