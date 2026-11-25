//! B1: commit → client latency at a steady 100 txn/s with 10 clients.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::sync::{mpsc, watch};
use walrus_protocol::{Commit, Op, Rows};

use crate::env::{ms, percentile, table, wall_us, with_engine};
use crate::guard::Outcome;
use crate::sse::Sse;

const ROWS: u64 = 1000;
const CLIENTS: usize = 10;
const TXN_PER_SEC: u64 = 100;
const SECONDS: u64 = 30;

const SETUP: &str = "
    CREATE TABLE bench (id bigint PRIMARY KEY, sent_us bigint NOT NULL, payload text NOT NULL);
    INSERT INTO bench SELECT i, 0, repeat('x', 100) FROM generate_series(1, 1000) i;";

pub async fn run() -> Result<Outcome> {
    with_engine(SETUP, &["public.bench"], async |env, engine| {
        let addr = engine.addr;
        let (ready_tx, mut ready_rx) = mpsc::channel(CLIENTS);
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            let received = Arc::new(AtomicU64::new(0));
            let task = tokio::spawn(client(
                addr,
                received.clone(),
                ready_tx.clone(),
                stop_rx.clone(),
            ));
            clients.push((received, task));
        }
        // Every client holds the initial state before the first update is sent.
        for _ in 0..CLIENTS {
            tokio::time::timeout(Duration::from_secs(30), ready_rx.recv())
                .await?
                .ok_or_else(|| anyhow::anyhow!("a client died before its first commit"))?;
        }

        let pg = env.client().await?;
        let total = TXN_PER_SEC * SECONDS;
        let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / TXN_PER_SEC));
        let mut commit_rtt = Vec::with_capacity(total as usize);
        let started = Instant::now();
        for _ in 0..total {
            tick.tick().await;
            let id = rand::random_range(1..=ROWS);
            // One round trip (BEGIN, UPDATE, COMMIT) with the timestamp taken right before it.
            let sql = format!(
                "BEGIN; UPDATE bench SET sent_us = {} WHERE id = {id}; COMMIT;",
                wall_us()
            );
            let t = Instant::now();
            pg.batch_execute(&sql).await?;
            commit_rtt.push(t.elapsed().as_micros() as u64);
        }
        let wrote_for = started.elapsed();

        // Wait for every client to have seen every update.
        let deadline = Instant::now() + Duration::from_secs(20);
        while clients.iter().any(|(r, _)| r.load(Relaxed) < total) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop_tx.send(true)?;
        let mut latencies = Vec::new();
        let mut counts = Vec::new();
        for (_, task) in clients {
            let (lat, n) = task.await??;
            latencies.extend(lat);
            counts.push(n);
        }
        // Not an error here: the guardrail tracks it as an exact metric.
        let missed = total * CLIENTS as u64 - counts.iter().sum::<u64>();

        latencies.sort_unstable();
        commit_rtt.sort_unstable();
        let row = |name: &str, v: &[u64]| {
            vec![
                name.to_string(),
                ms(percentile(v, 0.5)),
                ms(percentile(v, 0.95)),
                ms(percentile(v, 0.99)),
                ms(v.last().copied().unwrap_or(0)),
            ]
        };
        let t = table(
            &["measure", "p50 ms", "p95 ms", "p99 ms", "max ms"],
            &[
                row("commit sent → commit event received", &latencies),
                row("writer COMMIT round trip", &commit_rtt),
            ],
        );
        let metrics = vec![
            ("b1_p50_ms", percentile(&latencies, 0.5) as f64 / 1000.0),
            ("b1_p99_ms", percentile(&latencies, 0.99) as f64 / 1000.0),
            ("b1_missed_updates", missed as f64),
        ];
        let section = format!(
            "## B1: commit to client latency\n\n\
             Table `bench` with {ROWS} rows, {CLIENTS} clients, one writer at {TXN_PER_SEC} txn/s \
             for {SECONDS} s (ran {:.1} s). Each txn updates one random row; `sent_us` is taken \
             right before the `BEGIN; UPDATE; COMMIT` round trip. A client's latency for a row \
             is measured when the `commit` event that carries the row arrives.\n\n{t}\n\
             Updates written: {total}. Updates received per client: {counts:?} (missed in total: \
             {missed}). Latency samples: {}.\n",
            wrote_for.as_secs_f64(),
            latencies.len()
        );
        Ok(Outcome { section, metrics })
    })
    .await
}

/// Reads the stream; returns per-update latencies (µs) and how many updates it saw.
async fn client(
    addr: SocketAddr,
    received: Arc<AtomicU64>,
    ready: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
) -> Result<(Vec<u64>, u64)> {
    let mut sse = Sse::open(addr, "public.bench", None).await?;
    let mut started = false;
    let mut pending: Vec<u64> = Vec::new();
    let mut latencies = Vec::new();
    loop {
        let event = tokio::select! {
            _ = stop.changed() => break,
            e = sse.next() => e?,
        };
        let Some(event) = event else {
            bail!("server closed the stream")
        };
        match event.name.as_str() {
            "rows" => {
                let rows: Rows = serde_json::from_str(&event.data)?;
                for op in rows.ops {
                    // An update may arrive as a patch that leaves `payload` out, but
                    // `sent_us` always changes, so column 1 is always present.
                    let (Op::Upsert(row) | Op::Patch { row, .. }) = op else {
                        continue;
                    };
                    let sent: u64 = row
                        .get(1)
                        .and_then(|v| v.as_deref())
                        .unwrap_or("0")
                        .parse()?;
                    // The initial state has sent_us = 0.
                    if started && sent > 0 {
                        pending.push(sent);
                    }
                }
            }
            "commit" => {
                let _: Commit = serde_json::from_str(&event.data)?;
                let now = wall_us();
                received.fetch_add(pending.len() as u64, Relaxed);
                latencies.extend(pending.drain(..).map(|sent| now.saturating_sub(sent)));
                if !started {
                    started = true;
                    ready.send(()).await?;
                }
            }
            _ => {}
        }
    }
    let n = latencies.len() as u64;
    Ok((latencies, n))
}
