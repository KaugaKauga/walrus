//! B3: what a (re)connecting client costs: full catch-up, delta catch-up, forced full, no change.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio_postgres::Client;
use walrus_engine::metrics::Metrics;
use walrus_protocol::{Commit, Op, Rows};

use crate::env::{table, with_engine};
use crate::sse::Sse;

const ROWS: u64 = 100_000;
const TABLE: &str = "public.bench_big";

const SETUP: &str = "
    CREATE TABLE bench_big (id bigint PRIMARY KEY, payload text NOT NULL);
    INSERT INTO bench_big SELECT i, repeat(md5(i::text), 6) || '-' || i
      FROM generate_series(1, 100000) i;
    ANALYZE bench_big;";

/// 100 changed rows (0.1%) in one transaction: 70 updates, 20 deletes, 10 inserts.
const CHANGES: &str = "
    BEGIN;
    UPDATE bench_big SET payload = 'changed ' || payload WHERE id % 1000 = 7 AND id < 70000;
    DELETE FROM bench_big WHERE id % 1000 = 8 AND id < 20000;
    INSERT INTO bench_big SELECT i, repeat(md5(i::text), 6) || '-' || i
      FROM generate_series(100001, 100010) i;
    COMMIT;";

pub async fn run() -> Result<String> {
    with_engine(SETUP, &[TABLE], async |env, engine| {
        let (addr, m) = (engine.addr, &engine.metrics);
        let pg = env.client().await?;
        let pgss = pg
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await
            .is_ok()
            && pgss_read(&pg).await.is_ok();

        // A throwaway catch-up first: connection pool, caches and code paths are warm for (a).
        catch_up(addr, None).await?;

        let mut steps: Vec<Step> = Vec::new();
        let mut step = async |name: &str, offset: Option<&str>| -> Result<CatchUp> {
            if pgss {
                pgss_reset(&pg).await?;
            }
            let before = Counters::take(m);
            let c = catch_up(addr, offset).await?;
            let engine = Counters::take(m).since(&before);
            let pg_stats = if pgss {
                Some(pgss_read(&pg).await?)
            } else {
                None
            };
            steps.push(Step {
                name: name.to_string(),
                c: c.summary()?,
                engine,
                pg: pg_stats,
            });
            Ok(c)
        };

        let a = step("(a) full catch-up, no offset", None).await?;

        // The client is gone; 0.1% of the rows change while it is away.
        let applied = Metrics::get(&m.txns_applied);
        pg.batch_execute(CHANGES).await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Metrics::get(&m.txns_applied) == applied {
            if Instant::now() > deadline {
                bail!("engine did not apply the change transaction");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let b = step("(b) resume with offset, 100 rows changed", Some(&a.offset)).await?;
        step("(c) same changes, no offset (forced full)", None).await?;
        step("(d) resume with offset, nothing changed", Some(&b.offset)).await?;

        let mib = |b: u64| format!("{:.2} MiB", b as f64 / 1_048_576.0);
        let mut rows = Vec::new();
        for s in &steps {
            let (calls, exec, pg_rows) = match &s.pg {
                Some(p) => (
                    p.calls.to_string(),
                    format!("{:.1}", p.exec_ms),
                    p.rows.to_string(),
                ),
                None => ("n/a".into(), "n/a".into(), "n/a".into()),
            };
            rows.push(vec![
                s.name.clone(),
                format!("{:.1}", s.c.ms),
                mib(s.c.bytes),
                format!("{} / {} / {}", s.c.upserts, s.c.deletes, s.c.patches),
                s.c.resets.to_string(),
                format!("{} / {} / {}", s.engine.none, s.engine.delta, s.engine.full),
                s.engine.delta_keys.to_string(),
                format!("{} / {}", s.engine.rows_read_delta, s.engine.rows_read_full),
                calls,
                exec,
                pg_rows,
            ]);
        }
        let t = table(
            &[
                "step",
                "time to commit ms",
                "bytes",
                "ops u / d / p",
                "table-resets",
                "catch-ups none / delta / full",
                "delta_keys",
                "rows read delta / full",
                "PG calls",
                "PG exec ms",
                "PG rows",
            ],
            &rows,
        );
        let pg_note = if pgss {
            "PG columns come from `pg_stat_statements` (reset before each step), summed over \
             statements that mention `bench_big`."
        } else {
            "`pg_stat_statements` is not available, so Postgres time is not reported."
        };
        Ok(format!(
            "## B3: catch-up cost\n\n\
             Table `bench_big (id bigint pk, payload text)` with {ROWS} rows of about 200 bytes \
             of payload. Time is measured inside the client, from sending the request until the \
             `commit` event has been read. Bytes are the SSE bytes consumed up to and including \
             that event. A full catch-up was run once beforehand and discarded (warm-up). The \
             change set is one transaction: 70 updates, 20 deletes, 10 inserts (100 rows). \
             Rows events are parsed after the clock stops.\n\n{t}\n{pg_note}\n"
        ))
    })
    .await
}

struct Step {
    name: String,
    c: Summary,
    engine: Counters,
    pg: Option<PgStats>,
}

struct CatchUp {
    ms: f64,
    bytes: u64,
    offset: String,
    /// Raw `rows` event payloads, parsed only after the clock stopped.
    rows: Vec<String>,
    resets: u64,
}

struct Summary {
    ms: f64,
    bytes: u64,
    upserts: u64,
    deletes: u64,
    patches: u64,
    resets: u64,
}

impl CatchUp {
    fn summary(&self) -> Result<Summary> {
        let mut s = Summary {
            ms: self.ms,
            bytes: self.bytes,
            upserts: 0,
            deletes: 0,
            patches: 0,
            resets: self.resets,
        };
        for data in &self.rows {
            let rows: Rows = serde_json::from_str(data)?;
            for op in rows.ops {
                match op {
                    Op::Upsert(_) => s.upserts += 1,
                    Op::Delete(_) => s.deletes += 1,
                    Op::Patch { .. } => s.patches += 1,
                }
            }
        }
        Ok(s)
    }
}

/// Connect, read until the first `commit` event, and disconnect.
async fn catch_up(addr: SocketAddr, offset: Option<&str>) -> Result<CatchUp> {
    let start = Instant::now();
    let mut sse = Sse::open(addr, TABLE, offset).await?;
    let (mut rows, mut resets) = (Vec::new(), 0);
    loop {
        let event = sse.next().await?.context("stream closed before a commit")?;
        match event.name.as_str() {
            "rows" => rows.push(event.data),
            "table-reset" => resets += 1,
            "commit" => {
                let ms = start.elapsed().as_secs_f64() * 1000.0;
                let commit: Commit = serde_json::from_str(&event.data)?;
                return Ok(CatchUp {
                    ms,
                    bytes: sse.bytes,
                    offset: commit.offset,
                    rows,
                    resets,
                });
            }
            _ => {}
        }
    }
}

/// Engine counters that a catch-up changes.
struct Counters {
    none: u64,
    delta: u64,
    full: u64,
    delta_keys: u64,
    rows_read_delta: u64,
    rows_read_full: u64,
}

impl Counters {
    fn take(m: &Metrics) -> Counters {
        Counters {
            none: Metrics::get(&m.catchups_none),
            delta: Metrics::get(&m.catchups_delta),
            full: Metrics::get(&m.catchups_full),
            delta_keys: Metrics::get(&m.delta_keys),
            rows_read_delta: Metrics::get(&m.rows_read_delta),
            rows_read_full: Metrics::get(&m.rows_read_full),
        }
    }

    fn since(&self, earlier: &Counters) -> Counters {
        Counters {
            none: self.none - earlier.none,
            delta: self.delta - earlier.delta,
            full: self.full - earlier.full,
            delta_keys: self.delta_keys - earlier.delta_keys,
            rows_read_delta: self.rows_read_delta - earlier.rows_read_delta,
            rows_read_full: self.rows_read_full - earlier.rows_read_full,
        }
    }
}

struct PgStats {
    calls: i64,
    exec_ms: f64,
    rows: i64,
}

/// Reset only this database's statistics, so other users of the server are left alone.
async fn pgss_reset(pg: &Client) -> Result<()> {
    pg.batch_execute(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .await?;
    Ok(())
}

async fn pgss_read(pg: &Client) -> Result<PgStats> {
    let row = pg
        .query_one(
            "SELECT coalesce(sum(calls), 0)::bigint, coalesce(sum(total_exec_time), 0)::float8, \
                    coalesce(sum(rows), 0)::bigint \
             FROM pg_stat_statements \
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND query ILIKE '%bench_big%' AND query NOT ILIKE '%pg_stat_statements%'",
            &[],
        )
        .await?;
    Ok(PgStats {
        calls: row.get(0),
        exec_ms: row.get(1),
        rows: row.get(2),
    })
}
