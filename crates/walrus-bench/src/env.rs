//! A fresh database, slot, data directory and in-process engine per scenario.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use walrus_engine::Engine;
use walrus_engine::config::Config;

pub fn pg_base() -> String {
    std::env::var("WALRUS_BENCH_PG")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54329".into())
        .trim_end_matches('/')
        .to_string()
}

pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .with_context(|| format!("connect to {url}"))?;
    tokio::spawn(conn);
    Ok(client)
}

/// Wall-clock microseconds. Writer and clients share this process, so one clock.
pub fn wall_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

pub struct Env {
    pub url: String,
    name: String,
    slot: String,
    admin: Client,
    data_dir: tempfile::TempDir,
}

impl Env {
    async fn create() -> Result<Env> {
        let suffix = format!("{:08x}", rand::random::<u32>());
        let name = format!("walrus_bench_{suffix}");
        let admin = connect(&format!("{}/postgres", pg_base())).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .context("create database")?;
        Ok(Env {
            url: format!("{}/{name}", pg_base()),
            slot: name.clone(),
            name,
            admin,
            data_dir: tempfile::tempdir()?,
        })
    }

    pub async fn client(&self) -> Result<Client> {
        connect(&self.url).await
    }

    async fn start_engine(&self, tables: &[&str]) -> Result<Engine> {
        let config = Config {
            database_url: self.url.clone(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            data_dir: self.data_dir.path().to_path_buf(),
            listen_addr: "127.0.0.1:0".parse()?,
            slot: self.slot.clone(),
            publication: self.slot.clone(),
            catchup_connections: 4,
            retention: Duration::from_secs(86_400),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut engine = walrus_engine::start(config, listener).await?;
        tokio::time::timeout(Duration::from_secs(60), engine.wait_ready())
            .await
            .context("engine not ready in 60 s")??;
        Ok(engine)
    }

    /// Drop the slot (waiting for Postgres to notice the walsender is gone) and the database.
    async fn cleanup(self) {
        for _ in 0..100 {
            let dropped = self
                .admin
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = $1 AND NOT active",
                    &[&self.slot],
                )
                .await;
            let left = self
                .admin
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                )
                .await
                .map(|r| r.get::<_, i64>(0));
            if dropped.is_ok() && left.is_ok_and(|n| n == 0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = self
            .admin
            .batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.name
            ))
            .await;
    }
}

/// Run `body` against a fresh database (set up with `setup_sql`) and an engine streaming
/// `tables`. Everything is cleaned up afterwards, also when `body` fails.
pub async fn with_engine<T>(
    setup_sql: &str,
    tables: &[&str],
    body: impl AsyncFnOnce(&Env, &Engine) -> Result<T>,
) -> Result<T> {
    let env = Env::create().await?;
    let result = async {
        env.client()
            .await?
            .batch_execute(setup_sql)
            .await
            .context("setup sql")?;
        let engine = env.start_engine(tables).await?;
        let result = body(&env, &engine).await;
        let stopped = tokio::time::timeout(Duration::from_secs(30), engine.shutdown()).await;
        if stopped.is_err() {
            eprintln!("warning: engine did not stop within 30 s");
        }
        result
    }
    .await;
    env.cleanup().await;
    result
}

pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

pub fn ms(us: u64) -> String {
    format!("{:.2}", us as f64 / 1000.0)
}

pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = format!(
        "| {} |\n|{}\n",
        headers.join(" | "),
        "---|".repeat(headers.len())
    );
    for row in rows {
        out.push_str(&format!("| {} |\n", row.join(" | ")));
    }
    out
}
