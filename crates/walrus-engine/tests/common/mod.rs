//! Shared harness for integration tests against the compose Postgres (PLAN.md §11.1).
//!
//! Every test gets its own database (loaded with `fixture.sql`), slot, publication and data
//! directory, so tests run in parallel. Expected state always comes from Postgres itself:
//! `SELECT *` rendered as text with the engine's pinned session settings.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use walrus_client::{End, State, Stream};
use walrus_engine::Engine;
use walrus_engine::config::Config;
use walrus_engine::pg::setup::SESSION_OPTIONS;
use walrus_protocol::{Key, Row};

pub const PG: &str = "postgres://postgres:postgres@localhost:54329";
pub const TABLES: [&str; 3] = ["public.users", "public.lists", "public.todos"];
const FIXTURE: &str = include_str!("../fixture.sql");
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "walrus_engine=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
}

pub struct TestDb {
    pub name: String,
    pub url: String,
    pub slot: String,
    pub data_dir: tempfile::TempDir,
    admin: Client,
    cleaned: bool,
}

impl TestDb {
    pub async fn new() -> TestDb {
        init_tracing();
        let suffix = format!("{:016x}", rand::random::<u64>());
        let name = format!("walrus_t_{suffix}");
        let admin = connect(&format!("{PG}/postgres")).await;
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("create test database");
        let db = TestDb {
            url: format!("{PG}/{name}"),
            slot: format!("walrus_t_{suffix}"),
            data_dir: tempfile::tempdir().expect("tempdir"),
            name,
            admin,
            cleaned: false,
        };
        db.client()
            .await
            .batch_execute(FIXTURE)
            .await
            .expect("load fixture");
        db
    }

    pub async fn client(&self) -> Client {
        connect(&self.url).await
    }

    pub fn config(&self) -> Config {
        Config {
            database_url: self.url.clone(),
            tables: TABLES.iter().map(|t| t.to_string()).collect(),
            data_dir: self.data_dir.path().to_path_buf(),
            listen_addr: "127.0.0.1:0".parse().expect("addr"),
            slot: self.slot.clone(),
            publication: "walrus".into(),
            catchup_connections: 4,
            retention: Duration::from_secs(7 * 86_400),
        }
    }

    pub async fn start_engine(&self) -> Engine {
        self.start_with(self.config()).await.expect("engine start")
    }

    /// Start an engine with `config` and wait until it streams.
    pub async fn start_with(&self, config: Config) -> anyhow::Result<Engine> {
        let mut engine = self.start_unready(config).await?;
        tokio::time::timeout(Duration::from_secs(30), engine.wait_ready())
            .await
            .expect("engine ready in time")?;
        Ok(engine)
    }

    /// `walrus_engine::start` only: errors are startup errors, the stream may not be up yet.
    pub async fn start_unready(&self, config: Config) -> anyhow::Result<Engine> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        walrus_engine::start(config, listener).await
    }

    /// Like `start_engine`, but retries while Postgres still considers the slot of a just
    /// stopped engine active (a new epoch has to drop it).
    pub async fn restart_engine(&self) -> Engine {
        let mut last = None;
        for _ in 0..50 {
            match self.start_with(self.config()).await {
                Ok(e) => return e,
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("engine would not restart: {:#}", last.expect("error"));
    }

    /// `SELECT *` of a fixture table as the client would hold it: rows by key.
    pub async fn table(&self, client: &Client, table: &str) -> BTreeMap<Key, Row> {
        let key = key_columns(table);
        let mut rows = BTreeMap::new();
        for msg in client
            .simple_query(&format!("SELECT * FROM {table}"))
            .await
            .expect("select")
        {
            if let SimpleQueryMessage::Row(r) = msg {
                let row: Row = (0..r.len()).map(|i| r.get(i).map(str::to_string)).collect();
                let k: Key = key
                    .iter()
                    .map(|&i| row[i].clone().expect("key not null"))
                    .collect();
                rows.insert(k, row);
            }
        }
        rows
    }

    /// Drop the slot and the database. Also runs (from `Drop`) when a test panics, so failed
    /// tests don't leave slots behind that make Postgres keep WAL.
    pub async fn cleanup(mut self) {
        drop_test_db(&self.admin, &self.name, &self.slot).await;
        self.cleaned = true;
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        // Panic path: the test's runtime is unwinding, so clean up on a runtime of our own.
        let (name, slot) = (self.name.clone(), self.slot.clone());
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            rt.block_on(async {
                let admin = connect(&format!("{PG}/postgres")).await;
                drop_test_db(&admin, &name, &slot).await;
            });
        })
        .join();
    }
}

/// Stop whatever still streams from the slot, drop the slot, drop the database.
async fn drop_test_db(admin: &Client, name: &str, slot: &str) {
    for _ in 0..50 {
        let _ = admin
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active",
                &[&slot],
            )
            .await;
        let _ = admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND NOT active",
                &[&slot],
            )
            .await;
        let remaining: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(1);
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .await;
}

pub async fn connect(url: &str) -> Client {
    let mut config: tokio_postgres::Config = url.parse().expect("url");
    config.options(SESSION_OPTIONS);
    let (client, conn) = config.connect(NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

/// Key column indexes (column order) of the fixture tables.
pub fn key_columns(table: &str) -> Vec<usize> {
    match table {
        "public.todos" => vec![0, 1],
        _ => vec![0],
    }
}

/// A client streaming in the background; `state` is updated after every commit. Like a real
/// client (PLAN.md §6.5) it reconnects with its offset whenever the server closes the stream;
/// a protocol violation (`End::Broken`) ends it for good.
pub struct Subscriber {
    pub state: Arc<Mutex<State>>,
    pub tables: Vec<String>,
    /// Times the server closed the stream and the client resumed.
    pub reconnects: Arc<AtomicU64>,
    /// Why the stream broke, if it did.
    broken: Arc<Mutex<Option<String>>>,
    task: JoinHandle<()>,
}

impl Subscriber {
    /// Connect with `state` (its offset is sent if present) and keep applying commits.
    pub async fn start(engine: &Engine, tables: &[&str], state: State) -> Subscriber {
        let url = format!("http://{}", engine.addr);
        let first = Stream::connect(&url, tables, state.offset)
            .await
            .expect("connect stream");
        let shared = Arc::new(Mutex::new(state.clone()));
        let broken = Arc::new(Mutex::new(None));
        let reconnects = Arc::new(AtomicU64::new(0));
        let names: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
        let task = tokio::spawn({
            let (shared, broken, reconnects) = (shared.clone(), broken.clone(), reconnects.clone());
            let names = names.clone();
            let mut state = state;
            async move {
                let tables: Vec<&str> = names.iter().map(String::as_str).collect();
                let mut next = Some(first);
                loop {
                    let mut stream = match next.take() {
                        Some(s) => s,
                        None => match Stream::connect(&url, &tables, state.offset).await {
                            Ok(s) => s,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                continue;
                            }
                        },
                    };
                    loop {
                        match stream.next_commit(&mut state).await {
                            Ok(_) => *shared.lock().expect("lock") = state.clone(),
                            Err(End::Closed) => {
                                reconnects.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(End::Broken(reason)) => {
                                *broken.lock().expect("lock") = Some(reason);
                                return;
                            }
                        }
                    }
                }
            }
        });
        Subscriber {
            state: shared,
            tables: names,
            reconnects,
            broken,
            task,
        }
    }

    pub fn snapshot(&self) -> State {
        self.state.lock().expect("lock").clone()
    }

    /// Wait until every subscribed table equals Postgres. Panics with a diff on timeout, or if
    /// the stream ended or broke.
    pub async fn wait_converged(&self, db: &TestDb) {
        let client = db.client().await;
        let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Some(reason) = self.broken.lock().expect("lock").clone() {
                panic!("stream broke: {reason}");
            }
            let mut diff = None;
            for table in &self.tables {
                let expected = db.table(&client, table).await;
                let state = self.snapshot();
                let actual = state
                    .tables
                    .get(table)
                    .map(|t| t.rows.clone())
                    .unwrap_or_default();
                if actual != expected {
                    diff = Some(describe_diff(table, &expected, &actual));
                    break;
                }
            }
            match diff {
                None => return,
                Some(d) if tokio::time::Instant::now() > deadline => {
                    panic!("client did not converge: {d}")
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    pub async fn stop(self) -> State {
        self.task.abort();
        let _ = self.task.await;
        self.state.lock().expect("lock").clone()
    }
}

fn describe_diff(
    table: &str,
    expected: &BTreeMap<Key, Row>,
    actual: &BTreeMap<Key, Row>,
) -> String {
    for (k, row) in expected {
        match actual.get(k) {
            None => return format!("{table}: missing {k:?}"),
            Some(a) if a != row => return format!("{table}: {k:?} is {a:?}, expected {row:?}"),
            _ => {}
        }
    }
    for k in actual.keys() {
        if !expected.contains_key(k) {
            return format!("{table}: unexpected {k:?}");
        }
    }
    format!("{table}: equal")
}

/// One transaction for the model test: one to four statements over any tables, mixing the
/// `random_writes` statements with truncates, primary-key changes of every table, and large
/// values (up to ~1 MB).
pub fn model_txn(rng: &mut impl rand::Rng) -> String {
    let mut sql = String::from("BEGIN;");
    for _ in 0..rng.random_range(1..5) {
        if rng.random_range(0..10) < 7 {
            sql.push_str(&random_statement(rng));
            continue;
        }
        let n: i64 = rng.random_range(1..80);
        sql.push_str(&match rng.random_range(0..8) {
            0 => ["TRUNCATE todos;", "TRUNCATE lists;", "TRUNCATE users;"][rng.random_range(0..3)]
                .to_string(),
            1 => format!(
                "UPDATE lists SET id = id + 1000 WHERE id = {n} \
                 AND NOT EXISTS (SELECT 1 FROM lists WHERE id = {n} + 1000);"
            ),
            2 => format!(
                "UPDATE users SET id = md5('moved' || {n})::uuid WHERE email = 'user{n}@example.com' \
                 AND NOT EXISTS (SELECT 1 FROM users WHERE id = md5('moved' || {n})::uuid);"
            ),
            3 => format!(
                "UPDATE todos SET list_id = list_id + 100 WHERE position = {n} \
                 AND NOT EXISTS (SELECT 1 FROM todos t WHERE t.list_id = todos.list_id + 100 \
                                 AND t.position = {n});"
            ),
            4 => format!(
                "UPDATE todos SET notes = repeat('huge {n} ', {}) WHERE position = {n};",
                rng.random_range(1000..120_000)
            ),
            5 => format!(
                "UPDATE users SET avatar = decode(repeat('ab', {}), 'hex') WHERE email = 'user{n}@example.com';",
                rng.random_range(1..50_000)
            ),
            6 => format!(
                "INSERT INTO todos (list_id, position, title, done, tags, notes) \
                 SELECT {n} % 10 + 1, {n} + g, 'bulk', false, ARRAY['x', NULL], repeat('n', {}) \
                 FROM generate_series(1, {}) g ON CONFLICT DO NOTHING;",
                rng.random_range(0..3000),
                rng.random_range(1..30)
            ),
            _ => format!("DELETE FROM todos WHERE position BETWEEN {n} AND {n} + 20;"),
        });
    }
    sql.push_str("COMMIT;");
    sql
}

/// Random single- and multi-table transactions over the fixture, from `seed`.
pub async fn random_writes(client: &Client, seed: u64, txns: usize) {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    for _ in 0..txns {
        let mut sql = String::from("BEGIN;");
        for _ in 0..rng.random_range(1..4) {
            sql.push_str(&random_statement(&mut rng));
        }
        sql.push_str("COMMIT;");
        client.batch_execute(&sql).await.expect("random write");
    }
}

fn random_statement(rng: &mut impl rand::Rng) -> String {
    let n: i64 = rng.random_range(1..80);
    match rng.random_range(0..10) {
        0 => format!(
            "INSERT INTO lists VALUES ({n}, md5('user1')::uuid, 'ins {n}', false, '{{\"k\":{n}}}') \
             ON CONFLICT (id) DO UPDATE SET title = 'upd ' || {n};"
        ),
        1 => format!("UPDATE lists SET archived = NOT archived WHERE id = {n};"),
        2 => format!("DELETE FROM lists WHERE id = {n};"),
        3 => format!(
            "INSERT INTO todos (list_id, position, title, done) VALUES ({}, {n}, E'new\\t{n}', false) \
             ON CONFLICT DO NOTHING;",
            n % 10 + 1
        ),
        // Touches only `done`: large notes stay unchanged (TOAST path).
        4 => format!("UPDATE todos SET done = NOT done WHERE position = {n};"),
        5 => format!("UPDATE todos SET notes = repeat('big {n} ', 500) WHERE position = {n};"),
        6 => format!("DELETE FROM todos WHERE position = {n};"),
        // Primary-key change (keeps large notes unchanged).
        7 => format!(
            "UPDATE todos SET position = position + 1000 WHERE position = {n} \
             AND NOT EXISTS (SELECT 1 FROM todos WHERE position = {n} + 1000);"
        ),
        8 => format!(
            "UPDATE users SET name = 'renamed ' || {n} WHERE email = 'user{n}@example.com';"
        ),
        _ => format!(
            "INSERT INTO users VALUES (md5('new' || {n})::uuid, 'new{n}@example.com', NULL, now(), NULL) \
             ON CONFLICT (id) DO NOTHING;"
        ),
    }
}

pub async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting: {what}");
}
