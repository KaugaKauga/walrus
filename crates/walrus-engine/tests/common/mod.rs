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

/// One test's database, slot, publication and data directory.
pub struct TestDb {
    pub name: String,
    pub url: String,
    pub slot: String,
    pub data_dir: tempfile::TempDir,
    admin: Client,
}

impl TestDb {
    /// Create a fresh database with the fixture loaded.
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
        };
        db.client()
            .await
            .batch_execute(FIXTURE)
            .await
            .expect("load fixture");
        db
    }

    /// A SQL client with the engine's pinned display settings.
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

    /// Start an engine on this database and wait until it streams.
    pub async fn start_engine(&self) -> Engine {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let mut engine = walrus_engine::start(self.config(), listener)
            .await
            .expect("engine start");
        tokio::time::timeout(Duration::from_secs(30), engine.wait_ready())
            .await
            .expect("engine ready in time")
            .expect("engine ready");
        engine
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

    /// Drop the slot (waiting for it to become inactive) and the database.
    pub async fn cleanup(self) {
        for _ in 0..100 {
            let res = self
                .admin
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = $1 AND NOT active",
                    &[&self.slot],
                )
                .await;
            let remaining = self
                .admin
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                )
                .await
                .expect("count slots")
                .get::<_, i64>(0);
            if res.is_ok() && remaining == 0 {
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

    /// Stop streaming and return the last committed state (with its offset).
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

/// Poll `cond` every 50 ms for up to 10 s.
pub async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting: {what}");
}
