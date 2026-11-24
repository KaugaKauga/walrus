//! `take_snapshot` and `copy_rows` against a real Postgres (localhost:54329, postgres/postgres).
//! Each test creates and drops its own database.

use std::sync::atomic::{AtomicU32, Ordering};

use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};
use walrus_engine::pg::read;
use walrus_engine::schema::{ColumnSchema, Row, TableSchema};

static NEXT: AtomicU32 = AtomicU32::new(0);

fn config(dbname: &str) -> Config {
    let mut c = Config::new();
    c.host("localhost")
        .port(54329)
        .user("postgres")
        .password("postgres")
        .dbname(dbname);
    c
}

struct TestDb {
    name: String,
    admin: tokio_postgres::Client,
    pool: Pool,
}

impl TestDb {
    async fn create() -> TestDb {
        let (admin, conn) = config("postgres")
            .connect(NoTls)
            .await
            .expect("postgres at localhost:54329");
        tokio::spawn(conn);
        let name = format!(
            "walrus_read_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("create database");
        let manager = deadpool_postgres::Manager::from_config(
            config(&name),
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(2)
            .build()
            .expect("pool builds");
        TestDb { name, admin, pool }
    }

    async fn drop(self) {
        self.pool.close();
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .await
            .expect("drop database");
    }
}

fn col(name: &str, ty: &str) -> ColumnSchema {
    ColumnSchema {
        name: name.into(),
        type_name: ty.into(),
        type_oid: 0,
    }
}

fn row(cells: &[Option<&str>]) -> Row {
    cells.iter().map(|c| c.map(str::to_string)).collect()
}

async fn collect(tx: &deadpool_postgres::Transaction<'_>, sql: &str, ncols: usize) -> Vec<Row> {
    let rows = std::sync::Mutex::new(Vec::new());
    let rows_ref = &rows;
    let n = read::copy_rows(tx, sql, ncols, |batch| async move {
        rows_ref.lock().expect("not poisoned").extend(batch);
        Ok(())
    })
    .await
    .expect("copy");
    let mut rows = rows.into_inner().expect("not poisoned");
    assert_eq!(n as usize, rows.len());
    rows.sort();
    rows
}

#[tokio::test]
async fn snapshot_full_and_delta_reads() {
    let db = TestDb::create().await;
    let setup = db.pool.get().await.expect("connection");
    setup
        .batch_execute(
            "CREATE TABLE todos (id bigint PRIMARY KEY, title text);
             CREATE TABLE items (list_id bigint, pos int, v text, PRIMARY KEY (list_id, pos));
             INSERT INTO todos VALUES (1, 'milk'), (2, E'tab\\there\\nnewline \\\\ back'), (3, NULL),
                                      (4, 'it''s \"quoted\"');
             INSERT INTO items VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');",
        )
        .await
        .expect("setup");
    drop(setup);

    let todos = TableSchema {
        name: "public.todos".into(),
        columns: vec![col("id", "bigint"), col("title", "text")],
        key: vec![0],
    };
    let items = TableSchema {
        name: "public.items".into(),
        columns: vec![
            col("list_id", "bigint"),
            col("pos", "integer"),
            col("v", "text"),
        ],
        key: vec![0, 1],
    };

    let mut conn = db.pool.get().await.expect("connection");
    let tx = conn
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .expect("begin");
    let snapshot = read::take_snapshot(&tx)
        .await
        .expect("snapshot query")
        .expect("no streamed-but-invisible commits in a quiet database");
    assert!(snapshot.xmin <= snapshot.xmax);

    let all = collect(&tx, &read::full_query(&todos), 2).await;
    assert_eq!(
        all,
        vec![
            row(&[Some("1"), Some("milk")]),
            row(&[Some("2"), Some("tab\there\nnewline \\ back")]),
            row(&[Some("3"), None]),
            row(&[Some("4"), Some("it's \"quoted\"")]),
        ]
    );

    // A key that does not exist (99) is simply not returned.
    let keys = vec![
        vec!["2".to_string()],
        vec!["4".to_string()],
        vec!["99".to_string()],
    ];
    let delta = collect(&tx, &read::delta_query(&todos, &keys), 2).await;
    assert_eq!(delta, vec![all[1].clone(), all[3].clone()]);

    let keys = vec![
        vec!["1".to_string(), "2".to_string()],
        vec!["2".to_string(), "1".to_string()],
        vec!["2".to_string(), "2".to_string()],
    ];
    let delta = collect(&tx, &read::delta_query(&items, &keys), 3).await;
    assert_eq!(
        delta,
        vec![
            row(&[Some("1"), Some("2"), Some("b")]),
            row(&[Some("2"), Some("1"), Some("c")]),
        ]
    );
    tx.commit().await.expect("commit");
    drop(conn);
    db.drop().await;
}
