//! Tests for pg/setup.rs against a real Postgres (PLAN.md §11.1: localhost:54329, wal_level =
//! logical). Each test makes its own database and slot, since slots are cluster-wide.

use tokio_postgres::Client;
use walrus_engine::pg::setup::{
    self, PgParams, SESSION_OPTIONS, SlotState, check_server, create_publication_and_slot,
    read_schemas, slot_state,
};

const ADMIN_URL: &str = "postgres://postgres:postgres@localhost:54329/postgres";

/// A uniquely named database, plus a slot name derived from it.
struct TestDb {
    admin: Client,
    db: String,
    slot: String,
    params: PgParams,
    client: Client,
}

impl TestDb {
    async fn new() -> TestDb {
        let admin = setup::connect(&PgParams::from_url(ADMIN_URL).expect("admin url"))
            .await
            .expect("connect to the test Postgres on localhost:54329");
        let id = format!("{:08x}{:08x}", rand::random::<u32>(), std::process::id());
        let db = format!("walrus_t_{id}");
        admin
            .batch_execute(&format!("CREATE DATABASE {db}"))
            .await
            .expect("create database");
        let params = PgParams::from_url(&format!(
            "postgres://postgres:postgres@localhost:54329/{db}"
        ))
        .expect("url");
        let client = setup::connect(&params).await.expect("connect");
        TestDb {
            admin,
            slot: format!("slot_{id}"),
            db,
            params,
            client,
        }
    }

    async fn sql(&self, sql: &str) {
        self.client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Error text (with context chain) of reading `names`.
    async fn schema_error(&self, name: &str) -> String {
        let err = read_schemas(&self.client, &[name.to_string()])
            .await
            .expect_err("read_schemas should fail");
        format!("{err:#}")
    }

    /// Drop the slot, then the database.
    async fn cleanup(self) {
        let TestDb {
            admin,
            db,
            slot,
            client,
            ..
        } = self;
        drop(client);
        admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("drop slot");
        admin
            .batch_execute(&format!("DROP DATABASE {db} WITH (FORCE)"))
            .await
            .expect("drop database");
    }
}

#[test]
fn from_url_parses_parts_and_defaults() {
    let p = PgParams::from_url("postgres://bob:s3cret@db.example:6543/shop").expect("full url");
    assert_eq!(
        (
            p.host.as_str(),
            p.port,
            p.user.as_str(),
            p.password.as_str(),
            p.dbname.as_str()
        ),
        ("db.example", 6543, "bob", "s3cret", "shop")
    );

    let p = PgParams::from_url("postgres://alice@localhost").expect("minimal url");
    assert_eq!(
        (p.port, p.dbname.as_str(), p.password.as_str()),
        (5432, "alice", "")
    );

    assert!(PgParams::from_url("not a url at all %%%").is_err());
    let err = PgParams::from_url("postgres://u:p@%2Fvar%2Frun%2Fpostgresql/db")
        .expect_err("unix sockets are not supported");
    assert!(err.to_string().contains("TCP host"), "{err}");
    assert!(
        PgParams::from_url("postgres://localhost/db").is_err(),
        "user is required"
    );
}

#[test]
fn configs_carry_session_options() {
    let p = PgParams::from_url(ADMIN_URL).expect("url");
    assert_eq!(p.tokio_config().get_options(), Some(SESSION_OPTIONS));
    assert_eq!(p.tokio_config().get_application_name(), Some("walrus"));
    let r = p.replication_config("s", "p");
    assert_eq!(r.options.as_deref(), Some(SESSION_OPTIONS));
    assert_eq!(
        (r.port, r.slot.as_str(), r.start_lsn.as_u64()),
        (54329, "s", 0)
    );
}

#[tokio::test]
async fn check_server_accepts_the_test_postgres() {
    let db = TestDb::new().await;
    check_server(&db.client, 4).await.expect("server is fine");
    let err = check_server(&db.client, 1_000_000)
        .await
        .expect_err("cannot have a million connections");
    assert!(err.to_string().contains("max_connections"), "{err}");
    db.cleanup().await;
}

#[tokio::test]
async fn read_schemas_orders_columns_and_keys() {
    let db = TestDb::new().await;
    db.sql(
        "CREATE TABLE public.orders (
             id bigint,
             note text,
             region text,
             total numeric(10,2),
             twice bigint GENERATED ALWAYS AS (id * 2) STORED,
             scrap int,
             PRIMARY KEY (region, id)
         );
         ALTER TABLE public.orders DROP COLUMN scrap;
         CREATE SCHEMA \"My Schema\";
         CREATE TABLE \"My Schema\".\"to\"\"do\" (k uuid PRIMARY KEY, d date);",
    )
    .await;

    let names = vec!["public.orders".to_string(), "My Schema.to\"do".to_string()];
    let tables = read_schemas(&db.client, &names).await.expect("schemas");
    assert_eq!(tables.len(), 2);

    let orders = &tables[0];
    let cols: Vec<_> = orders
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.type_name.as_str(), c.type_oid))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", "bigint", 20),
            ("note", "text", 25),
            ("region", "text", 25),
            ("total", "numeric(10,2)", 1700),
        ],
        "attnum order, generated and dropped columns excluded"
    );
    // The constraint says (region, id); keys are in column order: id (0), region (2).
    assert_eq!(orders.key, vec![0, 2]);

    assert_eq!(tables[1].name, "My Schema.to\"do");
    assert_eq!(tables[1].key, vec![0]);
    assert_eq!(tables[1].columns[0].type_name, "uuid");
    db.cleanup().await;
}

#[tokio::test]
async fn read_schemas_rejects_unsupported_tables() {
    let db = TestDb::new().await;
    db.sql(
        "CREATE TABLE public.no_pk (a int, b text);
         CREATE TABLE public.num_key (a numeric PRIMARY KEY);
         CREATE TABLE public.float_key (a int, b float8, PRIMARY KEY (a, b));
         CREATE TABLE public.ident_nothing (a int PRIMARY KEY);
         ALTER TABLE public.ident_nothing REPLICA IDENTITY NOTHING;
         CREATE TABLE public.ident_full (a int PRIMARY KEY);
         ALTER TABLE public.ident_full REPLICA IDENTITY FULL;
         CREATE VIEW public.a_view AS SELECT 1 AS a;
         CREATE COLLATION public.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
         CREATE TABLE public.ci_key (a text COLLATE public.ci PRIMARY KEY);
         CREATE TABLE public.det_key (a text COLLATE \"C\" PRIMARY KEY, b varchar(20));",
    )
    .await;

    let missing = db.schema_error("public.nope").await;
    assert!(
        missing.contains("public.nope") && missing.contains("does not exist"),
        "{missing}"
    );
    let no_pk = db.schema_error("public.no_pk").await;
    assert!(no_pk.contains("no primary key"), "{no_pk}");
    let numeric = db.schema_error("public.num_key").await;
    assert!(
        numeric.contains("numeric") && numeric.contains("not supported"),
        "{numeric}"
    );
    let float = db.schema_error("public.float_key").await;
    assert!(float.contains("double precision"), "{float}");
    let nothing = db.schema_error("public.ident_nothing").await;
    assert!(nothing.contains("REPLICA IDENTITY NOTHING"), "{nothing}");
    let view = db.schema_error("public.a_view").await;
    assert!(view.contains("not an ordinary table"), "{view}");
    let ci = db.schema_error("public.ci_key").await;
    assert!(ci.contains("nondeterministic collation"), "{ci}");

    // Accepted: REPLICA IDENTITY FULL, deterministic collations, varchar(n) non-key columns.
    let ok = read_schemas(
        &db.client,
        &[
            "public.ident_full".to_string(),
            "public.det_key".to_string(),
        ],
    )
    .await
    .expect("supported tables");
    assert_eq!(ok[1].columns[1].type_name, "character varying(20)");
    db.cleanup().await;
}

#[tokio::test]
async fn slot_state_missing_then_ready() {
    let db = TestDb::new().await;
    assert_eq!(
        slot_state(&db.client, &db.slot).await.expect("state"),
        SlotState::Missing
    );
    db.sql(&format!(
        "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
        db.slot
    ))
    .await;
    assert_eq!(
        slot_state(&db.client, &db.slot).await.expect("state"),
        SlotState::Ready
    );
    db.cleanup().await;
}

/// Tables in a publication, `schema.table` comma-separated.
async fn published(client: &Client, publication: &str) -> Option<String> {
    client
        .query_one(
            "SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY tablename)
             FROM pg_publication_tables WHERE pubname = $1",
            &[&publication],
        )
        .await
        .expect("pg_publication_tables")
        .get(0)
}

#[tokio::test]
async fn creates_publication_and_slot_and_recreates_them() {
    let db = TestDb::new().await;
    db.sql(
        "CREATE TABLE public.a (id int PRIMARY KEY);
         CREATE TABLE public.b (id int PRIMARY KEY, v text);",
    )
    .await;
    let publication = format!("pub_{}", &db.slot[5..]);
    let tables = read_schemas(
        &db.client,
        &["public.a".to_string(), "public.b".to_string()],
    )
    .await
    .expect("schemas");

    // First time: publication created, slot created at a nonzero consistent point.
    let c1 = create_publication_and_slot(&db.params, &db.client, &db.slot, &publication, &tables)
        .await
        .expect("create");
    assert!(c1 > 0);
    assert_eq!(
        slot_state(&db.client, &db.slot).await.expect("state"),
        SlotState::Ready
    );
    assert_eq!(
        published(&db.client, &publication).await.as_deref(),
        Some("public.a,public.b")
    );
    // Slot creation never exports a snapshot, and it reports the same point Postgres stored.
    let confirmed: String = db
        .client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&db.slot],
        )
        .await
        .expect("slot row")
        .get(0);
    assert_eq!(
        pgwire_replication::Lsn::parse(&confirmed)
            .expect("lsn")
            .as_u64(),
        c1
    );

    // Second time (a new epoch): existing slot is dropped and recreated, the publication's
    // table list is replaced, and the consistent point does not go backwards.
    let only_a = &tables[..1];
    let c2 = create_publication_and_slot(&db.params, &db.client, &db.slot, &publication, only_a)
        .await
        .expect("recreate");
    assert!(c2 >= c1);
    assert_eq!(
        published(&db.client, &publication).await.as_deref(),
        Some("public.a")
    );
    assert_eq!(
        slot_state(&db.client, &db.slot).await.expect("state"),
        SlotState::Ready
    );
    db.cleanup().await;
}
