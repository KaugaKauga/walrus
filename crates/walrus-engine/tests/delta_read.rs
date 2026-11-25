//! PLAN.md §11.3 #13: delta reads use the primary-key index, and arbitrary key text survives
//! the inlined literal.

mod common;

use common::TestDb;
use tokio_postgres::SimpleQueryMessage;
use walrus_engine::pg::read::{copy_rows, delta_query};
use walrus_engine::pg::setup::{self, PgParams, read_schemas};

fn explain_input(copy: &str) -> &str {
    copy.strip_prefix("COPY (")
        .and_then(|s| s.strip_suffix(") TO STDOUT"))
        .expect("COPY (...) TO STDOUT")
}

async fn plan(client: &tokio_postgres::Client, copy: &str) -> String {
    let sql = format!("EXPLAIN (FORMAT JSON) {}", explain_input(copy));
    let mut out = String::new();
    for msg in client.simple_query(&sql).await.expect("explain") {
        if let SimpleQueryMessage::Row(r) = msg {
            out.push_str(r.get(0).expect("plan"));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_reads_use_the_primary_key_index() {
    let db = TestDb::new().await;
    let client = db.client().await;
    client
        .batch_execute(
            "INSERT INTO lists SELECT i, md5('user1')::uuid, 'l' || i, false, NULL
               FROM generate_series(100, 10100) i;
             INSERT INTO todos (list_id, position, title, done)
               SELECT i % 100 + 100, i + 1000, 't', false FROM generate_series(1, 10000) i;
             ANALYZE lists; ANALYZE todos;",
        )
        .await
        .expect("bulk load");

    let params = PgParams::from_url(&db.url).expect("url");
    let pg = setup::connect(&params).await.expect("connect");
    let tables = read_schemas(&pg, &common::TABLES.map(String::from))
        .await
        .expect("schemas");
    let lists = &tables[1];
    let todos = &tables[2];

    let keys: Vec<_> = (0..20).map(|i| vec![(500 + i).to_string()]).collect();
    let p = plan(&client, &delta_query(lists, &keys)).await;
    assert!(p.contains("lists_pkey"), "single-column key: {p}");
    assert!(!p.contains("Seq Scan"), "single-column key: {p}");

    let keys: Vec<_> = (1..20)
        .map(|i| vec![(i % 100 + 100).to_string(), (i + 1000).to_string()])
        .collect();
    let p = plan(&client, &delta_query(todos, &keys)).await;
    assert!(p.contains("todos_pkey"), "composite key: {p}");
    assert!(!p.contains("Seq Scan"), "composite key: {p}");

    drop(client);
    drop(pg);
    db.cleanup().await;
}

/// Keys with quotes, backslashes, braces, commas, unicode and the empty string come back as
/// exactly the rows asked for.
#[tokio::test(flavor = "multi_thread")]
async fn nasty_keys_round_trip() {
    let db = TestDb::new().await;
    let client = db.client().await;
    client
        .batch_execute(
            "CREATE TABLE kv (k text PRIMARY KEY, v int);
             CREATE TABLE kv2 (a text, b text, PRIMARY KEY (a, b));",
        )
        .await
        .expect("create");
    let nasty = [
        "",
        "'",
        "''",
        "\"",
        "\\",
        "\\\\",
        "a\\'b",
        "{}",
        "{a,b}",
        "a,b",
        " ",
        "NULL",
        "null",
        "ünï🦭cödé",
        "\"quoted\", {braces}, it's",
        "tab\there",
        "new\nline",
        "$1",
        "--",
        ";",
    ];
    for (i, k) in nasty.iter().enumerate() {
        client
            .execute("INSERT INTO kv VALUES ($1, $2)", &[k, &(i as i32)])
            .await
            .expect("insert");
        client
            .execute(
                "INSERT INTO kv2 VALUES ($1, $2)",
                &[k, &nasty[(i + 1) % nasty.len()]],
            )
            .await
            .expect("insert");
    }
    client
        .batch_execute("INSERT INTO kv VALUES ('not requested', 0)")
        .await
        .expect("insert");

    let params = PgParams::from_url(&db.url).expect("url");
    let pg = setup::connect(&params).await.expect("connect");
    let tables = read_schemas(&pg, &["public.kv".to_string(), "public.kv2".to_string()])
        .await
        .expect("schemas");
    let pool = walrus_engine::pg::read::create_pool(&params, 1).expect("pool");
    let mut conn = pool.get().await.expect("pool connection");

    let keys: Vec<Vec<String>> = nasty.iter().map(|k| vec![k.to_string()]).collect();
    let keys2: Vec<Vec<String>> = nasty
        .iter()
        .enumerate()
        .map(|(i, k)| vec![k.to_string(), nasty[(i + 1) % nasty.len()].to_string()])
        .collect();
    let tx = conn.transaction().await.expect("tx");
    for (table, keys) in [(&tables[0], keys), (&tables[1], keys2)] {
        let mut got = Vec::new();
        let sql = delta_query(table, &keys);
        copy_rows(&tx, &sql, table.columns.len(), |rows| {
            got.extend(rows);
            async { Ok(()) }
        })
        .await
        .expect("delta read");
        let mut got_keys: Vec<_> = got.iter().map(|r| table.key_of(r).expect("key")).collect();
        got_keys.sort();
        let mut want = keys.clone();
        want.sort();
        assert_eq!(got_keys, want, "{}", table.name);
    }
    tx.commit().await.expect("commit");

    drop(client);
    drop(pg);
    db.cleanup().await;
}
