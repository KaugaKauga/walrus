//! End-to-end sync tests against a real Postgres (PLAN.md §11.3). Needs `docker compose up -d`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{Subscriber, TABLES, TestDb, random_writes};
use walrus_client::State;
use walrus_engine::metrics::Metrics;

/// #1 New epoch, then live: the first connect is a full catch-up, then inserts, updates
/// (including unchanged large values and key changes) and deletes stream in.
#[tokio::test(flavor = "multi_thread")]
async fn full_catch_up_then_live() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(&db).await;
    assert_eq!(Metrics::get(&engine.metrics.catchups_full), 1);

    let client = db.client().await;
    client
        .batch_execute(
            "INSERT INTO lists VALUES (100, md5('user1')::uuid, 'new', false, NULL);
             UPDATE users SET name = E'changed\\ttab' WHERE email = 'user3@example.com';
             DELETE FROM todos WHERE position = 2;
             UPDATE todos SET done = NOT done WHERE position = 1;       -- large notes unchanged: patch
             UPDATE todos SET position = 500 WHERE position = 6;       -- key change, large notes unchanged
             BEGIN; DELETE FROM lists WHERE id = 1; UPDATE lists SET title = 'x' WHERE id = 2; COMMIT;",
        )
        .await
        .expect("writes");
    sub.wait_converged(&db).await;

    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #2/#3 The join: clients connect (full) and resume (delta) while writers run throughout.
/// Every client must end up equal to `SELECT *`.
#[tokio::test(flavor = "multi_thread")]
async fn join_under_concurrent_writes() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..3)
        .map(|w| {
            let stop = stop.clone();
            let url = db.url.clone();
            tokio::spawn(async move {
                let client = common::connect(&url).await;
                let mut seed = w * 1_000_003;
                while !stop.load(Ordering::Relaxed) {
                    random_writes(&client, seed, 5).await;
                    seed += 1;
                }
            })
        })
        .collect();

    // A transaction left open across the first snapshots, committed later.
    let open = db.client().await;
    open.batch_execute("BEGIN; UPDATE lists SET title = 'held open' WHERE id = 7;")
        .await
        .expect("open txn");

    let mut subs = Vec::new();
    for _ in 0..4 {
        subs.push(Subscriber::start(&engine, &TABLES, State::default()).await);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    open.batch_execute("COMMIT").await.expect("commit open txn");

    // Resume half of them with their offsets, still under load.
    let mut resumed = Vec::new();
    for sub in subs.drain(..2) {
        let state = sub.stop().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        resumed.push(Subscriber::start(&engine, &TABLES, state).await);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.expect("writer");
    }
    for sub in subs.iter().chain(resumed.iter()) {
        sub.wait_converged(&db).await;
    }
    assert!(
        Metrics::get(&engine.metrics.catchups_delta) + Metrics::get(&engine.metrics.catchups_none)
            >= 2
    );

    for sub in subs.into_iter().chain(resumed) {
        sub.stop().await;
    }
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #3 (asserted) and #8: a resume reads only the changed rows; nothing changed → no read.
#[tokio::test(flavor = "multi_thread")]
async fn resume_reads_only_the_delta() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let m = &engine.metrics;

    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(&db).await;
    let state = sub.stop().await;
    assert!(state.offset.is_some());

    // Nothing changed: answered from the index alone.
    let sub = Subscriber::start(&engine, &TABLES, state).await;
    sub.wait_converged(&db).await;
    assert_eq!(Metrics::get(&m.catchups_none), 1);
    let state = sub.stop().await;

    // Three keys changed (one of them deleted) while away.
    let client = db.client().await;
    client
        .batch_execute(
            "UPDATE lists SET title = 'away' WHERE id = 3;
             DELETE FROM lists WHERE id = 4;
             INSERT INTO users VALUES (md5('away')::uuid, 'away@example.com', NULL, now(), NULL);",
        )
        .await
        .expect("writes");
    // Let the engine index them before resuming.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let full_rows = Metrics::get(&m.rows_read_full);
    let sub = Subscriber::start(&engine, &TABLES, state).await;
    sub.wait_converged(&db).await;
    assert_eq!(Metrics::get(&m.catchups_delta), 1);
    assert_eq!(Metrics::get(&m.delta_keys), 3);
    assert_eq!(
        Metrics::get(&m.rows_read_delta),
        2,
        "the deleted key returns no row"
    );
    assert_eq!(Metrics::get(&m.rows_read_full), full_rows, "no full read");

    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #6 Restart: stop the engine, write, start it again on the same store and slot. A resuming
/// client gets only the delta.
#[tokio::test(flavor = "multi_thread")]
async fn restart_keeps_the_index() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let epoch = engine.epoch;
    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(&db).await;
    let state = sub.stop().await;
    engine.shutdown().await.expect("shutdown");

    db.client()
        .await
        .batch_execute("UPDATE todos SET title = 'while down' WHERE position = 9;")
        .await
        .expect("write while down");

    let engine = db.start_engine().await;
    assert_eq!(engine.epoch, epoch, "same epoch after a clean restart");
    // The write made while the engine was down arrives on the resumed stream; resume the client
    // once it is indexed so the catch-up (not the live stream) delivers it.
    common::wait_for(
        || Metrics::get(&engine.metrics.txns_applied) >= 1,
        "write indexed",
    )
    .await;
    let sub = Subscriber::start(&engine, &TABLES, state).await;
    sub.wait_converged(&db).await;
    assert_eq!(Metrics::get(&engine.metrics.catchups_full), 0);
    assert_eq!(Metrics::get(&engine.metrics.delta_keys), 1);

    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #9 Truncate raises the floor: live clients get a table-reset, and a client resuming from
/// before the truncate gets that table in full (and only that table).
#[tokio::test(flavor = "multi_thread")]
async fn truncate_resets_the_table() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let live = Subscriber::start(&engine, &TABLES, State::default()).await;
    let away = Subscriber::start(&engine, &TABLES, State::default()).await;
    live.wait_converged(&db).await;
    away.wait_converged(&db).await;
    let away_state = away.stop().await;

    db.client()
        .await
        .batch_execute(
            "TRUNCATE todos; INSERT INTO todos (list_id, position, title, done) VALUES (1, 1, 'after', true);",
        )
        .await
        .expect("truncate");
    live.wait_converged(&db).await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    let full_before = Metrics::get(&engine.metrics.catchups_full);
    let back = Subscriber::start(&engine, &TABLES, away_state).await;
    back.wait_converged(&db).await;
    assert_eq!(Metrics::get(&engine.metrics.catchups_full), full_before + 1);
    assert_eq!(
        Metrics::get(&engine.metrics.rows_read_full),
        150 + 150 + 1,
        "only todos re-read"
    );

    live.stop().await;
    back.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// Subscribing to a subset of tables only streams those tables.
#[tokio::test(flavor = "multi_thread")]
async fn subset_of_tables() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let sub = Subscriber::start(&engine, &["public.lists"], State::default()).await;
    sub.wait_converged(&db).await;
    random_writes(&db.client().await, 42, 30).await;
    sub.wait_converged(&db).await;
    let state = sub.stop().await;
    assert_eq!(
        state.tables.keys().collect::<Vec<_>>(),
        vec!["public.lists"]
    );
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}
