//! PLAN.md §11.3 #14 (slow client), #15 (oversized transaction), #16 (large delta).

mod common;

use std::time::Duration;

use common::{Subscriber, TABLES, TestDb, wait_for};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use walrus_client::State;
use walrus_engine::encode::MAX_TXN_BYTES;
use walrus_engine::metrics::Metrics;
use walrus_engine::session::DELTA_CAP;

/// #15 One transaction larger than `MAX_TXN_BYTES` on a subscribed table closes live sessions;
/// the reconnecting client converges through a catch-up.
#[tokio::test(flavor = "multi_thread")]
async fn oversized_transaction_closes_sessions() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let m = &engine.metrics;

    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    let other = Subscriber::start(&engine, &["public.users"], State::default()).await;
    sub.wait_converged(&db).await;
    other.wait_converged(&db).await;
    let reconnects = || sub.reconnects.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(reconnects(), 0);

    // Two rows, each above half the limit, in one transaction.
    let client = db.client().await;
    let size = MAX_TXN_BYTES / 2 + 1000;
    client
        .batch_execute(&format!(
            "BEGIN;
             UPDATE todos SET notes = repeat('x', {size}) WHERE position = 1;
             UPDATE todos SET notes = repeat('y', {size}) WHERE position = 2;
             COMMIT;"
        ))
        .await
        .expect("big transaction");

    wait_for(
        || Metrics::get(&m.closes_oversized) >= 1,
        "oversized session closed",
    )
    .await;
    sub.wait_converged(&db).await;
    assert!(reconnects() >= 1);
    let key = vec!["3".to_string(), "2".to_string()];
    let row = &sub.snapshot().tables["public.todos"].rows[&key];
    assert_eq!(row[9].as_ref().map(String::len), Some(size));

    // The client that doesn't subscribe to the table is not closed, and live updates continue.
    assert_eq!(
        other.reconnects.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    client
        .batch_execute("UPDATE todos SET done = NOT done WHERE position = 3")
        .await
        .expect("write");
    sub.wait_converged(&db).await;

    sub.stop().await;
    other.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #16 More than `DELTA_CAP` changed keys while the client is away: a full catch-up.
#[tokio::test(flavor = "multi_thread")]
async fn large_delta_falls_back_to_full() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let m = &engine.metrics;

    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(&db).await;
    let state = sub.stop().await;
    assert_eq!(Metrics::get(&m.catchups_full), 1);

    let n = DELTA_CAP + 500;
    let client = db.client().await;
    client
        .batch_execute(&format!(
            "INSERT INTO lists SELECT i, md5('user1')::uuid, 'bulk ' || i, false, NULL
               FROM generate_series(1000, {}) i;
             DELETE FROM lists WHERE id < 10;",
            1000 + n
        ))
        .await
        .expect("bulk write");
    // Wait for the engine to index it before the client returns.
    wait_for(|| Metrics::get(&m.txns_applied) >= 1, "bulk applied").await;

    let sub = Subscriber::start(&engine, &TABLES, state).await;
    sub.wait_converged(&db).await;
    assert_eq!(Metrics::get(&m.delta_over_cap), 1);
    assert_eq!(Metrics::get(&m.catchups_full), 2);
    let lists = &sub.snapshot().tables["public.lists"];
    assert_eq!(lists.rows.len(), 50 - 9 + n + 1);

    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// #14 A client that sends its request and never reads is closed once it falls behind the live
/// ring, while other clients keep converging.
#[tokio::test(flavor = "multi_thread")]
async fn slow_client_is_closed() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let m = &engine.metrics;

    let healthy = Subscriber::start(&engine, &TABLES, State::default()).await;
    healthy.wait_converged(&db).await;

    let mut slow = TcpStream::connect(engine.addr).await.expect("connect");
    slow.write_all(
        b"GET /v1/stream?tables=public.todos HTTP/1.1\r\nHost: walrus\r\nAccept: text/event-stream\r\n\r\n",
    )
    .await
    .expect("request");
    wait_for(
        || Metrics::get(&m.sessions_active) >= 2,
        "slow session open",
    )
    .await;

    // Enough transactions, each carrying a sizable value, to fill the socket buffers, the
    // session's channel and then overrun the live ring.
    let client = db.client().await;
    let mut writes = 0;
    while Metrics::get(&m.closes_lagged) == 0 && writes < 30 {
        let mut sql = String::new();
        for i in 0..200 {
            sql.push_str(&format!(
                "BEGIN; UPDATE todos SET notes = repeat('s{writes}{i} ', 1000) \
                 WHERE position = {}; COMMIT;",
                i % 50 + 1
            ));
        }
        client.batch_execute(&sql).await.expect("writes");
        writes += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_for(|| Metrics::get(&m.closes_lagged) >= 1, "slow client closed").await;
    healthy.wait_converged(&db).await;

    drop(slow);
    healthy.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}

/// A client that stops reading must not hold up shutdown, even before it falls behind the
/// live ring (its session is blocked sending to a full socket).
#[tokio::test(flavor = "multi_thread")]
async fn stuck_client_does_not_block_shutdown() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let m = engine.metrics.clone();

    let mut stuck = TcpStream::connect(engine.addr).await.expect("connect");
    stuck
        .write_all(
            b"GET /v1/stream?tables=public.todos HTTP/1.1\r\nHost: walrus\r\nAccept: text/event-stream\r\n\r\n",
        )
        .await
        .expect("request");
    wait_for(|| Metrics::get(&m.sessions_active) >= 1, "session open").await;

    // Enough data to fill the socket buffers and the session's channel, but far fewer
    // transactions than the live ring holds, so the session is blocked, not lagged.
    let client = db.client().await;
    for round in 0..4 {
        let mut sql = String::new();
        for i in 0..20 {
            sql.push_str(&format!(
                "BEGIN; UPDATE todos SET notes = repeat('k{round}{i} ', 20000) WHERE position = {}; COMMIT;",
                i % 50 + 1
            ));
        }
        client.batch_execute(&sql).await.expect("writes");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(Metrics::get(&m.closes_lagged), 0, "blocked, not lagged");

    tokio::time::timeout(Duration::from_secs(10), engine.shutdown())
        .await
        .expect("shutdown completes despite the stuck client")
        .expect("shutdown");
    drop(stuck);
    db.cleanup().await;
}
