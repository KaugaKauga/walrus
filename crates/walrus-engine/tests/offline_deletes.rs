//! PLAN.md §11.3 #10: deletes while a client is away.

mod common;

use common::{Subscriber, TABLES, TestDb};
use walrus_client::State;
use walrus_engine::metrics::Metrics;

async fn away_and_back(db: &TestDb, restart_between: bool) {
    let mut engine = db.start_engine().await;
    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(db).await;
    let state = sub.stop().await;

    let client = db.client().await;
    // Inserted and deleted while away: the client never saw the row.
    client
        .batch_execute(
            "INSERT INTO lists VALUES (200, md5('user1')::uuid, 'ghost', false, NULL);
             DELETE FROM lists WHERE id = 200;
             INSERT INTO todos (list_id, position, title, done) VALUES (1, 900, 'ghost', false);
             DELETE FROM todos WHERE position = 900;",
        )
        .await
        .expect("ghost rows");
    // Deleted and re-inserted (different content, then identical content), across transactions
    // and within one.
    client
        .batch_execute(
            "DELETE FROM lists WHERE id = 2;
             INSERT INTO lists VALUES (2, md5('user2')::uuid, 'reborn', true, '{\"a\":1}');
             BEGIN;
               DELETE FROM lists WHERE id = 4;
               INSERT INTO lists VALUES (4, md5('user4')::uuid, 'same txn', false, NULL);
             COMMIT;
             DELETE FROM users WHERE email = 'user5@example.com';
             INSERT INTO users SELECT id, email, name, created_at, avatar FROM users WHERE false;",
        )
        .await
        .expect("re-inserts");
    // Plain deletes, including a composite key and a key that is deleted twice over its life.
    client
        .batch_execute(
            "DELETE FROM lists WHERE id IN (7, 8, 9);
             DELETE FROM todos WHERE position IN (3, 4, 5);
             DELETE FROM users WHERE email IN ('user1@example.com', 'user2@example.com');
             INSERT INTO lists VALUES (300, md5('user1')::uuid, 'x', false, NULL);
             UPDATE lists SET title = 'y' WHERE id = 300;
             DELETE FROM lists WHERE id = 300;",
        )
        .await
        .expect("deletes");

    if restart_between {
        engine.shutdown().await.expect("shutdown");
        engine = db.restart_engine().await;
        assert_eq!(
            Metrics::get(&engine.metrics.catchups_full),
            0,
            "restart keeps the epoch"
        );
    }
    // The engine must have indexed everything before the client returns (delta, not full).
    let m = engine.metrics.clone();
    let before = Metrics::get(&m.catchups_delta);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let sub = loop {
        let sub = Subscriber::start(&engine, &TABLES, state.clone()).await;
        sub.wait_converged(db).await;
        if Metrics::get(&m.catchups_delta) > before || tokio::time::Instant::now() > deadline {
            break sub;
        }
        sub.stop().await;
    };
    assert!(Metrics::get(&m.catchups_delta) > before, "resumed by delta");
    assert_eq!(Metrics::get(&m.delta_over_cap), 0);
    sub.wait_converged(db).await;
    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn deletes_while_offline() {
    let db = TestDb::new().await;
    away_and_back(&db, false).await;
    db.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deletes_while_offline_across_restart() {
    let db = TestDb::new().await;
    away_and_back(&db, true).await;
    db.cleanup().await;
}
