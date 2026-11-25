//! PLAN.md §7.2 / §11.3 #4: Postgres can stream a commit before it is visible to new snapshots.
//!
//! `synchronous_standby_names` naming a standby that doesn't exist makes every commit flush
//! locally and then wait. The walsender streams such a commit, so the engine indexes it, while
//! new snapshots still see the transaction as in progress. A catch-up must not use such a
//! snapshot; once the wait is cancelled the commit becomes visible and the client converges.
//!
//! The setting is server-wide and stalls every commit, so this is its own test binary (cargo
//! runs test binaries one at a time) and the setting is always restored.

mod common;

use std::time::Duration;

use common::{Subscriber, TABLES, TestDb, wait_for};
use walrus_client::State;
use walrus_engine::metrics::Metrics;

#[tokio::test(flavor = "multi_thread")]
async fn commit_streamed_before_visible() {
    let db = TestDb::new().await;
    let admin = common::connect(&format!("{}/postgres", common::PG)).await;

    let result = tokio::spawn(scenario(db)).await;

    set_standby(&admin, None).await;
    let db = match result {
        Ok(db) => db,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    };
    db.cleanup().await;
}

async fn scenario(db: TestDb) -> TestDb {
    let engine = db.start_engine().await;
    let m = engine.metrics.clone();
    let admin = common::connect(&format!("{}/postgres", common::PG)).await;
    set_standby(&admin, Some("walrus_missing_standby")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // This commit flushes locally, then waits for the standby forever.
    let writer = db.client().await;
    let pid: i32 = writer
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let applied_before = Metrics::get(&m.txns_applied);
    let blocked = tokio::spawn(async move {
        let _ = writer
            .batch_execute("UPDATE lists SET title = 'late' WHERE id = 5")
            .await;
        writer
    });

    // The engine receives and indexes it although nobody can see it yet.
    wait_for(
        || Metrics::get(&m.txns_applied) > applied_before,
        "engine applies the commit",
    )
    .await;
    let observer = db.client().await;
    let title: String = observer
        .query_one("SELECT title FROM lists WHERE id = 5", &[])
        .await
        .expect("select")
        .get(0);
    assert_eq!(title, "list 5", "streamed commit must not be visible yet");

    // A connecting client must not complete its catch-up on such a snapshot.
    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    wait_for(
        || Metrics::get(&m.join_retries) > 0,
        "catch-up retries its snapshot",
    )
    .await;
    assert!(
        sub.snapshot().offset.is_none(),
        "no catch-up completed on a bad snapshot"
    );

    // Cancel the wait: the transaction (already committed locally) becomes visible.
    admin
        .execute("SELECT pg_cancel_backend($1)", &[&pid])
        .await
        .expect("cancel wait");
    set_standby(&admin, None).await;
    let _ = blocked.await;

    sub.wait_converged(&db).await;
    let state = sub.stop().await;
    let row = &state.tables["public.lists"].rows[&vec!["5".to_string()]];
    assert_eq!(row[2].as_deref(), Some("late"));

    engine.shutdown().await.expect("shutdown");
    db
}

/// Set (or reset) `synchronous_standby_names` server-wide. `ALTER SYSTEM` can't run inside a
/// transaction block, so it is its own statement.
async fn set_standby(admin: &tokio_postgres::Client, name: Option<&str>) {
    let alter = match name {
        Some(n) => format!("ALTER SYSTEM SET synchronous_standby_names = '{n}'"),
        None => "ALTER SYSTEM RESET synchronous_standby_names".to_string(),
    };
    admin.simple_query(&alter).await.expect("alter system");
    admin
        .simple_query("SELECT pg_reload_conf()")
        .await
        .expect("reload");
}
