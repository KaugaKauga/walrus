//! PLAN.md §11.3 #7 (crash during new-epoch setup) and #17 (startup validation, schema change,
//! slot and table-set changes).

mod common;

use std::time::Duration;

use common::{Subscriber, TABLES, TestDb, random_writes};
use walrus_client::State;
use walrus_engine::change_index::FILE_NAME;

/// Run a first engine for a while so there is a store and a client state with an offset.
async fn first_run(db: &TestDb) -> (u64, State) {
    let engine = db.start_engine().await;
    let epoch = engine.epoch;
    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    random_writes(&db.client().await, 1, 10).await;
    sub.wait_converged(db).await;
    let state = sub.stop().await;
    assert!(state.offset.is_some());
    engine.shutdown().await.expect("shutdown");
    (epoch, state)
}

/// After a restart that must be a new epoch: old state resumes (as a full catch-up) and a fresh
/// client converges too.
async fn assert_new_epoch(db: &TestDb, old_epoch: u64, old_state: State) {
    let engine = db.restart_engine().await;
    assert_ne!(engine.epoch, old_epoch, "expected a new epoch");
    random_writes(&db.client().await, 2, 10).await;
    let fresh = Subscriber::start(&engine, &TABLES, State::default()).await;
    let resumed = Subscriber::start(&engine, &TABLES, old_state).await;
    fresh.wait_converged(db).await;
    resumed.wait_converged(db).await;
    random_writes(&db.client().await, 3, 10).await;
    fresh.wait_converged(db).await;
    resumed.wait_converged(db).await;
    fresh.stop().await;
    resumed.stop().await;
    engine.shutdown().await.expect("shutdown");
}

/// #7 The store looks half-created (`initialized` = 0) while the slot exists.
#[tokio::test(flavor = "multi_thread")]
async fn crash_during_setup_not_initialized() {
    let db = TestDb::new().await;
    let (epoch, state) = first_run(&db).await;

    let conn = rusqlite::Connection::open(db.data_dir.path().join(FILE_NAME)).expect("open store");
    conn.execute("UPDATE meta SET value = 0 WHERE key = 'initialized'", [])
        .expect("clear initialized");
    drop(conn);

    assert_new_epoch(&db, epoch, state).await;
    db.cleanup().await;
}

/// #7 The store file is gone (never written) while the slot exists.
#[tokio::test(flavor = "multi_thread")]
async fn crash_during_setup_no_store() {
    let db = TestDb::new().await;
    let (epoch, state) = first_run(&db).await;
    walrus_engine::change_index::remove(db.data_dir.path()).expect("remove store");
    assert_new_epoch(&db, epoch, state).await;
    db.cleanup().await;
}

/// #7 A file that is not even a SQLite database with `meta` (created, nothing committed).
#[tokio::test(flavor = "multi_thread")]
async fn crash_during_setup_empty_store() {
    let db = TestDb::new().await;
    let (epoch, state) = first_run(&db).await;
    walrus_engine::change_index::remove(db.data_dir.path()).expect("remove store");
    std::fs::write(db.data_dir.path().join(FILE_NAME), b"").expect("empty store");
    assert_new_epoch(&db, epoch, state).await;
    db.cleanup().await;
}

fn error_text(r: anyhow::Result<walrus_engine::Engine>) -> String {
    match r {
        Ok(_) => panic!("engine should not have started"),
        Err(e) => format!("{e:#}"),
    }
}

/// #17 Tables walrus cannot sync fail startup with a clear message.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_tables_fail_startup() {
    let db = TestDb::new().await;
    db.client()
        .await
        .batch_execute(
            "CREATE TABLE nopk (a int, b text);
             CREATE TABLE numpk (id numeric PRIMARY KEY, b text);",
        )
        .await
        .expect("create tables");

    let mut config = db.config();
    config.tables = vec!["public.nopk".into()];
    let e = error_text(db.start_unready(config).await);
    assert!(
        e.contains("public.nopk") && e.contains("no primary key"),
        "{e}"
    );

    let mut config = db.config();
    config.tables = vec!["public.numpk".into()];
    let e = error_text(db.start_unready(config).await);
    assert!(
        e.contains("public.numpk") && e.contains("primary-key column") && e.contains("numeric"),
        "{e}"
    );

    let mut config = db.config();
    config.tables = vec!["public.missing".into()];
    let e = error_text(db.start_unready(config).await);
    assert!(e.contains("does not exist"), "{e}");

    db.cleanup().await;
}

/// #17 `ALTER TABLE ... ADD COLUMN` + a write stops the engine with a schema-change error; the
/// next start is a new epoch and a fresh client converges.
#[tokio::test(flavor = "multi_thread")]
async fn schema_change_stops_the_engine() {
    let db = TestDb::new().await;
    let engine = db.start_engine().await;
    let epoch = engine.epoch;
    let client = db.client().await;

    let sub = Subscriber::start(&engine, &TABLES, State::default()).await;
    sub.wait_converged(&db).await;
    let state = sub.stop().await;

    client
        .batch_execute("ALTER TABLE lists ADD COLUMN extra int")
        .await
        .expect("alter");
    client
        .batch_execute("UPDATE lists SET title = 'after alter' WHERE id = 3")
        .await
        .expect("write");

    let result = tokio::time::timeout(Duration::from_secs(20), engine.run_until_stopped())
        .await
        .expect("engine stops after a schema change");
    let err = format!("{:#}", result.expect_err("engine must fail"));
    assert!(err.contains("schema") && err.contains("changed"), "{err}");

    assert_new_epoch(&db, epoch, state).await;
    db.cleanup().await;
}

/// #17 The slot was dropped while the engine was stopped.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_slot_is_a_new_epoch() {
    let db = TestDb::new().await;
    let (epoch, state) = first_run(&db).await;

    let admin = common::connect(&format!("{}/postgres", common::PG)).await;
    let mut dropped = false;
    for _ in 0..50 {
        if admin
            .execute("SELECT pg_drop_replication_slot($1)", &[&db.slot])
            .await
            .is_ok()
        {
            dropped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(dropped, "could not drop the slot");
    // Writes while no slot exists are not in any stream.
    random_writes(&db.client().await, 9, 10).await;

    assert_new_epoch(&db, epoch, state).await;
    db.cleanup().await;
}

/// #17 The configured table set changed.
#[tokio::test(flavor = "multi_thread")]
async fn changed_table_set_is_a_new_epoch() {
    let db = TestDb::new().await;
    let (epoch, _) = first_run(&db).await;

    let mut config = db.config();
    config.tables.retain(|t| t != "public.todos");
    let mut engine = None;
    for _ in 0..50 {
        match db.start_with(config.clone()).await {
            Ok(e) => {
                engine = Some(e);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let engine = engine.expect("engine starts with fewer tables");
    assert_ne!(engine.epoch, epoch);

    let two = ["public.users", "public.lists"];
    let sub = Subscriber::start(&engine, &two, State::default()).await;
    random_writes(&db.client().await, 4, 10).await;
    sub.wait_converged(&db).await;
    sub.stop().await;
    engine.shutdown().await.expect("shutdown");
    db.cleanup().await;
}
