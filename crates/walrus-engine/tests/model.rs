//! PLAN.md §11.4: a seeded random workload of writes, client connects, stops and resumes, and
//! engine restarts. At the end every client must equal Postgres.
//!
//! Replay a failure with `WALRUS_MODEL_SEED=<seed> cargo test --test model`.

mod common;

use common::{Subscriber, TABLES, TestDb, model_txn};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use walrus_client::State;

const OPS: usize = 300;

#[tokio::test(flavor = "multi_thread")]
async fn model() {
    let seeds: Vec<u64> = match std::env::var("WALRUS_MODEL_SEED") {
        Ok(s) => vec![s.parse().expect("WALRUS_MODEL_SEED is a u64")],
        Err(_) => (0..3).map(|_| rand::random()).collect(),
    };
    for seed in seeds {
        println!("model seed {seed}");
        run(seed).await;
    }
}

type Saved = (Vec<String>, State);

async fn run(seed: u64) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let db = TestDb::new().await;
    let writer = db.client().await;
    let mut engine = db.start_engine().await;

    let mut restarts: Vec<usize> = (0..rng.random_range(2..4))
        .map(|_| rng.random_range(20..OPS - 20))
        .collect();
    restarts.sort();
    let mut active: Vec<Subscriber> = Vec::new();
    let mut saved: Vec<Saved> = Vec::new();

    for op in 0..OPS {
        if restarts.contains(&op) {
            println!("op {op}: restart");
            for sub in active.drain(..) {
                saved.push((sub.tables.clone(), sub.stop().await));
            }
            engine.shutdown().await.expect("shutdown");
            // Writes while the engine is down are replayed from the slot on the next start.
            for _ in 0..rng.random_range(0..4) {
                run_txn(&writer, &mut rng, seed, op).await;
            }
            engine = db.restart_engine().await;
            continue;
        }
        match rng.random_range(0..100) {
            0..=69 => run_txn(&writer, &mut rng, seed, op).await,
            70..=81 => {
                // Connect: resume a saved client (if any, usually) or start a fresh one.
                let (tables, state) = if !saved.is_empty() && rng.random_bool(0.7) {
                    saved.swap_remove(rng.random_range(0..saved.len()))
                } else {
                    (random_tables(&mut rng), State::default())
                };
                let names: Vec<&str> = tables.iter().map(String::as_str).collect();
                active.push(Subscriber::start(&engine, &names, state).await);
            }
            82..=91 if !active.is_empty() => {
                let sub = active.swap_remove(rng.random_range(0..active.len()));
                saved.push((sub.tables.clone(), sub.stop().await));
            }
            92..=95 if !active.is_empty() => {
                // A mid-run convergence check on a random client.
                active[rng.random_range(0..active.len())]
                    .wait_converged(&db)
                    .await;
            }
            _ => tokio::task::yield_now().await,
        }
    }

    for (tables, state) in saved.drain(..) {
        let names: Vec<&str> = tables.iter().map(String::as_str).collect();
        active.push(Subscriber::start(&engine, &names, state).await);
    }
    active.push(Subscriber::start(&engine, &TABLES, State::default()).await);
    for sub in &active {
        sub.wait_converged(&db).await;
    }
    for sub in active {
        sub.stop().await;
    }
    engine.shutdown().await.expect("shutdown");
    drop(writer);
    db.cleanup().await;
}

async fn run_txn(writer: &tokio_postgres::Client, rng: &mut impl Rng, seed: u64, op: usize) {
    let sql = model_txn(rng);
    if let Err(e) = writer.batch_execute(&sql).await {
        panic!("seed {seed} op {op}: {e}\n{}", &sql[..sql.len().min(2000)]);
    }
}

fn random_tables(rng: &mut impl Rng) -> Vec<String> {
    loop {
        let t: Vec<String> = TABLES
            .iter()
            .filter(|_| rng.random_bool(0.6))
            .map(|t| t.to_string())
            .collect();
        if !t.is_empty() {
            return t;
        }
        let _ = TABLES.choose(rng);
    }
}
