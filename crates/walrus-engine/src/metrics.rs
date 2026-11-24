//! Counters for `/metrics` (Prometheus text format) and for tests (PLAN.md §9).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

macro_rules! metrics {
    ($($name:ident: $help:literal,)*) => {
        #[derive(Debug, Default)]
        pub struct Metrics {
            $(#[doc = $help] pub $name: AtomicU64,)*
        }

        impl Metrics {
            pub fn render(&self) -> String {
                let mut out = String::new();
                $(
                    let _ = writeln!(out, "# HELP walrus_{} {}", stringify!($name), $help);
                    let _ = writeln!(out, "walrus_{} {}", stringify!($name), self.$name.load(Relaxed));
                )*
                out
            }
        }
    };
}

metrics! {
    applied_lsn: "Commit LSN of the last transaction applied to the change index.",
    server_wal_end: "Server WAL end from the latest keepalive.",
    txns_applied: "Transactions applied to the change index.",
    txns_skipped: "Replayed transactions skipped (commit LSN <= last_lsn).",
    changes_applied: "Row changes applied to the change index.",
    index_commits: "Change index store commits (group commits).",
    sessions_active: "Open SSE sessions.",
    catchups_none: "Catch-ups answered from the index alone (nothing changed).",
    catchups_delta: "Catch-ups that read only changed rows from Postgres.",
    catchups_full: "Catch-ups that read at least one whole table from Postgres.",
    delta_over_cap: "Tables that fell back from delta to full because too many keys changed.",
    delta_keys: "Delta keys looked up in Postgres.",
    rows_read_full: "Rows read from Postgres by full catch-ups.",
    rows_read_delta: "Rows read from Postgres by delta catch-ups.",
    join_retries: "Catch-up snapshots retried because a streamed commit was not yet visible.",
    closes_lagged: "Sessions closed because they fell behind the live ring.",
    closes_oversized: "Sessions closed because of an oversized transaction.",
    closes_join: "Sessions closed because the join could not be resolved per key.",
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Relaxed);
    }

    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Relaxed)
    }
}
