//! The regression guardrail: which metrics are tracked, and when a change counts as a regression.
//!
//! Timings on one shared laptop are noisy, so they get wide tolerances; bytes, row counts and
//! query counts are deterministic and are compared exactly or nearly so.

/// What a scenario hands back: its report section and the tracked metrics it measured.
pub struct Outcome {
    pub section: String,
    pub metrics: Vec<(&'static str, f64)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Lower,
    Higher,
    Exact,
}

pub struct Tracked {
    pub key: &'static str,
    pub label: &'static str,
    pub unit: &'static str,
    pub direction: Direction,
    /// Relative tolerance (0.5 = 50%).
    pub tol: f64,
    /// Absolute slack in the metric's unit, so tiny baselines don't make the rule hair-trigger.
    pub slack: f64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Ok,
    Regression,
    Improved,
}

impl Status {
    pub fn text(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Regression => "REGRESSION",
            Status::Improved => "improved",
        }
    }
}

const fn t(
    key: &'static str,
    label: &'static str,
    unit: &'static str,
    direction: Direction,
    tol: f64,
    slack: f64,
) -> Tracked {
    Tracked {
        key,
        label,
        unit,
        direction,
        tol,
        slack,
    }
}

use Direction::{Exact, Higher, Lower};

pub static METRICS: &[Tracked] = &[
    t(
        "b1_p50_ms",
        "B1 commit→client latency p50",
        "ms",
        Lower,
        1.0,
        2.0,
    ),
    t(
        "b1_p99_ms",
        "B1 commit→client latency p99",
        "ms",
        Lower,
        1.0,
        5.0,
    ),
    t(
        "b1_missed_updates",
        "B1 updates written × clients − received",
        "count",
        Exact,
        0.0,
        0.0,
    ),
    t(
        "b2_changes_per_s",
        "B2 engine applied changes/s",
        "/s",
        Higher,
        0.2,
        0.0,
    ),
    // How far behind the engine happens to be when the writers stop: 0.17–2.75 s across five
    // runs of the same code. Only an engine that can't keep up (several seconds more) counts;
    // sustained throughput is b2_changes_per_s's job.
    t(
        "b2_drain_s",
        "B2 engine drain time after writers stop",
        "s",
        Lower,
        1.0,
        5.0,
    ),
    t(
        "b2_max_lag_mib",
        "B2 max lag vs pg_current_wal_lsn()",
        "MiB",
        Lower,
        1.0,
        16.0,
    ),
    t(
        "b2_store_commit_ms",
        "B2 mean change-index commit time",
        "ms",
        Lower,
        0.5,
        0.5,
    ),
    t(
        "b3_full_ms",
        "B3 full catch-up, 100k rows",
        "ms",
        Lower,
        0.3,
        20.0,
    ),
    t(
        "b3_full_bytes",
        "B3 full catch-up bytes",
        "bytes",
        Lower,
        0.02,
        0.0,
    ),
    t(
        "b3_full_pg_ms",
        "B3 Postgres exec time, full catch-up",
        "ms",
        Lower,
        0.5,
        20.0,
    ),
    t(
        "b3_delta_ms",
        "B3 delta resume, 100 changed rows",
        "ms",
        Lower,
        1.0,
        3.0,
    ),
    t(
        "b3_delta_bytes",
        "B3 delta resume bytes",
        "bytes",
        Lower,
        0.02,
        0.0,
    ),
    t(
        "b3_delta_rows_read",
        "B3 rows read from Postgres for the delta",
        "rows",
        Exact,
        0.0,
        0.0,
    ),
    t(
        "b3_delta_pg_ms",
        "B3 Postgres exec time, delta",
        "ms",
        Lower,
        1.0,
        2.0,
    ),
    t(
        "b3_none_ms",
        "B3 resume, nothing changed",
        "ms",
        Lower,
        1.0,
        2.0,
    ),
    t(
        "b3_none_pg_calls",
        "B3 Postgres queries when nothing changed",
        "count",
        Exact,
        0.0,
        0.0,
    ),
];

pub fn tracked(key: &str) -> Option<&'static Tracked> {
    METRICS.iter().find(|m| m.key == key)
}

impl Tracked {
    pub fn judge(&self, base: f64, value: f64) -> Status {
        match self.direction {
            Lower if value > base * (1.0 + self.tol) + self.slack => Status::Regression,
            Lower if value < base * (1.0 - self.tol) - self.slack => Status::Improved,
            Higher if value < base * (1.0 - self.tol) - self.slack => Status::Regression,
            Higher if value > base * (1.0 + self.tol) + self.slack => Status::Improved,
            Exact if value != base => Status::Regression,
            _ => Status::Ok,
        }
    }

    /// Counts and bytes print as integers, everything else with two decimals.
    pub fn format(&self, v: f64) -> String {
        match self.unit {
            "count" | "bytes" | "rows" | "/s" => format!("{v:.0}"),
            _ => format!("{v:.2}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_is_better_uses_tolerance_and_slack() {
        let m = tracked("b1_p50_ms").unwrap(); // 100% + 2 ms
        assert_eq!(m.judge(1.0, 4.0), Status::Ok); // limit is 1*2+2 = 4
        assert_eq!(m.judge(1.0, 4.1), Status::Regression);
        // A 100% tolerance can never report "improved" (the bound would be below zero).
        assert_eq!(m.judge(10.0, 1.0), Status::Ok);
        let m = tracked("b3_full_ms").unwrap(); // 30% + 20 ms: improved below 100*0.7-20
        assert_eq!(m.judge(100.0, 49.0), Status::Improved);
        assert_eq!(m.judge(100.0, 51.0), Status::Ok);
    }

    #[test]
    fn higher_is_better_and_exact() {
        let m = tracked("b2_changes_per_s").unwrap(); // 20%
        assert_eq!(m.judge(1000.0, 800.0), Status::Ok);
        assert_eq!(m.judge(1000.0, 799.0), Status::Regression);
        assert_eq!(m.judge(1000.0, 1300.0), Status::Improved);
        let e = tracked("b3_delta_rows_read").unwrap();
        assert_eq!(e.judge(80.0, 80.0), Status::Ok);
        assert_eq!(e.judge(80.0, 79.0), Status::Regression);
    }
}
