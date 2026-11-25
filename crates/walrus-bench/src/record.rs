//! The machine-readable result of one run (`<date>-<commit>[-dirty].json`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{Value, json};

/// Recorded runs: the history (committed).
pub const RESULTS_DIR: &str = "bench/results";
/// The latest recorded run, which every run is compared against.
pub const BASELINE: &str = "bench/baseline.json";
/// Where every run writes first. Dev runs stay here (git-ignored); `record` copies to RESULTS_DIR.
pub const SCRATCH_DIR: &str = "target/bench";

pub struct Record {
    pub timestamp: u64,
    pub date: String,
    pub commit: String,
    pub dirty: bool,
    pub hardware: String,
    pub postgres: String,
    pub profile: String,
    pub scenarios: Vec<String>,
    pub metrics: BTreeMap<String, f64>,
}

impl Record {
    pub fn to_json(&self) -> String {
        let v = json!({
            "timestamp": self.timestamp,
            "date": self.date,
            "commit": self.commit,
            "dirty": self.dirty,
            "hardware": self.hardware,
            "postgres": self.postgres,
            "profile": self.profile,
            "scenarios": self.scenarios,
            "metrics": self.metrics,
        });
        let mut s = serde_json::to_string_pretty(&v).expect("a JSON value always serializes");
        s.push('\n');
        s
    }

    pub fn load(path: &Path) -> Result<Record> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let v: Value =
            serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        let text_field = |k: &str| v[k].as_str().unwrap_or_default().to_string();
        Ok(Record {
            timestamp: v["timestamp"].as_u64().unwrap_or(0),
            date: text_field("date"),
            commit: text_field("commit"),
            dirty: v["dirty"].as_bool().unwrap_or(false),
            hardware: text_field("hardware"),
            postgres: text_field("postgres"),
            profile: text_field("profile"),
            scenarios: v["scenarios"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            metrics: v["metrics"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_f64()?)))
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// `2026-11-25 df3dfbc`, with `+dirty` for a run from a modified tree.
    pub fn label(&self) -> String {
        let dirty = if self.dirty { "+dirty" } else { "" };
        format!("{} {}{dirty}", self.date, self.commit)
    }
}

/// Every `*.json` result in `dir`, oldest first, with its path.
pub fn load_all(dir: &str) -> Result<Vec<(PathBuf, Record)>> {
    let mut all = Vec::new();
    let Ok(dir) = std::fs::read_dir(dir) else {
        return Ok(all);
    };
    for entry in dir {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "json") {
            let record = Record::load(&path)?;
            all.push((path, record));
        }
    }
    all.sort_by(|a, b| (a.1.timestamp, &a.0).cmp(&(b.1.timestamp, &b.0)));
    Ok(all)
}

pub fn newest(dir: &str) -> Result<PathBuf> {
    load_all(dir)?
        .pop()
        .map(|(path, _)| path)
        .with_context(|| format!("no result JSON in {dir}; run `walrus-bench run` first"))
}
