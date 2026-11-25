//! `walrus-bench`: the MVP performance baseline (PLAN.md §10.2) and its regression guardrail.
//!
//! ```text
//! walrus-bench run [b1|b2|b3|all]     run, write target/bench/<date>-<commit>.{md,json}
//! walrus-bench compare [result.json]  compare with bench/baseline.json (exit 1 on a regression);
//!                                     default: the newest run in target/bench
//! walrus-bench record [--force]       full run of the committed tree; unless it regressed (or
//!                                     --force), save it to bench/results, make it the
//!                                     baseline and regenerate bench/HISTORY.md
//! ```
//!
//! Dev runs (`run` + `compare`) never write under `bench/`; only `record` does. A bare
//! `b1|b2|b3|all` means `run`.

mod b1;
mod b2;
mod b3;
mod env;
mod guard;
mod hardware;
mod history;
mod record;
mod sse;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "walrus_engine=warn".into()))
        .try_init();
    match real_main().await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

async fn real_main() -> Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize| args.get(i).map(String::as_str);
    match arg(0) {
        Some("run") => {
            run(arg(1).unwrap_or("all")).await?;
        }
        Some("compare") => {
            let path = match arg(1) {
                Some(p) => PathBuf::from(p),
                None => record::newest(record::SCRATCH_DIR)?,
            };
            if history::compare(&path)? {
                return Ok(ExitCode::FAILURE);
            }
        }
        Some("record") => return record_run(arg(1) == Some("--force")).await,
        other => {
            run(other.unwrap_or("all")).await?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// The end-of-work run: measure the committed tree and, unless it regressed, add it to the
/// history as the new baseline.
async fn record_run(force: bool) -> Result<ExitCode> {
    // A recorded run must describe a commit, or the history can't say what it measured.
    if hardware::dirty() {
        bail!(
            "uncommitted changes in crates/, Cargo.* or docker-compose.yml: commit them first, \
             so the recorded run describes a commit (see `git status`)"
        );
    }
    let json = run("all").await?;
    if history::compare(&json)? && !force {
        println!(
            "\nNot recorded: a tracked metric regressed. Fix it, or if the trade-off is \
             intended, record anyway with `scripts/bench.sh --record --force`."
        );
        return Ok(ExitCode::FAILURE);
    }

    std::fs::create_dir_all(record::RESULTS_DIR)?;
    let name = json
        .file_stem()
        .and_then(|s| s.to_str())
        .context("result file name")?;
    let stem = unique_stem(record::RESULTS_DIR, name);
    std::fs::copy(json.with_extension("md"), format!("{stem}.md"))?;
    std::fs::copy(&json, format!("{stem}.json"))?;
    std::fs::copy(&json, record::BASELINE)?;
    history::write_history()?;
    println!(
        "\nRecorded {stem}.{{json,md}}; it is now the baseline ({}). Updated {}.\n\
         Commit them: git add bench && git commit -m \"bench: record {}\"",
        record::BASELINE,
        history::HISTORY,
        hardware::git_hash()
    );
    Ok(ExitCode::SUCCESS)
}

/// `dir/name`, or `dir/name-2`, `-3`, ... if that result already exists, so no run overwrites
/// another (several runs of one commit on one day).
fn unique_stem(dir: &str, name: &str) -> String {
    let stem = format!("{dir}/{name}");
    let mut candidate = stem.clone();
    let mut n = 1;
    while std::path::Path::new(&format!("{candidate}.json")).exists() {
        n += 1;
        candidate = format!("{stem}-{n}");
    }
    candidate
}

/// Run scenarios and write the report and result into `target/bench`; returns the JSON path.
async fn run(which: &str) -> Result<PathBuf> {
    let scenarios: &[&str] = match which {
        "b1" => &["b1"],
        "b2" => &["b2"],
        "b3" => &["b3"],
        "all" => &["b1", "b2", "b3"],
        other => bail!("unknown scenario {other:?}; use b1, b2, b3 or all"),
    };

    let meta = hardware::Meta::collect().await;
    let mut report = meta.header();
    let mut metrics = BTreeMap::new();
    for name in scenarios {
        eprintln!("running {name} ...");
        let outcome = match *name {
            "b1" => b1::run().await,
            "b2" => b2::run().await,
            _ => b3::run().await,
        }
        .with_context(|| format!("scenario {name}"))?;
        println!("{}", outcome.section);
        report.push_str(&outcome.section);
        report.push('\n');
        metrics.extend(outcome.metrics.into_iter().map(|(k, v)| (k.to_string(), v)));
    }

    let dirty = hardware::dirty();
    let suffix = if which == "all" {
        String::new()
    } else {
        format!("-{which}")
    };
    std::fs::create_dir_all(record::SCRATCH_DIR)?;
    let base = unique_stem(
        record::SCRATCH_DIR,
        &format!(
            "{}-{}{}{suffix}",
            hardware::date(),
            hardware::git_hash(),
            if dirty { "-dirty" } else { "" }
        ),
    );

    let rec = record::Record {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        date: hardware::date(),
        commit: hardware::git_hash(),
        dirty,
        hardware: meta.hardware,
        postgres: meta.postgres,
        profile: meta.profile.to_string(),
        scenarios: scenarios.iter().map(|s| s.to_string()).collect(),
        metrics,
    };
    std::fs::write(format!("{base}.md"), report)?;
    std::fs::write(format!("{base}.json"), rec.to_json())?;
    eprintln!("wrote {base}.md and {base}.json");
    Ok(PathBuf::from(format!("{base}.json")))
}

/// Stdout of a command, trimmed; `None` if it cannot run or fails.
fn command_output(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}
