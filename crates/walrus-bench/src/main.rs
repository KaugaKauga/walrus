//! `walrus-bench`: the MVP performance baseline (PLAN.md §10.2) and its regression guardrail.
//!
//! ```text
//! walrus-bench run [b1|b2|b3|all]     run scenarios, write bench/results/<date>-<commit>.{md,json}
//! walrus-bench compare [result.json]  compare with bench/baseline.json (exit 1 on a regression)
//! walrus-bench accept [result.json]   make a result the new baseline
//! ```
//!
//! A bare `b1|b2|b3|all` still means `run`. Without a result path, the newest one is used.

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
        Some("run") => run(arg(1).unwrap_or("all")).await?,
        Some("compare") => {
            let path = result_path(arg(1))?;
            if history::compare(&path)? {
                return Ok(ExitCode::FAILURE);
            }
        }
        Some("accept") => history::accept(&result_path(arg(1))?)?,
        // The original interface: `walrus-bench [b1|b2|b3|all]`.
        other => run(other.unwrap_or("all")).await?,
    }
    Ok(ExitCode::SUCCESS)
}

fn result_path(given: Option<&str>) -> Result<PathBuf> {
    match given {
        Some(p) => Ok(p.into()),
        None => record::newest(),
    }
}

async fn run(which: &str) -> Result<()> {
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
    // Several runs of one commit on one day get -2, -3, ... so none overwrites another.
    std::fs::create_dir_all(record::RESULTS_DIR)?;
    let stem = format!(
        "{}/{}-{}{}{suffix}",
        record::RESULTS_DIR,
        hardware::date(),
        hardware::git_hash(),
        if dirty { "-dirty" } else { "" }
    );
    let mut base = stem.clone();
    let mut n = 1;
    while std::path::Path::new(&format!("{base}.json")).exists() {
        n += 1;
        base = format!("{stem}-{n}");
    }

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
    Ok(())
}

/// Stdout of a command, trimmed; `None` if it cannot run or fails.
fn command_output(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}
