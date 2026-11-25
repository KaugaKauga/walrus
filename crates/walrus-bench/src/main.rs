//! `walrus-bench`: the MVP performance baseline (PLAN.md §10.2).
//!
//! `cargo run --release -p walrus-bench -- [b1|b2|b3|all]`

mod b1;
mod b2;
mod b3;
mod env;
mod hardware;
mod sse;

use std::process::Command;

use anyhow::{Context, Result, bail};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "walrus_engine=warn".into()))
        .try_init();

    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let scenarios: &[&str] = match which.as_str() {
        "b1" => &["b1"],
        "b2" => &["b2"],
        "b3" => &["b3"],
        "all" => &["b1", "b2", "b3"],
        other => bail!("unknown scenario {other:?}; use b1, b2, b3 or all"),
    };

    let mut report = hardware::header().await;
    for name in scenarios {
        eprintln!("running {name} ...");
        let section = match *name {
            "b1" => b1::run().await,
            "b2" => b2::run().await,
            _ => b3::run().await,
        }
        .with_context(|| format!("scenario {name}"))?;
        println!("{section}");
        report.push_str(&section);
        report.push('\n');
    }

    let dir = "bench/results";
    std::fs::create_dir_all(dir)?;
    let suffix = if which == "all" {
        String::new()
    } else {
        format!("-{which}")
    };
    let path = format!(
        "{dir}/{}-{}{suffix}.md",
        hardware::date(),
        hardware::git_hash()
    );
    std::fs::write(&path, report)?;
    eprintln!("wrote {path}");
    Ok(())
}

/// Stdout of a command, trimmed; `None` if it cannot run or fails.
fn command_output(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}
