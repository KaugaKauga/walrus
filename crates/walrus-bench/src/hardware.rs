//! The report header: date, commit, hardware, Postgres version, build profile.

use crate::command_output;
use crate::env::{connect, pg_base};

pub fn date() -> String {
    command_output("date", &["+%Y-%m-%d"]).unwrap_or_else(|| "unknown-date".into())
}

pub fn git_hash() -> String {
    command_output("git", &["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "nogit".into())
}

fn machine() -> String {
    if let Some(out) = command_output(
        "sysctl",
        &["-n", "machdep.cpu.brand_string", "hw.ncpu", "hw.memsize"],
    ) {
        let lines: Vec<&str> = out.lines().collect();
        if let [cpu, ncpu, mem] = lines[..] {
            let gib = mem.parse::<u64>().unwrap_or(0) as f64 / (1u64 << 30) as f64;
            return format!("{cpu}, {ncpu} logical CPUs, {gib:.0} GiB RAM (macOS)");
        }
    }
    // Linux and others: best effort.
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let cpu = cpuinfo
        .lines()
        .find_map(|l| l.strip_prefix("model name"))
        .map_or("unknown CPU", |v| v.trim_start_matches([' ', '\t', ':']));
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let kib: f64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or(0.0);
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    format!(
        "{cpu}, {cpus} logical CPUs, {:.0} GiB RAM",
        kib / 1_048_576.0
    )
}

/// True when anything that can change the numbers differs from HEAD (modified or untracked):
/// the code, the dependency lock and the Postgres setup. Docs, scripts and `bench/` (where runs
/// write their own results) don't make a run dirty.
pub fn dirty() -> bool {
    command_output(
        "git",
        &[
            "status",
            "--porcelain",
            "--",
            "crates",
            "Cargo.toml",
            "Cargo.lock",
            "docker-compose.yml",
        ],
    )
    .is_some_and(|out| !out.is_empty())
}

pub struct Meta {
    pub hardware: String,
    pub postgres: String,
    pub profile: &'static str,
}

impl Meta {
    pub async fn collect() -> Meta {
        let postgres = async {
            let pg = connect(&format!("{}/postgres", pg_base())).await.ok()?;
            pg.query_one("SHOW server_version", &[])
                .await
                .ok()
                .map(|r| r.get::<_, String>(0))
        }
        .await
        .unwrap_or_else(|| "unknown".into());
        let profile = if cfg!(debug_assertions) {
            "debug (numbers are not meaningful)"
        } else {
            "release"
        };
        Meta {
            hardware: machine(),
            postgres,
            profile,
        }
    }

    pub fn header(&self) -> String {
        format!(
            "# walrus MVP performance baseline\n\n\
             - Date: {}\n\
             - Commit: {}{}\n\
             - Hardware: {}\n\
             - Postgres: {} (Docker, local; the engine, the load generator and the clients \
             all run on the same machine)\n\
             - Build profile: {}\n\n",
            date(),
            git_hash(),
            if dirty() {
                " (uncommitted changes)"
            } else {
                ""
            },
            self.hardware,
            self.postgres,
            self.profile
        )
    }
}
