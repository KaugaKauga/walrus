//! Configuration from environment variables (PLAN.md §8).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// Schema-qualified table names, e.g. `public.todos`.
    pub tables: Vec<String>,
    pub data_dir: PathBuf,
    pub listen_addr: SocketAddr,
    pub slot: String,
    pub publication: String,
    pub catchup_connections: usize,
    pub retention: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let config = Config {
            database_url: var("WALRUS_DATABASE_URL").context("WALRUS_DATABASE_URL is required")?,
            tables: var("WALRUS_TABLES")
                .context("WALRUS_TABLES is required")?
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
            data_dir: var("WALRUS_DATA_DIR")
                .unwrap_or_else(|| "./data".into())
                .into(),
            listen_addr: var("WALRUS_LISTEN_ADDR")
                .unwrap_or_else(|| "127.0.0.1:4000".into())
                .parse()
                .context("WALRUS_LISTEN_ADDR")?,
            slot: var("WALRUS_SLOT").unwrap_or_else(|| "walrus".into()),
            publication: var("WALRUS_PUBLICATION").unwrap_or_else(|| "walrus".into()),
            catchup_connections: var("WALRUS_CATCHUP_CONNECTIONS")
                .map(|v| v.parse())
                .transpose()
                .context("WALRUS_CATCHUP_CONNECTIONS")?
                .unwrap_or(4),
            retention: var("WALRUS_RETENTION")
                .map(|v| parse_duration(&v))
                .transpose()
                .context("WALRUS_RETENTION")?
                .unwrap_or(Duration::from_secs(7 * 86_400)),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.tables.is_empty() {
            bail!("WALRUS_TABLES lists no tables");
        }
        for t in &self.tables {
            let ok = t
                .split_once('.')
                .is_some_and(|(s, n)| is_ident(s) && is_ident(n));
            if !ok {
                bail!("table {t:?} must be schema-qualified, like public.todos");
            }
        }
        for (what, name) in [
            ("WALRUS_SLOT", &self.slot),
            ("WALRUS_PUBLICATION", &self.publication),
        ] {
            let ok = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !ok {
                bail!("{what} {name:?} may only contain a-z, 0-9 and _");
            }
        }
        if self.catchup_connections == 0 {
            bail!("WALRUS_CATCHUP_CONNECTIONS must be at least 1");
        }
        Ok(())
    }
}

fn is_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `7d`, `12h`, `30m`, `45s`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = num.parse().with_context(|| format!("bad duration {s:?}"))?;
    let secs = match unit {
        "d" => n * 86_400,
        "h" => n * 3_600,
        "m" => n * 60,
        "s" => n,
        _ => bail!("bad duration {s:?}: use a number followed by d, h, m or s"),
    };
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("d").is_err());
    }
}
