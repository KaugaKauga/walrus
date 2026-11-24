use anyhow::Result;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = walrus_engine::config::Config::from_env()?;
    let listener = TcpListener::bind(config.listen_addr).await?;
    let engine = walrus_engine::start(config, listener).await?;
    tracing::info!(addr = %engine.addr, "walrus listening");
    engine.run_until_stopped().await
}
