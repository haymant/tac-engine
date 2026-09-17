use std::path::Path;

use anyhow::Context;
use rmcp::{ServiceExt, transport::stdio};
use tracing_subscriber::EnvFilter;

use tac_engine::alpaca::AlpacaClient;
use tac_engine::config::Config;
use tac_engine::lake::Lake;
use tac_engine::server::TradeacServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Auto-load `.env` from the repo root (this crate's manifest dir). This is a
    // local-dev convenience for source builds; for a shipped standalone binary
    // set the variables in the process environment instead (env!("CARGO_MANIFEST_DIR")
    // is baked at compile time). Existing process env always wins — dotenvy does
    // not override already-set variables.
    dotenvy::from_path(Path::new(env!("CARGO_MANIFEST_DIR")).join(".env")).ok();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().context("failed to load Alpaca configuration")?;
    tracing::info!(
        trading = %cfg.base_url,
        data = %cfg.data_base_url,
        stream = %cfg.stream_base_url,
        "starting tac-engine MCP server"
    );

    let client = AlpacaClient::new(cfg).map_err(|err| anyhow::anyhow!(err.message.to_string()))?;
    let lake = Lake::from_env()?;
    let server = TradeacServer::new(client, lake);
    let service = server
        .serve(stdio())
        .await
        .context("failed to start MCP stdio transport")?;
    service
        .waiting()
        .await
        .context("MCP server terminated with error")?;
    Ok(())
}
