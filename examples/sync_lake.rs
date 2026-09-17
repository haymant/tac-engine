//! One-shot deep backfill of 1d bars for every symbol in the lake, from
//! `--start` (default `2000-01-01`) through today.
//!
//! Uses the lake's normal lazy path (`Lake::ensure_bars`): Alpaca first, with a
//! Yahoo Finance gap-fill fallback for the pre-2016/pre-2020 history Alpaca does
//! not carry. Idempotent — re-running only fills uncovered gaps.
//!
//! ```sh
//! # load repo .env (APCA keys + TAC_LAKE_DIR) unless overridden
//! TAC_LAKE_DIR=/path/to/lake cargo run --release --example sync_lake -- --start 2000-01-01
//! # subset of symbols
//! cargo run --release --example sync_lake -- --symbols AAPL,MSFT,QQQ
//! ```

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use serde_json::Value;
use tracing_subscriber::EnvFilter;

use tac_engine::alpaca::AlpacaClient;
use tac_engine::config::Config;
use tac_engine::lake::{Lake, norm_ts};

fn parse_args() -> (String, Option<Vec<String>>) {
    let mut start = "2000-01-01".to_string();
    let mut symbols: Option<Vec<String>> = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--start" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    start = v.clone();
                }
            }
            "--symbols" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    symbols = Some(
                        v.split(',')
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_uppercase)
                            .collect(),
                    );
                }
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    (start, symbols)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::from_path(Path::new(env!("CARGO_MANIFEST_DIR")).join(".env")).ok();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let (start, symbols) = parse_args();
    let start = norm_ts(&start)?;
    let end = norm_ts(&chrono::Utc::now().to_rfc3339())?;

    let cfg = Config::from_env().context("failed to load Alpaca configuration")?;
    let client = AlpacaClient::new(cfg).map_err(|e| anyhow::anyhow!(e.message.to_string()))?;
    let lake = Lake::from_env()?;

    let syms = match symbols {
        Some(s) => s,
        None => lake
            .read_symbols()?
            .iter()
            .filter_map(|r| r.get("symbol").and_then(Value::as_str).map(String::from))
            .collect(),
    };
    if syms.is_empty() {
        anyhow::bail!("no symbols to sync (lake empty?)");
    }

    println!("lake={} timeframe=1d start={start} end={end} symbols={}", lake_dir(&lake), syms.len());
    for sym in &syms {
        let t0 = std::time::Instant::now();
        match lake
            .ensure_bars(&client, "US", sym, "1d", &start, &end, "iex", "raw", true)
            .await
        {
            Ok(res) => {
                let cov = lake
                    .get_coverage("US", "1d", sym)
                    .map(|c| {
                        format!(
                            "first={} last={} n={} feed={}",
                            c.first_t.as_deref().unwrap_or("-"),
                            c.last_t.as_deref().unwrap_or("-"),
                            c.num_bars,
                            c.feed
                        )
                    })
                    .unwrap_or_default();
                println!(
                    "{sym}: source={:<8} window_bars={:<5} elapsed={:.1}s | {cov}",
                    res.source,
                    res.bars.len(),
                    t0.elapsed().as_secs_f64()
                );
            }
            Err(e) => {
                eprintln!("{sym}: ERROR {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    Ok(())
}

fn lake_dir(lake: &Lake) -> String {
    lake.status()
        .ok()
        .and_then(|v| v.get("dir").and_then(Value::as_str).map(String::from))
        .unwrap_or_else(|| "?".to_string())
}
