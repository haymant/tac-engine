//! Recompute and persist 1d TA features for every lake symbol across the full
//! available history (default `--start 2000-01-01` through today).
//!
//! Idempotent: `compute_ta` pulls the lookback warm-up, recomputes the
//! indicator set, and merges rows into the existing features parquet by `t`.
//!
//! ```sh
//! # TAC_LAKE_DIR is required (absolute path): local e.g. /home/data/lake
//! TAC_LAKE_DIR=/home/data/lake cargo run --release --example backfill_features
//! # subset of symbols
//! cargo run --release --example backfill_features -- --symbols AAPL,MSFT
//! ```

use std::path::Path;

use anyhow::Context;
use serde_json::Value;
use tracing_subscriber::EnvFilter;

use tac_engine::config::Config;
use tac_engine::lake::{Lake, norm_ts};

const INDICATORS: &str = "adx-14,aroon-14,atr-14,ema-20,macd,max-14,rsi-14,sma-5,sma-20,stoch,wma-20";

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

    let _cfg = Config::from_env().context("failed to load Alpaca configuration")?;
    let lake = Lake::from_env()?;

    let indicators: Vec<String> = INDICATORS
        .split(',')
        .map(String::from)
        .collect();

    let syms = match symbols {
        Some(s) => s,
        None => lake
            .read_symbols()?
            .iter()
            .filter_map(|r| r.get("symbol").and_then(Value::as_str).map(String::from))
            .collect(),
    };
    if syms.is_empty() {
        anyhow::bail!("no symbols to backfill (lake empty?)");
    }

    println!("lake={} timeframe=1d start={start} end={end} symbols={}", lake_dir(&lake), syms.len());
    for sym in &syms {
        let t0 = std::time::Instant::now();
        match lake.compute_ta("US", sym, "1d", &start, &end, &indicators, true) {
            Ok(rows) => {
                let first = rows.first().and_then(|r| r.get("t").and_then(Value::as_str));
                let last = rows.last().and_then(|r| r.get("t").and_then(Value::as_str));
                println!(
                    "{sym}: rows={:<5} elapsed={:.1}s | first={} last={}",
                    rows.len(),
                    t0.elapsed().as_secs_f64(),
                    first.unwrap_or("-"),
                    last.unwrap_or("-")
                );
            }
            Err(e) => {
                eprintln!("{sym}: ERROR {e}");
            }
        }
    }
    Ok(())
}

fn lake_dir(lake: &Lake) -> String {
    lake.status()
        .ok()
        .and_then(|v| v.get("dir").and_then(Value::as_str).map(String::from))
        .unwrap_or_else(|| "?".to_string())
}
