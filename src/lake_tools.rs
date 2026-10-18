use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use chrono::{Duration, Utc};
use ndarray::Array2;
use rmcp::{
    ErrorData as McpError, handler::server::wrapper::Parameters, tool, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::alpaca::json_text;
use crate::corr::{dampen, ffill, kendall_tau_b, packed_kendall_upper, pct_returns, unpack_pairs};
use crate::fx::{DEFAULT_FX_PAIRS, FX_MARKET, FX_TIMEFRAME};
use crate::lake::{DEFAULT_ADJUSTMENT, DEFAULT_FEED, DEFAULT_MARKET, Lake, lake_timeframe, norm_ts};
use crate::lake_macro::{MACRO_MARKET, MACRO_TIMEFRAME};
use crate::lake_params::{
    BackfillCalendarParams, CorrBenchmarkParams, CorrCalcParams, CorrMatrixParams,
    CorrQueryParams, LakeBarsParams, LakeBulkLoadParams, LakeCalendarParams,
    LakeCoverageParams, LakeFeaturesParams, LakeFxParams, LakeFxVolParams, LakeLoadStatusParams,
    LakeRebuildParams, LakeSPParams, LakeStatusParams, LakeSymbolsParams, LakeTAParams,
    LakeValidateParams, LakeWatchlistParams, LoadLakeFxParams, MacroBarsParams,
    MacroBulkLoadParams, MacroFeaturesParams, MacroPutSymbolParams, MacroSymbolsParams,
    VolSurfaceParams, VolSurfaceStatusParams,
};
use crate::macro_sources::{CatalogRow, MacroFetcher};
use crate::server::TradeacServer;
use crate::vol;

// ---------------------------------------------------------------------------
// Background load job state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoadJob {
    job_id: String,
    status: String, // "running" | "completed" | "failed"
    market: String,
    timeframe: String,
    start: String,
    end: String,
    symbols: Vec<String>,
    processed: usize,
    results: Vec<Value>,
    error: Option<String>,
    started_at: String,
    completed_at: Option<String>,
}

fn load_jobs() -> &'static Mutex<HashMap<String, Arc<Mutex<LoadJob>>>> {
    static JOBS: OnceLock<Mutex<HashMap<String, Arc<Mutex<LoadJob>>>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn default_range(start: Option<String>, end: Option<String>) -> Result<(String, String), McpError> {
    let end = match end {
        Some(e) => norm_ts(&e)?,
        None => Utc::now().to_rfc3339(),
    };
    let start = match start {
        Some(s) => norm_ts(&s)?,
        None => {
            let dt = chrono::DateTime::parse_from_rfc3339(&end)
                .map_err(|e| McpError::invalid_params(format!("bad end timestamp: {e}"), None))?;
            (dt - Duration::days(30)).to_rfc3339()
        }
    };
    Ok((start, end))
}

fn csv_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(|x| x.to_uppercase())
        .collect()
}

/// Non-partition, non-timestamp columns of a feature row, in key order.
fn feature_columns(features: &[Value]) -> Vec<String> {
    features
        .first()
        .and_then(Value::as_object)
        .map(|m| {
            m.keys()
                .filter(|k| !matches!(k.as_str(), "market" | "symbol" | "timeframe" | "t"))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// FX + correlation helpers
// ---------------------------------------------------------------------------

/// FX pair list from a comma-separated string, defaulting to the standard
/// major/cross universe when empty.
fn fx_pairs(list: Option<&str>) -> Vec<String> {
    list.map(csv_symbols)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_FX_PAIRS.iter().map(|s| s.to_string()).collect())
}

/// Resolve a leg token to `(market, symbol)`: a 6-letter all-alpha token is an
/// FX pair (`market=FX`); anything else is a US-lake equity.
fn resolve_leg(leg: &str) -> (String, String) {
    let s = leg.trim().to_uppercase();
    if s.len() == 6 && s.bytes().all(|b| b.is_ascii_alphabetic()) {
        (FX_MARKET.to_string(), s)
    } else {
        (DEFAULT_MARKET.to_string(), s)
    }
}

/// Sorted union of the observation dates across all legs.
fn union_axis(legs: &[Vec<(String, f64)>]) -> Vec<String> {
    let mut set: Vec<String> = Vec::new();
    for leg in legs {
        for (d, c) in leg {
            if c.is_finite() {
                set.push(d.clone());
            }
        }
    }
    set.sort();
    set.dedup();
    set
}

/// One leg's closes aligned to `axis` (missing dates → `None`).
fn align_to_axis(closes: &[(String, f64)], axis: &[String]) -> Vec<Option<f64>> {
    let mut map: HashMap<&str, f64> = HashMap::with_capacity(closes.len());
    for (d, c) in closes {
        if c.is_finite() {
            map.insert(d.as_str(), *c);
        }
    }
    axis.iter().map(|d| map.get(d.as_str()).copied()).collect()
}

/// Percentage-return columns (rows = time) for legs already aligned on an
/// axis: forward-fill gaps first, then diff. Leading warm-up rows are kept so
/// the caller slices the trailing window.
fn returns_columns(axis_cols: &[Vec<Option<f64>>]) -> Vec<Vec<f64>> {
    axis_cols
        .iter()
        .map(|col| pct_returns(&ffill_iter(col)))
        .collect()
}

fn ffill_iter(col: &[Option<f64>]) -> Vec<f64> {
    ffill(col).into_iter().map(|c| c.unwrap_or(f64::NAN)).collect()
}

/// Materialize an `Array2` with **columns = legs**, rows = the trailing
/// `window_days` of each leg's return series.
fn returns_matrix(columns: &[Vec<f64>], window: usize) -> Result<Array2<f64>, McpError> {
    let ncols = columns.len();
    if ncols < 2 {
        return Err(McpError::invalid_params("need at least 2 legs", None));
    }
    let mut m = Array2::<f64>::zeros((window, ncols));
    for (j, col) in columns.iter().enumerate() {
        if col.len() < window {
            return Err(McpError::invalid_params(format!(
                "leg {j} has {} return rows < window {window}",
                col.len()
            ), None));
        }
        let off = col.len() - window;
        for (i, v) in col.iter().skip(off).enumerate() {
            m[[i, j]] = *v;
        }
    }
    Ok(m)
}

/// Leg closes for a resolved symbol over `[start, end]` from the lake.
fn leg_closes(lake: &Lake, market: &str, symbol: &str, start: &str, end: &str) -> Result<Vec<(String, f64)>, McpError> {
    let tf = if market == FX_MARKET { FX_TIMEFRAME } else { "1d" };
    lake.read_daily_closes(market, tf, symbol, start, end)
}

/// Deterministic SplitMix64 uniform in [0, 1).
fn splitmix(seed: u64, i: u64) -> f64 {
    let mut s = seed ^ i.wrapping_mul(0x9E3779B97F4A7C15);
    s ^= s >> 30;
    s = s.wrapping_mul(0xBF58476D1CE4E5B9);
    s ^= s >> 27;
    s = s.wrapping_mul(0x94D049BB133111EB);
    s ^= s >> 31;
    (s >> 11) as f64 / (1u64 << 53) as f64
}

/// Approximate standard normal (Irwin–Hall n=12 minus 6); `i` seeds the stream.
fn gauss(seed: u64, i: u64) -> f64 {
    let mut s = 0.0;
    for k in 0..12 {
        s += splitmix(seed, i * 12 + k as u64);
    }
    s - 6.0
}

/// Synthetic geometric-random-walk closes on `axis` for a seeded leg. Fallback
/// only when the lake holds no real stock names at all (see `corr_benchmark`).
fn synthetic_closes(axis: &[String], seed: u64, idx: u64) -> Vec<(String, f64)> {
    let mut close = 100.0;
    let mut sig = 0.01;
    // each synthetic leg gets a distinct volatility signature
    sig *= 0.5 + splitmix(seed ^ 0x51DE, idx) * 2.0;
    let mut out = Vec::with_capacity(axis.len());
    for (i, d) in axis.iter().enumerate() {
        close *= 1.0 + sig * gauss(seed, idx * 1_000_003 + i as u64);
        out.push((d.clone(), close));
    }
    out
}

/// Augmented variant of a real leg's aligned close column (``AAPL -> AAPL1``):
/// each day's close is scaled by `(1 + eps·noise)` with `eps` 1–5% drawn from
/// the (seeded, deterministic) per-variant stream. The variant stays highly
/// correlated with its parent — a factor-model-style augmentation, not new
/// market data. `None` gaps are preserved for the shared forward-fill.
fn augment_col(base: &[Option<f64>], seed: u64, variant: u64) -> Vec<Option<f64>> {
    let eps = 0.01 + splitmix(seed ^ 0xA1B2C3, variant) * 0.04;
    base.iter()
        .copied()
        .enumerate()
        .map(|(i, c)| match c {
            Some(x) => Some(x * (1.0 + eps * gauss(seed, variant * 1_000_007 + i as u64))),
            None => None,
        })
        .collect()
}

/// Excel serial (ACT/365 days since 1899-12-30) for a calendar date.
fn excel_serial_date(date: chrono::NaiveDate) -> i64 {
    let epoch = chrono::NaiveDate::from_ymd_opt(1899, 12, 30).expect("epoch date");
    (date - epoch).num_days()
}

/// Parse an OCC option symbol `ROOT YYMMDD C/P STRIKE(8 digits, *1000)`.
fn parse_occ(symbol: &str) -> Option<(i64, bool, f64)> {
    if symbol.len() < 16 {
        return None;
    }
    let tail = &symbol[symbol.len() - 15..];
    let year: i32 = 2000 + tail.get(0..2)?.parse::<i32>().ok()?;
    let month: u32 = tail.get(2..4)?.parse::<u32>().ok()?;
    let day: u32 = tail.get(4..6)?.parse::<u32>().ok()?;
    let is_call = match tail.get(6..7)? {
        "C" => true,
        "P" => false,
        _ => return None,
    };
    let strike = tail.get(7..15)?.parse::<i64>().ok()? as f64 / 1000.0;
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    Some((excel_serial_date(date), is_call, strike))
}

/// Build the fina-risk `vol_surfaces` block from a calibrated surface.
fn vol_surface_block(
    symbol: &str,
    reference: f64,
    eval_serial: i64,
    deal_expiry: i64,
    surface: &vol::VolSurface,
    grid_points: usize,
    n_obs: usize,
) -> Value {
    let points = grid_points.clamp(5, 41);
    let mut strikes: Vec<f64> = Vec::with_capacity(points);
    for index in 0..points {
        let moneyness = 0.6 + 0.8 * (index as f64) / ((points - 1) as f64);
        strikes.push((reference * moneyness * 10_000.0).round() / 10_000.0);
    }
    let maturities_t: Vec<f64> = surface.slices.iter().map(|slice| slice.t).collect();
    let maturities: Vec<i64> = maturities_t
        .iter()
        .map(|t| eval_serial + (t * 365.0).round() as i64)
        .collect();
    let grid = surface.vol_grid(&strikes, &maturities_t);
    let volatility: Vec<Vec<f64>> = grid
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| (value.clamp(0.03, 0.9) * 10_000.0).round() / 10_000.0)
                .collect()
        })
        .collect();
    let params: Vec<Value> = surface
        .slices
        .iter()
        .map(|slice| {
            json!({"t": slice.t, "a": slice.svi.a, "b": slice.svi.b, "rho": slice.svi.rho, "m": slice.svi.m, "sigma": slice.svi.sigma})
        })
        .collect();
    json!({
        "underlying": symbol,
        "strike_type": "absolute",
        "reference": reference,
        "maturities": maturities,
        "strikes": strikes,
        "volatility": volatility,
        "model": "svi",
        "params": params,
        "calibrated_at": Utc::now().to_rfc3339(),
        "calibrated_at_epoch": Utc::now().timestamp(),
        "deal_expiry": deal_expiry,
        "n_obs": n_obs,
        "source": "alpaca",
        "arbitrage": surface.arbitrage_violations(),
    })
}

#[tool_router(router = lake_tools, vis = "pub")]
impl TradeacServer {
    #[tool(
        description = "Cache-first stock bars from the local parquet lake. Reads hive-partitioned bar files (market/timeframe/symbol); when lazy=true (default), missing windows are fetched from Alpaca IEX feed, persisted, and coverage/symbols/calendar metadata updated. For daily bars, Yahoo Finance fills gaps before Alpaca's earliest available date; with no Alpaca credentials, Yahoo supplies daily US stock history directly. Intraday bars and non-US markets require Alpaca. Feed defaults to iex; SIP is never used (requires license). Returns bars keyed by symbol; with quiet=true returns a per-symbol summary (count/first_t/last_t/source) instead of the bar rows."
    )]
    async fn get_lake_bars(
        &self,
        Parameters(params): Parameters<LakeBarsParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let timeframe = lake_timeframe(&params.timeframe)?;
        let (start, end) = default_range(params.start, params.end)?;
        let feed = params.feed.unwrap_or_else(|| DEFAULT_FEED.to_string());
        let adjustment = params
            .adjustment
            .unwrap_or_else(|| DEFAULT_ADJUSTMENT.to_string());
        let lazy = params.lazy.unwrap_or(true);
        let quiet = params.quiet.unwrap_or(false);
        let symbols = csv_symbols(&params.symbols);
        if symbols.is_empty() {
            return Err(McpError::invalid_params("symbols is required", None));
        }

        let mut bars: Map<String, Value> = Map::new();
        let mut source: Map<String, Value> = Map::new();
        let mut summary: Map<String, Value> = Map::new();
        for sym in &symbols {
            let res = self
                .lake
                .ensure_bars(&self.client, &market, sym, &timeframe, &start, &end, &feed, &adjustment, lazy)
                .await?;
            source.insert(sym.clone(), json!(res.source));
            if quiet {
                summary.insert(
                    sym.clone(),
                    json!({
                        "count": res.bars.len(),
                        "first_t": res.bars.first().map(|b| b.t.clone()),
                        "last_t": res.bars.last().map(|b| b.t.clone()),
                    }),
                );
            } else {
                let arr: Vec<Value> = res
                    .bars
                    .iter()
                    .filter_map(|b| serde_json::to_value(b).ok())
                    .collect();
                bars.insert(sym.clone(), Value::Array(arr));
            }
        }

        if quiet {
            Ok(json_text(json!({
                "request": { "market": market, "timeframe": timeframe, "symbols": symbols, "start": start, "end": end, "feed": feed, "adjustment": adjustment, "lazy": lazy, "quiet": true },
                "source": source,
                "summary": summary,
            })))
        } else {
            Ok(json_text(json!({
                "request": { "market": market, "timeframe": timeframe, "symbols": symbols, "start": start, "end": end, "feed": feed, "adjustment": adjustment, "lazy": lazy },
                "source": source,
                "bars": bars,
            })))
        }
    }

    #[tool(
        description = "Compute technical-analysis indicators (kand/ta-lib style) from lake bars and optionally persist them to the hive-partitioned features/ directory (features/market=*/timeframe=*/family=ta/symbol=*.parquet). Accepts comma-separated indicator specs like `sma-5,sma-20,ema-12,rsi-14,macd,bbands,atr-14,obv` (`name-period`, `name_period` or `name5`; bare names use defaults; `bb` aliases `bbands`). Supports the full kand indicator set grouped by ta-lib category: Overlap (sma, ema, dema, tema, trima, wma, t3, rma, bbands, midpoint, medprice, typprice, wclprice, vwap, sar, supertrend, vegas, ecl, ha), Momentum (adx, adxr, aroon, aroonosc, bop, cci, dx, macd, mfi, mom, plus_di, plus_dm, minus_di, minus_dm, roc, rocp, rocr, rocr100, rsi, stoch, trix, willr, cdl_* patterns), Statistics (max, min, stddev, sum, var), Volatility (atr, natr, trange, adr), Volume (ad, adosc, obv). Lookback is pulled automatically; returned rows cover [start, end]; with quiet=true returns a summary (count + column names) instead of the feature rows."
    )]
    async fn get_lake_ta(
        &self,
        Parameters(params): Parameters<LakeTAParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let timeframe = lake_timeframe(&params.timeframe)?;
        let (start, end) = default_range(params.start, params.end)?;
        let indicators: Vec<String> = params
            .indicators
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(String::from)
            .collect();
        let persist = params.persist.unwrap_or(true);
        let quiet = params.quiet.unwrap_or(false);
        let symbol = params.symbol.to_uppercase();
        let features = self
            .lake
            .compute_ta(&market, &symbol, &timeframe, &start, &end, &indicators, persist)?;
        if quiet {
            Ok(json_text(json!({
                "market": market,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "count": features.len(),
                "columns": feature_columns(&features),
                "persisted": persist,
                "quiet": true,
            })))
        } else {
            Ok(json_text(json!({
                "market": market,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "count": features.len(),
                "features": features,
            })))
        }
    }

    #[tool(
        description = "Compute stochastic-process features (sp_* columns) from lake bars and optionally persist them into the hive-partitioned features/ directory (features/market=*/timeframe=*/family=sp/symbol=*.parquet). Rust port of sp_features.py on the stochastic-rs stack: `hmm` (2-state Gaussian HMM regime posterior, stochastic-rs econometrics::hmm), `hurst` (rescaled-range R/S exponent, stochastic-rs hurst::rs), `jump` (bipower-variation jump share + tail zscore + signed max up/down move, stochastic-rs realized), `moments` (realized skewness/kurtosis/downside semi-variance at 1/5/22d, stochastic-rs realized), `ou` (OU/AR(1) half-life + distance zscore), `har` (HAR-RV at 1/5/22d + ratios + RV lag-1 autocorr + RV coefficient of variation), `trend` (log-price momentum slopes 5/20/60 + log level), `signature` (path-signature lead/lag integrals, levels 1/2 at lag 1 and 5), `momentum` (multi-horizon total returns 22/63/126/252d + 22d Sharpe-like ratio), `garch` (GARCH(1,1) MLE: conditional variance, standardized residual, persistence alpha+beta). Accepts comma-separated families: `ou,hmm,jump,har,trend,hurst,signature,moments,momentum,garch` (default: all). The HMM is fit only on returns up to `fit_end` (no lookahead) but posteriors cover the whole window. Lookback is pulled automatically; returned rows cover [start, end]; with quiet=true returns a summary (count + sp_* column names) instead of the feature rows."
    )]
    async fn get_lake_sp(
        &self,
        Parameters(params): Parameters<LakeSPParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let timeframe = lake_timeframe(&params.timeframe)?;
        let (start, end) = default_range(params.start, params.end)?;
        let families: Vec<String> = params
            .families
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(String::from)
            .collect();
        let fit_end = params.fit_end.map(|fe| norm_ts(&fe)).transpose()?;
        let persist = params.persist.unwrap_or(true);
        let quiet = params.quiet.unwrap_or(false);
        let symbol = params.symbol.to_uppercase();
        let features = self
            .lake
            .compute_sp(&market, &symbol, &timeframe, &start, &end, &families, fit_end.as_deref(), persist)?;
        let sp_cols: Vec<&str> = features
            .first()
            .and_then(Value::as_object)
            .map(|m| m.keys().map(String::as_str).filter(|k| k.starts_with("sp_")).collect())
            .unwrap_or_default();
        if quiet {
            Ok(json_text(json!({
                "market": market,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "fit_end": fit_end,
                "count": features.len(),
                "sp_columns": sp_cols,
                "persisted": persist,
                "quiet": true,
            })))
        } else {
            Ok(json_text(json!({
                "market": market,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "fit_end": fit_end,
                "count": features.len(),
                "sp_columns": sp_cols,
                "features": features,
            })))
        }
    }

    #[tool(
        description = "Read persisted TA features (hive-partitioned features/market=*/timeframe=*/family=ta|sp/symbol=*.parquet) with optional market/symbol/timeframe/range filters. Rows from both families (ta and sp) are merged by timestamp. With quiet=true returns a summary (row count + available feature columns) instead of the feature rows."
    )]
    async fn get_lake_features(
        &self,
        Parameters(params): Parameters<LakeFeaturesParams>,
    ) -> Result<String, McpError> {
        let market = params.market.as_deref();
        let symbol = params.symbol.as_deref().map(str::to_uppercase);
        let timeframe = params.timeframe.as_deref();
        let start = params.start.as_deref();
        let end = params.end.as_deref();
        let quiet = params.quiet.unwrap_or(false);
        let rows = self.lake.read_features(market, symbol.as_deref(), timeframe, start, end)?;
        if quiet {
            Ok(json_text(json!({
                "count": rows.len(),
                "columns": feature_columns(&rows),
                "quiet": true,
            })))
        } else {
            Ok(json_text(json!({
                "count": rows.len(),
                "features": rows,
            })))
        }
    }

    #[tool(description = "Read the lake asset master from symbols.parquet. Optional symbol filter.")]
    async fn get_lake_symbols(
        &self,
        Parameters(params): Parameters<LakeSymbolsParams>,
    ) -> Result<String, McpError> {
        let rows = self.lake.read_symbols()?;
        let rows: Vec<Value> = match params.symbol {
            Some(sym) => rows
                .into_iter()
                .filter(|r| {
                    r.get("symbol")
                        .and_then(Value::as_str)
                        .map(|s| s.eq_ignore_ascii_case(&sym))
                        .unwrap_or(false)
                })
                .collect(),
            None => rows,
        };
        Ok(json_text(json!({ "count": rows.len(), "symbols": rows })))
    }

    #[tool(description = "Read the lake watchlists from watchlist.parquet.")]
    async fn get_lake_watchlist(
        &self,
        Parameters(_params): Parameters<LakeWatchlistParams>,
    ) -> Result<String, McpError> {
        let rows = self.lake.read_watchlist()?;
        Ok(json_text(json!({ "count": rows.len(), "watchlist": rows })))
    }

    #[tool(description = "Read the lake trading calendar from calendar.parquet with optional market/date filters.")]
    async fn get_lake_calendar(
        &self,
        Parameters(params): Parameters<LakeCalendarParams>,
    ) -> Result<String, McpError> {
        let rows = self
            .lake
            .read_calendar(params.market.as_deref(), params.start.as_deref(), params.end.as_deref())?;
        Ok(json_text(json!({ "count": rows.len(), "calendar": rows })))
    }

    #[tool(description = "Read the lake coverage index (per market/timeframe/symbol loaded windows) from coverage.parquet.")]
    async fn get_lake_coverage(
        &self,
        Parameters(params): Parameters<LakeCoverageParams>,
    ) -> Result<String, McpError> {
        let rows = self.lake.read_coverage(
            params.market.as_deref(),
            params.timeframe.as_deref(),
            params.symbol.as_deref(),
        )?;
        Ok(json_text(json!({ "count": rows.len(), "coverage": rows })))
    }

    #[tool(description = "Lake status: root dir, manifest, bar partition inventory and metadata files.")]
    async fn get_lake_status(
        &self,
        Parameters(_params): Parameters<LakeStatusParams>,
    ) -> Result<String, McpError> {
        Ok(json_text(self.lake.status()?))
    }

    #[tool(
        description = "Gap-fill: seed the lake trading calendar (calendar.parquet) from Alpaca historical auctions (feed=sip only; auctions exist only on trading days). Use before lazy bar loads so the 1d completeness check knows the expected trading-day set."
    )]
    async fn backfill_lake_calendar(
        &self,
        Parameters(params): Parameters<BackfillCalendarParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let end = match params.end {
            Some(e) => norm_ts(&e)?,
            None => Utc::now().to_rfc3339(),
        };
        let start = match params.start {
            Some(s) => norm_ts(&s)?,
            None => {
                let dt = chrono::DateTime::parse_from_rfc3339(&end)
                    .map_err(|e| McpError::invalid_params(format!("bad end timestamp: {e}"), None))?;
                (dt - Duration::days(30)).to_rfc3339()
            }
        };
        let symbols = csv_symbols(&params.symbols);
        if symbols.is_empty() {
            return Err(McpError::invalid_params("symbols is required", None));
        }
        let added = self
            .lake
            .backfill_calendar_from_auctions(&self.client, &market, &symbols, &start, &end)
            .await?;
        Ok(json_text(json!({
            "market": market,
            "symbols": symbols,
            "start": start,
            "end": end,
            "calendar_days_added": added,
        })))
    }

    #[tool(
        description = "Delete bar + feature parquet files and re-fetch bars from TAC_LAKE_START_DATE (default 2000-01-03) to now for a single symbol. Resets coverage so the next lazy get_lake_bars call will re-download the full history."
    )]
    async fn rebuild_lake_symbol(
        &self,
        Parameters(params): Parameters<LakeRebuildParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let symbol = params.symbol.to_uppercase();
        let timeframe = lake_timeframe(&params.timeframe)?;
        let feed = params.feed.unwrap_or_else(|| DEFAULT_FEED.to_string());
        let adjustment = params
            .adjustment
            .unwrap_or_else(|| DEFAULT_ADJUSTMENT.to_string());
        let res = self
            .lake
            .rebuild_symbol(&self.client, &market, &timeframe, &symbol, &feed, &adjustment)
            .await?;
        Ok(json_text(json!({
            "market": market,
            "symbol": symbol,
            "timeframe": timeframe,
            "bars_fetched": res.bars.len(),
            "source": res.source,
            "first_t": res.bars.first().map(|b| b.t.clone()),
            "last_t": res.bars.last().map(|b| b.t.clone()),
        })))
    }

    #[tool(
        description = "Bulk-load historical OHLCV bars + compute + persist TA and SP features for a comma-separated list of symbols. Runs in a background thread and returns immediately with a `job_id`. Default start is TAC_LAKE_START_DATE (2000-01-03) if not specified. For each symbol: fetches bars (IEX + Yahoo Finance fallback for daily), persists to the lake, computes TA indicators (family=ta), and computes stochastic-process features (family=sp). Poll `load_lake_status` with the returned `job_id` to track progress."
    )]
    async fn load_lake_symbols(
        &self,
        Parameters(params): Parameters<LakeBulkLoadParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let timeframe = lake_timeframe(&params.timeframe)?;
        let feed = params.feed.unwrap_or_else(|| DEFAULT_FEED.to_string());
        let adjustment = params
            .adjustment
            .unwrap_or_else(|| DEFAULT_ADJUSTMENT.to_string());
        let symbols: Vec<String> = csv_symbols(&params.symbols);
        if symbols.is_empty() {
            return Err(McpError::invalid_params("symbols is required", None));
        }
        let end = match params.end {
            Some(e) => norm_ts(&e)?,
            None => Utc::now().to_rfc3339(),
        };
        let lake_start = Lake::lake_start_date();
        let indicators: Vec<String> = params
            .indicators
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let families: Vec<String> = params
            .families
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let job_id = format!("load-{}", Utc::now().format("%Y%m%d-%H%M%S"));
        let job = Arc::new(Mutex::new(LoadJob {
            job_id: job_id.clone(),
            status: "running".into(),
            market: market.clone(),
            timeframe: timeframe.clone(),
            start: lake_start.clone(),
            end: end.clone(),
            symbols: symbols.clone(),
            processed: 0,
            results: Vec::new(),
            error: None,
            started_at: Utc::now().to_rfc3339(),
            completed_at: None,
        }));
        load_jobs().lock().unwrap().insert(job_id.clone(), job.clone());

        let lake = self.lake.clone();
        let client = self.client.clone();
        let resp_symbols = symbols.clone();
        let resp_market = market.clone();
        let resp_timeframe = timeframe.clone();
        let resp_lake_start = lake_start.clone();
        let resp_end = end.clone();
        tokio::spawn(async move {
            for sym in &symbols {
                let mut entry = json!({ "symbol": sym });

                // Determine per-symbol start based on lake coverage:
                // - No data or first_t > lake_start → fetch from lake_start (fill gap or full load)
                // - first_t <= lake_start → fetch from last_t (incremental append only)
                let sym_start = match lake.get_coverage(&market, &timeframe, sym) {
                    Ok(cov) => {
                        if let (Some(first), Some(_)) = (&cov.first_t, &cov.last_t) {
                            if first.as_str() > lake_start.as_str() {
                                lake_start.clone()
                            } else {
                                // Already have data from before lake_start — just append from latest
                                cov.last_t.clone().unwrap_or_else(|| lake_start.clone())
                            }
                        } else {
                            lake_start.clone()
                        }
                    }
                    Err(_) => lake_start.clone(),
                };
                entry["fetch_start"] = json!(sym_start);

                // 1. Fetch + persist bars
                match lake
                    .ensure_bars(&client, &market, sym, &timeframe, &sym_start, &end, &feed, &adjustment, true)
                    .await
                {
                    Ok(res) => {
                        entry["bars_count"] = json!(res.bars.len());
                        entry["bars_source"] = json!(res.source);
                        entry["bars_first_t"] = json!(res.bars.first().map(|b| b.t.clone()));
                        entry["bars_last_t"] = json!(res.bars.last().map(|b| b.t.clone()));
                    }
                    Err(e) => {
                        entry["bars_error"] = json!(e.message);
                    }
                }

                // 2. Compute TA features (full range, persisted to family=ta)
                match lake.compute_ta(&market, sym, &timeframe, &lake_start, &end, &indicators, true) {
                    Ok(ta) => {
                        let cols = feature_columns(&ta);
                        entry["ta_count"] = json!(ta.len());
                        entry["ta_columns"] = json!(cols);
                    }
                    Err(e) => {
                        entry["ta_error"] = json!(e.message);
                    }
                }

                // 3. Compute SP features (full range, persisted to family=sp)
                match lake.compute_sp(&market, sym, &timeframe, &lake_start, &end, &families, None, true)
                {
                    Ok(sp) => {
                        let cols: Vec<String> = sp
                            .first()
                            .and_then(Value::as_object)
                            .map(|m| {
                                m.keys()
                                    .filter(|k| k.starts_with("sp_"))
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();
                        entry["sp_count"] = json!(sp.len());
                        entry["sp_columns"] = json!(cols);
                    }
                    Err(e) => {
                        entry["sp_error"] = json!(e.message);
                    }
                }

                // Update job state (brief lock, no await while held)
                {
                    let mut job = job.lock().unwrap();
                    job.results.push(entry);
                    job.processed += 1;
                }
            }
            // Mark completed
            {
                let mut job = job.lock().unwrap();
                job.status = "completed".into();
                job.completed_at = Some(Utc::now().to_rfc3339());
            }
        });

        Ok(json_text(json!({
            "job_id": job_id,
            "status": "started",
            "symbols": resp_symbols,
            "market": resp_market,
            "timeframe": resp_timeframe,
            "lake_start": resp_lake_start,
            "end": resp_end,
            "note": "load running in background -- poll load_lake_status with this job_id to track progress",
        })))
    }

    #[tool(
        description = "Query the status of a background bulk-load job started by `load_lake_symbols` or `load_macro_symbols`. Returns the job's progress (processed/total symbols), per-symbol results (bar counts, TA/SP column counts, errors), and completion status."
    )]
    async fn load_lake_status(
        &self,
        Parameters(params): Parameters<LakeLoadStatusParams>,
    ) -> Result<String, McpError> {
        let jobs = load_jobs().lock().unwrap();
        let job = jobs
            .get(&params.job_id)
            .ok_or_else(|| McpError::invalid_params(format!("unknown job_id: {}", params.job_id), None))?;
        let snap = job.lock().unwrap().clone();
        Ok(json_text(json!({
            "job_id": snap.job_id,
            "status": snap.status,
            "market": snap.market,
            "timeframe": snap.timeframe,
            "start": snap.start,
            "end": snap.end,
            "total_symbols": snap.symbols.len(),
            "processed": snap.processed,
            "results": snap.results,
            "error": snap.error,
            "started_at": snap.started_at,
            "completed_at": snap.completed_at,
        })))
    }

    #[tool(
        description = "Validate dataset quality BEFORE a qlib workflow run. Catches the failure modes qlib silently tolerates: (1) feature columns that are all-NaN over the window (DropAllNaN would silently drop them and the model trains on fewer features), (2) features stored where consumers cannot see them (family=ta|sp partition misalignment or flat-layout orphans), and (3) hollow coverage / stale date ranges (coverage claims a wide span but the bar file is empty, truncated, or sparse). Scans every symbol in coverage.parquet for the market/timeframe (or a comma-separated subset via `symbols`). Pass `start`/`end` to also check feature-vs-bar row alignment and per-column all-NaN status in that window. Returns a report with a verdict (OK/WARNINGS/ERRORS), summary, cross-symbol partition health, and per-symbol issues."
    )]
    async fn validate_lake_dataset(
        &self,
        Parameters(params): Parameters<LakeValidateParams>,
    ) -> Result<String, McpError> {
        let market = params
            .market
            .unwrap_or_else(|| DEFAULT_MARKET.to_string())
            .to_uppercase();
        let timeframe = match params.timeframe {
            Some(tf) => lake_timeframe(&tf)?,
            None => "1d".to_string(),
        };
        let report = self
            .lake
            .validate_lake_dataset(&market, &timeframe, params.symbols.as_deref(), params.start.as_deref(), params.end.as_deref())?;
        Ok(json_text(report))
    }

    // ---- macro (synthetic symbols, market=MACRO) ----------------------------

    #[tool(
        description = "Read the managed synthetic macro-symbol catalog from macro_catalog.parquet (see the tac-lake-macro skill). Each row: symbol (e.g. FRED_CPIAUCSL, CBOE_VIX), source (fred|cboe|oecd|worldbank), source_id, frequency, units, release_lag_days, enabled. Optional symbol filter."
    )]
    async fn get_macro_symbols(
        &self,
        Parameters(params): Parameters<MacroSymbolsParams>,
    ) -> Result<String, McpError> {
        let rows = self
            .lake
            .read_macro_catalog(params.symbol.as_deref(), false)?;
        Ok(json_text(json!({ "count": rows.len(), "symbols": rows })))
    }

    #[tool(
        description = "Insert or update one row in the synthetic macro-symbol catalog (see the tac-lake-macro skill). symbol is your MACRO-market name (e.g. WB_GDP_US); source picks the provider: fred (needs FRED_API_KEY), cboe, oecd, worldbank; source_id is provider-specific (FRED series id / CBOE CSV filename / OECD SDMX dataflow path / World Bank COUNTRY/INDICATOR). frequency D/W/M/Q/A is a hint -- inferred from data when omitted. After adding, load it with load_macro_symbols."
    )]
    async fn put_macro_symbol(
        &self,
        Parameters(params): Parameters<MacroPutSymbolParams>,
    ) -> Result<String, McpError> {
        // Validate the provider spelling before persisting.
        crate::macro_sources::macro_source_kind(&params.source)?;
        let row = CatalogRow {
            symbol: params.symbol.trim().to_uppercase().replace(' ', "_"),
            source: params.source.trim().to_ascii_lowercase(),
            source_id: params.source_id.trim().to_string(),
            title: params.title.unwrap_or_default(),
            frequency: params.frequency.map(|f| f.trim().to_ascii_uppercase()).unwrap_or_default(),
            units: params.units.unwrap_or_default(),
            seasonal_adj: false,
            release_lag_days: params.release_lag_days.unwrap_or(0),
            enabled: params.enabled.unwrap_or(true),
        };
        self.lake.ensure_macro_catalog()?;
        self.lake.upsert_macro_catalog_rows(vec![row.clone()])?;
        Ok(json_text(json!({ "upserted": row.symbol, "row": row.to_value() })))
    }

    #[tool(
        description = "Bulk-load synthetic macro symbols from free providers (FRED needs FRED_API_KEY env; CBOE/OECD/World Bank need no key) into market=MACRO/timeframe=1d as OHLCV-style bars (o=h=l=c=value, v=0), then compute and persist the freq-aware family=macro features (families: transform,regime,align). Loads every enabled catalog row by default; pass `symbols` to load a subset (must exist in the catalog — see get_macro_symbols). Incremental: symbols already covered from TAC_LAKE_START_DATE only fetch their tail. Runs in a background thread and returns immediately with a `job_id` — poll `load_lake_status` to track progress."
    )]
    async fn load_macro_symbols(
        &self,
        Parameters(params): Parameters<MacroBulkLoadParams>,
    ) -> Result<String, McpError> {
        self.lake.ensure_macro_catalog()?;
        let catalog = self.lake.read_macro_catalog(None, true)?;
        let requested: Option<Vec<String>> = params.symbols.as_deref().map(|s| {
            s.split(',')
                .map(|x| x.trim().to_uppercase())
                .filter(|x| !x.is_empty())
                .collect()
        });
        let mut rows: Vec<CatalogRow> = Vec::new();
        for v in &catalog {
            let Some(row) = CatalogRow::from_value(v) else { continue };
            match &requested {
                Some(want) if want.contains(&row.symbol) => rows.push(row),
                Some(_) => {}
                None => rows.push(row),
            }
        }
        if let Some(want) = &requested {
            let known: Vec<&str> = rows.iter().map(|r| r.symbol.as_str()).collect();
            let unknown: Vec<&String> =
                want.iter().filter(|s| !known.contains(&s.as_str())).collect();
            if !unknown.is_empty() {
                return Err(McpError::invalid_params(
                    format!(
                        "symbols not in macro_catalog (add them first or check spelling): {}",
                        unknown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                    ),
                    None,
                ));
            }
        }

        let end_ts = match params.end {
            Some(e) => norm_ts(&e)?,
            None => Utc::now().to_rfc3339(),
        };
        let lake_start = Lake::lake_start_date();
        let start_ts = params.start.as_deref().map(norm_ts).transpose()?.unwrap_or(lake_start.clone());
        let families: Vec<String> = params
            .families
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let job_id = format!("load-macro-{}", Utc::now().format("%Y%m%d-%H%M%S"));
        let job = Arc::new(Mutex::new(LoadJob {
            job_id: job_id.clone(),
            status: "running".into(),
            market: MACRO_MARKET.to_string(),
            timeframe: MACRO_TIMEFRAME.to_string(),
            start: start_ts.clone(),
            end: end_ts.clone(),
            symbols: rows.iter().map(|r| r.symbol.clone()).collect(),
            processed: 0,
            results: Vec::new(),
            error: None,
            started_at: Utc::now().to_rfc3339(),
            completed_at: None,
        }));
        load_jobs().lock().unwrap().insert(job_id.clone(), job.clone());

        let lake = self.lake.clone();
        let resp_symbols = rows.iter().map(|r| r.symbol.clone()).collect::<Vec<_>>();
        let spawn_start = start_ts.clone();
        let spawn_end = end_ts.clone();
        tokio::spawn(async move {
            // One shared HTTP client; FRED_API_KEY failures surface per-symbol.
            let fetcher = MacroFetcher::new();
            let Ok(fetcher) = fetcher else {
                let mut job = job.lock().unwrap();
                job.status = "failed".into();
                job.error = Some(fetcher.unwrap_err().message.to_string());
                return;
            };
            for row in &rows {
                let mut entry = json!({ "symbol": row.symbol });

                // Coverage-driven window: tail-only when the symbol is already
                // covered back to the lake start, full history otherwise.
                let sym_start = match lake.get_coverage(MACRO_MARKET, MACRO_TIMEFRAME, &row.symbol) {
                    Ok(cov) => match (&cov.first_t, &cov.last_t) {
                        (Some(first), Some(last)) => {
                            if first.as_str() > spawn_start.as_str() {
                                spawn_start.clone()
                            } else {
                                last.clone()
                            }
                        }
                        _ => spawn_start.clone(),
                    },
                    Err(_) => spawn_start.clone(),
                };
                entry["fetch_start"] = json!(sym_start);

                let sd = date_of(&sym_start);
                let ed = date_of(&spawn_end);
                match lake.load_macro_symbol(&fetcher, row, sd, ed, &families).await {
                    Ok(e) => entry = e,
                    Err(e) => entry["error"] = json!(e.message),
                }
                {
                    let mut job = job.lock().unwrap();
                    job.results.push(entry);
                    job.processed += 1;
                }
            }
            {
                let mut job = job.lock().unwrap();
                job.status = "completed".into();
                job.completed_at = Some(Utc::now().to_rfc3339());
            }
        });

        Ok(json_text(json!({
            "job_id": job_id,
            "status": "started",
            "symbols": resp_symbols,
            "market": MACRO_MARKET,
            "timeframe": MACRO_TIMEFRAME,
            "start": start_ts,
            "end": end_ts,
            "note": "load running in background -- poll load_lake_status with this job_id to track progress",
        })))
    }

    #[tool(
        description = "Read stored synthetic macro bars from market=MACRO/timeframe=1d (loaded beforehand via load_macro_symbols). Bars carry o=h=l=c=value and v=0 at native observation dates (daily/weekly/monthly/quarterly); forward-fill to daily on your side or use get_macro_features. Comma-separated symbols; quiet=true returns per-symbol summaries."
    )]
    async fn get_macro_bars(
        &self,
        Parameters(params): Parameters<MacroBarsParams>,
    ) -> Result<String, McpError> {
        let (start, end) = default_range(params.start, params.end)?;
        let quiet = params.quiet.unwrap_or(false);
        let symbols = csv_symbols(&params.symbols);
        if symbols.is_empty() {
            return Err(McpError::invalid_params("symbols is required", None));
        }
        let mut bars: Map<String, Value> = Map::new();
        let mut summary: Map<String, Value> = Map::new();
        for sym in &symbols {
            let rows = self.lake.read_bars_json(MACRO_MARKET, MACRO_TIMEFRAME, sym, &start, &end)?;
            if quiet {
                summary.insert(
                    sym.clone(),
                    json!({
                        "count": rows.len(),
                        "first_t": rows.first().and_then(|r| r.get("t").cloned()),
                        "last_t": rows.last().and_then(|r| r.get("t").cloned()),
                    }),
                );
            } else {
                bars.insert(sym.clone(), Value::Array(rows));
            }
        }
        if quiet {
            Ok(json_text(json!({
                "request": { "market": MACRO_MARKET, "timeframe": MACRO_TIMEFRAME, "symbols": symbols, "start": start, "end": end, "quiet": true },
                "summary": summary,
            })))
        } else {
            Ok(json_text(json!({
                "request": { "market": MACRO_MARKET, "timeframe": MACRO_TIMEFRAME, "symbols": symbols, "start": start, "end": end },
                "bars": bars,
            })))
        }
    }

    #[tool(
        description = "Read persisted macro features (features/market=MACRO/timeframe=1d/family=macro/symbol=*.parquet): macro_yoy / macro_mom / macro_zscore on native observation dates (freq-aware windows), macro_above_sma200 + macro_trend_slope_60 for daily series, and macro_value_ffill + macro_age_days projected onto US trading days with release-lag awareness. Optional symbol/start/end filters; quiet=true returns a summary."
    )]
    async fn get_macro_features(
        &self,
        Parameters(params): Parameters<MacroFeaturesParams>,
    ) -> Result<String, McpError> {
        let symbol = params.symbol.as_deref().map(str::to_uppercase);
        if params.recompute.unwrap_or(false) {
            let targets: Vec<String> = match &symbol {
                Some(s) => vec![s.clone()],
                None => self
                    .lake
                    .read_macro_catalog(None, true)?
                    .iter()
                    .filter_map(|v| v.get("symbol").and_then(Value::as_str).map(String::from))
                    .collect(),
            };
            let mut recomputed = serde_json::Map::new();
            for sym in targets {
                // compute_macro_features always warms up on the full bar
                // history; rows cover the whole range when no filter is given.
                let rows = self.lake.compute_macro_features(
                    &sym,
                    params.start.as_deref().unwrap_or("1900-01-01"),
                    params.end.as_deref().unwrap_or("2999-12-31"),
                    &[
                        "transform".to_string(),
                        "regime".to_string(),
                        "align".to_string(),
                    ],
                    true,
                )?;
                recomputed.insert(sym, json!(rows.len()));
            }
        }
        let rows = self
            .lake
            .read_macro_features(symbol.as_deref(), params.start.as_deref(), params.end.as_deref())?;
        if params.quiet.unwrap_or(false) {
            Ok(json_text(json!({
                "count": rows.len(),
                "columns": feature_columns(&rows),
                "quiet": true,
            })))
        } else {
            Ok(json_text(json!({
                "count": rows.len(),
                "features": rows,
            })))
        }
    }

    // ---- FX + correlation -------------------------------------------------

    #[tool(
        description = "Cache-first daily FX bars from the lake (market=FX/1d, symbols like `EURUSD`). Reads hive-partitioned bar files; when lazy=true (default) and the requested window is not fully covered, missing history is fetched from the free-source chain (Yahoo → Frankfurter/ECB) and persisted — Alpaca's forex API 403s on most plans. Returns bars keyed by pair; quiet=true returns a per-pair summary (count/first_t/last_t/source)."
    )]
    async fn get_lake_fx(
        &self,
        Parameters(params): Parameters<LakeFxParams>,
    ) -> Result<String, McpError> {
        let pairs = fx_pairs(Some(&params.pairs));
        let (start, end) = default_range(params.start, params.end)?;
        let lazy = params.lazy.unwrap_or(true);
        let quiet = params.quiet.unwrap_or(false);

        let mut bars: Map<String, Value> = Map::new();
        let mut source: Map<String, Value> = Map::new();
        let mut summary: Map<String, Value> = Map::new();
        for pair in &pairs {
            let res = self.lake.ensure_fx_bars(pair, &start, &end, lazy).await?;
            source.insert(pair.clone(), json!(res.source));
            if quiet {
                summary.insert(
                    pair.clone(),
                    json!({
                        "count": res.bars.len(),
                        "first_t": res.bars.first().map(|b| b.t.clone()),
                        "last_t": res.bars.last().map(|b| b.t.clone()),
                    }),
                );
            } else {
                let arr: Vec<Value> = res
                    .bars
                    .iter()
                    .filter_map(|b| serde_json::to_value(b).ok())
                    .collect();
                bars.insert(pair.clone(), Value::Array(arr));
            }
        }
        if quiet {
            Ok(json_text(json!({
                "request": { "market": FX_MARKET, "timeframe": FX_TIMEFRAME, "pairs": pairs, "start": start, "end": end, "lazy": lazy, "quiet": true },
                "source": source,
                "summary": summary,
            })))
        } else {
            Ok(json_text(json!({
                "request": { "market": FX_MARKET, "timeframe": FX_TIMEFRAME, "pairs": pairs, "start": start, "end": end, "lazy": lazy },
                "source": source,
                "bars": bars,
            })))
        }
    }

    #[tool(
        description = "Realized FX volatility from lake closes (market=FX/1d): rv20_ann / rv60_ann (annualized realized vol of daily log returns), plus the raw close and log return per day. Ensures lake coverage first (lazy fetch through the Yahoo → Frankfurter chain). With persist=true (default) the series are stored under family=fxvol; quiet=true returns a summary."
    )]
    async fn get_lake_fx_vol(
        &self,
        Parameters(params): Parameters<LakeFxVolParams>,
    ) -> Result<String, McpError> {
        let pairs = fx_pairs(Some(&params.pairs));
        let (start, end) = default_range(params.start, params.end)?;
        let persist = params.persist.unwrap_or(true);
        let quiet = params.quiet.unwrap_or(false);

        let mut rows: Vec<Value> = Vec::new();
        let mut summary: Map<String, Value> = Map::new();
        for pair in &pairs {
            let _ = self.lake.ensure_fx_bars(pair, &start, &end, true).await?;
            let r = self
                .lake
                .compute_fx_vol(FX_MARKET, pair, FX_TIMEFRAME, &start, &end, persist)?;
            if quiet {
                summary.insert(
                    pair.clone(),
                    json!({ "count": r.len(), "columns": feature_columns(&r) }),
                );
            } else {
                rows.extend(r);
            }
        }
        if quiet {
            Ok(json_text(json!({
                "by_pair": summary,
                "quiet": true,
            })))
        } else {
            Ok(json_text(json!({ "features": rows, "count": rows.len() })))
        }
    }

    #[tool(
        description = "Bulk-load daily FX bars (+ realized-vol features, family=fxvol) into the lake for the default or a custom pair list via the Yahoo → Frankfurter/ECB chain. Runs in a background thread; poll `load_lake_status` with the returned `job_id` to track progress."
    )]
    async fn load_lake_fx(
        &self,
        Parameters(params): Parameters<LoadLakeFxParams>,
    ) -> Result<String, McpError> {
        let pairs = fx_pairs(params.pairs.as_deref());
        let end = match params.end {
            Some(e) => norm_ts(&e)?,
            None => Utc::now().to_rfc3339(),
        };
        let start = match params.start {
            Some(s) => norm_ts(&s)?,
            None => Lake::lake_start_date(),
        };
        let job_id = format!("loadfx-{}", Utc::now().format("%Y%m%d-%H%M%S"));
        let job = Arc::new(Mutex::new(LoadJob {
            job_id: job_id.clone(),
            status: "running".into(),
            market: FX_MARKET.into(),
            timeframe: FX_TIMEFRAME.into(),
            start: start.clone(),
            end: end.clone(),
            symbols: pairs.clone(),
            processed: 0,
            results: Vec::new(),
            error: None,
            started_at: Utc::now().to_rfc3339(),
            completed_at: None,
        }));
        load_jobs().lock().unwrap().insert(job_id.clone(), job.clone());

        let lake = self.lake.clone();
        let resp_pairs = pairs.clone();
        let resp_start = start.clone();
        let resp_end = end.clone();
        tokio::spawn(async move {
            for pair in &pairs {
                let mut entry = json!({ "symbol": pair });
                match lake.ensure_fx_bars(pair, &start, &end, true).await {
                    Ok(res) => {
                        entry["bars_count"] = json!(res.bars.len());
                        entry["bars_source"] = json!(res.source);
                        entry["bars_last_t"] = json!(res.bars.last().map(|b| b.t.clone()));
                    }
                    Err(e) => {
                        entry["bars_error"] = json!(e.message);
                    }
                }
                match lake.compute_fx_vol(FX_MARKET, pair, FX_TIMEFRAME, &start, &end, true) {
                    Ok(v) => {
                        entry["vol_count"] = json!(v.len());
                        entry["vol_columns"] = json!(feature_columns(&v));
                    }
                    Err(e) => {
                        entry["vol_error"] = json!(e.message);
                    }
                }
                {
                    let mut job = job.lock().unwrap();
                    job.results.push(entry);
                    job.processed += 1;
                }
            }
            {
                let mut job = job.lock().unwrap();
                job.status = "completed".into();
                job.completed_at = Some(Utc::now().to_rfc3339());
            }
        });

        Ok(json_text(json!({
            "job_id": job_id,
            "status": "started",
            "pairs": resp_pairs,
            "market": FX_MARKET,
            "timeframe": FX_TIMEFRAME,
            "start": resp_start,
            "end": resp_end,
            "note": "load running in background -- poll load_lake_status with this job_id to track progress",
        })))
    }

    #[tool(
        description = "Calibrated implied-vol surface (SVI per expiry, interpolated in total variance) for an underlying, from the live option chain. Serves vol_surfaces/<SYM>.json when younger than max_age_secs (default 600), else recalibrates, persists and returns the fina-risk vol_surfaces block."
    )]
    async fn vol_surface(
        &self,
        Parameters(params): Parameters<VolSurfaceParams>,
    ) -> Result<String, McpError> {
        let symbol = params.underlying.trim().to_uppercase();
        if symbol.is_empty() {
            return Err(McpError::invalid_params("underlying is required", None));
        }
        let eval_serial = params
            .evaluation_date
            .unwrap_or_else(|| excel_serial_date(Utc::now().date_naive()));
        let mut reference = params.reference.unwrap_or(0.0);
        let max_age = params.max_age_secs.unwrap_or(600).max(0);
        let grid_points = params.grid_points.unwrap_or(21);
        let deal_expiry = params.deal_expiry.unwrap_or(0);
        let tenor_window = params.tenor_window_days.unwrap_or(200).max(30);
        let cache = self.lake.meta_path(&format!("vol_surfaces/{symbol}.json"));

        if params.refresh != Some(true) {
            if let Ok(text) = std::fs::read_to_string(&cache) {
                if let Ok(mut value) = serde_json::from_str::<Value>(&text) {
                    let age_ok = value
                        .get("calibrated_at_epoch")
                        .and_then(Value::as_i64)
                        .map(|epoch| Utc::now().timestamp() - epoch <= max_age)
                        .unwrap_or(false);
                    // Also require the request signature to match, so a stale
                    // surface (different deal tenor / reference) is recalibrated.
                    let signature_ok = value.get("deal_expiry").and_then(Value::as_i64).unwrap_or(0) == deal_expiry
                        && (reference <= 0.0
                            || value
                                .get("reference")
                                .and_then(Value::as_f64)
                                .map(|cached| (cached - reference).abs() < 1e-6)
                                .unwrap_or(false));
                    if age_ok && signature_ok {
                        if let Some(object) = value.as_object_mut() {
                            object.insert("cache".into(), json!("hit"));
                        }
                        return Ok(json_text(value));
                    }
                }
            }
        }

        let value = self
            .client
            .data_get(
                &format!("/v1beta1/options/snapshots/{symbol}"),
                &[
                    ("feed", "indicative".to_string()),
                    ("limit", "1000".to_string()),
                ],
            )
            .await?;

        // Collect filtered quotes per expiry, then drop local IV outliers (the
        // same median rule as the FinAP local fit) so SVI is not dragged up by a
        // sawtooth smile.
        let mut by_expiry: BTreeMap<i64, Vec<(f64, f64, bool, f64)>> = BTreeMap::new();
        if let Some(snapshots) = value.get("snapshots").and_then(Value::as_object) {
            for (occ, snapshot) in snapshots {
                let Some((expiry, is_call, strike)) = parse_occ(occ) else {
                    continue;
                };
                let t = (expiry - eval_serial) as f64 / 365.0;
                if !(t > 0.0 && t <= 3.0) {
                    continue;
                }
                if deal_expiry > 0 && (expiry - deal_expiry).abs() > tenor_window {
                    continue;
                }
                let Some(raw_iv) = snapshot.get("impliedVolatility").and_then(Value::as_f64) else {
                    continue;
                };
                let iv = if raw_iv > 3.0 { raw_iv / 100.0 } else { raw_iv };
                if !(0.05..=0.9).contains(&iv) {
                    continue;
                }
                let (Some(bp), Some(ap)) = (
                    snapshot.pointer("/latestQuote/bp").and_then(Value::as_f64),
                    snapshot.pointer("/latestQuote/ap").and_then(Value::as_f64),
                ) else {
                    continue;
                };
                if !(bp > 0.0 && ap > 0.0 && ap >= bp) {
                    continue;
                }
                let mid = 0.5 * (bp + ap);
                if mid <= 0.0 || (ap - bp) / mid > 0.5 {
                    continue;
                }
                let weight = snapshot
                    .pointer("/dailyBar/v")
                    .and_then(Value::as_f64)
                    .unwrap_or(1.0)
                    .max(1.0);
                by_expiry.entry(expiry).or_default().push((strike, iv, is_call, weight));
            }
        }
        let mut quotes: Vec<vol::Quote> = Vec::new();
        let mut strikes: Vec<f64> = Vec::new();
        for (expiry, mut points) in by_expiry {
            points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let count = points.len();
            if count < 3 {
                continue;
            }
            let t = (expiry - eval_serial) as f64 / 365.0;
            for (index, (strike, iv, is_call, weight)) in points.iter().enumerate() {
                let low = index.saturating_sub(3);
                let high = (index + 3).min(count - 1);
                let mut window: Vec<f64> = points[low..=high].iter().map(|point| point.1).collect();
                window.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let median = window[window.len() / 2];
                if (iv - median).abs() > (0.25 * median).max(0.08) {
                    continue;
                }
                strikes.push(*strike);
                quotes.push(vol::Quote {
                    t,
                    strike: *strike,
                    iv: *iv,
                    is_call: *is_call,
                    weight: *weight,
                });
            }
        }
        if quotes.len() < 6 {
            return Err(McpError::internal_error(
                format!("insufficient clean option quotes for {symbol}"),
                None,
            ));
        }
        if reference <= 0.0 {
            strikes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            reference = strikes[strikes.len() / 2];
        }
        let surface = vol::calibrate_quotes(reference, reference, &quotes);
        let block = vol_surface_block(
            &symbol,
            reference,
            eval_serial,
            deal_expiry,
            &surface,
            grid_points,
            quotes.len(),
        );
        if let Some(parent) = cache.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(text) = serde_json::to_string_pretty(&block) {
            let _ = std::fs::write(&cache, text);
        }
        let mut out = block;
        if let Some(object) = out.as_object_mut() {
            object.insert("cache".into(), json!("miss"));
        }
        Ok(json_text(out))
    }

    #[tool(description = "Vol-surface cache status for an underlying (age, SVI params, arbitrage).")]
    async fn vol_surface_status(
        &self,
        Parameters(params): Parameters<VolSurfaceStatusParams>,
    ) -> Result<String, McpError> {
        let cache = self
            .lake
            .meta_path(&format!("vol_surfaces/{}.json", params.underlying.trim().to_uppercase()));
        match std::fs::read_to_string(&cache) {
            Ok(text) => Ok(text),
            Err(_) => Ok(json_text(json!({"underlying": params.underlying, "cached": false}))),
        }
    }

    #[tool(
        description = "Kendall tau-b correlation (with dampening) between two legs over a trailing window of daily returns. Legs resolve by name: a 6-letter token is an FX pair (market=FX), anything else is a US-lake equity. Both legs are pulled from the lake (FX pairs are lazy-fetched first). Returns raw rho0 and the dampened rho = (rho0 + alpha)/(1 + alpha)."
    )]
    async fn corr_calc(
        &self,
        Parameters(params): Parameters<CorrCalcParams>,
    ) -> Result<String, McpError> {
        let window = params.window_days.unwrap_or(252) as usize;
        let alpha = params.alpha.unwrap_or(0.4);
        let end = Utc::now().to_rfc3339();
        let start = (Utc::now() - Duration::days((window * 2 + 90) as i64)).to_rfc3339();

        let (ma, sa) = resolve_leg(&params.leg_a);
        let (mb, sb) = resolve_leg(&params.leg_b);
        if sa == sb {
            return Err(McpError::invalid_params("legs must differ", None));
        }
        if ma == FX_MARKET {
            let _ = self.lake.ensure_fx_bars(&sa, &start, &end, true).await?;
        }
        if mb == FX_MARKET {
            let _ = self.lake.ensure_fx_bars(&sb, &start, &end, true).await?;
        }
        let a = leg_closes(&self.lake, &ma, &sa, &start, &end)?;
        let b = leg_closes(&self.lake, &mb, &sb, &start, &end)?;
        if a.len() < window + 2 || b.len() < window + 2 {
            return Ok(json_text(json!({
                "error": "insufficient lake history for the requested window",
                "leg_a": sa, "leg_b": sb,
                "rows_a": a.len(), "rows_b": b.len(), "window_days": window,
            })));
        }
        let axis = union_axis(&[a.clone(), b.clone()]);
        let cols = returns_columns(&[align_to_axis(&a, &axis), align_to_axis(&b, &axis)]);
        let (xa, xb) = (trailing(&cols[0], window), trailing(&cols[1], window));
        let rho0 = kendall_tau_b(&xa, &xb);
        Ok(json_text(json!({
            "leg_a": sa, "leg_b": sb,
            "window_days": window,
            "n_obs": xa.len(),
            "axis_days": axis.len(),
            "axis_start": axis.first().cloned().unwrap_or_default(),
            "axis_end": axis.last().cloned().unwrap_or_default(),
            "alpha": alpha,
            "rho0": rho0.map(|v| json!(round4(v))).unwrap_or(Value::Null),
            "rho": rho0.map(|v| json!(round4(dampen(v, alpha)))).unwrap_or(Value::Null),
        })))
    }

    #[tool(
        description = "Full Kendall tau-b matrix (with dampening) over a comma-separated list of legs, using a trailing window of daily returns. Standalone computation of the pairwise rank correlations (monotone-transform invariant, scipy-compatible tie handling) — see corr_benchmark for the large-scale N≈1600 case persisted to the lake."
    )]
    async fn corr_matrix(
        &self,
        Parameters(params): Parameters<CorrMatrixParams>,
    ) -> Result<String, McpError> {
        let raw_legs = csv_symbols(&params.legs);
        if raw_legs.len() < 2 {
            return Err(McpError::invalid_params("need at least 2 legs", None));
        }
        let legs: Vec<(String, String)> = raw_legs.iter().map(|s| resolve_leg(s)).collect();
        let names: Vec<String> = legs.iter().map(|(_, s)| s.clone()).collect();
        let window = params.window_days.unwrap_or(252) as usize;
        let alpha = params.alpha.unwrap_or(0.4);
        let end = Utc::now().to_rfc3339();
        let start = (Utc::now() - Duration::days((window * 2 + 90) as i64)).to_rfc3339();

        let mut closes: Vec<Vec<(String, f64)>> = Vec::with_capacity(legs.len());
        for (m, s) in &legs {
            if m == FX_MARKET {
                let _ = self.lake.ensure_fx_bars(s, &start, &end, true).await?;
            }
            closes.push(leg_closes(&self.lake, m, s, &start, &end)?);
        }
        let axis = union_axis(&closes);
        let axis_cols: Vec<Vec<Option<f64>>> =
            closes.iter().map(|c| align_to_axis(c, &axis)).collect();
        let retcols = returns_columns(&axis_cols);
        let m = returns_matrix(&retcols, window)?;
        let packed = packed_kendall_upper(&m).map_err(|e| McpError::internal_error(e, None))?;

        let mut mat = vec![vec![f64::NAN; legs.len()]; legs.len()];
        for (i, j, v) in unpack_pairs(legs.len(), &packed) {
            mat[i][j] = v;
            mat[j][i] = v;
        }
        for i in 0..legs.len() {
            mat[i][i] = 1.0;
        }
        let mat_v: Vec<Vec<Value>> = mat
            .iter()
            .map(|row| row.iter().map(|&v| if v.is_nan() { Value::Null } else { json!(round4(v)) }).collect())
            .collect();

        let pairs: Vec<Value> = unpack_pairs(legs.len(), &packed)
            .into_iter()
            .map(|(i, j, v)| {
                let rho = if v.is_nan() { 0.0 } else { dampen(v, alpha) };
                json!({
                    "i": i, "j": j,
                    "leg_a": names[i], "leg_b": names[j],
                    "rho0": if v.is_nan() { Value::Null } else { json!(round4(v)) },
                    "rho": round4(rho),
                })
            })
            .collect();
        let window_end_idx = axis.len().saturating_sub(1);

        Ok(json_text(json!({
            "legs": names,
            "window_days": window,
            "alpha": alpha,
            "n_obs": window,
            "axis_days": axis.len(),
            "window_start": axis.get(window_end_idx.wrapping_sub(window - 1)).cloned().unwrap_or_default(),
            "window_end": axis.get(window_end_idx).cloned().unwrap_or_default(),
            "matrix": mat_v,
            "pairs": { "count": pairs.len(), "rows": pairs },
        })))
    }

    #[tool(
        description = "Large-scale correlation benchmark: expand the real US-lake stock underlyings into augmentations (AAPL -> AAPL1, AAPL2, ...) until `n_stock_legs` names exist, cross every leg against the FX universe over a 1-year trailing window, compute the full Kendall tau-b matrix (vectorized, rayon-parallel), dampen each pair with alpha, and write ~N²/2 rows to the lake correlation store (`correlations.parquet`). No 1500-symbol fetch — lake names are read directly and augmentations are seeded variants of their parent's real return series. Default N=1600 + 15 CCY legs ≈ 1.3M rows. Returns leg counts, axis stats, top-20 pair rows and a cpu/io wall-clock breakdown (seconds)."
    )]
    async fn corr_benchmark(
        &self,
        Parameters(params): Parameters<CorrBenchmarkParams>,
    ) -> Result<String, McpError> {
        use std::time::Instant;
        let n_stock = params.n_stock_legs.unwrap_or(1600).clamp(2, 100_000) as usize;
        let ccy = fx_pairs(params.ccy_pairs.as_deref());
        let window = params.window_days.unwrap_or(252) as usize;
        let alpha = params.alpha.unwrap_or(0.4);
        let seed = params.seed.unwrap_or(2026);
        let persist = params.persist.unwrap_or(true);
        let end = Utc::now().to_rfc3339();
        let start = (Utc::now() - Duration::days((window * 2 + 90) as i64)).to_rfc3339();
        let t_total = Instant::now();

        // 1. FX closes (lazy-fetch each via the free chain) — I/O
        let t_fetch = Instant::now();
        let mut fxcloses: Vec<Vec<(String, f64)>> = Vec::with_capacity(ccy.len());
        for pair in &ccy {
            match self.lake.ensure_fx_bars(pair, &start, &end, true).await {
                Ok(res) => {
                    let closes: Vec<(String, f64)> = res
                        .bars
                        .iter()
                        .filter_map(|b| {
                            let c = b.c;
                            if !c.is_finite() {
                                None
                            } else {
                                Some((b.date.clone(), c))
                            }
                        })
                        .collect();
                    fxcloses.push(closes);
                }
                Err(e) => {
                    tracing::warn!(pair, %e, "corr_benchmark: FX fetch failed");
                    fxcloses.push(Vec::new());
                }
            }
        }
        let t_read = Instant::now();
        let fetch_time_s = t_fetch.elapsed().as_secs_f64();

        // 2. real US-lake stock legs with 1d coverage, capped at n_stock — I/O
        let mut stock: Vec<(String, String)> = Vec::new();
        if let Ok(syms) = self.lake.read_coverage(Some(DEFAULT_MARKET), Some("1d"), None) {
            for row in syms {
                if stock.len() >= n_stock {
                    break;
                }
                if let Some(s) = row.get("symbol").and_then(Value::as_str) {
                    stock.push((DEFAULT_MARKET.to_string(), s.to_string()));
                }
            }
        }
        let real = stock.len();
        let mut stockcloses: Vec<Vec<(String, f64)>> = Vec::with_capacity(stock.len());
        for (m, s) in &stock {
            stockcloses.push(leg_closes(&self.lake, m, s, &start, &end)?);
        }
        let read_time_s = t_read.elapsed().as_secs_f64();
        let mut all_series: Vec<Vec<(String, f64)>> = Vec::new();
        all_series.extend(fxcloses.iter().cloned());
        all_series.extend(stockcloses.iter().cloned());
        let mut axis = union_axis(&all_series);
        if axis.is_empty() {
            axis = weekday_axis(&start, &end);
        }

        // 4. columns: real stocks, augmentations (AAPL -> AAPL1/2/3...), CCY legs
        let mut legs: Vec<String> = Vec::with_capacity(n_stock + ccy.len());
        let mut cols: Vec<Vec<Option<f64>>> = Vec::with_capacity(legs.capacity());
        for (i, c) in stockcloses.iter().enumerate() {
            legs.push(stock[i].1.clone());
            cols.push(align_to_axis(c, &axis));
        }
        let mut aug_n = 0usize;
        let mut synth_n = 0usize;
        if real == 0 {
            for k in 0..n_stock {
                let name = format!("SYN{}", k + 1);
                legs.push(name);
                cols.push(align_to_axis(&synthetic_closes(&axis, seed, k as u64), &axis));
                synth_n += 1;
            }
        } else {
            for k in real..n_stock {
                let v = k - real + 1; // 1-based variant index
                let base_idx = k % real;
                let name = format!("{}{}", stock[base_idx].1, v);
                legs.push(name);
                cols.push(augment_col(&cols[base_idx], seed, v as u64));
                aug_n += 1;
            }
        }
        for (i, c) in fxcloses.iter().enumerate() {
            if c.is_empty() {
                continue;
            }
            legs.push(ccy[i].clone());
            cols.push(align_to_axis(c, &axis));
        }

        // 5. returns matrix (columns = legs) over the trailing window — CPU
        let t_build = Instant::now();
        let retcols = returns_columns(&cols);
        let m = returns_matrix(&retcols, window)?;
        let build_time_s = t_build.elapsed().as_secs_f64();
        let t_corr = Instant::now();
        let packed = packed_kendall_upper(&m).map_err(|e| McpError::internal_error(e, None))?;

        // 6. dampen + persist rows (t, leg_a, leg_b, rho0, rho, alpha, window_days, n_obs, source) — I/O
        let pairs: Vec<(usize, usize, f64)> = unpack_pairs(legs.len(), &packed);
        let n_pairs = pairs.len();
        let t_end = axis.last().cloned().unwrap_or_else(|| end[..10.min(end.len())].to_string());
        let corr_time_s = t_corr.elapsed().as_secs_f64();
        let t_write = Instant::now();
        let mut write_time_s = 0.0;
        if persist && !pairs.is_empty() {
            if let Err(e) = self
                .lake
                .write_correlations(&t_end, &legs, alpha, window, "benchmark", &pairs)
            {
                tracing::warn!(%e, "corr_benchmark: write correlations failed");
            }
            write_time_s = t_write.elapsed().as_secs_f64();
        }
        let total_time_s = t_total.elapsed().as_secs_f64();

        // 7. top-20 rows by |rho0|
        let mut scored: Vec<(f64, usize, usize)> = Vec::with_capacity(n_pairs);
        for (i, j, v) in unpack_pairs(legs.len(), &packed) {
            scored.push((v, i, j));
        }
        scored.sort_by(|a, b| {
            b.0.abs()
                .partial_cmp(&a.0.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let top: Vec<Value> = scored
            .iter()
            .take(20)
            .map(|(v, i, j)| json!({
                "leg_a": legs[*i], "leg_b": legs[*j],
                "rho0": if v.is_nan() { Value::Null } else { json!(round4(*v)) },
                "rho": if v.is_nan() { Value::Null } else { json!(round4(dampen(*v, alpha))) },
            }))
            .collect();

        // cpu: matrix build + kendall + dampen; io: fetch + read + write
        let cpu_time_s = build_time_s + corr_time_s;
        let io_time_s = fetch_time_s + read_time_s + write_time_s;

        Ok(json_text(json!({
            "summary": {
                "n_legs": legs.len(),
                "n_pairs": n_pairs,
                "n_stock_legs": n_stock,
                "real_stock": real,
                "augmentations": aug_n,
                "synthetic_stock": synth_n,
                "n_ccy": ccy.len(),
                "window_days": window,
                "alpha": alpha,
                "seed": seed,
                "axis_start": axis.first().cloned().unwrap_or_default(),
                "axis_end": t_end,
                "axis_days": axis.len(),
                "n_obs": window,
                "persisted": persist && !pairs.is_empty(),
                "timings_seconds": {
                    "fetch_time_s": round4(fetch_time_s),
                    "read_time_s": round4(read_time_s),
                    "build_time_s": round4(build_time_s),
                    "corr_time_s": round4(corr_time_s),
                    "write_time_s": round4(write_time_s),
                    "cpu_time_s": round4(cpu_time_s),
                    "io_time_s": round4(io_time_s),
                    "total_time_s": round4(total_time_s),
                },
            },
            "top_pairs": top,
            "sample_legs": legs.iter().take(30).cloned().collect::<Vec<_>>(),
        })))
    }

    #[tool(
        description = "Query the lake correlation store (correlations.parquet) written by `corr_benchmark`. Filters by leg_a/leg_b and the `t` date; newest first, capped by `limit` (default 1000, max 1,000,000)."
    )]
    async fn get_correlations(
        &self,
        Parameters(params): Parameters<CorrQueryParams>,
    ) -> Result<String, McpError> {
        let rows = self.lake.read_correlations(
            params.leg_a.as_deref(),
            params.leg_b.as_deref(),
            params.start.as_deref(),
            params.end.as_deref(),
            params.limit.unwrap_or(1000),
        )?;
        Ok(json_text(json!({
            "count": rows.len(),
            "correlations": rows,
        })))
    }
}

fn trailing(col: &[f64], window: usize) -> Vec<f64> {
    let skip = col.len().saturating_sub(window);
    col.iter().skip(skip).copied().collect()
}

fn round4(v: f64) -> f64 {
    (v * 10000.0).round() / 10000.0
}

/// Monday–Friday daily axis between two RFC-3339 / date strings.
fn weekday_axis(start: &str, end: &str) -> Vec<String> {
    use chrono::Datelike;
    let mut d = date_of(start);
    let stop = date_of(end);
    let mut out = Vec::new();
    while d <= stop {
        if d.weekday().num_days_from_monday() < 5 {
            out.push(d.format("%Y-%m-%d").to_string());
        }
        d += Duration::days(1);
    }
    out
}

/// First 10 chars of an RFC3339/naive timestamp as a NaiveDate.
fn date_of(ts: &str) -> chrono::NaiveDate {
    chrono::NaiveDate::parse_from_str(&ts[..10.min(ts.len())], "%Y-%m-%d")
        .unwrap_or_else(|_| Utc::now().date_naive())
}
