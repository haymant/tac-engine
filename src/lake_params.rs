use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeBarsParams {
    /// Market partition, e.g. `US` (default), `CRYPTO`.
    pub market: Option<String>,
    /// Comma-separated symbols, e.g. `AAPL,MSFT`.
    pub symbols: String,
    /// Lake timeframe name, e.g. `1m`, `10m`, `1h`, `1d`. Maps to Alpaca spellings.
    pub timeframe: String,
    /// Inclusive start. `YYYY-MM-DD` or RFC-3339. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. `YYYY-MM-DD` or RFC-3339. Default: now.
    pub end: Option<String>,
    /// Data feed: `sip` (default) or `iex`.
    pub feed: Option<String>,
    /// Adjustment: `raw` (default), `split`, `dividend`, `all`, or comma combos.
    pub adjustment: Option<String>,
    /// If true (default), lazy-load missing windows from Alpaca and persist; if false, read the lake only.
    pub lazy: Option<bool>,
    /// If true, return a summary (per-symbol counts / first-last timestamps /
    /// source) instead of the full bar rows. Use for backfill-to-lake jobs that
    /// don't need the payload echoed back.
    pub quiet: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeTAParams {
    /// Market partition, e.g. `US`.
    pub market: Option<String>,
    /// Symbol, e.g. `AAPL`.
    pub symbol: String,
    /// Lake timeframe name, e.g. `1d`.
    pub timeframe: String,
    /// Inclusive start. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// Comma-separated indicators: `sma_5,sma_20,ema_12,ema_26,rsi_14,macd,bb,atr_14,adx_14`.
    /// Default: all supported indicators.
    pub indicators: Option<String>,
    /// If true (default), persist computed features into the hive-partitioned
    /// `features/market=*/timeframe=*/symbol=*.parquet` files.
    pub persist: Option<bool>,
    /// If true, return only a summary (count + computed column names) instead of
    /// the full feature rows. Use when the purpose is persisting indicators to the lake.
    pub quiet: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeSPParams {
    /// Market partition, e.g. `US`.
    pub market: Option<String>,
    /// Symbol, e.g. `AAPL`.
    pub symbol: String,
    /// Lake timeframe name, e.g. `1d`.
    pub timeframe: String,
    /// Inclusive start. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// HMM fit cutoff (`YYYY-MM-DD` or RFC-3339). The 2-state Gaussian HMM is fit
    /// only on returns up to this date (no lookahead); regime posteriors are still
    /// computed over the whole requested window. Default: fit on the whole window.
    pub fit_end: Option<String>,
    /// Comma-separated stochastic-process feature families:
    /// `ou,hmm,jump,har,trend,hurst,signature,moments,momentum,garch`. Default: all supported families.
    pub families: Option<String>,
    /// If true (default), persist computed `sp_*` columns into the hive-partitioned
    /// `features/market=*/timeframe=*/symbol=*.parquet` files, replacing stale `sp_*`
    /// columns (e.g. from a previous run with a different family selection).
    pub persist: Option<bool>,
    /// If true, return only a summary (count + `sp_*` column names) instead of
    /// the full feature rows. Use when the purpose is persisting features to the lake.
    pub quiet: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeFeaturesParams {
    /// Market filter.
    pub market: Option<String>,
    /// Symbol filter.
    pub symbol: Option<String>,
    /// Timeframe filter.
    pub timeframe: Option<String>,
    /// Inclusive start filter.
    pub start: Option<String>,
    /// Inclusive end filter.
    pub end: Option<String>,
    /// If true, return only a summary (row count + available feature columns)
    /// instead of the full feature rows.
    pub quiet: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeSymbolsParams {
    /// Optional symbol filter.
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeWatchlistParams {}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeCalendarParams {
    /// Market filter, e.g. `US`.
    pub market: Option<String>,
    /// Inclusive start (`YYYY-MM-DD`).
    pub start: Option<String>,
    /// Inclusive end (`YYYY-MM-DD`).
    pub end: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeCoverageParams {
    /// Market filter.
    pub market: Option<String>,
    /// Timeframe filter.
    pub timeframe: Option<String>,
    /// Symbol filter.
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeStatusParams {}

/// Gap-fill tool: seed the lake trading calendar from Alpaca historical auctions.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct BackfillCalendarParams {
    /// Market partition, e.g. `US` (default).
    pub market: Option<String>,
    /// Comma-separated symbols whose auction days seed the calendar.
    pub symbols: String,
    /// Inclusive start (`YYYY-MM-DD` or RFC-3339).
    pub start: Option<String>,
    /// Inclusive end (`YYYY-MM-DD` or RFC-3339). Default: now.
    pub end: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeRebuildParams {
    /// Market partition, e.g. `US`.
    pub market: Option<String>,
    /// Symbol, e.g. `AAPL`.
    pub symbol: String,
    /// Lake timeframe name, e.g. `1d`.
    pub timeframe: String,
    /// Data feed: `iex` (default). SIP is never used.
    pub feed: Option<String>,
    /// Adjustment: `raw` (default), `split`, `dividend`, `all`.
    pub adjustment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeBulkLoadParams {
    /// Market partition, e.g. `US`.
    pub market: Option<String>,
    /// Comma-separated symbols, e.g. `AAPL,MSFT,GOOGL`.
    pub symbols: String,
    /// Lake timeframe name, e.g. `1d`.
    pub timeframe: String,
    /// Inclusive start. `YYYY-MM-DD` or RFC-3339. Default: end minus 4 years.
    pub start: Option<String>,
    /// Inclusive end. `YYYY-MM-DD` or RFC-3339. Default: now.
    pub end: Option<String>,
    /// Data feed: `iex` (default). SIP is never used.
    pub feed: Option<String>,
    /// Adjustment: `raw` (default), `split`, `dividend`, `all`.
    pub adjustment: Option<String>,
    /// TA indicators to compute (comma-separated). Default: all.
    pub indicators: Option<String>,
    /// SP feature families to compute (comma-separated). Default: all.
    pub families: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeLoadStatusParams {
    /// Job ID returned by `load_lake_symbols`.
    pub job_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeValidateParams {
    /// Market partition, e.g. `US`.
    pub market: Option<String>,
    /// Lake timeframe name, e.g. `1d`.
    pub timeframe: Option<String>,
    /// Optional comma-separated symbol filter; default: all symbols in coverage.
    pub symbols: Option<String>,
    /// Optional window start for feature/row alignment + all-NaN checks.
    pub start: Option<String>,
    /// Optional window end for feature/row alignment + all-NaN checks.
    pub end: Option<String>,
}

// ---- macro (synthetic symbols, market=MACRO) --------------------------------

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MacroSymbolsParams {
    /// Optional catalog symbol filter, e.g. `FRED_CPIAUCSL`.
    pub symbol: Option<String>,
}

/// Insert or update one row in macro_catalog.parquet (see the tac-lake-macro
/// skill). Existing rows are replaced wholesale by symbol.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MacroPutSymbolParams {
    /// Catalog symbol, e.g. `FRED_CPIAUCSL`, `CBOE_VIX`, `WB_GDP_US`.
    pub symbol: String,
    /// Provider: `fred` | `cboe` | `oecd` | `worldbank`.
    pub source: String,
    /// Provider-specific id: FRED series id (CPIAUCSL); CBOE CSV filename
    /// (VIX_History.csv); OECD SDMX `{AGENCY},{DATAFLOW_ID}/{KEY}` with an
    /// EMPTY version (OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.PRVM.IX.C.Y._Z);
    /// World Bank `COUNTRY/INDICATOR` (US/NY.GDP.MKTP.KD).
    pub source_id: String,
    /// Human-readable title.
    pub title: Option<String>,
    /// Cadence hint D/W/M/Q/A; when omitted it is inferred from the data at load.
    pub frequency: Option<String>,
    /// Units label, e.g. `index`, `%`, `USD bn`.
    pub units: Option<String>,
    /// Typical days between observation date and public availability.
    pub release_lag_days: Option<i64>,
    /// Enable/disable the row for bulk loads (default true).
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MacroBarsParams {
    /// Comma-separated synthetic macro symbols, e.g. `CBOE_VIX,FRED_DGS10`.
    pub symbols: String,
    /// Inclusive start. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// If true, return a per-symbol summary instead of the full bar rows.
    pub quiet: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MacroFeaturesParams {
    /// Optional symbol filter, e.g. `FRED_CPIAUCSL`. Omit for all loaded symbols.
    pub symbol: Option<String>,
    /// Inclusive start filter.
    pub start: Option<String>,
    /// Inclusive end filter.
    pub end: Option<String>,
    /// If true, recompute + persist family=macro features from the stored bars
    /// (all three families) before reading back — no provider fetch. Use after
    /// fixing a catalog row (e.g. adding a missing `frequency`).
    pub recompute: Option<bool>,
    /// If true, return only a summary (row count + column names).
    pub quiet: Option<bool>,
}

/// Bulk-load synthetic macro symbols from free providers (FRED, CBOE, OECD,
/// World Bank) into market=MACRO and refresh family=macro features. Runs in a
/// background thread; poll `load_lake_status` with the returned `job_id`.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MacroBulkLoadParams {
    /// Comma-separated catalog symbols to load. Default: every enabled row in
    /// macro_catalog.parquet. Unknown symbols are reported as errors — the
    /// catalog must know them first (see get_macro_symbols).
    pub symbols: Option<String>,
    /// Inclusive observation-date start. Default: TAC_LAKE_START_DATE (2000-01-03).
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// Macro feature families to compute (comma-separated):
    /// `transform,regime,align`. Default: all.
    pub families: Option<String>,
}

// ---- FX (market=FX) + correlation store -------------------------------------

/// Cache-first daily FX bars from `market=FX/1d` (fetched via the Yahoo →
/// Frankfurter/ECB chain when `lazy`). Pairs are 6-letter codes (`EURUSD`).
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeFxParams {
    /// Comma-separated currency pairs, e.g. `EURUSD,USDJPY`.
    pub pairs: String,
    /// Inclusive start. `YYYY-MM-DD` or RFC-3339. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// If true (default), lazy-load missing windows from free sources and persist.
    pub lazy: Option<bool>,
    /// If true, return a per-pair summary (count / first_t / last_t / source).
    pub quiet: Option<bool>,
}

/// Realized FX vol (`rv20_ann`, `rv60_ann`) computed from `market=FX` closes.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LakeFxVolParams {
    /// Comma-separated currency pairs.
    pub pairs: String,
    /// Inclusive start. Default: end minus 30 days.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
    /// If true (default), persist the computed series under `family=fxvol`.
    pub persist: Option<bool>,
    /// If true, return only a summary (row count + column names).
    pub quiet: Option<bool>,
}

/// Bulk-load daily FX bars (+ realized-vol features) into the lake.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoadLakeFxParams {
    /// Comma-separated currency pairs. Default: the standard major/cross list.
    pub pairs: Option<String>,
    /// Inclusive start. Default: TAC_LAKE_START_DATE.
    pub start: Option<String>,
    /// Inclusive end. Default: now.
    pub end: Option<String>,
}

/// Single-pair Kendall tau (+ dampening) from lake closes.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CorrCalcParams {
    /// Leg A symbol, e.g. `AAPL` (market US) or `EURUSD` (market FX).
    pub leg_a: String,
    /// Leg B symbol.
    pub leg_b: String,
    /// Lookback in trading days (default 252 ≈ 1 year).
    pub window_days: Option<i64>,
    /// Dampening weight toward +1, `rho = rho0 + alpha*(1-rho)` (default 0.4).
    pub alpha: Option<f64>,
}

/// Full upper-triangle Kendall tau matrix among a comma-separated leg list.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CorrMatrixParams {
    /// Comma-separated legs (stock symbols resolve in market US, 6-letter
    /// codes resolve in market FX).
    pub legs: String,
    /// Lookback in trading days (default 252).
    pub window_days: Option<i64>,
    /// Dampening weight (default 0.4).
    pub alpha: Option<f64>,
}

/// Calibrated implied-vol surface for one underlying from the live option chain.
///
/// Serves a cached calibration (`vol_surfaces/<SYMBOL>.json`) when it is younger
/// than `max_age_secs` (default 600 = 10 minutes); otherwise fetches the chain,
/// fits SVI per expiry, persists and returns the fina-risk `vol_surfaces` block.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct VolSurfaceParams {
    /// Underlying ticker, e.g. `ADBE` (bare, not the market-qualified id).
    pub underlying: String,
    /// Evaluation date as an Excel serial; defaults to today.
    pub evaluation_date: Option<i64>,
    /// Reference spot for the strike grid; defaults to the median chain strike.
    pub reference: Option<f64>,
    /// Freshness window in seconds (default 600).
    pub max_age_secs: Option<i64>,
    /// Force recalibration, ignoring the cache.
    pub refresh: Option<bool>,
    /// Number of grid strikes (default 21, clamped 5..=41).
    pub grid_points: Option<usize>,
    /// Deal expiry (Excel serial). Restricts calibration to expiries within
    /// ±`tenor_window_days` of it, so short-dated weeklies don't distort the
    /// term structure of a longer deal.
    pub deal_expiry: Option<i64>,
    /// Half-window around `deal_expiry` in days (default 200).
    pub tenor_window_days: Option<i64>,
}

/// Force-recalibrate (ignore the cache) or inspect a cached surface.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct VolSurfaceStatusParams {
    /// Underlying ticker.
    pub underlying: String,
    /// Recalibrate even when the cache is fresh.
    pub refresh: Option<bool>,
    /// Evaluation date as an Excel serial; defaults to today.
    pub evaluation_date: Option<i64>,
    /// Reference spot override.
    pub reference: Option<f64>,
    /// Grid strike count.
    pub grid_points: Option<usize>,
}

/// Large-scale benchmark: build N stock legs (real lake symbols + synthetic
/// `AAPL1..N`-style clones when the lake is thin) × the FX universe, compute
/// the full Kendall correlation matrix with dampening, and write ~N²/2 pair
/// rows to the lake correlation store.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CorrBenchmarkParams {
    /// Number of stock underlying legs (default 1600).
    pub n_stock_legs: Option<i64>,
    /// Comma-separated FX pairs to cross against. Default: DEFAULT_FX_PAIRS.
    pub ccy_pairs: Option<String>,
    /// Lookback in trading days (default 252).
    pub window_days: Option<i64>,
    /// Dampening weight (default 0.4).
    pub alpha: Option<f64>,
    /// RNG seed for synthetic leg generation (deterministic; default 2026).
    pub seed: Option<u64>,
    /// If true (default), write the pair rows to `correlations.parquet`.
    pub persist: Option<bool>,
}

/// Query the correlation store.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CorrQueryParams {
    /// Filter by the first leg.
    pub leg_a: Option<String>,
    /// Filter by the second leg.
    pub leg_b: Option<String>,
    /// Inclusive `t` lower bound.
    pub start: Option<String>,
    /// Inclusive `t` upper bound.
    pub end: Option<String>,
    /// Max rows (default 1000, cap 1,000,000).
    pub limit: Option<i64>,
}
