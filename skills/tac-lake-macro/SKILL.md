---
name: tac-lake-macro
description: Guide agents to manage synthetic macro symbols in the TradeAC parquet+DuckDB data lake — fetch free macro/volatility series (FRED, CBOE, OECD, World Bank) through new Rust providers built into tac-engine, store them as OHLCV-style synthetic instruments under market=MACRO, compute a dedicated family=macro feature set, and join them against equity data for regime/market-state studies. Addon to tradeac-lake (read that skill first).
---

# tac-lake-macro

Synthetic **macro symbols** in the TradeAC lake: free economic/volatility time series
(FRED, CBOE, OECD, World Bank) normalized into the same bar schema as real tickers so
every downstream pipeline — DuckDB reads, qlib handlers, backtests — stays uniform.

This is an **addon skill to `tradeac-lake`** (`tac-engine/skills/tradeac-lake/SKILL.md`):
same lake root (`$TAC_LAKE_DIR`), same hive conventions, same metadata files. It adds one
new market partition, one new feature family, a symbol catalog, and a small set of
tac-engine MCP tools backed by new Rust fetcher modules. Companion example:
`tac-qlib/examples/macro-pred.py` (the CSV prototype this lake integration supersedes).

> **Build status:** **shipped** (tac-engine commit `815b61d`). The `market=MACRO` partition,
> `family=macro` features, the managed catalog, and all five MCP tools below are implemented
> in Rust (`src/macro_sources.rs`, `src/lake_macro.rs`) and verified against the running
> engine — call `get_macro_*` / `put_macro_symbol` / `load_macro_symbols` freely; no manual
> fetch scripts needed.

## Why synthetic symbols

A macro series is stored exactly like a stock: one parquet of bars per symbol. The
mapping is trivial and lossless:

| bar field | macro series |
|-----------|--------------|
| `t` / `date` | observation date (daily, weekly, monthly, quarterly — **native cadence**, no resampling) |
| `o`, `h`, `l`, `c` | all = the observed value |
| `v`, `n` | 0 |
| `vw` | null |

Consumers forward-fill to daily frequency themselves (see "Alignment rules"), so the lake
never bakes in an interpolation policy.

## Lake layout additions

```
$TAC_LAKE_DIR/
├── market=MACRO/
│   └── timeframe=1d/
│       ├── symbol=FRED_CPIAUCSL.parquet     # same Bar schema as market=US
│       ├── symbol=CBOE_VIX.parquet
│       └── ...
├── features/
│   └── market=MACRO/
│       └── timeframe=1d/
│           └── family=macro/
│               └── symbol=FRED_CPIAUCSL.parquet   # t + macro_* columns
├── macro_catalog.parquet           # NEW metadata file: the managed symbol catalog
├── symbols.parquet                 # macro rows: asset_class="macro", tradable=false
└── coverage.parquet                # rows for (MACRO,1d,<symbol>) like any other
```

- **`market=MACRO`** keeps synthetic symbols out of every US-equity universe query
  (`read_parquet('market=US/...')` never sees them by construction).
- **`family=macro`** is reserved exclusively for synthetic macro symbols — never persist
  `family=macro` for a real ticker, and never persist `family=ta|sp` for a macro symbol
  (TA/HMM on step-function series is meaningless).
- Bars upsert by timestamp (same merge rule as tradeac-lake); feature writes are fresh
  writes per family.

## Symbol naming convention

`<SOURCE>_<SERIES_ID>`, uppercase, filename-safe (letters/digits/underscore):

| example | source | series |
|---------|--------|--------|
| `FRED_CPIAUCSL` | FRED | CPI-U SA |
| `CBOE_VIX` | CBOE | VIX close |

Aliases (e.g. `VIX` → `CBOE_VIX`) live in the catalog, not the filesystem — one canonical
name per symbol.

### Starter catalog

| symbol | source / id | freq | what |
|--------|-------------|------|------|
| `FRED_CPIAUCSL` | FRED CPIAUCSL | M | CPI-U, inflation level (use YoY log-diff feature) |
| `FRED_FEDFUNDS` | FRED FEDFUNDS | M | effective fed funds rate |
| `FRED_DGS10` | FRED DGS10 | D | 10y Treasury yield |
| `FRED_DGS2` | FRED DGS2 | D | 2y Treasury yield |
| `FRED_T10Y2Y` | FRED T10Y2Y | D | term spread (10y−2y) |
| `FRED_UNRATE` | FRED UNRATE | M | unemployment rate |
| `FRED_PAYEMS` | FRED PAYEMS | M | nonfarm payrolls |
| `FRED_INDPRO` | FRED INDPRO | M | industrial production |
| `FRED_UMCSENT` | FRED UMCSENT | M | consumer sentiment |
| `FRED_GDPC1` | FRED GDPC1 | Q | real GDP |
| `FRED_WALCL` | FRED WALCL | W | Fed balance sheet |
| `FRED_M2SL` | FRED M2SL | M | M2 money supply |
| `CBOE_VIX` | CBOE `VIX_History.csv` | D | implied vol index |
| `WB_GDP_US` | World Bank `US/NY.GDP.MKTP.KD` | A | US real GDP (constant USD) |
| `OECD_CLI_US` | OECD `…DF_CLI/USA.M.LI.IX._Z.AA.IX._Z.H` | M | composite leading indicator |
| `OECD_CPI_US` | OECD `…DF_KEI/USA.M.CP.IX._Z._Z._Z` | M | CPI all items |
| `OECD_IPI_US` | OECD `…DF_KEI/USA.M.PRVM.IX.C.Y._Z` | M | industrial production |

Extend the catalog with `put_macro_symbol` — any FRED id works with zero new code (the id
IS the symbol); CBOE/OECD/WorldBank rows need their provider-specific `source_id`
(CBOE CSV filename, OECD `{AGENCY},{FLOW}/{KEY}`, World Bank `COUNTRY/INDICATOR`).

## `macro_catalog.parquet`

| col | type | notes |
|-----|------|-------|
| `symbol` | VARCHAR (pk) | e.g. `FRED_CPIAUCSL` |
| `source` | VARCHAR | `fred` \| `cboe` \| `oecd` \| `worldbank` |
| `source_id` | VARCHAR | series id / URL key the provider needs (OECD: `{AGENCY},{DATAFLOW_ID}/{KEY}` with EMPTY version) |
| `title` | VARCHAR | human label |
| `frequency` | VARCHAR | `D` \| `W` \| `M` \| `Q` \| `A` |
| `units` | VARCHAR | index, %, level… |
| `seasonal_adj` | BOOLEAN | |
| `release_lag_days` | INT | approx publication delay (CPI ≈ 14, GDP ≈ 90); used for point-in-time joins |
| `enabled` | BOOLEAN | loader skips disabled rows |
| `updated_at` | TIMESTAMPTZ | |

Managed by the load tool; hand-edit only to add entries before first load.

## Implementation in tac-engine (Rust)

All in the existing crate (`tac-engine/src/`), reusing its deps: `reqwest` (rustls),
`chrono`, `serde_json`, `duckdb` bundled, plus `csv` for the CBOE provider.

### 1. `src/macro_sources.rs` — providers

```rust
pub trait MacroSource {
    fn fetch(&self, cfg: &CatalogRow, start: NaiveDate, end: NaiveDate)
        -> Result<Vec<Bar>, McpError>;   // lake Bar struct; o=h=l=c=value, v=n=0
}

pub struct FredSource      { pub api_key: String }   // env FRED_API_KEY
pub struct CboeSource;                               // cdn.cboe.com daily CSVs
pub struct OecdSource;                               // SDMX-JSON
pub struct WorldBankSource;                          // v2 JSON API
```

Per `.tmp/macro-sym.md`: each provider parses its native format (FRED/OECD/WB JSON,
CBOE CSV) and emits synthetic bars. Provider-specific parsing notes:
- FRED: `observations[].value` arrives as **string**; `"."` = missing → skip the row.
- OECD SDMX: keys are `YYYY-MM` → normalize to first-of-month dates.
- World Bank: annual only (`YYYY`) → Dec-31 stamps; low priority, keep behind `enabled`.
- CBOE: `DATE` is `%m/%d/%Y`; column 4 is close.
- All timestamps UTC-stamped like lake bars (`04:00Z` convention for date-only rows).

### 2. Feature computation — `family=macro`

`src/lake_macro.rs` computes level-free transforms (raw levels are unit-dependent
and mostly non-stationary) and persists to
`features/market=MACRO/timeframe=1d/family=macro/symbol=<SYM>.parquet`:

| family | columns | notes |
|--------|---------|-------|
| `transform` | `macro_yoy`, `macro_mom`, `macro_zscore` | YoY/MoM log-diff or diff, rolling z-score |
| `regime` | `macro_above_sma200`, `macro_trend_slope_60` | only meaningful for daily series |
| `align` | `macro_value_ffill`, `macro_age_days` | daily forward-filled value + staleness (join helper) |

Family selection mirrors `get_lake_sp` (`families=` comma list, default all). Monthly/
quarterly series get `transform` + `align`; skip `regime` unless freq `D`.

### 3. MCP tools (registered in `src/lake_tools.rs`, following the lake-tool pattern)

| tool | args | does |
|------|------|------|
| `get_macro_symbols` | `{symbol?}` | read `macro_catalog.parquet` |
| `put_macro_symbol` | `{symbol, source, source_id, title?, frequency?, units?, release_lag_days?, enabled?}` | upsert one catalog row; validates the provider spelling. Use this to add symbols beyond the seeds |
| `load_macro_symbols` | `{symbols?, start?, end?, families?}` | background job like `load_lake_symbols`: fetch → upsert `market=MACRO/timeframe=1d` bars → update coverage/symbols → compute + persist `family=macro`; returns `job_id` (poll with `load_lake_status`) |
| `get_macro_bars` | `{symbols, start?, end?, quiet?}` | read stored MACRO bars (`o=h=l=c=value`); no lazy refetch — load first via `load_macro_symbols` |
| `get_macro_features` | `{symbol?, start?, end?, recompute?, quiet?}` | read persisted `family=macro` rows; `recompute: true` rebuilds + persists them from stored bars (no provider fetch) — use after fixing a catalog row |

Quiet-mode semantics identical to tradeac-lake (`quiet=true` returns summaries, use when
the goal is writing).

**Secrets policy (binding):** `FRED_API_KEY` is read from the environment only — never
hardcoded, never written to catalog/lake files, never echoed in tool output. Free FRED
keys: https://fred.stlouisfed.org/docs/api/api_key.html. CBOE/OECD/WB need no key.

## Alignment rules for consumers (agents MUST follow)

1. **Forward-fill on the consumer side.** Join macro obs to daily data with
   `ASOF JOIN` (DuckDB) or pandas `ffill().reindex(trading_days)`; never inner-join
   monthly rows to daily bars and assume missing = flat.
2. **Respect release lag for point-in-time work.** A CPI row stamped 2026-07-01 was
   public ~2026-07-15. For signal research shift the observation date by
   `release_lag_days` (catalog column) before asof-joining; for descriptive studies
   plain asof is fine — say which you did.
3. **Never mix `market=MACRO` into equity universes** and never feed raw levels to
   models — use the persisted transforms (`macro_yoy`, `macro_zscore`, …).
4. Quarterly GDP: prefer the aligned `macro_*` feature file over raw bars.

### Canonical DuckDB reads

```sql
-- daily equity bars joined to latest-known VIX (both from the lake)
SELECT b.symbol, b.date, b.c,
       m.c AS vix_close
FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=SPY.parquet') b
ASOF JOIN read_parquet('$TAC_LAKE_DIR/market=MACRO/timeframe=1d/symbol=CBOE_VIX.parquet') m
  ON m.t <= b.date;

-- persisted macro transform features
SELECT t, macro_yoy, macro_zscore
FROM read_parquet('$TAC_LAKE_DIR/features/market=MACRO/timeframe=1d/family=macro/symbol=FRED_CPIAUCSL.parquet')
WHERE t >= DATE '2021-01-01';
```

Python equivalent of the prototype pipeline
(`tac-qlib/examples/macro-pred.py` currently derives `MKT_*` synthetic tickers from the
equity panel itself): now that `load_macro_symbols` is live, those MKT_* CSVs gain lake-backed
siblings — breadth-style indices stay computed-from-universe (they are not fetched),
while CPI/VIX/rates become `market=MACRO` symbols consumed via the queries above.

## Implementation checklist (as built)

All items below are done (commit `815b61d`):

1. `Cargo.toml`: `csv = "1"` added.
2. `src/macro_sources.rs`: trait + 4 providers + unit tests with fixture payloads
   (FRED `"."` skipping, CBOE `%m/%d/%Y`, OECD month-stamping).
3. `src/lake_macro.rs`: transforms + persistence targeting `family=macro`, reusing the
   helpers used by `compute_ta`/`compute_sp` in `src/lake.rs`.
4. `Lake::upsert_bars` handles the merge with `market="MACRO"`.
5. Tools registered in `src/lake_tools.rs` (`get_macro_symbols`, `put_macro_symbol`,
   `load_macro_symbols`, `get_macro_bars`, `get_macro_features`); honor `quiet`,
   `job_id` conventions; catalog bootstrapped with the starter table above.
6. `validate_lake_dataset` is market-parameterized, so MACRO is scanned by passing
   `market="MACRO"`; judging macro symbols by catalog frequency (not daily staleness)
   is still open.

## Ops notes

- FRED: ~120 req/min without key tiers; batch loads are tiny (≤ a few dozen series).
- OECD SDMX (verified 2026-08): the data endpoint is
  `https://sdmx.oecd.org/public/rest/data/{AGENCY},{DATAFLOW_ID}/{KEY}?format=csvfile&startPeriod=…`
  — the version field MUST be empty (`DF_KEI,/USA.M.…`); an explicit version that
  doesn't match the current flow version (e.g. `4.0` when the flow is at `4.1`)
  silently 404s `NoRecordsFound`. `dimensionAtObservation=AllDimensions` is sent by
  default. Cloudflare rate-limits bursts (HTTP 429) — space requests a few seconds
  apart. Discover keys via `/dataflow/{AGENCY}/{FLOW}/?references=all` (structure JSON)
  or pull one small window with key `/all` and inspect real dimension values; note some
  flows have no US rows at all (e.g. the COICOP CPI release) and NAAG GDP is annual
  PPP-only — prefer FRED for US levels.
- CBOE CSVs change layout rarely but do — parse header names, not positions, if
  possible; keep a fixture test.
- Coverage rows for monthly symbols will look "stale" next to daily symbols — expected;
  `validate_lake_dataset` should judge MACRO symbols by their catalog frequency.
- Never delete-and-refetch a macro symbol blindly: sources occasionally revise history
  (seasonal adjustment), which is fine, but a full refetch also erases local knowledge of
  gaps. Prefer tail refresh; full rebuild only on schema drift.
