---
name: tradeac-lake
description: Guide agents to build and query the TradeAC parquet+DuckDB data lake on the local filesystem — hive-partitioned bar store (market/timeframe/symbol) plus symbols, watchlist, calendar, features and coverage metadata — with lazy backfill from the tac-engine MCP get_stock_bars tool (tradeac-alpaca skill).
---

# tradeac-lake

Local-first market data lake: **Apache Parquet** files on disk, consumed with **DuckDB** (or Apache Arrow). Bar data is the core payload; the lake also keeps small metadata parquet files (symbols, watchlist, calendar, features, coverage) at the lake root.

Reading is a **cache-first** pattern: if the requested range is already in the lake, serve it directly from parquet; otherwise **lazy-load** the missing window via the tac-engine MCP `get_stock_bars` tool (see `tac-engine/skills/tradeac-alpaca/SKILL.md`), persist it, update metadata, then return.

## MCP-first policy

- **Prefer the tac-engine lake MCP tools** (`get_lake_bars`, `get_lake_ta`, `get_lake_sp`, `get_lake_features`, `get_lake_status`, `get_lake_coverage`, `get_lake_calendar`, `backfill_lake_calendar`, …) whenever they cover the need. They handle coverage checks, lazy backfill, feed fallback, metadata updates and pagination for you — do not reimplement that in DuckDB/pyarrow scripts.
- **Direct parquet reads are only for verification** (DuckDB CLI / pyarrow snippets below) or when no lake tool covers the query (e.g. an arbitrary ad-hoc SQL join). Keep hand-rolled lake *writes* off the happy path — the write path is what the MCP tools automate.
- **NEVER script directly against the MCP server** (spawning the engine binary, stdio JSON-RPC, bash/curl) unless a tool genuinely can't do the job — then **stop and ask the user to confirm first**.
- The engine bundles its own DuckDB; direct verification only needs the `duckdb` CLI or a Python venv with `duckdb` + `pyarrow` (see dependencies below).

## Env / root

| Var | Default | Purpose |
|-----|---------|---------|
| `TAC_LAKE_DIR` | **required** (no default) | lake root on the local filesystem. Local dev: absolute path (e.g. `/home/data/lake`).  |

```bash
export TAC_LAKE_DIR=/path/to/lake
mkdir -p "$TAC_LAKE_DIR"
```

## Secrets policy

- NEVER write secrets into files: API keys, DB passwords, OAuth tokens, or credential-bearing URLs (`DATABASE_URL`, `APCA_*`) in scripts, configs, notes or committed code.
- NEVER read `*.env` / `.env.*` directly (`cat`/`tail`/`grep`/`sed`/`head` on `.env`). That pulls secrets into this session and leaks them to any agent sharing it.
- When a tool or command needs an env var, ASK the user to set it in the environment (shell/container env, or the user-owned `.env`) and reference it by name (`$VAR`), never by value. If it's missing, report which variable is required instead of reading it yourself.
- If you find a committed secret, flag it, remove it, and replace it with a placeholder.

## Lake layout

Hive partition convention, partitioned by `market`, `timeframe`, `symbol`. Metadata parquet files live alongside the partition dirs at the lake root.

```
$TAC_LAKE_DIR/
├── market=US/
│   └── timeframe=1d/
│       ├── symbol=AAPL.parquet
│       ├── symbol=MSFT.parquet
│       └── ...
│   └── timeframe=10m/
│       └── symbol=AAPL.parquet
├── market=CRYPTO/...            # optional: BTC/USD etc.
├── features/                   # TA + SP indicators, hive-partitioned with family tier
│   └── market=US/
│       └── timeframe=1d/
│           ├── family=ta/
│           │   └── symbol=AAPL.parquet   # TA indicators (sma, rsi, macd, ...)
│           └── family=sp/
│               └── symbol=AAPL.parquet   # Stochastic-process features (ou, hmm, har, ...)
├── symbols.parquet              # asset master seen/known to the lake
├── watchlist.parquet            # watchlists
├── calendar.parquet             # trading days per market (coverage ground truth)
├── coverage.parquet             # per (market,timeframe,symbol) loaded window
└── manifest.yaml                # lake config: feed, adjustment, timezone
```

Convention: **one parquet file per symbol per timeframe per family** under the partition dirs. Bar upserts **merge by canonical timestamp** (read existing file → overlay new bars → write the full set atomically); feature persistence is a **fresh write** per family (Appender-based, no merge with existing).

## Lake MCP tools (tac-engine)

The tac-engine MCP server exposes a **lake tools** category that wraps the read/write paths below. `symbols`/`market` are uppercased, `timeframe` is normalized to lake spelling, and `start`/`end` accept `YYYY-MM-DD` or RFC-3339 (default `end=now`, `start=end-30d`).

| Tool | Purpose |
|------|---------|
| `get_lake_bars` | Cache-first bars: `{market?, symbols, timeframe, start?, end?, feed?, adjustment?, lazy?, quiet?}`. Feed defaults to `iex`; SIP is never used (requires license). For daily bars, Yahoo Finance fills gaps before Alpaca's earliest available date. `lazy=true` (default) backfills missing windows via Alpaca and persists; `lazy=false` reads the lake only. Returns `{request, source, bars: {SYM: [{t,o,h,l,c,v,n,vw}]}}`; `source` is `lake` (complete hit), `partial` (present but missing windows and `lazy=false`), or `fetched` (gaps backfilled). With `quiet=true` returns `{request, source, summary: {SYM: {count, first_t, last_t}}}` instead of the bar rows — use for backfill-to-lake jobs. Bar writes **merge by timestamp** (read existing file, overlay new bars, write the full set atomically) — safe for both tail appends and leading-gap backfills. Coverage/symbols/calendar metadata are reconciled against the actual file on each write. |
| `get_lake_ta` | Compute + optionally persist indicators: `{market?, symbol, timeframe, start?, end?, indicators?, persist?, quiet?}`. `indicators` is comma-separated, default all: `sma_5,sma_20,ema_12,ema_26,rsi_14,macd,bb,atr_14,adx_14`. Lookback is pulled automatically; returned rows cover `[start,end]`. When `persist=true`, features are written to `features/market=*/timeframe=*/family=ta/symbol=*.parquet`. With `quiet=true` returns `{count, columns, persisted}` instead of the feature rows — use when the goal is persisting indicators. |
| `get_lake_sp` | Compute + optionally persist **stochastic-process features** (`sp_*` columns) from lake bars: `{market?, symbol, timeframe, start?, end?, fit_end?, families?, persist?, quiet?}`. Rust port of `sp_features.py` on the stochastic-rs stack. `families` is comma-separated, default all: `ou,hmm,jump,har,trend,hurst,signature,moments`. In addition to `sp_rv*`/`sp_vol_ratio_*` the `har` family also emits `sp_rv_ac1` (RV lag-1 autocorr) + `sp_rv_cv_22` (RV coefficient of variation); `jump` also emits `sp_max_up`/`sp_max_down` (signed max-move asymmetry); `signature` also emits the lag-5 level-2 cross terms `sp_sig_level2_{lead_lag,lag_lead}_5`; and `moments` emits the scale-free realized skewness/kurtosis (`sp_rskew_{5,22}`, `sp_rkurt_{5,22}`; a 1-day window is undefined) and downside semi-variance (`sp_dsv_{1,5,22}`, `sp_dsv_ratio_{1,5,22}`) via stochastic-rs `realized`. `fit_end` limits the 2-state Gaussian-HMM fit window (no lookahead; posteriors still cover the whole window). On `persist=true`, features are written to `features/market=*/timeframe=*/family=sp/symbol=*.parquet`. With `quiet=true` returns `{count, sp_columns, persisted}` instead of the feature rows. Deferred (not in stochastic-rs): `garch`, `entropy`, `catch22`. |
| `get_lake_features` | Read persisted TA + SP features (hive-partitioned `features/` dir, families `ta` and `sp` merged by timestamp): `{market?, symbol?, timeframe?, start?, end?, quiet?}`. With `quiet=true` returns `{count, columns}` instead of the full feature rows. |
| `get_lake_symbols` | Read `symbols.parquet` asset master; optional `{symbol?}` filter. |
| `get_lake_watchlist` | Read `watchlist.parquet`. |
| `get_lake_calendar` | Read `calendar.parquet` trading days: `{market?, start?, end?}`. |
| `get_lake_coverage` | Read `coverage.parquet` cache index: `{market?, timeframe?, symbol?}`. |
| `get_lake_status` | Lake root, `manifest.yaml`, bar partition inventory and metadata file sizes. |
| `rebuild_lake_symbol` | Delete bar + feature parquet files and re-fetch from `TAC_LAKE_START_DATE` (default `2000-01-03`) for a single symbol: `{market?, symbol, timeframe, feed?, adjustment?}`. Resets coverage so the next `get_lake_bars` call re-downloads the full history. Use after changing `TAC_LAKE_START_DATE` or to fix stale/corrupt data. |
| `load_lake_symbols` | **Bulk-load + persist** bars + TA + SP features for a comma-separated list of symbols. Runs in a background thread and returns immediately with a `job_id`: `{market?, symbols, timeframe, start?, end?, feed?, adjustment?, indicators?, families?}`. Per-symbol start is computed automatically from lake coverage: if the lake has no data or `first_t > TAC_LAKE_START_DATE`, fetches from `TAC_LAKE_START_DATE` (default 2000-01-03); if `first_t <= TAC_LAKE_START_DATE`, fetches only from `last_t` (tail refresh). TA/SP features are always computed over the full `TAC_LAKE_START_DATE` to `end` range. Poll `load_lake_status` with the returned `job_id` to track progress. |
| `load_lake_status` | Query the status of a background bulk-load job: `{job_id}`. Returns `{job_id, status, total_symbols, processed, results, error, started_at, completed_at}` where `status` is `running`, `completed`, or `failed`, and `results` contains per-symbol bar counts, TA/SP column counts, and any errors. |
| `backfill_lake_calendar` | **Gap-fill tool**: seed/enrich `calendar.parquet` from Alpaca historical auctions (feed=iex; records exist only on trading days): `{market?, symbols, start?, end?}`. Returns `{market, symbols, start, end, calendar_days_added}`. Call this before lazy bar loads so the `1d` completeness check knows the expected trading-day set. |
| `validate_lake_dataset` | **Pre-workflow quality gate**: `{market?, timeframe?, symbols?, start?, end?}`. Scans every symbol in coverage (or a comma-separated `symbols` subset) and reports `verdict: OK/WARNINGS/ERRORS` plus per-symbol issues. Catches the failure modes qlib silently tolerates: **all-NaN feature columns** (would be dropped by `DropAllNaN` — the model trains on fewer features without notice), **missing TA/SP feature files**, **hollow coverage / stale date ranges** (coverage claims a wide span but the bar file is empty/truncated/sparse), **stale coverage** (first/last/num_bars vs the actual file), and **partition misalignment** (flat-layout feature orphans the family=ta|sp consumers can't see). Pass `start`/`end` to also check feature-vs-bar row alignment and per-column all-NaN status in that window. Call before `rd_run_workflow` / `rd_train` to fail fast instead of training on silent data holes. |
| `corr_calc` | Kendall τ-b (dampened) between **two** legs over a trailing daily-return window: `{leg_a, leg_b, window_days?, alpha?}`. Legs resolve by name — a 6-letter token is an FX pair (`market=FX`), anything else is a US-lake equity; both are pulled from the lake. Returns `rho0` and `rho=(rho0+alpha)/(1+alpha)`. |
| `corr_matrix` | Full Kendall τ-b matrix (dampened) over a comma-separated leg list: `{legs, window_days?, alpha?}`. Same leg resolution as `corr_calc`. Primary source for a worst-of basket's `market_data.correlations`. |
| `vol_surface` | **Spec (pending):** calibrated implied-vol surface for a symbol (SVI per expiry / eSSVI across expiries) with a **10-minute lake cache**: `{underlying, as_of?, max_age_secs=600, model="essvi"}`. Reads `$TAC_LAKE_DIR/vol_surfaces.parquet` when fresh, else fetches `get_option_chain`, calibrates, persists, returns the fina-risk `vol_surfaces` block. |
| `vol_surface_calibrate` | **Spec (pending):** force recalibrate + persist a symbol's surface: `{underlying, model?}`. |
| `rate_curve` | **Spec (pending):** bootstrap FRED tenor rates into a discount/projection curve: `{currency, as_of?, max_age_secs=600, day_count="ACT/365"}` → `market_data.curves[0]`. Backed by `tac-lake-macro` symbols. |
| `fx_vol_surface` | **Spec (pending):** realized/implied FX vol grid for a pair: `{pair, as_of?, max_age_secs=600}`. |

Example:
```json
{"symbols": "AAPL,MSFT", "timeframe": "1d", "start": "2026-06-06", "lazy": true}
```
→ `{"request": {...}, "source": {"AAPL": "fetched", "MSFT": "lake"}, "bars": {"AAPL": [{...}], "MSFT": [{...}]}}`

## Derived market data (correlation, vol surface, curves, FX)

Beyond raw bars, the lake backs **calibrated market data** for pricing and
research. Correlation is live (`corr_matrix`); the vol-surface, rate-curve and
FX-vol tools are specified there with their lake stores and a 10-minute cache.
See **`tac-engine/skills/tac-market-data/SKILL.md`** for the full contracts and the
mapping onto a fina-risk `pricing-request` `market_data` block.

- `corr_matrix` / `corr_calc` — Kendall τ-b (dampened) over lake closes → `market_data.correlations`.
- `vol_surface` (spec) — SVI/eSSVI surface from `get_option_chain`, cached in `vol_surfaces.parquet` → `market_data.vol_surfaces`.
- `rate_curve` (spec) — FRED tenors (`tac-lake-macro`) → `market_data.curves`.
- `fx_vol_surface` (spec) — realized/implied FX vol → FX factor.

## Quiet mode

`get_lake_bars`, `get_lake_ta`, `get_lake_sp` and `get_lake_features` accept `"quiet": true`. When the point of the call is **writing to the lake** (backfill/fetch bars, compute + persist indicators or `sp_*` features), use `quiet: true` — the tool still performs the full backfill / computation / persist, but returns a **summary** instead of echoing back the potentially huge payload (thousands of bar rows / feature rows). Full-row output (`bars` / `features`) is the default, so requests that *need* the data to read it must leave `quiet` unset/false.

| Tool | `quiet: true` response |
|------|------------------------|
| `get_lake_bars` | `{request, source: {SYM: lake\|partial\|fetched}, summary: {SYM: {count, first_t, last_t}}}` |
| `get_lake_ta` | `{market, symbol, timeframe, start, end, count, columns, persisted}` |
| `get_lake_sp` | `{market, symbol, timeframe, start, end, fit_end, count, sp_columns, persisted}` |
| `get_lake_features` | `{count, columns}` |

Backfill-to-lake job (no payload echoed):
```json
{"symbols": "AAPL,MSFT", "timeframe": "1d", "start": "2026-06-06", "lazy": true, "quiet": true}
```
→ `{"request": {...}, "source": {"AAPL": "fetched", "MSFT": "lake"}, "summary": {"AAPL": {"count": 44, "first_t": "2026-06-06T04:00:00Z", "last_t": "2026-08-05T04:00:00Z"}, "MSFT": {...}}}`

Persist indicators to the lake (summary only):
```json
{"symbol": "AAPL", "timeframe": "1d", "indicators": "sma_5,sma_20,rsi_14", "persist": true, "quiet": true}
```
→ `{"market": "US", "symbol": "AAPL", "timeframe": "1d", "count": 44, "columns": ["sma_5","sma_20","rsi_14"], "persisted": true}`

```json
{"symbols": "AAPL,MSFT", "start": "2026-06-06"}
```
→ `{"market": "US", "symbols": ["AAPL","MSFT"], "start": ..., "end": ..., "calendar_days_added": 44}`

## Conventions

- `market`: `US` (equities), `CRYPTO`, `FOREX`. Uppercase.
- `timeframe`: normalized lake name — lowercase, `1m 5m 10m 15m 30m 1h 2h 4h 1d 1w 1M`. The MCP tool spells them differently; always map:
  | Lake | MCP `timeframe` | Lake | MCP `timeframe` |
  |------|-----------------|------|-----------------|
  | `1m` | `1Min` | `2h` | `2Hour` |
  | `5m` | `5Min` | `4h` | `4Hour` |
  | `10m` | `10Min` | `1d` | `1Day` |
  | `15m` | `15Min` | `1w` | `1Week` |
  | `30m` | `30Min` | `1M` | `1Month` |
  | `1h` | `1Hour` | | |
- `symbol`: uppercase, e.g. `AAPL`. Hyphens/`.` in special symbols (e.g. `BRK-B`, `SPY`) are valid filenames; avoid `/` and spaces.
- All timestamps stored as **UTC** instants (`TIMESTAMPTZ`). Alpaca returns RFC-3339 UTC; normalize on write.
- `1d` bars: `t` is the session date at `04:00Z` (midnight ET — Alpaca stamps daily bars at `04:00:00Z`); also store a `date` column (`CAST(t AS DATE)`, UTC) for calendar joins. A date-only `end` (e.g. `2026-08-05`) is treated as **inclusive of the whole end day**, so the end-day bar is not dropped.

## Bar parquet schema (`market=…/timeframe=…/symbol=….parquet`)

| col | type | source field |
|-----|------|--------------|
| `t` | TIMESTAMPTZ | bar `t` (UTC) |
| `o` | DOUBLE | `o` |
| `h` | DOUBLE | `h` |
| `l` | DOUBLE | `l` |
| `c` | DOUBLE | `c` |
| `v` | BIGINT | `v` |
| `n` | BIGINT | `n` |
| `vw` | DOUBLE | `vw` |

Partition columns `market`/`timeframe`/`symbol` are derived from the path; DuckDB exposes them automatically when reading a hive glob.

## Metadata parquet files

All written with DuckDB `COPY … (FORMAT PARQUET)` from in-memory `SELECT`, or `pyarrow.parquet`.

`symbols.parquet`
| col | type | notes |
|-----|------|-------|
| `symbol` | VARCHAR (pk) |
| `name` | VARCHAR |
| `asset_class` | VARCHAR |
| `exchange` | VARCHAR |
| `tradable` | BOOLEAN |
| `status` | VARCHAR |
| `first_seen` | TIMESTAMPTZ | the symbol's earliest bar in the lake (its first trading date), not the load timestamp |
| `updated_at` | TIMESTAMPTZ | |

`watchlist.parquet`
| col | type |
|-----|------|
| `watchlist_id` | VARCHAR |
| `name` | VARCHAR |
| `symbol` | VARCHAR |
| `added_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

`calendar.parquet` — the trading-day ground truth per market (see “Calendar gap” below). Bars only seed which dates are trading days; per-symbol prices/session times are NOT attributed by the bars path (no symbol column, 1d bars all share `t=04:00`).
| col | type | notes |
|-----|------|-------|
| `market` | VARCHAR | pk + `date` |
| `date` | DATE | a trading day (UTC) |
| `session_open` | TIMESTAMPTZ | from auctions `o[0].t` only (nullable; not set by bars) |
| `session_close` | TIMESTAMPTZ | from auctions `c[0].t` only (nullable; not set by bars) |
| `open_price` | DOUBLE | opening auction price (nullable) |
| `close_price` | DOUBLE | closing auction price (nullable) |
| `source` | VARCHAR | `auctions` \| `bars` \| `manual` |
| `updated_at` | TIMESTAMPTZ | |

`features/` — TA + stochastic-process indicators, **wide** format, hive-partitioned with a `family` tier: `features/market=US/timeframe=1d/family=ta/symbol=AAPL.parquet` and `family=sp/symbol=AAPL.parquet`. Each row is one `t`, with one column per indicator. The partition columns (market/symbol/timeframe) come from the directory structure; the file itself stores `t` + indicator columns (e.g. `sma_5`, `sma_20`, `ema_12`, `ema_26`, `rsi_14` for `family=ta`; `sp_ou_halflife`, `sp_hmm_regime`, `sp_har_rv_5` for `family=sp`), all DOUBLE. Writes are Appender-based fresh writes per family (no read-merge-write cycle).
| col | type |
|-----|------|
| `t` | TIMESTAMPTZ |
| `sma_5`, `sma_20`, `ema_12`, `ema_26` | DOUBLE |
| `rsi_14` | DOUBLE |
| `macd`, `macd_signal`, `macd_hist` | DOUBLE |
| `bb_upper`, `bb_middle`, `bb_lower` | DOUBLE |
| `atr_14`, `adx_14`, `stoch_k`, `stoch_d` | DOUBLE |
| `_feature_<name>` | DOUBLE |

`coverage.parquet` — **the cache index**: the exact loaded window per bar set. This is what makes direct hits fast.
| col | type |
|-----|------|
| `market` | VARCHAR |
| `timeframe` | VARCHAR |
| `symbol` | VARCHAR |
| `first_t` | TIMESTAMPTZ |
| `last_t` | TIMESTAMPTZ |
| `num_bars` | BIGINT |
| `feed` | VARCHAR |
| `adjustment` | VARCHAR |
| `loaded_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

`manifest.yaml` (plain text, not parquet) — lake config so reads/writes stay consistent:
```yaml
lake_version: 1
default_market: US
default_feed: iex         # iex is the default; SIP is never used (requires license)
default_adjustment: raw    # raw|split|dividend|all — pick once per lake
timezone: UTC
features_lib: ta-lib
```

## Read path (cache-first)

### DuckDB

```bash
duckdb :memory:
```

```sql
-- hive glob adds market/timeframe/symbol columns automatically
SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=*/timeframe=*/symbol=*.parquet');
```

Canonical queries:
```sql
-- past 2 months, 1d bars
SELECT symbol, date, o, h, l, c, v, n, vw
FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=*.parquet')
WHERE symbol = 'AAPL'
  AND t >= now() - INTERVAL 2 MONTH
ORDER BY t;

-- past 2 days, 10m bars
SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=10m/symbol=*.parquet')
WHERE symbol = 'AAPL' AND t >= now() - INTERVAL 2 DAY ORDER BY t;

-- past 2 hours, 1m bars
SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1m/symbol=*.parquet')
WHERE symbol = 'AAPL' AND t >= now() - INTERVAL 2 HOUR ORDER BY t;
```

Join with features (hive-partitioned, family=ta):
```sql
SELECT b.t, b.c, f.sma_20, f.rsi_14
FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet') b
LEFT JOIN read_parquet('$TAC_LAKE_DIR/features/market=US/timeframe=1d/family=ta/symbol=AAPL.parquet') f
       ON f.t=b.t
WHERE b.t >= now() - INTERVAL 2 MONTH;
```

Join with SP features (family=sp):
```sql
SELECT b.t, b.c, sp.sp_ou_halflife, sp.sp_hmm_regime
FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet') b
LEFT JOIN read_parquet('$TAC_LAKE_DIR/features/market=US/timeframe=1d/family=sp/symbol=AAPL.parquet') sp
       ON sp.t=b.t
WHERE b.t >= now() - INTERVAL 2 MONTH;
```

### Apache Arrow / Python

```python
import pyarrow.parquet as pq
t = pq.read_table(
    "$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=*.parquet",
    filters=[("symbol", "==", "AAPL")],
)
df = t.to_pandas()
```

## Verify lake data (duckdb CLI)

Any parquet file in the lake can be inspected directly with the **DuckDB CLI** — no MCP call needed. Handy for confirming a `get_lake_bars`/`backfill_lake_calendar` write landed:

### Dependencies (duckdb + apache arrow)

`duckdb` and `pyarrow` are declared in `tac-qlib/pyproject.toml` (installed into the repo `.venv` by `uv`). If the runtime venv lacks them, **lazy-install** rather than falling back to another SQL tool:

```bash
uv pip install --python $VIRTUAL_ENV/bin/python duckdb pyarrow   # or: uv pip install -e ./tac-qlib
```

Then re-check with `python -c "import duckdb, pyarrow"`. Only use the DuckDB CLI / pyarrow path when the MCP lake tools can't answer (see MCP-first policy above).

```bash
duckdb :memory: "SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet') LIMIT 10;"
```

Or interactively:
```bash
duckdb :memory:
SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet') LIMIT 10;
```

Quick checks:
- **Bars written:** `SELECT count(*), min(t), max(t) FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet');`
- **Coverage index:** `SELECT * FROM read_parquet('$TAC_LAKE_DIR/coverage.parquet') LIMIT 10;`
- **Metadata:** `SELECT * FROM read_parquet('$TAC_LAKE_DIR/symbols.parquet') LIMIT 10;`
- **Calendar:** `SELECT * FROM read_parquet('$TAC_LAKE_DIR/calendar.parquet') LIMIT 10;`

> Note: `TAC_LAKE_DIR` is **mandatory** and must be an absolute path — do **not** use `~` or `$HOME`
> (no fallback/expansion logic exists; a literal `~` is not expanded by shells/duckdb inside an env var).

## Lazy-load write path

The core procedure when the requested range is **not** fully covered. Steps 1–9 are automated by the **`get_lake_bars`** lake tool (`lazy=true`) — the manual walk-through below documents what it does under the hood, and is the pattern to follow if writing the lake directly (DuckDB/pyarrow):

1. **Normalize the request.** `market`, lake `timeframe` (map back to MCP spelling), `symbols`, `start`, `end`. Decide `feed` and `adjustment` from `manifest.yaml` (or request overrides). Keep them fixed per lake — mixing feeds/adjustments corrupts history.
2. **Check coverage** (`coverage.parquet`). See decision table below.
3. **Compute the missing window(s).** e.g. request `[S,E]`, lake has `[S,M]` → fetch `(M,E]`; no row → fetch `[S,E]`.
4. **Call MCP `get_stock_bars`** (multi-symbol variant; comma-separated `symbols`):

   ```json
   {"symbols":"AAPL,MSFT","timeframe":"1Day","start":"2026-06-06T00:00:00Z","end":"2026-08-06T00:00:00Z","feed":"iex","adjustment":"raw","limit":10000}
   ```

   Response: `{"bars": {"AAPL": [{t,o,h,l,c,v,n,vw}, …], …}, "next_page_token": "…"}`. Bars are sorted symbol-first, so a page may contain only some symbols — **loop with `next_page_token`** until `null`.
5. **Parse + normalize.** Keep `t,o,h,l,c,v,n,vw`; convert `t` to UTC `TIMESTAMPTZ`; add `date` for `1d`.
6. **Merge into the partition file** `$TAC_LAKE_DIR/market=<m>/timeframe=<tf>/symbol=<s>.parquet`: read the existing file, overlay the fetched bars keyed by canonical timestamp (new wins on duplicate `t`), write the full merged set to a tmp file, then atomically rename over the old one. This is safe for both tail appends and leading-gap backfills (a file that already holds the newest bar still accepts older fetched history).
7. **Update `coverage.parquet`**: recompute `first_t`/`last_t`/`num_bars` from the **actual file contents** (not the fetched range) — a fetch that landed nothing must not widen the span into a hollow coverage.
8. **Update metadata**: upsert `symbols.parquet` (`first_seen` = the symbol's earliest bar in the lake) and `calendar.parquet` (distinct `date`s observed in bars, `source='bars'`; the bars path records only trading days — no session/prices).
9. **Return the requested range** from the lake (the read path above).

### Coverage decision table

For a request `(market, timeframe, symbol, S, E)` against `coverage.parquet`:

| coverage row | action |
|--------------|--------|
| missing | backfill whole `[S,E]` |
| `first_t <= S` and `last_t >= E` | **direct hit** — read from lake, no fetch |
| `first_t > S` | fetch `[S, first_t)` prefix, merge |
| `last_t < E` | fetch `(last_t, E]` suffix, merge |
| (with `calendar`) for `1d`: expected trading days `∈ [S,E]` == bars present | consider complete |

Use `calendar.parquet` for the `1d` completeness check — a weekend/holiday gap is normal, so “no bar on Saturday” must **not** trigger a refetch. Also treat the in-progress current session carefully: an intraday `end=now` should not trigger a refetch loop on the forming bar.

### Bulk loading multiple symbols

For loading bars + TA + SP features for many symbols at once, use **`load_lake_symbols`**. It runs in a background thread and returns immediately with a `job_id`:

```json
{"symbols":"AAPL,MSFT,GOOGL,AMZN","timeframe":"1d","start":"2020-01-03","end":"2026-08-15"}
```

Response:
```json
{"job_id":"load-20260815-143022","status":"started","symbols":["AAPL","MSFT","GOOGL","AMZN"],"note":"load running in background -- poll load_lake_status with this job_id to track progress"}
```

Poll progress with **`load_lake_status`**:
```json
{"job_id":"load-20260815-143022"}
```

Response (while running):
```json
{"job_id":"load-20260815-143022","status":"running","total_symbols":4,"processed":2,"results":[...]}
```

Response (when done):
```json
{"job_id":"load-20260815-143022","status":"completed","total_symbols":4,"processed":4,"results":[...],"completed_at":"2026-08-15T14:35:00Z"}
```

Each entry in `results` contains per-symbol `fetch_start` (the date the load started from), `bars_count`, `bars_source`, `ta_count`, `ta_columns`, `sp_count`, `sp_columns`, and any `*_error` fields.

### Calendar gap — why `get_stock_auctions`

The MCP tool surface has **no calendar endpoint**, but the coverage check needs to know which days are trading days before bars exist. The **auctions** tool fills this gap:

- `get_stock_auctions` only accepts `feed: "sip"` (SIP is the only valid feed for auctions).
- Auction records exist **only on trading days** → the set of distinct dates `d` across symbols is the trading-day set.
- Response shape: `{"auctions": {"AAPL": [{"d":"2026-06-09","o":[{t,x,p,c}…],"c":[{t,x,p,c}…]}, …]}, "next_page_token": "…"}` — `o` = opening auctions, `c` = closing auctions.

```json
{"symbols":"AAPL","feed":"sip","start":"2026-06-06","end":"2026-08-06"}
```

Usage: to seed/enrich `calendar.parquet` for a range **before** loading bars, call the **`backfill_lake_calendar`** lake tool (it loops `get_stock_auctions` internally across symbols and pages, inserts one row per distinct `d` with `source='auctions'`, `session_open`/`open_price` from `o[0]`, `session_close`/`close_price` from `c[0]`, and reports `calendar_days_added`). Direct call equivalent:

```json
{"symbols":"AAPL","feed":"sip","start":"2026-06-06","end":"2026-08-06"}
```

Cheap single-day confirmation for “was this a trading day?” and first pass of daily open/close. Intraday bars and per-symbol coverage still come from `get_stock_bars`.

## Operations notes

- **Rate limits:** Alpaca data API ~200 req/min. On `429` back off (exponential, start 1s) and retry. Batch symbols in one call, but page through `next_page_token`.
- **Atomicity:** write parquet to a `.<name>.tmp` then `rename()`; readers never see partial files. Apply the same pattern to metadata upserts.
- **Consistency:** one `feed` + one `adjustment` per lake (record in `manifest.yaml`). Refetching a window with a different feed/adjustment would silently corrupt merged history.
- **Feed / history limits (Alpaca):** SIP is never used (requires license). IEX goes back to **2020-07-27** for daily bars. For earlier data, Yahoo Finance fills gaps automatically (daily bars only). The `TAC_LAKE_START_DATE` (default `2000-01-03`) controls the earliest date requested; Yahoo provides data back to ~1970 for most symbols.
- **Dedup / merge:** bar upserts read the existing file, merge by canonical timestamp (new wins on duplicate `t`), and write the full set atomically — safe for both tail appends and leading-gap backfills (a file that already holds the newest bar still accepts older fetched history). Feature persistence is a fresh write per family, so no dedup needed.
- **Features** are derived from the lake bars (compute after bars are persisted, keyed `(market, symbol, timeframe, t)`), so indicator history stays aligned with bar history.

## End-to-end example (1d, 2 months, AAPL)

1. `coverage.parquet` has no `(US,1d,AAPL)` row → backfill.
2. Seed calendar: `backfill_lake_calendar` `{"symbols":"AAPL","start":…,"end":…}` (loops `get_stock_auctions`) → `calendar.parquet` trading days.
3. `get_lake_bars` `{"symbols":"AAPL","timeframe":"1d","start":…,"end":…,"lazy":true,"quiet":true}` → auto backfills the window, persists bars, updates coverage/symbols/calendar, returns a `{count, first_t, last_t}` summary instead of the bar rows.
4. Answer: DuckDB `SELECT * FROM read_parquet('$TAC_LAKE_DIR/market=US/timeframe=1d/symbol=AAPL.parquet') WHERE t >= now() - INTERVAL 2 MONTH` (or `get_lake_bars` again).
5. **Next identical request is a direct hit** (`source:"lake"`) from step 1’s decision table — no Alpaca fetch.
