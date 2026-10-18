# tac-engine

Alpaca trading + market-data **MCP server** with a local parquet + DuckDB data
lake. Exposes brokerage ops, market data, realtime streams, and a cache-first
history lake (`get_lake_bars` / `get_lake_ta` / `get_lake_sp` / …) over the
Model Context Protocol (stdio).

Standalone crate — no dependency on any external workspace or repository. Build
it from source or use the published binary.

## Build

Requires Rust stable (edition 2024).

```sh
cargo build --release
./target/release/tac-engine   # starts an MCP stdio server
```

Examples (deep-backfill tools):

```sh
cargo run --release --example sync_lake -- --start 2000-01-01          # backfill 1d bars for every lake symbol
cargo run --release --example backfill_features                        # recompute/persist 1d TA features
```

## Configuration: environment variables

Config comes from the process environment. For source builds, a `.env` file in
the **repo root** is auto-loaded (existing process env wins; copy
[`.env.example`](.env.example)). For a deployed binary, set variables in the
host environment (systemd / docker / process manager) — `.env` path is baked at
compile time and not honored from the binary's install location.

### Required

| Variable | Notes |
|----------|-------|
| `TAC_LAKE_DIR` | Data-lake root on the local filesystem (**mandatory, no default**). Must be an **absolute path**; `~` / `$HOME` are not expanded. Created if missing. |

Missing/empty `TAC_LAKE_DIR` aborts startup with an explicit error. Alpaca keys are optional, but must be set as a pair when used.

### Optional

| Variable | Default | Notes |
|----------|---------|-------|
| `APCA_API_KEY_ID` + `APCA_API_SECRET_KEY` | unset | Optional pair. Without both, daily stock history falls back to Yahoo Finance, daily FX uses free Yahoo/ECB sources, and account/order tools are omitted. Intraday bars, quotes, options, streams, and other Alpaca-only endpoints remain unavailable. |
| `APCA_BASE_URL` | `https://paper-api.alpaca.markets` | Trading REST host. Live: `https://api.alpaca.markets`. |
| `APCA_DATA_BASE_URL` | `https://data.alpaca.markets` | Market-data REST host. |
| `APCA_STREAM_BASE_URL` | `wss://stream.data.alpaca.markets` | Market-data WebSocket host. |
| `TAC_LAKE_START_DATE` | `2000-01-03` | Earliest date backfills / rebuilds fetch from. |
| `TAC_YAHOO_DISABLED` | unset | Set to `1` (or `true`) to disable the Yahoo Finance daily-bar history fallback. |
| `FRED_API_KEY` | unset | Free key from https://fred.stlouisfed.org/docs/api/api_key.html; required only to load `market=MACRO` (FRED) symbols via `load_macro_symbols`. |
| `RUST_LOG` | `info` | `tracing_subscriber` filter, e.g. `RUST_LOG=tac_engine=debug`. |

## Example `.env`

```dotenv
APCA_API_KEY_ID=your-key-id
APCA_API_SECRET_KEY=your-secret
TAC_LAKE_DIR=/absolute/path/to/lake
TAC_LAKE_START_DATE=2000-01-03
# TAC_YAHOO_DISABLED=1
# FRED_API_KEY=your-fred-key
```

## What it does / architecture

- **MCP server** (`rmcp`, stdio transport, protocol 2026-07-28): trading
  (account, positions, orders, watchlists), market data (bars, quotes, trades,
  news, corporate actions, snapshots, options, FX), stream sampling, and a
  family of **lake tools** backed by DuckDB over hive-partitioned parquet.
- **Data lake**: `TAC_LAKE_DIR` holds `market=*/timeframe=*/symbol=*.parquet`
  bar files plus `symbols`, `watchlist`, `calendar`, `coverage`, `features`,
  `manifest.yaml` metadata. Reads are cache-first with lazy backfill from
  Alpaca (IEX feed; Yahoo Finance fills pre-2016/2020 gaps). With no Alpaca
  keys, Yahoo supplies daily US stock history directly. One feed + one
  adjustment per lake.
- **Features**: TA indicators (`family=ta`), stochastic-process features
  (`family=sp`), and regime/transform features for synthetic macro symbols
  (`family=macro`) computed and persisted in the same lake.

## Additional docs

- `skills/` — agent guides: `tradeac-alpaca` (MCP tool catalog + hosts),
  `tradeac-lake` (lake layout, schema, lazy-load write path),
  `tac-lake-macro` (synthetic macro symbols).

## Development

- `cargo check` — typecheck the crate standalone (`Cargo.lock` is committed so
  remote `--locked` builds are reproducible).
- Release binaries (manylinux x86_64) are published to GitHub Releases by
  `.github/workflows/publish.yml` on `tac-engine-v*` tags.
