//! TradeAC engine: Alpaca trading + market-data MCP server with a local
//! parquet+DuckDB lake (see `tradeac-lake` skill). Also exposes the lake as a
//! library so one-off tools (e.g. `examples/sync_lake.rs`) can backfill it.

pub mod alpaca;
pub mod config;
pub mod corr;
pub mod fx;
pub mod lake;
pub mod lake_macro;
pub mod lake_params;
pub mod lake_tools;
pub mod macro_sources;
pub mod market;
pub mod market_params;
pub mod params;
pub mod query;
pub mod server;
pub mod sp_features;
pub mod stream;
pub mod vol;
pub mod yahoo;
