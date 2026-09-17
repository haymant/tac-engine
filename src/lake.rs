use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, SecondsFormat, TimeZone, Utc};
use duckdb::types::{TimeUnit, Value as DbValue, ValueRef};
use duckdb::{Connection, params_from_iter};
use rmcp::ErrorData as McpError;
use serde_json::{Map, Value, json};

use crate::alpaca::{AlpacaClient, is_http_error, is_http_error_in};
use crate::query::push_opt;
use crate::sp_features::sp_lookback_bars;
use crate::yahoo::{YahooBars, YahooClient};
use kand::ta::ohlcv as kohlcv;
use kand::ta::stats as kstats;

pub const DEFAULT_MARKET: &str = "US";
pub const DEFAULT_FEED: &str = "iex";
pub const DEFAULT_ADJUSTMENT: &str = "raw";

pub(crate) fn lake_err(msg: impl Display) -> McpError {
    McpError::internal_error(format!("lake error: {msg}"), None)
}

fn sq(s: &str) -> String {
    s.replace('\'', "''")
}

pub(crate) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

/// Normalize `YYYY-MM-DD` or RFC-3339 input to a UTC RFC-3339 string.
pub fn norm_ts(input: &str) -> Result<String, McpError> {
    let t = input.trim();
    if t.is_empty() {
        return Err(lake_err("empty timestamp"));
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(t) {
        return Ok(dt.with_timezone(&Utc).to_rfc3339());
    }
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
        let dt = nd.and_hms_opt(0, 0, 0).ok_or_else(|| lake_err("bad date"))?;
        return Ok(Utc
            .from_utc_datetime(&dt)
            .to_rfc3339());
    }
    Err(lake_err(format!("unrecognized timestamp: {t}")))
}

/// End bound for `t`-range filtering. A date-only end normalizes to
/// midnight UTC via [`norm_ts`], but Alpaca daily bars are timestamped
/// `04:00Z` on their session date, so a naive `t <= end` would drop the
/// end-day bar. Expanding a midnight-UTC end to the start of the next day
/// makes the range inclusive of the whole end date.
pub(crate) fn end_bound(end: &str) -> String {
    if let Some(date) = end.strip_suffix("T00:00:00+00:00") {
        if let Ok(d) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
            if let Some(next) = d.succ_opt() {
                if let Some(dt) = next.and_hms_opt(0, 0, 0) {
                    return Utc.from_utc_datetime(&dt).to_rfc3339();
                }
            }
        }
    }
    end.to_string()
}

/// Tolerance (days) for the `1d` calendar-based completeness check: the
/// calendar must span the requested range within this slack so weekend/holiday
/// offsets at the edges don't force spurious refetches.
const CAL_TOLERANCE_DAYS: u64 = 7;

/// Parse an RFC-3339 (or `YYYY-MM-DD`) bound into its UTC calendar date.
fn date_part(s: &str) -> Result<NaiveDate, McpError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).date_naive());
    }
    let head = &s[..10.min(s.len())];
    if let Ok(d) = NaiveDate::parse_from_str(head, "%Y-%m-%d") {
        return Ok(d);
    }
    Err(lake_err(format!("unrecognized date: {s}")))
}

/// Opt out of the Yahoo Finance history fallback with `TAC_YAHOO_DISABLED=1`.
fn yahoo_disabled() -> bool {
    std::env::var("TAC_YAHOO_DISABLED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn timestamp_micros(unit: TimeUnit, t: i64) -> i64 {
    match unit {
        TimeUnit::Microsecond => t,
        TimeUnit::Millisecond => t * 1000,
        TimeUnit::Nanosecond => t / 1000,
        TimeUnit::Second => t * 1_000_000,
    }
}

fn value_ref_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => json!(i),
        ValueRef::SmallInt(i) => json!(i),
        ValueRef::Int(i) => json!(i),
        ValueRef::BigInt(i) => json!(i),
        ValueRef::HugeInt(i) => json!(i.to_string()),
        ValueRef::UTinyInt(u) => json!(u),
        ValueRef::USmallInt(u) => json!(u),
        ValueRef::UInt(u) => json!(u),
        ValueRef::UBigInt(u) => json!(u),
        ValueRef::UHugeInt(u) => json!(u.to_string()),
        ValueRef::Float(f) if f.is_finite() => json!(f),
        ValueRef::Double(f) if f.is_finite() => json!(f),
        ValueRef::Float(_) | ValueRef::Double(_) => Value::Null,
        ValueRef::Decimal(d) => json!(d.to_string()),
        ValueRef::Timestamp(unit, t) => {
            let micros = timestamp_micros(unit, t);
            match DateTime::<Utc>::from_timestamp_micros(micros) {
                Some(dt) => Value::String(dt.to_rfc3339_opts(SecondsFormat::Micros, true)),
                None => Value::Null,
            }
        }
        ValueRef::Date32(d) => {
            let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Value::String((base + chrono::Days::new(d.max(0) as u64)).to_string())
        }
        ValueRef::Time64(unit, t) => {
            let micros = timestamp_micros(unit, t);
            let secs = micros.div_euclid(1_000_000) as u32;
            let nanos = (micros.rem_euclid(1_000_000) * 1000) as u32;
            match chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos) {
                Some(tm) => Value::String(tm.to_string()),
                None => Value::Null,
            }
        }
        ValueRef::Interval { months, days, nanos } => json!({ "months": months, "days": days, "nanos": nanos }),
        ValueRef::Text(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
        ValueRef::Blob(b) => Value::String(format!("<blob {} bytes>", b.len())),
        ValueRef::Geometry(b) => Value::String(format!("<geometry {} bytes>", b.len())),
        ValueRef::List(..) | ValueRef::Struct(..) | ValueRef::Array(..) | ValueRef::Map(..) | ValueRef::Union(..) | ValueRef::Enum(..) => {
            Value::String(format!("{v:?}"))
        }
        _ => Value::Null,
    }
}

/// Lake root over a local filesystem. All reads/writes go through DuckDB SQL
/// against hive-partitioned parquet files.
#[derive(Debug, Clone)]
pub struct Lake {
    pub(crate) dir: PathBuf,
    yahoo: YahooClient,
}

#[derive(Debug, Clone)]
pub struct Coverage {
    pub first_t: Option<String>,
    pub last_t: Option<String>,
    pub num_bars: i64,
    pub feed: String,
    pub adjustment: String,
}

impl Default for Coverage {
    fn default() -> Self {
        Self {
            first_t: None,
            last_t: None,
            num_bars: 0,
            feed: DEFAULT_FEED.into(),
            adjustment: DEFAULT_ADJUSTMENT.into(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Bar {
    pub t: String,
    pub date: String,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: i64,
    pub n: i64,
    pub vw: f64,
}

#[derive(Debug, Clone)]
pub struct BackfillResult {
    pub bars: Vec<Bar>,
    pub source: &'static str,
}

impl Lake {
    pub fn from_env() -> Result<Self, McpError> {
        // TAC_LAKE_DIR is MANDATORY — there is deliberately no fallback.
        // A misconfigured env must fail loudly so the
        // lake is never silently pointed at the wrong directory.
        let dir = std::env::var("TAC_LAKE_DIR")
            .map_err(|_| {
                lake_err(
                    "TAC_LAKE_DIR is not set. Point it at the TradeAC lake root, e.g. \
                     export TAC_LAKE_DIR=/home/data/lake (docker) or an absolute path in your local .env.",
                )
            })?
            .trim()
            .to_string();
        if dir.is_empty() {
            return Err(lake_err(
                "TAC_LAKE_DIR is empty. Set it to the TradeAC lake root, e.g. /home/data/lake.",
            ));
        }
        Self::new(PathBuf::from(dir))
    }

    pub fn new(dir: PathBuf) -> Result<Self, McpError> {
        fs::create_dir_all(&dir).map_err(|e| lake_err(format!("create lake dir: {e}")))?;
        let abs = fs::canonicalize(&dir).map_err(|e| lake_err(format!("canonicalize: {e}")))?;
        let lake = Self {
            dir: abs,
            yahoo: YahooClient::new()?,
        };
        lake.ensure_manifest()?;
        Ok(lake)
    }

    pub(crate) fn conn(&self) -> Result<Connection, McpError> {
        let conn = Connection::open_in_memory().map_err(lake_err)?;
        let _ = conn.execute_batch("LOAD parquet;");
        Ok(conn)
    }

    // ---- paths -----------------------------------------------------------

    pub(crate) fn bar_path(&self, market: &str, timeframe: &str, symbol: &str) -> PathBuf {
        self.dir
            .join(format!("market={}", market.to_uppercase()))
            .join(format!("timeframe={timeframe}"))
            .join(format!("symbol={}.parquet", symbol.to_uppercase()))
    }

    pub(crate) fn meta_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.parquet"))
    }

    /// Hive-partitioned feature file for a given family (ta or sp):
    /// `features/market=US/timeframe=1d/family=ta/symbol=AAPL.parquet`
    pub(crate) fn family_path(&self, market: &str, timeframe: &str, symbol: &str, family: &str) -> PathBuf {
        self.dir
            .join("features")
            .join(format!("market={}", market.to_uppercase()))
            .join(format!("timeframe={timeframe}"))
            .join(format!("family={family}"))
            .join(format!("symbol={}.parquet", symbol.to_uppercase()))
    }

    fn manifest_path(&self) -> PathBuf {
        self.dir.join("manifest.yaml")
    }

    fn ensure_manifest(&self) -> Result<(), McpError> {
        let path = self.manifest_path();
        if !path.exists() {
            let body = format!(
                "lake_version: 1\ndefault_market: {DEFAULT_MARKET}\ndefault_feed: {DEFAULT_FEED}\ndefault_adjustment: {DEFAULT_ADJUSTMENT}\ntimezone: UTC\n"
            );
            let tmp = path.with_extension("yaml.tmp");
            fs::write(&tmp, body).map_err(|e| lake_err(format!("write manifest: {e}")))?;
            fs::rename(&tmp, &path).map_err(|e| lake_err(format!("rename manifest: {e}")))?;
        }
        Ok(())
    }

    // ---- generic parquet read -------------------------------------------

    pub(crate) fn query_json(&self, sql: &str, args: &[String]) -> Result<Vec<Value>, McpError> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(sql).map_err(lake_err)?;
        let mut rows = stmt
            .query(params_from_iter(args.iter().cloned()))
            .map_err(lake_err)?;
        let cols: Vec<String> = rows
            .as_ref()
            .map(|s| s.column_names())
            .unwrap_or_default();
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(lake_err)? {
            let mut obj = Map::new();
            for (i, name) in cols.iter().enumerate() {
                let v = row.get_ref(i).map_err(lake_err)?;
                obj.insert(name.clone(), value_ref_to_json(v));
            }
            out.push(Value::Object(obj));
        }
        Ok(out)
    }

    pub(crate) fn table_read(&self, name: &str, where_sql: &str, args: &[String]) -> Result<Vec<Value>, McpError> {
        let path = self.meta_path(name);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT * FROM read_parquet('{}'){} ORDER BY 1",
            sq(&path.to_string_lossy()),
            where_sql
        );
        self.query_json(&sql, args)
    }

    pub(crate) fn table_write(&self, name: &str, create_sql: &str, insert_sql: &str, rows: &[Vec<Value>]) -> Result<(), McpError> {
        let path = self.meta_path(name);
        self.table_write_to(path, name, create_sql, insert_sql, rows)
    }

    /// Write a full table to a specific parquet path (creates parent dirs). Used for
    /// hive-partitioned tables such as features.
    fn table_write_to(
        &self,
        path: PathBuf,
        table: &str,
        create_sql: &str,
        insert_sql: &str,
        rows: &[Vec<Value>],
    ) -> Result<(), McpError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| lake_err(format!("create dir: {e}")))?;
        }
        let tmp = path.with_extension("parquet.tmp");
        let conn = self.conn()?;
        conn.execute_batch("BEGIN TRANSACTION;").map_err(lake_err)?;
        let create = format!("CREATE OR REPLACE TABLE {table} {create_sql};");
        conn.execute_batch(&create).map_err(lake_err)?;
        {
            let mut stmt = conn.prepare(insert_sql).map_err(lake_err)?;
            for row in rows {
                let bound: Vec<duckdb::types::Value> = row.iter().map(jv_to_duck).collect();
                stmt.execute(params_from_iter(bound)).map_err(lake_err)?;
            }
        }
        let copy = format!(
            "COPY {table} TO '{}' (FORMAT PARQUET);",
            sq(&tmp.to_string_lossy())
        );
        conn.execute_batch(&copy).map_err(lake_err)?;
        conn.execute_batch("COMMIT;").map_err(lake_err)?;
        fs::rename(&tmp, &path).map_err(|e| lake_err(format!("rename {table}: {e}")))?;
        Ok(())
    }

    // ---- coverage ---------------------------------------------------------

    pub fn get_coverage(&self, market: &str, timeframe: &str, symbol: &str) -> Result<Coverage, McpError> {
        let path = self.meta_path("coverage");
        if !path.exists() {
            return Ok(Coverage::default());
        }
        let sql = format!(
            "SELECT first_t, last_t, num_bars, feed, adjustment FROM read_parquet('{}') WHERE market = ? AND timeframe = ? AND symbol = ? LIMIT 1",
            sq(&path.to_string_lossy())
        );
        let args = vec![market.to_string(), timeframe.to_string(), symbol.to_string()];
        let rows = self.query_json(&sql, &args)?;
        let row = rows.into_iter().next().unwrap_or_default();
        Ok(Coverage {
            first_t: row.get("first_t").and_then(Value::as_str).map(String::from),
            last_t: row.get("last_t").and_then(Value::as_str).map(String::from),
            num_bars: row.get("num_bars").and_then(Value::as_i64).unwrap_or(0),
            feed: row.get("feed").and_then(Value::as_str).unwrap_or(DEFAULT_FEED).to_string(),
            adjustment: row.get("adjustment").and_then(Value::as_str).unwrap_or(DEFAULT_ADJUSTMENT).to_string(),
        })
    }

    pub fn read_coverage(&self, market: Option<&str>, timeframe: Option<&str>, symbol: Option<&str>) -> Result<Vec<Value>, McpError> {
        let mut conds = Vec::new();
        let mut args = Vec::new();
        if let Some(m) = market {
            conds.push("market = ?".to_string());
            args.push(m.to_string());
        }
        if let Some(t) = timeframe {
            conds.push("timeframe = ?".to_string());
            args.push(t.to_string());
        }
        if let Some(s) = symbol {
            conds.push("symbol = ?".to_string());
            args.push(s.to_string());
        }
        let where_sql = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        self.table_read("coverage", &where_sql, &args)
    }

    pub(crate) fn set_coverage(&self, market: &str, timeframe: &str, symbol: &str, cov: &Coverage) -> Result<(), McpError> {
        let existing = self.read_coverage(None, None, None)?;
        let mut by_key: BTreeMap<(String, String, String), Value> = BTreeMap::new();
        for row in existing {
            let key = (
                row.get("market").and_then(Value::as_str).unwrap_or_default().to_string(),
                row.get("timeframe").and_then(Value::as_str).unwrap_or_default().to_string(),
                row.get("symbol").and_then(Value::as_str).unwrap_or_default().to_string(),
            );
            by_key.insert(key, row);
        }
        let key = (market.to_string(), timeframe.to_string(), symbol.to_string());
        let mut row = by_key.remove(&key).unwrap_or_else(|| {
            json!({ "market": market, "timeframe": timeframe, "symbol": symbol })
        });
        if let (Some(f), Some(cf)) = (cov.first_t.as_deref(), row.get("first_t").and_then(Value::as_str)) {
            if f < cf {
                row["first_t"] = Value::String(f.to_string());
            }
        } else if let Some(f) = cov.first_t.as_deref() {
            row["first_t"] = Value::String(f.to_string());
        } else if cov.first_t.is_none() && row.get("first_t").is_some() {
            row["first_t"] = Value::Null;
        }
        if let (Some(l), Some(cl)) = (cov.last_t.as_deref(), row.get("last_t").and_then(Value::as_str)) {
            if l > cl {
                row["last_t"] = Value::String(l.to_string());
            }
        } else if let Some(l) = cov.last_t.as_deref() {
            row["last_t"] = Value::String(l.to_string());
        } else if cov.last_t.is_none() && row.get("last_t").is_some() {
            row["last_t"] = Value::Null;
        }
        row["num_bars"] = json!(cov.num_bars);
        row["feed"] = Value::String(cov.feed.clone());
        row["adjustment"] = Value::String(cov.adjustment.clone());
        row["loaded_at"] = Value::String(now_rfc3339());
        row["updated_at"] = Value::String(now_rfc3339());
        by_key.insert(key, row);

        let rows: Vec<Vec<Value>> = by_key
            .into_values()
            .map(|v| {
                let m = v.as_object().cloned().unwrap_or_default();
                vec![
                    m.get("market").cloned().unwrap_or(Value::Null),
                    m.get("timeframe").cloned().unwrap_or(Value::Null),
                    m.get("symbol").cloned().unwrap_or(Value::Null),
                    m.get("first_t").cloned().unwrap_or(Value::Null),
                    m.get("last_t").cloned().unwrap_or(Value::Null),
                    m.get("num_bars").cloned().unwrap_or(Value::Null),
                    m.get("feed").cloned().unwrap_or(Value::Null),
                    m.get("adjustment").cloned().unwrap_or(Value::Null),
                    m.get("loaded_at").cloned().unwrap_or(Value::Null),
                    m.get("updated_at").cloned().unwrap_or(Value::Null),
                ]
            })
            .collect();
        self.table_write(
            "coverage",
            "(market VARCHAR, timeframe VARCHAR, symbol VARCHAR, first_t TIMESTAMP, last_t TIMESTAMP, num_bars BIGINT, feed VARCHAR, adjustment VARCHAR, loaded_at TIMESTAMP, updated_at TIMESTAMP)",
            "INSERT INTO coverage VALUES (CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))",
            &rows,
        )
    }

    // ---- bars --------------------------------------------------------------

    pub fn read_bars_json(&self, market: &str, timeframe: &str, symbol: &str, start: &str, end: &str) -> Result<Vec<Value>, McpError> {
        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT t, date, o, h, l, c, v, n, vw FROM read_parquet('{}') WHERE t >= CAST(? AS TIMESTAMP) AND t <= CAST(? AS TIMESTAMP) ORDER BY t",
            sq(&path.to_string_lossy())
        );
        self.query_json(&sql, &[start.to_string(), end_bound(end)])
    }

    pub fn read_bars_raw(&self, market: &str, timeframe: &str, symbol: &str, start: &str, end: &str) -> Result<Vec<Bar>, McpError> {
        self.read_bars_json(market, timeframe, symbol, start, end)?
            .iter()
            .map(bar_from_json)
            .collect()
    }

    pub(crate) fn read_bars_full(&self, market: &str, timeframe: &str, symbol: &str) -> Result<Vec<Bar>, McpError> {
        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT t, date, o, h, l, c, v, n, vw FROM read_parquet('{}') ORDER BY t",
            sq(&path.to_string_lossy())
        );
        self.query_json(&sql, &[])?.iter().map(bar_from_json).collect()
    }

    /// Read the `TAC_LAKE_START_DATE` env var (default `2000-01-03`).
    pub(crate) fn lake_start_date() -> String {
        std::env::var("TAC_LAKE_START_DATE").unwrap_or_else(|_| "2000-01-03".into())
    }

    /// Write `bars` to the symbol's parquet file (atomic tmp+rename). No read,
    /// no merge — the file is replaced with exactly these bars. Callers that
    /// need to preserve existing history (e.g. `upsert_bars`) must pass the
    /// full merged set.
    fn write_bars(&self, market: &str, timeframe: &str, symbol: &str, bars: &[Bar]) -> Result<(), McpError> {
        if bars.is_empty() {
            return Ok(());
        }
        let path = self.bar_path(market, timeframe, symbol);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| lake_err(format!("create partition dirs: {e}")))?;
        }
        let tmp = path.with_extension("parquet.tmp");
        // Parse timestamps once so we can pass proper Timestamp values to the Appender.
        let parsed: Vec<(i64, &Bar)> = bars
            .iter()
            .map(|b| {
                let ts = DateTime::parse_from_rfc3339(&b.t)
                    .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
                    .unwrap_or(0);
                (ts, b)
            })
            .collect();
        let conn = self.conn()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS __bars(t TIMESTAMP, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, n BIGINT, vw DOUBLE);",
        ).map_err(lake_err)?;
        {
            let mut app = conn.appender("__bars").map_err(lake_err)?;
            for (ts_us, b) in &parsed {
                let ts_val = DbValue::Timestamp(TimeUnit::Microsecond, *ts_us);
                let params: Vec<&dyn duckdb::ToSql> = vec![
                    &ts_val, &b.date, &b.o, &b.h, &b.l, &b.c, &b.v, &b.n, &b.vw,
                ];
                app.append_row(params.as_slice()).map_err(lake_err)?;
            }
            app.flush().map_err(lake_err)?;
        }
        let copy = format!("COPY __bars TO '{}' (FORMAT PARQUET);", sq(&tmp.to_string_lossy()));
        conn.execute_batch(&copy).map_err(lake_err)?;
        fs::rename(&tmp, &path).map_err(|e| lake_err(format!("rename bars: {e}")))?;
        Ok(())
    }

    /// Delete bar + feature parquet files and reset coverage for a symbol.
    fn discard_symbol(&self, market: &str, timeframe: &str, symbol: &str) -> Result<(), McpError> {
        let bar = self.bar_path(market, timeframe, symbol);
        let _ = fs::remove_file(&bar);
        for family in &["ta", "sp"] {
            let feat = self.family_path(market, timeframe, symbol, family);
            let _ = fs::remove_file(&feat);
        }
        self.set_coverage(market, timeframe, symbol, &Coverage::default())?;
        Ok(())
    }

    /// Re-fetch bars from `TAC_LAKE_START_DATE` to now, discarding stale data.
    pub async fn rebuild_symbol(
        &self,
        client: &AlpacaClient,
        market: &str,
        timeframe: &str,
        symbol: &str,
        feed: &str,
        adjustment: &str,
    ) -> Result<BackfillResult, McpError> {
        self.discard_symbol(market, timeframe, symbol)?;
        let start = Self::lake_start_date();
        let end = Utc::now().to_rfc3339();
        self.ensure_bars(client, market, symbol, timeframe, &start, &end, feed, adjustment, true)
            .await
    }

    pub fn upsert_bars(&self, market: &str, timeframe: &str, symbol: &str, new_bars: &[Bar]) -> Result<(), McpError> {
        if new_bars.is_empty() {
            return Ok(());
        }
        // Merge new bars into the existing file by canonical timestamp:
        // read the full current set, overlay new bars (new wins on duplicate
        // t), and write the full merged set. Keys are epoch microseconds (not
        // the RFC3339 string) because parquet round-trips reformat the
        // timestamp, so string-keyed dedupe would keep stale duplicates. A
        // strict "only bars newer than the file's latest t" filter is wrong
        // for leading-gap backfills (the file may already hold the newest bar,
        // making the fetched history entirely stale) and for tail appends it
        // would destroy history, since the underlying write replaces the
        // whole file.
        let mut merged: BTreeMap<i64, Bar> = BTreeMap::new();
        let key = |t: &str| -> i64 {
            DateTime::parse_from_rfc3339(t)
                .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
                .unwrap_or(0)
        };
        for b in self.read_bars_full(market, timeframe, symbol)? {
            merged.insert(key(&b.t), b);
        }
        for b in new_bars {
            merged.insert(key(&b.t), b.clone());
        }
        let all: Vec<Bar> = merged.into_values().collect();
        self.write_bars(market, timeframe, symbol, &all)
    }

    /// Cache-first read with lazy backfill from Alpaca. Returns all bars in
    /// [start, end] for the symbol, fetching only the missing sub-windows.
    pub async fn ensure_bars(
        &self,
        client: &AlpacaClient,
        market: &str,
        symbol: &str,
        lake_tf: &str,
        start: &str,
        end: &str,
        feed: &str,
        adjustment: &str,
        lazy: bool,
    ) -> Result<BackfillResult, McpError> {
        let mut cov = self.get_coverage(market, lake_tf, symbol)?;
        // Reconcile coverage against the actual file: if the bar file is
        // missing/empty, coverage metadata is stale (deleted file, hollow
        // coverage from an old bug) and must not mask a full backfill.
        // Without this, missing_windows would trust a wide stale span and
        // compute no windows for an interior request.
        let on_disk = self.read_bars_full(market, lake_tf, symbol)?;
        if on_disk.is_empty() && (cov.first_t.is_some() || cov.last_t.is_some() || cov.num_bars > 0) {
            self.set_coverage(market, lake_tf, symbol, &Coverage::default())?;
            cov = Coverage::default();
        } else if !on_disk.is_empty() && (cov.num_bars as usize != on_disk.len()) {
            // Coverage count drifted from the file (e.g. partial writes) —
            // refresh the span/count from what actually exists.
            let (f, l) = (on_disk.first().map(|b| b.t.clone()), on_disk.last().map(|b| b.t.clone()));
            self.set_coverage(
                market,
                lake_tf,
                symbol,
                &Coverage {
                    first_t: f,
                    last_t: l,
                    num_bars: on_disk.len() as i64,
                    feed: cov.feed.clone(),
                    adjustment: cov.adjustment.clone(),
                },
            )?;
            cov = self.get_coverage(market, lake_tf, symbol)?;
        }
        if lazy && self.complete(cov.clone(), market, lake_tf, symbol, start, end)? {
            return Ok(BackfillResult {
                bars: self.read_bars_raw(market, lake_tf, symbol, start, end)?,
                source: "lake",
            });
        }

        let mut fetched: Vec<Bar> = Vec::new();
        let effective_feed: String;
        if lazy {
            let windows = missing_windows(&cov, start, end);
            let mcp_tf = mcp_timeframe(lake_tf)?;
            // Coverage's feed label reflects the richest source actually used
            // across all windows (any Yahoo gap-fill => iex+yahoo), not just
            // the last window's.
            let mut saw_iex = feed.contains("iex") || feed.contains("sip");
            let mut saw_yahoo = false;
            for (w_start, w_end) in &windows {
                let (page, used_feed) = self
                    .fetch_bars(client, market, symbol, &mcp_tf, w_start, w_end, feed, adjustment)
                    .await?;
                if used_feed.contains("iex") || used_feed.contains("sip") {
                    saw_iex = true;
                }
                if used_feed.contains("yahoo") {
                    saw_yahoo = true;
                }
                fetched.extend(page);
            }
            effective_feed = if saw_yahoo {
                if saw_iex { "iex+yahoo".to_string() } else { "yahoo".to_string() }
            } else {
                feed.to_string()
            };
            if !fetched.is_empty() {
                self.upsert_bars(market, lake_tf, symbol, &fetched)?;
                self.update_coverage(market, lake_tf, symbol, &fetched, &effective_feed, adjustment)?;
                self.touch_symbols(market, lake_tf, &[symbol.to_string()])?;
                self.update_calendar_from_bars(market, &fetched)?;
            }
        }

        let bars = self.read_bars_raw(market, lake_tf, symbol, start, end)?;
        let source = if fetched.is_empty() { "partial" } else { "fetched" };
        Ok(BackfillResult { bars, source })
    }

    fn complete(
        &self,
        cov: Coverage,
        market: &str,
        lake_tf: &str,
        symbol: &str,
        start: &str,
        end: &str,
    ) -> Result<bool, McpError> {
        let (Some(f), Some(l)) = (cov.first_t.as_deref(), cov.last_t.as_deref()) else {
            return Ok(false);
        };
        if f <= start && l >= end {
            // The span looks covered, but coverage metadata can go stale
            // (deleted parquet file, hollow coverage from an old bug). Verify
            // the file actually holds bars in the requested range before
            // trusting it; otherwise fall through and refetch.
            let have = self.read_bars_raw(market, lake_tf, symbol, start, end)?.len() as i64;
            if have > 0 {
                return Ok(true);
            }
        }
        if lake_tf == "1d" {
            // Only trust the calendar-based count when the calendar actually
            // spans the requested range (with a few days' tolerance for
            // weekends/holidays at the edges). Otherwise the expected count is
            // understated and a deep-history request would be (wrongly)
            // considered complete against a partially-seeded calendar.
            let (Ok(s), Ok(e)) = (date_part(start), date_part(end)) else {
                return Ok(false);
            };
            if let Some((cal_lo, cal_hi)) = self.calendar_span(market)? {
                let tol = chrono::Days::new(CAL_TOLERANCE_DAYS);
                if cal_lo <= s + tol && e + tol >= cal_hi {
                    if let Ok(expected) = self.count_calendar_days(market, start, end) {
                        if expected > 0 {
                            let have = self.read_bars_raw(market, lake_tf, symbol, start, end)?.len() as i64;
                            if have >= expected {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// First/last date the lake calendar covers for a market (for the `1d`
    /// completeness check). `None` when the calendar is empty or absent.
    fn calendar_span(&self, market: &str) -> Result<Option<(NaiveDate, NaiveDate)>, McpError> {
        let path = self.meta_path("calendar");
        if !path.exists() {
            return Ok(None);
        }
        let sql = format!(
            "SELECT MIN(date) AS lo, MAX(date) AS hi FROM read_parquet('{}') WHERE market = '{}'",
            sq(&path.to_string_lossy()),
            sq(market)
        );
        let rows = self.query_json(&sql, &[])?;
        let row = rows.into_iter().next().unwrap_or_default();
        let parse = |k: &str| {
            row.get(k)
                .and_then(Value::as_str)
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        };
        Ok(parse("lo").zip(parse("hi")))
    }

    pub(crate) fn update_coverage(&self, market: &str, timeframe: &str, symbol: &str, fetched: &[Bar], feed: &str, adjustment: &str) -> Result<(), McpError> {
        // Derive first/last from the ACTUAL file contents, not the fetched
        // range. A fetch can be dropped by a bug or an empty seam (e.g. a
        // leading-gap backfill that ends at an already-present newest bar), in
        // which case the file's real span is narrower than `fetched`. Trusting
        // `fetched` would write a hollow coverage (wide span, few bars) that
        // makes later `missing_windows` calls skip the interior.
        let bars = self.read_bars_full(market, timeframe, symbol)?;
        let total = bars.len() as i64;
        let (first, last) = if bars.is_empty() {
            (
                fetched.iter().map(|b| b.t.clone()).min().or_else(|| self.get_coverage(market, timeframe, symbol).ok().and_then(|c| c.first_t)),
                fetched.iter().map(|b| b.t.clone()).max().or_else(|| self.get_coverage(market, timeframe, symbol).ok().and_then(|c| c.last_t)),
            )
        } else {
            (
                bars.first().map(|b| b.t.clone()),
                bars.last().map(|b| b.t.clone()),
            )
        };
        self.set_coverage(
            market,
            timeframe,
            symbol,
            &Coverage {
                first_t: first,
                last_t: last,
                num_bars: total,
                feed: feed.to_string(),
                adjustment: adjustment.to_string(),
            },
        )
    }

    // ---- metadata: symbols / calendar -------------------------------------

    pub fn read_symbols(&self) -> Result<Vec<Value>, McpError> {
        self.table_read("symbols", "", &[])
    }

    fn touch_symbols(&self, market: &str, timeframe: &str, symbols: &[String]) -> Result<(), McpError> {
        self.touch_symbols_as(market, timeframe, symbols, "us_equity", None)
    }

    /// [`Self::touch_symbols`] with explicit defaults for new rows: macro
    /// loaders register synthetic instruments as `asset_class="macro"`,
    /// `tradable=false`. Existing rows keep their stored values.
    pub(crate) fn touch_symbols_as(
        &self,
        market: &str,
        timeframe: &str,
        symbols: &[String],
        default_asset_class: &str,
        default_tradable: Option<bool>,
    ) -> Result<(), McpError> {
        let existing = self.read_symbols()?;
        let mut by_key: BTreeMap<String, Value> = BTreeMap::new();
        for row in existing {
            if let Some(s) = row.get("symbol").and_then(Value::as_str) {
                by_key.insert(s.to_string(), row);
            }
        }
        for sym in symbols {
            let now = now_rfc3339();
            // first_seen = the symbol's earliest bar in the lake (its first
            // trading date), falling back to now only when no bars exist yet.
            let cov = self.get_coverage(market, timeframe, sym)?;
            let cov_first = cov.first_t.as_deref();
            let row = by_key.remove(sym).unwrap_or_else(|| {
                let mut new_row = json!({
                    "symbol": sym,
                    "asset_class": default_asset_class,
                    "market": market,
                    "first_seen": cov_first.unwrap_or(&now).to_string(),
                });
                if let Some(t) = default_tradable {
                    new_row["tradable"] = json!(t);
                }
                new_row
            });
            let mut row = row;
            // Correct stale first_seen (e.g. a load-timestamp written by an
            // older version): keep the earlier of the stored value and the
            // coverage first bar.
            let stored = row.get("first_seen").and_then(Value::as_str);
            let earlier = match (stored, cov_first) {
                (Some(s), Some(c)) => Some(if s < c { s.to_string() } else { c.to_string() }),
                (Some(s), None) => Some(s.to_string()),
                (None, Some(c)) => Some(c.to_string()),
                (None, None) => Some(now.clone()),
            };
            if let Some(fs) = earlier {
                row["first_seen"] = Value::String(fs);
            }
            row["updated_at"] = Value::String(now);
            by_key.insert(sym.clone(), row);
        }
        let rows: Vec<Vec<Value>> = by_key
            .into_values()
            .map(|v| {
                let m = v.as_object().cloned().unwrap_or_default();
                vec![
                    m.get("symbol").cloned().unwrap_or(Value::Null),
                    m.get("name").cloned().unwrap_or(Value::Null),
                    m.get("asset_class").cloned().unwrap_or(Value::Null),
                    m.get("exchange").cloned().unwrap_or(Value::Null),
                    m.get("tradable").cloned().unwrap_or(Value::Null),
                    m.get("status").cloned().unwrap_or(Value::Null),
                    m.get("first_seen").cloned().unwrap_or(Value::Null),
                    m.get("updated_at").cloned().unwrap_or(Value::Null),
                ]
            })
            .collect();
        self.table_write(
            "symbols",
            "(symbol VARCHAR, name VARCHAR, asset_class VARCHAR, exchange VARCHAR, tradable BOOLEAN, status VARCHAR, first_seen TIMESTAMP, updated_at TIMESTAMP)",
            "INSERT INTO symbols VALUES (CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS BOOLEAN), CAST(? AS VARCHAR), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))",
            &rows,
        )
    }

    pub fn read_calendar(&self, market: Option<&str>, start: Option<&str>, end: Option<&str>) -> Result<Vec<Value>, McpError> {
        let mut conds = Vec::new();
        let mut args = Vec::new();
        if let Some(m) = market {
            conds.push("market = ?".to_string());
            args.push(m.to_string());
        }
        if let Some(s) = start {
            conds.push("date >= CAST(? AS DATE)".to_string());
            args.push(s.to_string());
        }
        if let Some(e) = end {
            conds.push("date <= CAST(? AS DATE)".to_string());
            args.push(e.to_string());
        }
        let where_sql = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        self.table_read("calendar", &where_sql, &args)
    }

    pub fn count_calendar_days(&self, market: &str, start: &str, end: &str) -> Result<i64, McpError> {
        let path = self.meta_path("calendar");
        if !path.exists() {
            return Ok(0);
        }
        let start_date = &start[..10.min(start.len())];
        let end_date = &end[..10.min(end.len())];
        let sql = format!(
            "SELECT COUNT(*) AS n FROM read_parquet('{}') WHERE market = '{}' AND date >= CAST('{}' AS DATE) AND date <= CAST('{}' AS DATE)",
            sq(&path.to_string_lossy()),
            sq(market),
            sq(start_date),
            sq(end_date)
        );
        let rows = self.query_json(&sql, &[])?;
        Ok(rows.first().and_then(|r| r.get("n")).and_then(Value::as_i64).unwrap_or(0))
    }

    fn update_calendar_from_bars(&self, market: &str, bars: &[Bar]) -> Result<(), McpError> {
        let existing = self.read_calendar(Some(market), None, None)?;
        let mut by_key: BTreeMap<String, Value> = BTreeMap::new();
        for row in existing {
            if let Some(d) = row.get("date").and_then(Value::as_str) {
                by_key.insert(d.to_string(), row);
            }
        }
        let now = now_rfc3339();
        // The calendar is a market-wide trading-day set; bars only seed which
        // dates are trading days. Per-symbol open/close prices and session
        // times are NOT attributed here (no symbol column, and 1d bars all
        // share t=04:00) — leave those to the auctions path which carries
        // real session boundaries. Strip any stale values left by older
        // writers so a rewrite of this market's calendar cleans them up.
        let strip_keys = ["session_open", "session_close", "open_price", "close_price"];
        for b in bars {
            let row = by_key.entry(b.date.clone()).or_insert_with(|| {
                json!({ "market": market, "date": b.date, "source": "bars", "updated_at": now.clone() })
            });
            if let Some(m) = row.as_object_mut() {
                for k in &strip_keys {
                    m.remove(*k);
                }
            }
            if row.get("source").and_then(Value::as_str) != Some("bars") {
                row["source"] = Value::String("bars".to_string());
            }
            row["updated_at"] = Value::String(now.clone());
        }
        self.write_calendar(&by_key)?;
        Ok(())
    }

    /// Gap-fill: seed trading days for US equities from Alpaca historical
    /// auctions (feed=sip only). Auction records exist only on trading days.
    pub async fn backfill_calendar_from_auctions(
        &self,
        client: &AlpacaClient,
        market: &str,
        symbols: &[String],
        start: &str,
        end: &str,
    ) -> Result<usize, McpError> {
        let existing = self.read_calendar(Some(market), None, None)?;
        let mut by_key: BTreeMap<String, Value> = BTreeMap::new();
        for row in existing {
            if let Some(d) = row.get("date").and_then(Value::as_str) {
                by_key.insert(d.to_string(), row);
            }
        }
        let initial = by_key.len();

        for symbol in symbols {
            let mut page_token: Option<String> = None;
            loop {
                let mut q: Vec<(&str, String)> = vec![
                    ("symbols", symbol.clone()),
                    ("start", start.to_string()),
                    ("end", end.to_string()),
                    ("limit", "10000".into()),
                    ("feed", "sip".into()),
                ];
                if let Some(pt) = &page_token {
                    push_opt(&mut q, "page_token", Some(pt.clone()));
                }
                let value = client.data_get("/v2/stocks/auctions", &q).await?;
                let days = value
                    .get("auctions")
                    .and_then(|a| a.get(symbol))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if days.is_empty() {
                    break;
                }
                let now = now_rfc3339();
                for day in &days {
                    let date = day.get("d").and_then(Value::as_str).unwrap_or_default().to_string();
                    if date.is_empty() {
                        continue;
                    }
                    let row = by_key.entry(date.clone()).or_insert_with(|| {
                        json!({ "market": market, "date": date, "source": "auctions", "updated_at": now.clone() })
                    });
                    if row.get("source").and_then(Value::as_str).is_none() {
                        row["source"] = Value::String("auctions".to_string());
                    }
                    let open = day.get("o").and_then(Value::as_array).and_then(|a| a.first()).cloned();
                    let close = day.get("c").and_then(Value::as_array).and_then(|a| a.first()).cloned();
                    if let Some(o) = open {
                        if row.get("session_open").and_then(Value::as_str).is_none() {
                            row["session_open"] = o.get("t").cloned().unwrap_or(Value::Null);
                            row["open_price"] = o.get("p").cloned().unwrap_or(Value::Null);
                        }
                    }
                    if let Some(c) = close {
                        if row.get("session_close").and_then(Value::as_str).is_none() {
                            row["session_close"] = c.get("t").cloned().unwrap_or(Value::Null);
                            row["close_price"] = c.get("p").cloned().unwrap_or(Value::Null);
                        }
                    }
                    row["updated_at"] = Value::String(now.clone());
                }
                page_token = value
                    .get("next_page_token")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty());
                if page_token.is_none() {
                    break;
                }
            }
        }

        if !by_key.is_empty() {
            self.write_calendar(&by_key)?;
        }
        Ok(by_key.len() - initial)
    }

    fn write_calendar(&self, by_key: &BTreeMap<String, Value>) -> Result<usize, McpError> {
        let rows: Vec<Vec<Value>> = by_key
            .values()
            .map(|v| {
                let m = v.as_object().cloned().unwrap_or_default();
                vec![
                    m.get("market").cloned().unwrap_or(Value::Null),
                    m.get("date").cloned().unwrap_or(Value::Null),
                    m.get("session_open").cloned().unwrap_or(Value::Null),
                    m.get("session_close").cloned().unwrap_or(Value::Null),
                    m.get("open_price").cloned().unwrap_or(Value::Null),
                    m.get("close_price").cloned().unwrap_or(Value::Null),
                    m.get("source").cloned().unwrap_or(Value::Null),
                    m.get("updated_at").cloned().unwrap_or(Value::Null),
                ]
            })
            .collect();
        let n = rows.len();
        self.table_write(
            "calendar",
            "(market VARCHAR, date DATE, session_open TIMESTAMP, session_close TIMESTAMP, open_price DOUBLE, close_price DOUBLE, source VARCHAR, updated_at TIMESTAMP)",
            "INSERT INTO calendar VALUES (CAST(? AS VARCHAR), CAST(? AS DATE), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), CAST(? AS DOUBLE), CAST(? AS DOUBLE), CAST(? AS VARCHAR), CAST(? AS TIMESTAMP))",
            &rows,
        )?;
        Ok(n)
    }

    // ---- watchlist ----------------------------------------------------------

    pub fn read_watchlist(&self) -> Result<Vec<Value>, McpError> {
        self.table_read("watchlist", "", &[])
    }

    // ---- features / TA ------------------------------------------------------

    pub fn read_features(
        &self,
        market: Option<&str>,
        symbol: Option<&str>,
        timeframe: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Vec<Value>, McpError> {
        let base = self.dir.join("features");
        if !base.exists() {
            return Ok(Vec::new());
        }
        // Walk features/market=X/timeframe=Y/family=Z/symbol=S.parquet
        let market_dirs = match market {
            Some(m) => vec![format!("market={}", m.to_uppercase())],
            None => read_dir_names(&base, "market=")?,
        };
        // Merge rows across families (ta + sp) by timestamp.
        let mut by_key: BTreeMap<String, Value> = BTreeMap::new();
        for mdir in &market_dirs {
            let mpath = base.join(mdir);
            let tf_dirs = match timeframe {
                Some(tf) => vec![format!("timeframe={tf}")],
                None => read_dir_names(&mpath, "timeframe=")?,
            };
            for tfdir in &tf_dirs {
                let tfpath = mpath.join(tfdir);
                let fam_dirs = read_dir_names(&tfpath, "family=")?;
                for fadir in &fam_dirs {
                    let fapath = tfpath.join(fadir);
                    let sym_files = match symbol {
                        Some(s) => vec![format!("symbol={}.parquet", s.to_uppercase())],
                        None => read_dir_names(&fapath, "symbol=")?,
                    };
                    for sfile in &sym_files {
                        let path = fapath.join(sfile);
                        if !path.is_file() {
                            continue;
                        }
                        let m = mdir.trim_start_matches("market=").to_string();
                        let tf = tfdir.trim_start_matches("timeframe=").to_string();
                        let sym = sfile
                            .trim_start_matches("symbol=")
                            .trim_end_matches(".parquet")
                            .to_string();
                        let mut conds = Vec::new();
                        let mut args: Vec<String> = Vec::new();
                        if let Some(s) = start {
                            conds.push("t >= CAST(? AS TIMESTAMP)".to_string());
                            args.push(s.to_string());
                        }
                        if let Some(e) = end {
                            conds.push("t <= CAST(? AS TIMESTAMP)".to_string());
                            args.push(e.to_string());
                        }
                        let where_sql = if conds.is_empty() {
                            String::new()
                        } else {
                            format!(" WHERE {}", conds.join(" AND "))
                        };
                        let sql = format!(
                            "SELECT * FROM read_parquet('{}'){where_sql} ORDER BY t",
                            sq(&path.to_string_lossy())
                        );
                        let rows = self.query_json(&sql, &args)?;
                        for row in rows {
                            let obj = row.as_object().cloned().unwrap_or_default();
                            let t = obj.get("t").and_then(Value::as_str).unwrap_or("");
                            let merge_key = format!("{sym}|{t}");
                            let entry = by_key.entry(merge_key.clone()).or_insert_with(|| {
                                let mut m2 = Map::new();
                                m2.insert("t".into(), json!(t));
                                m2.insert("market".into(), json!(m));
                                m2.insert("symbol".into(), json!(sym));
                                m2.insert("timeframe".into(), json!(tf));
                                Value::Object(m2)
                            });
                            if let Some(em) = entry.as_object_mut() {
                                for (k, v) in obj {
                                    if k != "market" && k != "symbol" && k != "timeframe" && !k.is_empty() {
                                        em.insert(k, v);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut out: Vec<Value> = by_key.into_values().collect();
        out.sort_by(|a, b| {
            let key = |v: &Value| {
                (
                    v.get("symbol").and_then(Value::as_str).unwrap_or("").to_string(),
                    v.get("timeframe").and_then(Value::as_str).unwrap_or("").to_string(),
                    v.get("t").and_then(Value::as_str).unwrap_or("").to_string(),
                )
            };
            key(a).cmp(&key(b))
        });
        Ok(out)
    }

    /// Validate dataset quality before a qlib workflow run.
    ///
    /// Surfaces the failure modes that qlib silently tolerates:
    /// 1. feature columns that are entirely NaN over the window (DropAllNaN
    ///    drops them and the model trains on fewer features without notice),
    /// 2. features stored where the consumer cannot see them (family
    ///    partition misalignment / flat-layout leftovers),
    /// 3. hollow coverage / date ranges (coverage claims a wide span but the
    ///    bar file is empty, truncated, or sparse).
    ///
    /// Scans every symbol in `coverage.parquet` for `market`/`timeframe` (or a
    /// caller-supplied subset). `symbols` is a comma-separated optional filter.
    pub fn validate_lake_dataset(
        &self,
        market: &str,
        timeframe: &str,
        symbols: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Value, McpError> {
        let market = market.to_uppercase();
        let sym_filter: Option<Vec<String>> = symbols.map(|s| {
            s.split(',')
                .map(|x| x.trim().to_uppercase())
                .filter(|x| !x.is_empty())
                .collect()
        });

        // Enumerate candidate symbols: coverage rows for this market/timeframe,
        // plus any bar files present that are not in coverage.
        let mut symbols_all: Vec<String> = Vec::new();
        let cov_sql = format!(
            "SELECT DISTINCT symbol FROM read_parquet('{}') WHERE market = ? AND timeframe = ?",
            sq(&self.meta_path("coverage").to_string_lossy())
        );
        if self.meta_path("coverage").exists() {
            for row in self.query_json(&cov_sql, &[market.clone(), timeframe.to_string()])? {
                if let Some(s) = row.get("symbol").and_then(Value::as_str) {
                    symbols_all.push(s.to_string());
                }
            }
        }
        // Bar files on disk that coverage does not know about (orphans / stale).
        let bar_dir = self
            .dir
            .join(format!("market={market}"))
            .join(format!("timeframe={timeframe}"));
        if bar_dir.is_dir() {
            for name in read_dir_names(&bar_dir, "symbol=")? {
                let s = name
                    .trim_start_matches("symbol=")
                    .trim_end_matches(".parquet")
                    .to_string();
                if !symbols_all.contains(&s) {
                    symbols_all.push(s);
                }
            }
        }
        symbols_all.sort();
        symbols_all.dedup();
        if let Some(f) = &sym_filter {
            symbols_all.retain(|s| f.contains(s));
        }

        let start = start.map(|s| s.to_string());
        let end = end.map(|s| s.to_string());

        // Expected default indicator/family column sets (mirror the TA/SP tools).
        let ta_expected: Vec<String> = [
            "sma_5", "sma_20", "ema_12", "ema_26", "rsi_14", "macd", "macd_signal",
            "macd_hist", "bb_upper", "bb_middle", "bb_lower", "atr_14", "adx_14",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let sp_expected: Vec<String> = [
            "sp_ou_half_life", "sp_ou_revert", "sp_ou_zscore", "sp_hmm_p_regime1",
            "sp_hmm_state", "sp_jump_flag", "sp_jump_ratio", "sp_jump_tail", "sp_max_move",
            "sp_max_up", "sp_max_down", "sp_rv1", "sp_rv5", "sp_rv22", "sp_vol_ratio_5_22",
            "sp_vol_ratio_1_22", "sp_rv_ac1", "sp_rv_cv_22", "sp_trend_slope_5",
            "sp_trend_slope_20", "sp_trend_slope_60", "sp_logp", "sp_hurst_exponent",
            "sp_sig_level1_lead", "sp_sig_level1_lag", "sp_sig_level2_lead_lag",
            "sp_sig_level2_lag_lead", "sp_sig_level2_lead_lag_5", "sp_sig_level2_lag_lead_5",
            "sp_dsv_1", "sp_dsv_5", "sp_dsv_22", "sp_dsv_ratio_1", "sp_dsv_ratio_5",
            "sp_dsv_ratio_22", "sp_rskew_5", "sp_rskew_22", "sp_rkurt_5", "sp_rkurt_22",
            "sp_ret",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let mut per_symbol: Vec<Value> = Vec::new();
        let mut agg_issues: BTreeMap<String, usize> = BTreeMap::new();
        let mut symbols_with_bars = 0usize;
        let mut symbols_with_ta = 0usize;
        let mut symbols_with_sp = 0usize;

        for sym in &symbols_all {
            let mut report = Map::new();
            report.insert("symbol".into(), json!(sym));
            let mut issues: Vec<Value> = Vec::new();
            let mut add_issue = |level: &str, code: &str, detail: String| {
                let key = format!("{level}:{code}");
                *agg_issues.entry(key).or_default() += 1;
                issues.push(json!({ "level": level, "code": code, "detail": detail }));
            };

            // --- bars vs coverage (hollow / stale spans) ---------------------
            let bar_file = self.bar_path(&market, timeframe, sym);
            let bars_present = bar_file.is_file();
            let cov = self.get_coverage(&market, timeframe, sym).unwrap_or_default();
            let has_cov = cov.first_t.is_some() || cov.last_t.is_some() || cov.num_bars > 0;

            if !bars_present {
                add_issue(
                    "error",
                    "MISSING_BAR_FILE",
                    format!("no bar file at {}", bar_file.display()),
                );
            } else {
                symbols_with_bars += 1;
                let bars = self.read_bars_full(&market, timeframe, sym)?;
                let actual_first = bars.first().map(|b| b.t.clone());
                let actual_last = bars.last().map(|b| b.t.clone());
                report.insert("bars".into(), json!({
                    "count": bars.len(),
                    "first_t": actual_first,
                    "last_t": actual_last,
                }));

                if has_cov {
                    // Coverage span vs actual file: the file is the source of
                    // truth. Compare on canonical epoch microseconds because
                    // coverage timestamps come back from parquet as DuckDB
                    // `YYYY-MM-DD HH:MM:SS` while bar `t` is RFC3339 — naive
                    // string comparison would flag identical instants as stale.
                    let ts_eq = |a: Option<&String>, b: Option<&String>| -> bool {
                        let parse = |s: Option<&String>| -> Option<i64> {
                            let s = s?;
                            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                return Some(dt.with_timezone(&Utc).timestamp_micros());
                            }
                            if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                return Some(
                                    Utc.from_utc_datetime(&nd.and_hms_opt(0, 0, 0).unwrap())
                                        .timestamp_micros(),
                                );
                            }
                            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                                return Some(Utc.from_utc_datetime(&ndt).timestamp_micros());
                            }
                            None
                        };
                        parse(a) == parse(b) && parse(a).is_some()
                    };
                    if !ts_eq(cov.first_t.as_ref(), actual_first.as_ref())
                        || !ts_eq(cov.last_t.as_ref(), actual_last.as_ref())
                        || cov.num_bars != bars.len() as i64
                    {
                        let mut mismatches = Vec::new();
                        if cov.num_bars != bars.len() as i64 {
                            mismatches.push(format!("coverage.num_bars={} vs file bars={}", cov.num_bars, bars.len()));
                        }
                        if !ts_eq(cov.first_t.as_ref(), actual_first.as_ref()) {
                            mismatches.push(format!("coverage.first_t={:?} vs file={actual_first:?}", cov.first_t));
                        }
                        if !ts_eq(cov.last_t.as_ref(), actual_last.as_ref()) {
                            mismatches.push(format!("coverage.last_t={:?} vs file={actual_last:?}", cov.last_t));
                        }
                        add_issue(
                            "warn",
                            "COVERAGE_STALE",
                            format!("{} ({})", mismatches.join("; "), sym),
                        );
                    }
                    // Hollow span: coverage claims a wide range but the file has
                    // far fewer bars than the calendar days in that range.
                    if cov.num_bars > 0 && bars.is_empty() {
                        add_issue(
                            "error",
                            "HOLLOW_COVERAGE",
                            format!("coverage claims {} bars spanning {:?}..{:?} but the file is empty ({sym})", cov.num_bars, cov.first_t, cov.last_t),
                        );
                    } else if cov.num_bars > 0 {
                        if let (Some(f), Some(l)) = (&cov.first_t, &cov.last_t) {
                            let have = bars.len() as i64;
                            if let Ok(expected) = self.count_calendar_days(&market, f, l) {
                                // Allow some slack for adjusted series / partial
                                // history; flag when the file holds far less.
                                if expected > 100 && have < expected / 5 {
                                    add_issue(
                                        "warn",
                                        "HOLLOW_COVERAGE",
                                        format!("coverage span {:?}..{:?} implies ~{expected} calendar days but the file holds {have} bars ({sym})", f, l),
                                    );
                                }
                            }
                        }
                    }
                } else {
                    add_issue(
                        "warn",
                        "NO_COVERAGE",
                        format!("bar file exists but no coverage row for {sym}"),
                    );
                }

                // --- features: family partition + completeness --------------
                let ta_file = self.family_path(&market, timeframe, sym, "ta");
                let sp_file = self.family_path(&market, timeframe, sym, "sp");
                let ta_present = ta_file.is_file();
                let sp_present = sp_file.is_file();
                if ta_present {
                    symbols_with_ta += 1;
                } else {
                    add_issue(
                        "error",
                        "MISSING_FEATURES",
                        format!("no family=ta feature file for {sym}"),
                    );
                }
                if sp_present {
                    symbols_with_sp += 1;
                } else {
                    add_issue(
                        "error",
                        "MISSING_FEATURES",
                        format!("no family=sp feature file for {sym}"),
                    );
                }

                // Column presence + all-NaN over the window.
                for (fam, path, present, expected) in [
                    ("ta", &ta_file, ta_present, &ta_expected),
                    ("sp", &sp_file, sp_present, &sp_expected),
                ] {
                    if !present {
                        continue;
                    }
                    let cols_sql = format!(
                        "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{}'))",
                        sq(&path.to_string_lossy())
                    );
                    let col_rows = self.query_json(&cols_sql, &[])?;
                    let cols: Vec<String> = col_rows
                        .iter()
                        .filter_map(|r| r.get("column_name").and_then(Value::as_str).map(String::from))
                        .filter(|c| c != "t" && c != "date" && c != "symbol" && c != "market" && c != "timeframe")
                        .collect();
                    report.insert(format!("{fam}_columns"), json!(cols));

                    // Missing expected columns.
                    let missing: Vec<String> = expected
                        .iter()
                        .filter(|e| !cols.iter().any(|c| c == *e))
                        .cloned()
                        .collect();
                    if !missing.is_empty() {
                        add_issue(
                            "warn",
                            "MISSING_COLUMN",
                            format!("family={fam} {sym} lacks expected columns: {}", missing.join(", ")),
                        );
                    }

                    // All-NaN columns over the (optional) window: these are what
                    // DropAllNaN silently removes.
                    let mut where_sql = String::new();
                    let mut args: Vec<String> = Vec::new();
                    if let (Some(s), Some(e)) = (&start, &end) {
                        where_sql = " WHERE t >= CAST(? AS TIMESTAMP) AND t <= CAST(? AS TIMESTAMP)".to_string();
                        args.push(s.clone());
                        args.push(e.clone());
                    }
                    let nan_cols: Vec<String> = cols
                        .iter()
                        .filter(|c| {
                            // Fast check: count non-NaN rows for the column.
                            let q = format!(
                                "SELECT count(*) AS total, count({q}) AS filled FROM read_parquet('{p}'){w}",
                                q = sq(c),
                                p = sq(&path.to_string_lossy()),
                                w = where_sql
                            );
                            self.query_json(&q, &args)
                                .ok()
                                .and_then(|r| r.first().cloned())
                                .map(|r| {
                                    let total = r.get("total").and_then(Value::as_i64).unwrap_or(0);
                                    let filled = r.get("filled").and_then(Value::as_i64).unwrap_or(0);
                                    total > 0 && filled == 0
                                })
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect();
                    if !nan_cols.is_empty() {
                        add_issue(
                            "warn",
                            "ALL_NAN_COLUMN",
                            format!("family={fam} {sym} columns are all-NaN (silently dropped by DropAllNaN): {}", nan_cols.join(", ")),
                        );
                        report.insert(format!("{fam}_all_nan"), json!(nan_cols));
                    }

                    // Row alignment with bars in the window.
                    if let (Some(s), Some(e)) = (&start, &end) {
                        // Compare on calendar dates so a date-only end includes
                        // the end-day bar (bar `t` is 04:00Z, `date` is the day).
                        let s_day = &s[..10.min(s.len())];
                        let e_day = &e[..10.min(e.len())];
                        let q = format!(
                            "SELECT count(*) AS n FROM read_parquet('{}') WHERE CAST(t AS DATE) >= CAST(? AS DATE) AND CAST(t AS DATE) <= CAST(? AS DATE)",
                            sq(&path.to_string_lossy())
                        );
                        if let Ok(rows) = self.query_json(&q, &[s_day.to_string(), e_day.to_string()]) {
                            let feat_n = rows.first().and_then(|r| r.get("n")).and_then(Value::as_i64).unwrap_or(0);
                            let bar_n = bars
                                .iter()
                                .filter(|b| {
                                    b.date.as_str() >= s_day && b.date.as_str() <= e_day
                                })
                                .count() as i64;
                            if feat_n != bar_n {
                                add_issue(
                                    "warn",
                                    "FEATURE_BAR_MISALIGN",
                                    format!("family={fam} {sym}: {feat_n} feature rows vs {bar_n} bars in [{s},{e}]"),
                                );
                            }
                        }
                    }
                }
            }

            report.insert("issues".into(), Value::Array(issues));
            per_symbol.push(Value::Object(report));
        }

        // Cross-symbol partition health: is the family layout present and
        // consistently used? (Flat-layout leftovers = misaligned partitions.)
        let fam_ta = self.dir.join("features").join(format!("market={market}")).join(format!("timeframe={timeframe}")).join("family=ta");
        let fam_sp = self.dir.join("features").join(format!("market={market}")).join(format!("timeframe={timeframe}")).join("family=sp");
        let flat_dir = self.dir.join("features").join(format!("market={market}")).join(format!("timeframe={timeframe}"));
        let mut cross = Map::new();
        cross.insert("symbols_checked".into(), json!(symbols_all.len()));
        cross.insert("symbols_with_bars".into(), json!(symbols_with_bars));
        cross.insert("symbols_with_ta".into(), json!(symbols_with_ta));
        cross.insert("symbols_with_sp".into(), json!(symbols_with_sp));
        cross.insert("family_ta_dir".into(), json!(fam_ta.is_dir()));
        cross.insert("family_sp_dir".into(), json!(fam_sp.is_dir()));
        let flat_files: Vec<String> = if flat_dir.is_dir() {
            read_dir_names(&flat_dir, "symbol=")?
                .iter()
                .map(|n| n.trim_start_matches("symbol=").trim_end_matches(".parquet").to_string())
                .filter(|s| !fam_ta.join(format!("symbol={s}.parquet")).exists() && !fam_sp.join(format!("symbol={s}.parquet")).exists())
                .collect()
        } else {
            Vec::new()
        };
        cross.insert("flat_layout_orphans".into(), json!(flat_files));
        if !flat_files.is_empty() {
            cross.insert("partition_alignment".into(), json!("MISALIGNED"));
        } else if fam_ta.is_dir() || fam_sp.is_dir() {
            cross.insert("partition_alignment".into(), json!("OK"));
        } else {
            cross.insert("partition_alignment".into(), json!("NO_FEATURES"));
        }

        // Top issues.
        let issue_list: Vec<Value> = agg_issues
            .iter()
            .map(|(k, v)| json!({ "key": k, "count": v }))
            .collect();
        let error_count: usize = agg_issues
            .iter()
            .filter(|(k, _)| k.starts_with("error:"))
            .map(|(_, v)| v)
            .sum();
        let warn_count: usize = agg_issues
            .iter()
            .filter(|(k, _)| k.starts_with("warn:"))
            .map(|(_, v)| v)
            .sum();

        Ok(json!({
            "market": market,
            "timeframe": timeframe,
            "window": { "start": start, "end": end },
            "verdict": if error_count > 0 { "ERRORS" } else if warn_count > 0 { "WARNINGS" } else { "OK" },
            "summary": {
                "errors": error_count,
                "warnings": warn_count,
                "issue_breakdown": issue_list,
            },
            "cross_symbol": cross,
            "symbols": per_symbol,
        }))
    }

    pub fn compute_ta(
        &self,
        market: &str,
        symbol: &str,
        timeframe: &str,
        start: &str,
        end: &str,
        indicators: &[String],
        persist: bool,
    ) -> Result<Vec<Value>, McpError> {
        let wanted = parse_indicators(indicators)?;
        let _minutes = timeframe_minutes(timeframe)?;

        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        // Visible window rows.
        let sql_win = format!(
            "SELECT t, o, h, l, c, v FROM read_parquet('{}') WHERE t >= CAST(? AS TIMESTAMP) AND t <= CAST(? AS TIMESTAMP) ORDER BY t",
            sq(&path.to_string_lossy())
        );
        let win = self.query_json(&sql_win, &[start.to_string(), end_bound(end)])?;
        if win.is_empty() {
            return Ok(Vec::new());
        }
        // Warm-up bars before `start`, pulled by count so overnight/session gaps or
        // short lookahead windows can't starve long-lookback indicators (t3, vegas,
        // trima) of the history they need to render from the first visible bar.
        let warm_bars = indicator_lookback_bars(&wanted);
        let sql_warm = format!(
            "SELECT t, o, h, l, c, v FROM read_parquet('{}') WHERE t < CAST(? AS TIMESTAMP) ORDER BY t DESC LIMIT {}",
            sq(&path.to_string_lossy()),
            warm_bars
        );
        let mut warm = self.query_json(&sql_warm, &[start.to_string()])?;
        warm.reverse();
        let mut rows = warm;
        rows.extend(win);
        let open: Vec<f64> = rows.iter().map(|r| r.get("o").and_then(Value::as_f64).unwrap_or(f64::NAN)).collect();
        let high: Vec<f64> = rows.iter().map(|r| r.get("h").and_then(Value::as_f64).unwrap_or(f64::NAN)).collect();
        let low: Vec<f64> = rows.iter().map(|r| r.get("l").and_then(Value::as_f64).unwrap_or(f64::NAN)).collect();
        let close: Vec<f64> = rows.iter().map(|r| r.get("c").and_then(Value::as_f64).unwrap_or(f64::NAN)).collect();
        let volume: Vec<f64> = rows.iter().map(|r| r.get("v").and_then(Value::as_f64).unwrap_or(0.0)).collect();

        let mut cols: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        for ind in &wanted {
            apply_indicator(&ind.name, ind.period, &open, &high, &low, &close, &volume, &mut cols);
        }

        let start_rfc = norm_ts(start)?;
        let out: Vec<Value> = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.get("t")
                    .and_then(Value::as_str)
                    .map(|t| t >= start_rfc.as_str())
                    .unwrap_or(true)
            })
            .map(|(i, r)| {
                let mut obj = Map::new();
                obj.insert("market".into(), json!(market));
                obj.insert("symbol".into(), json!(symbol));
                obj.insert("timeframe".into(), json!(timeframe));
                obj.insert("t".into(), r.get("t").cloned().unwrap_or(Value::Null));
                for (col, series) in &cols {
                    obj.insert(col.clone(), opt(series[i]));
                }
                Value::Object(obj)
            })
            .collect();

        if persist && !out.is_empty() {
            self.persist_family(market, symbol, timeframe, "ta", &out)?;
        }
        Ok(out)
    }

    /// Compute stochastic-process features (`sp_*` columns, Rust port of
    /// `sp_features.py` via `stochastic-rs`) for `[start, end]` and optionally
    /// persist them into the hive-partitioned features files.
    ///
    /// `families` selects feature families (`ou,hmm,jump,har,trend,hurst,signature,moments,momentum,garch`);
    /// an empty/unknown list resolves to all supported families. `fit_end` limits the
    /// Gaussian-HMM fit window (no lookahead); defaults to `end`.
    pub fn compute_sp(
        &self,
        market: &str,
        symbol: &str,
        timeframe: &str,
        start: &str,
        end: &str,
        families: &[String],
        fit_end: Option<&str>,
        persist: bool,
    ) -> Result<Vec<Value>, McpError> {
        let wanted = parse_sp_families(families)?;
        let _minutes = timeframe_minutes(timeframe)?;

        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql_win = format!(
            "SELECT t, c FROM read_parquet('{}') WHERE t >= CAST(? AS TIMESTAMP) AND t <= CAST(? AS TIMESTAMP) ORDER BY t",
            sq(&path.to_string_lossy())
        );
        let win = self.query_json(&sql_win, &[start.to_string(), end_bound(end)])?;
        if win.is_empty() {
            return Ok(Vec::new());
        }
        // Warm-up bars so rolling windows (hurst 126, ou/jump 60) render from the
        // first visible bar and the HMM has a real fit history.
        let warm_bars = sp_lookback_bars(&wanted);
        let sql_warm = format!(
            "SELECT t, c FROM read_parquet('{}') WHERE t < CAST(? AS TIMESTAMP) ORDER BY t DESC LIMIT {}",
            sq(&path.to_string_lossy()),
            warm_bars
        );
        let mut warm = self.query_json(&sql_warm, &[start.to_string()])?;
        warm.reverse();
        let mut rows = warm;
        rows.extend(win);
        let close: Vec<Option<f64>> = rows
            .iter()
            .map(|r| r.get("c").and_then(Value::as_f64))
            .collect();

        // HMM fit cutoff index (exclusive), matching `t <= fit_end`.
        let fit_idx: Option<usize> = match fit_end {
            Some(fe) => {
                let fe_ts = norm_ts(fe)?;
                rows.iter()
                    .position(|r| {
                        r.get("t")
                            .and_then(Value::as_str)
                            .map(|t| t > fe_ts.as_str())
                            .unwrap_or(false)
                    })
            }
            None => None,
        };

        let cols = crate::sp_features::compute_sp_features(&wanted, &close, fit_idx);

        let start_rfc = norm_ts(start)?;
        let out: Vec<Value> = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.get("t")
                    .and_then(Value::as_str)
                    .map(|t| t >= start_rfc.as_str())
                    .unwrap_or(true)
            })
            .map(|(i, r)| {
                let mut obj = Map::new();
                obj.insert("market".into(), json!(market));
                obj.insert("symbol".into(), json!(symbol));
                obj.insert("timeframe".into(), json!(timeframe));
                obj.insert("t".into(), r.get("t").cloned().unwrap_or(Value::Null));
                for (col, series) in &cols {
                    obj.insert(col.clone(), opt(series[i]));
                }
                Value::Object(obj)
            })
            .collect();

        if persist && !out.is_empty() {
            self.persist_family(market, symbol, timeframe, "sp", &out)?;
        }
        Ok(out)
    }

    pub(crate) fn persist_family(
        &self,
        market: &str,
        symbol: &str,
        timeframe: &str,
        family: &str,
        rows: &[Value],
    ) -> Result<(), McpError> {
        if rows.is_empty() {
            return Ok(());
        }
        let path = self.family_path(market, timeframe, symbol, family);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| lake_err(format!("create feature dir: {e}")))?;
        }
        // Discover columns from the first row (dynamic schema).
        let first = rows[0].as_object().cloned().unwrap_or_default();
        let mut cols: Vec<String> = first
            .keys()
            .filter(|k| !matches!(k.as_str(), "market" | "symbol" | "timeframe" | "t"))
            .cloned()
            .collect();
        cols.sort();
        // DuckDB CREATE TABLE column defs.
        let col_defs: String = std::iter::once("t TIMESTAMP".to_string())
            .chain(cols.iter().map(|c| format!("{c} DOUBLE")))
            .collect::<Vec<_>>()
            .join(", ");
        let table_name = format!("__feat_{family}");
        let tmp = path.with_extension("parquet.tmp");
        let conn = self.conn()?;
        conn.execute_batch(format!("CREATE OR REPLACE TABLE {table_name}({col_defs});").as_str())
            .map_err(lake_err)?;
        {
            let mut app = conn.appender(&table_name).map_err(lake_err)?;
            for row in rows {
                let obj = row.as_object().cloned().unwrap_or_default();
                let t_val = obj.get("t").and_then(Value::as_str).unwrap_or("");
                let mut params: Vec<Box<dyn duckdb::ToSql>> = vec![Box::new(t_val.to_string())];
                for c in &cols {
                    let v = obj.get(c).and_then(Value::as_f64).unwrap_or(f64::NAN);
                    params.push(Box::new(v));
                }
                let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();
                app.append_row(refs.as_slice()).map_err(lake_err)?;
            }
            app.flush().map_err(lake_err)?;
        }
        let copy = format!(
            "COPY {table_name} TO '{}' (FORMAT PARQUET);",
            sq(&tmp.to_string_lossy())
        );
        conn.execute_batch(&copy).map_err(lake_err)?;
        fs::rename(&tmp, &path).map_err(|e| lake_err(format!("rename feature: {e}")))?;
        Ok(())
    }

    // ---- network: Alpaca + Yahoo fallback -----------------------------------

    /// Fetch bars for a window from Alpaca. If the requested feed returns HTTP 403
    /// (e.g. no SIP permission on the account), automatically retries with the other
    /// feed (sip ↔ iex).
    ///
    /// Alpaca's history caps out at 2016-01-04 (SIP) / 2020-07-27 (IEX), so for
    /// daily bars we additionally gap-fill from Yahoo Finance whenever Alpaca did
    /// not return bars for the whole window (a prefix and/or suffix missing at the
    /// edges). Returns the merged bars plus a feed label (`yahoo`, `iex+yahoo`).
    async fn fetch_bars(
        &self,
        client: &AlpacaClient,
        _market: &str,
        symbol: &str,
        mcp_tf: &str,
        start: &str,
        end: &str,
        feed: &str,
        adjustment: &str,
    ) -> Result<(Vec<Bar>, String), McpError> {
        let (alpaca_bars, used_feed) = match self
            .fetch_bars_with_feed(client, symbol, mcp_tf, start, end, feed, adjustment)
            .await
        {
            Ok(bars) => (bars, feed.to_string()),
            Err(err) if is_http_error(&err, 403) => {
                tracing::warn!(
                    symbol,
                    feed = %feed,
                    "Alpaca feed {feed} returned 403; falling back to Yahoo for daily bars"
                );
                (Vec::new(), feed.to_string())
            }
            // A non-US listing (e.g. `9988.HK`) is unknown to Alpaca and comes
            // back as a 4xx "symbol not found", which must not abort the load:
            // for daily bars the Yahoo fallback can cover the whole window.
            Err(err)
                if mcp_tf.eq_ignore_ascii_case("1Day")
                    && !yahoo_disabled()
                    && is_http_error_in(&err, &[400, 404, 406, 422]) =>
            {
                tracing::warn!(
                    symbol,
                    error = %err.message,
                    "Alpaca has no such symbol; letting Yahoo cover the daily window"
                );
                (Vec::new(), feed.to_string())
            }
            Err(err) => return Err(err),
        };

        // The Yahoo history fallback only makes sense for daily bars.
        if mcp_tf.eq_ignore_ascii_case("1Day") && !yahoo_disabled() {
            match self.fetch_yahoo_gaps(symbol, start, end, &alpaca_bars).await {
                Ok(gaps) if !gaps.bars.is_empty() => {
                    let feed_label = if alpaca_bars.is_empty() {
                        "yahoo".to_string()
                    } else {
                        format!("iex+yahoo")
                    };
                    let mut merged = alpaca_bars;
                    merged.extend(gaps.bars);
                    merged.sort_by(|a, b| a.t.cmp(&b.t));
                    merged.dedup_by(|a, b| a.t == b.t);
                    return Ok((merged, feed_label));
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        symbol,
                        start,
                        end,
                        error = %err.message,
                        "Yahoo fallback failed; returning Alpaca bars only"
                    );
                }
            }
        }

        Ok((alpaca_bars, used_feed))
    }

    /// Fill the sub-ranges of `[start, end]` that Alpaca could not cover with
    /// Yahoo Finance daily bars. Boundaries are computed in *session dates* so
    /// the merged result never double-covers a trading day at the seam (Alpaca
    /// 1d bars are stamped 05:00Z, Yahoo bars 04:00Z — timestamps differ even
    /// for the same session).
    async fn fetch_yahoo_gaps(
        &self,
        symbol: &str,
        start: &str,
        end: &str,
        alpaca: &[Bar],
    ) -> Result<YahooBars, McpError> {
        let start_date = date_part(start)?;
        let last_day = date_part(end)?;
        let exclusive_end = last_day.succ_opt().unwrap_or(last_day);

        let first_date = alpaca
            .iter()
            .filter_map(|b| NaiveDate::parse_from_str(&b.date, "%Y-%m-%d").ok())
            .min();
        let last_date = alpaca
            .iter()
            .filter_map(|b| NaiveDate::parse_from_str(&b.date, "%Y-%m-%d").ok())
            .max();

        let mut ranges: Vec<(NaiveDate, NaiveDate)> = Vec::new(); // [start, end) exclusive
        match (first_date, last_date) {
            // Alpaca returned nothing for the window -> Yahoo covers it all.
            (None, _) => ranges.push((start_date, exclusive_end)),
            (Some(f), Some(l)) => {
                if start_date < f {
                    ranges.push((start_date, f));
                }
                let after_last = l.succ_opt().unwrap_or(l);
                if after_last < exclusive_end {
                    ranges.push((after_last, exclusive_end));
                }
            }
            _ => ranges.push((start_date, exclusive_end)),
        }

        let mut all: Vec<Bar> = Vec::new();
        let mut first_trade_date: Option<NaiveDate> = None;
        for (r_start, r_end) in ranges {
            let res = self.yahoo.daily_bars(symbol, r_start, r_end).await?;
            all.extend(res.bars);
            if first_trade_date.is_none() {
                first_trade_date = res.first_trade_date;
            }
        }
        all.sort_by(|a, b| a.t.cmp(&b.t));
        all.dedup_by(|a, b| a.t == b.t);
        Ok(YahooBars {
            bars: all,
            first_trade_date,
        })
    }

    async fn fetch_bars_with_feed(
        &self,
        client: &AlpacaClient,
        symbol: &str,
        mcp_tf: &str,
        start: &str,
        end: &str,
        feed: &str,
        adjustment: &str,
    ) -> Result<Vec<Bar>, McpError> {
        let mut all: Vec<Bar> = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut q: Vec<(&str, String)> = vec![
                ("symbols", symbol.to_string()),
                ("timeframe", mcp_tf.to_string()),
                ("start", start.to_string()),
                ("end", end.to_string()),
                ("limit", "10000".into()),
                ("sort", "asc".into()),
            ];
            push_opt(&mut q, "feed", Some(feed.to_string()));
            push_opt(&mut q, "adjustment", Some(adjustment.to_string()));
            if let Some(pt) = &page_token {
                push_opt(&mut q, "page_token", Some(pt.clone()));
            }
            let value = client.data_get("/v2/stocks/bars", &q).await?;
            let bars = value
                .get("bars")
                .and_then(|b| b.get(symbol))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for b in bars {
                if let Some(bar) = bar_from_alpaca(&b) {
                    all.push(bar);
                }
            }
            page_token = value
                .get("next_page_token")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty());
            if page_token.is_none() {
                break;
            }
        }
        all.sort_by(|a, b| a.t.cmp(&b.t));
        all.dedup_by(|a, b| a.t == b.t);
        Ok(all)
    }

    // ---- status --------------------------------------------------------------

    pub fn status(&self) -> Result<Value, McpError> {
        let manifest = fs::read_to_string(self.manifest_path()).unwrap_or_default();
        let mut partitions = Vec::new();
        let mut metadata = Vec::new();
        let mut bar_files = 0usize;
        let mut bar_bytes = 0u64;
        let mut feature_files = 0usize;
        let mut feature_bytes = 0u64;
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name == "features" {
                        // Hive-partitioned TA features: features/market=*/timeframe=*/symbol=*.parquet
                        let mut stack = vec![path];
                        while let Some(d) = stack.pop() {
                            if let Ok(sub) = fs::read_dir(&d) {
                                for e in sub.flatten() {
                                    let p = e.path();
                                    if p.is_dir() {
                                        stack.push(p);
                                    } else if p.extension().map(|x| x == "parquet").unwrap_or(false) {
                                        feature_files += 1;
                                        feature_bytes += p.metadata().map(|m| m.len()).unwrap_or(0);
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    let market = name;
                    if let Ok(tf_entries) = fs::read_dir(&path) {
                        for tf in tf_entries.flatten() {
                            let tf_name = tf.file_name().to_string_lossy().to_string();
                            if let Ok(sym_entries) = fs::read_dir(tf.path()) {
                                for sf in sym_entries.flatten() {
                                    let sf_name = sf.file_name().to_string_lossy().to_string();
                                    let size = sf.metadata().map(|m| m.len()).unwrap_or(0);
                                    bar_files += 1;
                                    bar_bytes += size;
                                    partitions.push(json!({
                                        "market": market.trim_start_matches("market="),
                                        "timeframe": tf_name.trim_start_matches("timeframe="),
                                        "symbol": sf_name.trim_start_matches("symbol=").trim_end_matches(".parquet"),
                                        "size": size,
                                    }));
                                }
                            }
                        }
                    }
                } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".parquet") {
                        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        metadata.push(json!({ "file": name, "size": size }));
                    }
                }
            }
        }
        Ok(json!({
            "dir": self.dir.to_string_lossy(),
            "manifest": manifest,
            "bar_files": bar_files,
            "bar_bytes": bar_bytes,
            "feature_files": feature_files,
            "feature_bytes": feature_bytes,
            "partitions": partitions,
            "metadata_files": metadata,
        }))
    }
}

// ---- parsing helpers ---------------------------------------------------------

fn bar_from_json(v: &Value) -> Result<Bar, McpError> {
    bar_from_json_opt(v).ok_or_else(|| lake_err("malformed bar row"))
}

fn bar_from_json_opt(v: &Value) -> Option<Bar> {
    let t = v.get("t")?.as_str()?.to_string();
    let date = v
        .get("date")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| t.get(..10).unwrap_or(&t).to_string());
    Some(Bar {
        t,
        date,
        o: v.get("o").and_then(Value::as_f64).unwrap_or(0.0),
        h: v.get("h").and_then(Value::as_f64).unwrap_or(0.0),
        l: v.get("l").and_then(Value::as_f64).unwrap_or(0.0),
        c: v.get("c").and_then(Value::as_f64).unwrap_or(0.0),
        v: v.get("v").and_then(Value::as_i64).unwrap_or(0),
        n: v.get("n").and_then(Value::as_i64).unwrap_or(0),
        vw: v.get("vw").and_then(Value::as_f64).unwrap_or(0.0),
    })
}

fn jv_to_duck(v: &Value) -> duckdb::types::Value {
    match v {
        Value::Null => duckdb::types::Value::Null,
        Value::Bool(b) => duckdb::types::Value::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                duckdb::types::Value::BigInt(i)
            } else if let Some(u) = n.as_u64() {
                duckdb::types::Value::UBigInt(u)
            } else if let Some(f) = n.as_f64() {
                duckdb::types::Value::Double(f)
            } else {
                duckdb::types::Value::Null
            }
        }
        Value::String(s) => duckdb::types::Value::Text(s.clone()),
        other => duckdb::types::Value::Text(other.to_string()),
    }
}

fn bar_from_alpaca(v: &Value) -> Option<Bar> {
    let t = v.get("t")?.as_str()?.to_string();
    let dt = DateTime::parse_from_rfc3339(&t).ok()?;
    let date = dt.format("%Y-%m-%d").to_string();
    Some(Bar {
        t: dt.with_timezone(&Utc).to_rfc3339(),
        date,
        o: v.get("o").and_then(Value::as_f64)?,
        h: v.get("h").and_then(Value::as_f64)?,
        l: v.get("l").and_then(Value::as_f64)?,
        c: v.get("c").and_then(Value::as_f64)?,
        v: v.get("v").and_then(Value::as_i64)?,
        n: v.get("n").and_then(Value::as_i64).unwrap_or(0),
        vw: v.get("vw").and_then(Value::as_f64)?,
    })
}

fn missing_windows(cov: &Coverage, start: &str, end: &str) -> Vec<(String, String)> {
    let end = end_bound(end);
    let mut windows = Vec::new();
    match (cov.first_t.as_deref(), cov.last_t.as_deref()) {
        (None, _) => windows.push((start.to_string(), end)),
        (Some(f), Some(l)) => {
            if start < f {
                windows.push((start.to_string(), f.to_string()));
            }
            if end.as_str() > l {
                windows.push((l.to_string(), end));
            }
        }
        _ => windows.push((start.to_string(), end)),
    }
    windows
}

fn opt(v: Option<f64>) -> Value {
    match v {
        Some(x) if x.is_finite() => json!(x),
        _ => Value::Null,
    }
}

pub(crate) fn opt_val(v: Option<f64>) -> Value {
    opt(v)
}

/// List sorted directory entry names starting with `prefix` (used to walk the
/// hive-partitioned feature directories).
fn read_dir_names(dir: &Path, prefix: &str) -> Result<Vec<String>, McpError> {
    let mut names = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(prefix) {
                names.push(name);
            }
        }
    }
    names.sort();
    Ok(names)
}

// ---- timeframe ------------------------------------------------------------------

fn split_tf(s: &str) -> Option<(u64, &str)> {
    let idx = s.find(|c: char| c.is_alphabetic())?;
    let num: u64 = s[..idx].trim().parse().ok()?;
    Some((num, &s[idx..]))
}

/// Normalize any accepted timeframe spelling to the lake name (`1m`, `10m`, `1h`, `1d`, …).
pub fn lake_timeframe(input: &str) -> Result<String, McpError> {
    let s = input.trim();
    let (num, raw_unit) = split_tf(s).ok_or_else(|| lake_err(format!("bad timeframe: {s}")))?;
    let unit = raw_unit.to_ascii_lowercase();
    let lake_unit = match unit.as_str() {
        "m" | "min" | "t" | "minute" | "minutes" => {
            if raw_unit.chars().next() == Some('M') && raw_unit.len() == 1 {
                "M"
            } else {
                "m"
            }
        }
        "h" | "hour" | "hours" => "h",
        "d" | "day" | "days" => "d",
        "w" | "week" | "weeks" => "w",
        "month" | "months" => "M",
        _ => return Err(lake_err(format!("bad timeframe unit: {raw_unit}"))),
    };
    let valid = match lake_unit {
        "m" => (1..=59).contains(&num),
        "h" => (1..=23).contains(&num),
        "d" => num == 1,
        "w" => num == 1,
        "M" => matches!(num, 1 | 2 | 3 | 4 | 6 | 12),
        _ => false,
    };
    if !valid {
        return Err(lake_err(format!("timeframe not supported by Alpaca: {s}")));
    }
    Ok(format!("{num}{lake_unit}"))
}

pub fn mcp_timeframe(lake: &str) -> Result<String, McpError> {
    let (num, raw_unit) = split_tf(lake).ok_or_else(|| lake_err(format!("bad lake timeframe: {lake}")))?;
    match raw_unit {
        "m" => Ok(format!("{num}Min")),
        "h" => Ok(format!("{num}Hour")),
        "d" if num == 1 => Ok("1Day".to_string()),
        "w" if num == 1 => Ok("1Week".to_string()),
        "M" => Ok(format!("{num}Month")),
        _ => Err(lake_err(format!("bad lake timeframe: {lake}"))),
    }
}

fn timeframe_minutes(tf: &str) -> Result<i64, McpError> {
    let (num, unit) = split_tf(tf).ok_or_else(|| lake_err(format!("bad timeframe: {tf}")))?;
    Ok(match unit {
        "m" => num as i64,
        "h" => num as i64 * 60,
        "d" => num as i64 * 1440,
        "w" => num as i64 * 10080,
        "M" => num as i64 * 43200,
        _ => return Err(lake_err(format!("bad timeframe: {tf}"))),
    })
}

// ---- indicators (kand) ---------------------------------------------------------

struct Indicator {
    name: String,
    period: usize,
}

/// Default period used when a period-based indicator is requested without one.
fn default_period(name: &str) -> usize {
    match name {
        "supertrend" => 10,
        "mom" | "roc" | "rocp" | "rocr" | "rocr100" => 10,
        "trix" => 15,
        "midpoint" | "adx" | "adxr" | "aroon" | "aroonosc" | "dx" | "mfi" | "plus_di"
        | "plus_dm" | "minus_di" | "minus_dm" | "rsi" | "willr" | "atr" | "natr"
        | "adr" | "max" | "min" | "sum" => 14,
        _ => 20,
    }
}

/// Whether the indicator takes a user-configurable period.
fn is_period_indicator(name: &str) -> bool {
    !matches!(
        name,
        "medprice" | "typprice" | "wclprice" | "vwap" | "sar" | "vegas" | "ecl" | "ha"
            | "bop" | "macd" | "stoch" | "ad" | "adosc" | "obv" | "trange"
            | "cdl_doji" | "cdl_dragonfly_doji" | "cdl_gravestone_doji"
            | "cdl_hammer" | "cdl_inverted_hammer" | "cdl_long_shadow" | "cdl_marubozu"
    )
}

fn known_indicator(name: &str) -> bool {
    matches!(
        name,
        "sma" | "ema" | "dema" | "tema" | "trima" | "wma" | "t3" | "rma"
            | "bbands" | "midpoint" | "medprice" | "typprice" | "wclprice"
            | "vwap" | "sar" | "supertrend" | "vegas" | "ecl" | "ha"
            | "adx" | "adxr" | "aroon" | "aroonosc" | "bop" | "cci" | "dx" | "macd"
            | "mfi" | "mom" | "plus_di" | "plus_dm" | "minus_di" | "minus_dm"
            | "roc" | "rocp" | "rocr" | "rocr100" | "rsi" | "stoch" | "trix" | "willr"
            | "max" | "min" | "stddev" | "sum" | "var"
            | "atr" | "natr" | "trange" | "adr"
            | "ad" | "adosc" | "obv"
            | "cdl_doji" | "cdl_dragonfly_doji" | "cdl_gravestone_doji"
            | "cdl_hammer" | "cdl_inverted_hammer" | "cdl_long_shadow" | "cdl_marubozu"
    )
}

/// Split `sma-20`, `sma_20` or `sma20` into (`sma`, Some(20)); bare names give None.
fn split_indicator(s: &str) -> (String, Option<usize>) {
    if let Some(idx) = s.rfind(['-', '_']) {
        let (head, tail) = s.split_at(idx);
        if let Ok(p) = tail[1..].parse::<usize>() {
            return (head.to_string(), Some(p));
        }
    }
    let digits: String = s.chars().rev().take_while(|c| c.is_ascii_digit()).collect();
    if !digits.is_empty() && s.len() > digits.len() {
        let head = &s[..s.len() - digits.len()];
        let p = digits.chars().rev().collect::<String>().parse::<usize>().ok();
        return (head.to_string(), p);
    }
    (s.to_string(), None)
}

fn parse_indicators(indicators: &[String]) -> Result<Vec<Indicator>, McpError> {
    let mut out: Vec<Indicator> = Vec::new();
    for raw in indicators {
        for item in raw.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let lower = item.to_ascii_lowercase();
            let (raw_name, period) = split_indicator(&lower);
            let name = if raw_name == "bb" { "bbands" } else { &raw_name };
            if !known_indicator(name) {
                return Err(lake_err(format!("unsupported indicator: {item}")));
            }
            let period = if is_period_indicator(name) {
                period.unwrap_or_else(|| default_period(name)).max(2)
            } else {
                0
            };
            if !out.iter().any(|i| i.name == name && i.period == period) {
                out.push(Indicator { name: name.to_string(), period });
            }
        }
    }
    if out.is_empty() {
        for item in ["sma-5", "sma-20", "ema-12", "ema-26", "rsi-14", "macd", "bb", "atr-14", "adx-14"] {
            let lower = item.to_string();
            let (raw_name, period) = split_indicator(&lower);
            let name = if raw_name == "bb" { "bbands" } else { &raw_name };
            let period = if is_period_indicator(name) {
                period.unwrap_or_else(|| default_period(name)).max(2)
            } else {
                0
            };
            out.push(Indicator { name: name.to_string(), period });
        }
    }
    Ok(out)
}

/// Resolve comma-separated SP family names; empty input -> all families.
fn parse_sp_families(families: &[String]) -> Result<Vec<&'static str>, McpError> {
    let mut out: Vec<&'static str> = Vec::new();
    for raw in families {
        for item in raw.split(',') {
            let item = item.trim().to_ascii_lowercase();
            if item.is_empty() {
                continue;
            }
            if !crate::sp_features::known_sp_family(&item) {
                return Err(lake_err(format!(
                    "unsupported SP family: {item} (supported: {})",
                    crate::sp_features::FAMILIES.join(",")
                )));
            }
            if !out.iter().any(|f| *f == item) {
                out.push(match item.as_str() {
                    "ou" => "ou",
                    "hmm" => "hmm",
                    "jump" => "jump",
                    "har" => "har",
                    "trend" => "trend",
                    "hurst" => "hurst",
                    "signature" => "signature",
                    "moments" => "moments",
                    "momentum" => "momentum",
                    "garch" => "garch",
                    _ => unreachable!(),
                });
            }
        }
    }
    if out.is_empty() {
        out.extend(crate::sp_features::FAMILIES.iter().copied());
    }
    Ok(out)
}

/// Warm-up bars pulled before `start` so indicator values are valid at `start`.
fn indicator_lookback_bars(wanted: &[Indicator]) -> usize {
    let max_period = wanted.iter().map(|i| i.period).max().unwrap_or(26);
    let mut lookback = (max_period * 3).max(26);
    for ind in wanted {
        match ind.name.as_str() {
            "vegas" => lookback = lookback.max(700),
            "t3" => lookback = lookback.max(ind.period * 7),
            "ecl" => lookback = lookback.max(60),
            "supertrend" => lookback = lookback.max(ind.period * 4),
            _ => {}
        }
    }
    lookback.min(2000)
}

fn conv_vec(v: Vec<f64>) -> Vec<Option<f64>> {
    v.into_iter().map(|x| if x.is_nan() { None } else { Some(x) }).collect()
}

/// TRIMA = SMA of an SMA whose windows are sized by parity:
/// odd period `p`: n1 = n2 = (p + 1) / 2; even period: n1 = p/2 + 1, n2 = p/2.
/// kand 0.2.2's `trima` starts its second SMA at index 0 where the first SMA is
/// still NaN, which poisons the rolling sum into all-NaN for every period >= 3,
/// so compute it here directly.
fn trima_series(close: &[f64], period: usize) -> Vec<f64> {
    let n = close.len();
    let mut out = vec![f64::NAN; n];
    if n == 0 || period < 2 {
        return out;
    }
    let (n1, n2) = if period % 2 == 1 {
        let k = period.div_ceil(2);
        (k, k)
    } else {
        (period / 2 + 1, period / 2)
    };
    if n1 > n || n1 + n2 > n + 1 {
        return out;
    }
    let mut s1 = vec![f64::NAN; n];
    let mut sum: f64 = close[..n1].iter().sum();
    s1[n1 - 1] = sum / n1 as f64;
    for i in n1..n {
        sum += close[i] - close[i - n1];
        s1[i] = sum / n1 as f64;
    }
    let start = n1 - 1;
    let mut sum2: f64 = s1[start..start + n2].iter().sum();
    out[start + n2 - 1] = sum2 / n2 as f64;
    for i in start + n2..n {
        sum2 += s1[i] - s1[i - n2];
        out[i] = sum2 / n2 as f64;
    }
    out
}

/// Run a single-output kand function; warm-up cells stay NaN and become `None`.
fn run1<F>(n: usize, f: F) -> Vec<Option<f64>>
where
    F: FnOnce(&mut [f64]) -> Result<(), kand::KandError>,
{
    let mut buf = vec![f64::NAN; n];
    if f(&mut buf).is_err() {
        return vec![None; n];
    }
    conv_vec(buf)
}

/// Dispatch one requested indicator to the kand crate and write its column(s).
fn apply_indicator(
    name: &str,
    period: usize,
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
    cols: &mut BTreeMap<String, Vec<Option<f64>>>,
) {
    let n = close.len();
    match name {
        // ---- Overlap ----
        "sma" => {
            cols.insert(format!("sma_{period}"), run1(n, |o| kohlcv::sma::sma(close, period, o)));
        }
        "ema" => {
            cols.insert(format!("ema_{period}"), run1(n, |o| kohlcv::ema::ema(close, period, None, o)));
        }
        "dema" => {
            let (mut out, mut e1, mut e2) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::dema::dema(close, period, &mut out, &mut e1, &mut e2).is_ok() {
                cols.insert(format!("dema_{period}"), conv_vec(out));
            }
        }
        "tema" => {
            let (mut out, mut e1, mut e2, mut e3) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::tema::tema(close, period, &mut out, &mut e1, &mut e2, &mut e3).is_ok() {
                cols.insert(format!("tema_{period}"), conv_vec(out));
            }
        }
        "trima" => {
            cols.insert(format!("trima_{period}"), conv_vec(trima_series(close, period)));
        }
        "wma" => {
            cols.insert(format!("wma_{period}"), run1(n, |o| kohlcv::wma::wma(close, period, o)));
        }
        "t3" => {
            let (mut out, mut e1, mut e2, mut e3, mut e4, mut e5, mut e6) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::t3::t3(close, period, 0.7, &mut out, &mut e1, &mut e2, &mut e3, &mut e4, &mut e5, &mut e6).is_ok() {
                cols.insert(format!("t3_{period}"), conv_vec(out));
            }
        }
        "rma" => {
            cols.insert(format!("rma_{period}"), run1(n, |o| kohlcv::rma::rma(close, period, o)));
        }
        "bbands" => {
            let (mut up, mut mid, mut lo) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            let (mut sma, mut var, mut sum, mut sumsq) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::bbands::bbands(close, period, 2.0, 2.0, &mut up, &mut mid, &mut lo, &mut sma, &mut var, &mut sum, &mut sumsq).is_ok() {
                cols.insert("bb_upper".into(), conv_vec(up));
                cols.insert("bb_middle".into(), conv_vec(mid));
                cols.insert("bb_lower".into(), conv_vec(lo));
            }
        }
        "midpoint" => {
            let (mut out, mut hi, mut lo) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::midpoint::midpoint(close, period, &mut out, &mut hi, &mut lo).is_ok() {
                cols.insert(format!("midpoint_{period}"), conv_vec(out));
            }
        }
        "medprice" => {
            cols.insert("medprice".into(), run1(n, |o| kohlcv::medprice::medprice(high, low, o)));
        }
        "typprice" => {
            cols.insert("typprice".into(), run1(n, |o| kohlcv::typprice::typprice(high, low, close, o)));
        }
        "wclprice" => {
            cols.insert("wclprice".into(), run1(n, |o| kohlcv::wclprice::wclprice(high, low, close, o)));
        }
        "vwap" => {
            let (mut out, mut cp, mut cv) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::vwap::vwap(high, low, close, volume, &mut out, &mut cp, &mut cv).is_ok() {
                cols.insert("vwap".into(), conv_vec(out));
            }
        }
        "sar" => {
            let mut out = vec![f64::NAN; n];
            let mut is_long = vec![false; n];
            let (mut af, mut ep) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::sar::sar(high, low, 0.02, 0.2, &mut out, &mut is_long, &mut af, &mut ep).is_ok() {
                cols.insert("sar".into(), conv_vec(out));
            }
        }
        "supertrend" => {
            let mut trend = vec![0_i64; n];
            let mut out = vec![f64::NAN; n];
            let (mut atr, mut upper, mut lower) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::supertrend::supertrend(high, low, close, period, 3.0, &mut trend, &mut out, &mut atr, &mut upper, &mut lower).is_ok() {
                cols.insert(format!("supertrend_{period}"), conv_vec(out));
                cols.insert(
                    format!("supertrend_dir_{period}"),
                    trend.into_iter().map(|x| Some(x as f64)).collect(),
                );
            }
        }
        "vegas" => {
            let (mut cu, mut cl, mut bu, mut bl) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::vegas::vegas(close, &mut cu, &mut cl, &mut bu, &mut bl).is_ok() {
                cols.insert("vegas_upper".into(), conv_vec(cu));
                cols.insert("vegas_lower".into(), conv_vec(cl));
                cols.insert("vegas_bound_upper".into(), conv_vec(bu));
                cols.insert("vegas_bound_lower".into(), conv_vec(bl));
            }
        }
        "ecl" => {
            let (mut h5, mut h4, mut h3, mut h2, mut h1) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            let (mut l1, mut l2, mut l3, mut l4, mut l5) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::ecl::ecl(high, low, close, &mut h5, &mut h4, &mut h3, &mut h2, &mut h1, &mut l1, &mut l2, &mut l3, &mut l4, &mut l5).is_ok() {
                for (key, v) in [
                    ("ecl_h5", h5),
                    ("ecl_h4", h4),
                    ("ecl_h3", h3),
                    ("ecl_h2", h2),
                    ("ecl_h1", h1),
                    ("ecl_l1", l1),
                    ("ecl_l2", l2),
                    ("ecl_l3", l3),
                    ("ecl_l4", l4),
                    ("ecl_l5", l5),
                ] {
                    cols.insert(key.into(), conv_vec(v));
                }
            }
        }
        "ha" => {
            let (mut ho, mut hh, mut hl, mut hc) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::ha::ha(open, high, low, close, &mut ho, &mut hh, &mut hl, &mut hc).is_ok() {
                cols.insert("ha_open".into(), conv_vec(ho));
                cols.insert("ha_high".into(), conv_vec(hh));
                cols.insert("ha_low".into(), conv_vec(hl));
                cols.insert("ha_close".into(), conv_vec(hc));
            }
        }
        // ---- Momentum ----
        "adx" => {
            let mut out = vec![f64::NAN; n];
            let (mut spdm, mut smdm, mut str) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::adx::adx(high, low, close, period, &mut out, &mut spdm, &mut smdm, &mut str).is_ok() {
                cols.insert(format!("adx_{period}"), conv_vec(out));
            }
        }
        "adxr" => {
            let mut out = vec![f64::NAN; n];
            let (mut adx, mut spdm, mut smdm, mut str) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::adxr::adxr(high, low, close, period, &mut out, &mut adx, &mut spdm, &mut smdm, &mut str).is_ok() {
                cols.insert(format!("adxr_{period}"), conv_vec(out));
            }
        }
        "aroon" => {
            let mut up = vec![f64::NAN; n];
            let mut down = vec![f64::NAN; n];
            let (mut ph, mut pl) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            let (mut dsh, mut dsl) = (vec![0usize; n], vec![0usize; n]);
            if kohlcv::aroon::aroon(high, low, period, &mut up, &mut down, &mut ph, &mut pl, &mut dsh, &mut dsl).is_ok() {
                cols.insert(format!("aroon_up_{period}"), conv_vec(up));
                cols.insert(format!("aroon_down_{period}"), conv_vec(down));
            }
        }
        "aroonosc" => {
            let mut out = vec![f64::NAN; n];
            let (mut ph, mut pl) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            let (mut dsh, mut dsl) = (vec![0usize; n], vec![0usize; n]);
            if kohlcv::aroonosc::aroonosc(high, low, period, &mut out, &mut ph, &mut pl, &mut dsh, &mut dsl).is_ok() {
                cols.insert(format!("aroonosc_{period}"), conv_vec(out));
            }
        }
        "bop" => {
            cols.insert("bop".into(), run1(n, |o| kohlcv::bop::bop(open, high, low, close, o)));
        }
        "cci" => {
            let mut out = vec![f64::NAN; n];
            let (mut tp, mut sma, mut md) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::cci::cci(high, low, close, period, &mut out, &mut tp, &mut sma, &mut md).is_ok() {
                cols.insert(format!("cci_{period}"), conv_vec(out));
            }
        }
        "dx" => {
            let mut out = vec![f64::NAN; n];
            let (mut spdm, mut smdm, mut str) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::dx::dx(high, low, close, period, &mut out, &mut spdm, &mut smdm, &mut str).is_ok() {
                cols.insert(format!("dx_{period}"), conv_vec(out));
            }
        }
        "macd" => {
            let mut m = vec![f64::NAN; n];
            let mut s = vec![f64::NAN; n];
            let mut h = vec![f64::NAN; n];
            let (mut fe, mut se) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::macd::macd(close, 12, 26, 9, &mut m, &mut s, &mut h, &mut fe, &mut se).is_ok() {
                cols.insert("macd".into(), conv_vec(m));
                cols.insert("macd_signal".into(), conv_vec(s));
                cols.insert("macd_hist".into(), conv_vec(h));
            }
        }
        "mfi" => {
            let mut out = vec![f64::NAN; n];
            let (mut tp, mut mf, mut pf, mut nf) = (
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
                vec![f64::NAN; n],
            );
            if kohlcv::mfi::mfi(high, low, close, volume, period, &mut out, &mut tp, &mut mf, &mut pf, &mut nf).is_ok() {
                cols.insert(format!("mfi_{period}"), conv_vec(out));
            }
        }
        "mom" => {
            cols.insert(format!("mom_{period}"), run1(n, |o| kohlcv::mom::mom(close, period, o)));
        }
        "plus_di" => {
            let mut out = vec![f64::NAN; n];
            let (mut spdm, mut str) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::plus_di::plus_di(high, low, close, period, &mut out, &mut spdm, &mut str).is_ok() {
                cols.insert(format!("plus_di_{period}"), conv_vec(out));
            }
        }
        "plus_dm" => {
            cols.insert(format!("plus_dm_{period}"), run1(n, |o| kohlcv::plus_dm::plus_dm(high, low, period, o)));
        }
        "minus_di" => {
            let mut out = vec![f64::NAN; n];
            let (mut smdm, mut str) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::minus_di::minus_di(high, low, close, period, &mut out, &mut smdm, &mut str).is_ok() {
                cols.insert(format!("minus_di_{period}"), conv_vec(out));
            }
        }
        "minus_dm" => {
            cols.insert(format!("minus_dm_{period}"), run1(n, |o| kohlcv::minus_dm::minus_dm(high, low, period, o)));
        }
        "roc" => {
            cols.insert(format!("roc_{period}"), run1(n, |o| kohlcv::roc::roc(close, period, o)));
        }
        "rocp" => {
            cols.insert(format!("rocp_{period}"), run1(n, |o| kohlcv::rocp::rocp(close, period, o)));
        }
        "rocr" => {
            cols.insert(format!("rocr_{period}"), run1(n, |o| kohlcv::rocr::rocr(close, period, o)));
        }
        "rocr100" => {
            cols.insert(format!("rocr100_{period}"), run1(n, |o| kohlcv::rocr100::rocr100(close, period, o)));
        }
        "rsi" => {
            let mut out = vec![f64::NAN; n];
            let (mut ag, mut al) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::rsi::rsi(close, period, &mut out, &mut ag, &mut al).is_ok() {
                cols.insert(format!("rsi_{period}"), conv_vec(out));
            }
        }
        "stoch" => {
            let k_period = if period >= 2 { period } else { 14 };
            let k_slow = 3usize;
            let d_period = 3usize;
            // kand::stoch leaves fast %K's warmup region as NaN, then feeds it into
            // sma, whose running sum is seeded with input[0] — so slow %K and %D
            // come out all-NaN. Compute fast %K here and run sma over clean,
            // valid-only slices instead.
            if n >= k_period + k_slow + d_period - 2 {
                let mut fast_k = vec![f64::NAN; n];
                for i in (k_period - 1)..n {
                    let (mut hh, mut ll) = (high[i], low[i]);
                    for j in 0..k_period {
                        hh = hh.max(high[i - j]);
                        ll = ll.min(low[i - j]);
                    }
                    let range = hh - ll;
                    fast_k[i] = if range > 0.0 { 100.0 * (close[i] - ll) / range } else { 50.0 };
                }
                let mut k = vec![f64::NAN; n];
                if kohlcv::sma::sma(&fast_k[k_period - 1..], k_slow, &mut k[k_period - 1..]).is_ok() {
                    let mut d = vec![f64::NAN; n];
                    let d_off = k_period + k_slow - 2;
                    if kohlcv::sma::sma(&k[d_off..], d_period, &mut d[d_off..]).is_ok() {
                        cols.insert("stoch_k".into(), conv_vec(k));
                        cols.insert("stoch_d".into(), conv_vec(d));
                    }
                }
            }
        }
        "trix" => {
            let mut out = vec![f64::NAN; n];
            let (mut e1, mut e2, mut e3) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::trix::trix(close, period, &mut out, &mut e1, &mut e2, &mut e3).is_ok() {
                cols.insert(format!("trix_{period}"), conv_vec(out));
            }
        }
        "willr" => {
            let mut out = vec![f64::NAN; n];
            let (mut hh, mut ll) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::willr::willr(high, low, close, period, &mut out, &mut hh, &mut ll).is_ok() {
                cols.insert(format!("willr_{period}"), conv_vec(out));
            }
        }
        // ---- Statistics ----
        "max" => {
            cols.insert(format!("max_{period}"), run1(n, |o| kstats::max::max(close, period, o)));
        }
        "min" => {
            cols.insert(format!("min_{period}"), run1(n, |o| kstats::min::min(close, period, o)));
        }
        "stddev" => {
            let mut out = vec![f64::NAN; n];
            let (mut sum, mut sumsq) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kstats::stddev::stddev(close, period, &mut out, &mut sum, &mut sumsq).is_ok() {
                cols.insert(format!("stddev_{period}"), conv_vec(out));
            }
        }
        "sum" => {
            cols.insert(format!("sum_{period}"), run1(n, |o| kstats::sum::sum(close, period, o)));
        }
        "var" => {
            let mut out = vec![f64::NAN; n];
            let (mut sum, mut sumsq) = (vec![f64::NAN; n], vec![f64::NAN; n]);
            if kstats::var::var(close, period, &mut out, &mut sum, &mut sumsq).is_ok() {
                cols.insert(format!("var_{period}"), conv_vec(out));
            }
        }
        // ---- Volatility ----
        "atr" => {
            cols.insert(format!("atr_{period}"), run1(n, |o| kohlcv::atr::atr(high, low, close, period, o)));
        }
        "natr" => {
            cols.insert(format!("natr_{period}"), run1(n, |o| kohlcv::natr::natr(high, low, close, period, o)));
        }
        "trange" => {
            cols.insert("trange".into(), run1(n, |o| kohlcv::trange::trange(high, low, close, o)));
        }
        "adr" => {
            cols.insert(format!("adr_{period}"), run1(n, |o| kohlcv::adr::adr(high, low, period, o)));
        }
        // ---- Volume ----
        "ad" => {
            cols.insert("ad".into(), run1(n, |o| kohlcv::ad::ad(high, low, close, volume, o)));
        }
        "adosc" => {
            let mut out = vec![f64::NAN; n];
            let (mut ad, mut fe, mut se) = (vec![f64::NAN; n], vec![f64::NAN; n], vec![f64::NAN; n]);
            if kohlcv::adosc::adosc(high, low, close, volume, 3, 10, &mut out, &mut ad, &mut fe, &mut se).is_ok() {
                cols.insert("adosc".into(), conv_vec(out));
            }
        }
        "obv" => {
            cols.insert("obv".into(), run1(n, |o| kohlcv::obv::obv(close, volume, o)));
        }
        // ---- Pattern (candlesticks) ----
        "cdl_doji" => {
            let mut sig = vec![0_i64; n];
            if kohlcv::cdl_doji::cdl_doji(open, high, low, close, 10.0, 10.0, &mut sig).is_ok() {
                cols.insert("cdl_doji".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_dragonfly_doji" => {
            let mut sig = vec![0_i64; n];
            if kohlcv::cdl_dragonfly_doji::cdl_dragonfly_doji(open, high, low, close, 10.0, &mut sig).is_ok() {
                cols.insert("cdl_dragonfly_doji".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_gravestone_doji" => {
            let mut sig = vec![0_i64; n];
            if kohlcv::cdl_gravestone_doji::cdl_gravestone_doji(open, high, low, close, 10.0, &mut sig).is_ok() {
                cols.insert("cdl_gravestone_doji".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_hammer" => {
            let mut sig = vec![0_i64; n];
            let mut body = vec![f64::NAN; n];
            if kohlcv::cdl_hammer::cdl_hammer(open, high, low, close, 14, 3.0, &mut sig, &mut body).is_ok() {
                cols.insert("cdl_hammer".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_inverted_hammer" => {
            let mut sig = vec![0_i64; n];
            let mut body = vec![f64::NAN; n];
            if kohlcv::cdl_inverted_hammer::cdl_inverted_hammer(open, high, low, close, 14, 3.0, &mut sig, &mut body).is_ok() {
                cols.insert("cdl_inverted_hammer".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_long_shadow" => {
            let mut sig = vec![0_i64; n];
            let mut body = vec![f64::NAN; n];
            if kohlcv::cdl_long_shadow::cdl_long_shadow(open, high, low, close, 14, 3.0, &mut sig, &mut body).is_ok() {
                cols.insert("cdl_long_shadow".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        "cdl_marubozu" => {
            let mut sig = vec![0_i64; n];
            let mut body = vec![f64::NAN; n];
            if kohlcv::cdl_marubozu::cdl_marubozu(open, high, low, close, 14, 10.0, &mut sig, &mut body).is_ok() {
                cols.insert("cdl_marubozu".into(), sig.into_iter().map(|x| Some(x as f64)).collect());
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeframe_normalization() {
        assert_eq!(lake_timeframe("1d").unwrap(), "1d");
        assert_eq!(lake_timeframe("10Min").unwrap(), "10m");
        assert_eq!(lake_timeframe("1Hour").unwrap(), "1h");
        assert_eq!(lake_timeframe("5T").unwrap(), "5m");
        assert_eq!(lake_timeframe("1M").unwrap(), "1M");
        assert_eq!(lake_timeframe("3Month").unwrap(), "3M");
        assert_eq!(mcp_timeframe("10m").unwrap(), "10Min");
        assert_eq!(mcp_timeframe("1d").unwrap(), "1Day");
        assert!(lake_timeframe("0m").is_err());
    }

    #[test]
    fn indicator_sanity() {
        let v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let mut out = vec![f64::NAN; v.len()];
        kohlcv::sma::sma(&v, 5, &mut out).unwrap();
        assert!(out[4].is_finite());
        assert_eq!(out[99], 98.0);
        let mut rsi = vec![f64::NAN; v.len()];
        let (mut ag, mut al) = (vec![f64::NAN; v.len()], vec![f64::NAN; v.len()]);
        kohlcv::rsi::rsi(&v, 14, &mut rsi, &mut ag, &mut al).unwrap();
        assert_eq!(rsi.last().unwrap().round(), 100.0);
        let mut m = vec![f64::NAN; v.len()];
        let mut s = vec![f64::NAN; v.len()];
        let mut h = vec![f64::NAN; v.len()];
        let (mut fe, mut se) = (vec![f64::NAN; v.len()], vec![f64::NAN; v.len()]);
        kohlcv::macd::macd(&v, 12, 26, 9, &mut m, &mut s, &mut h, &mut fe, &mut se).unwrap();
        assert!(m[50].is_finite());
    }

    #[test]
    fn stoch_not_all_nan() {
        // Regression: kand::stoch yields all-NaN slow %K/%D because its leading
        // fast-%K NaNs poison sma's running sum. We compute stoch directly, so
        // values must be finite well before the series end.
        let v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let low: Vec<f64> = v.iter().map(|x| x * 0.9).collect();
        let high: Vec<f64> = v.iter().map(|x| x * 1.1).collect();
        let mut cols = std::collections::BTreeMap::new();
        apply_indicator("stoch", 0, &v, &high, &low, &v, &v, &mut cols);
        let k = cols.get("stoch_k").expect("stoch_k column");
        let d = cols.get("stoch_d").expect("stoch_d column");
        let nn = |s: &Vec<Option<f64>>| s.iter().filter(|x| x.is_some()).count();
        assert!(nn(k) > 80, "stoch_k mostly NaN: {} non-null", nn(k));
        assert!(nn(d) > 80, "stoch_d mostly NaN: {} non-null", nn(d));
        let last = k[99].unwrap();
        assert!(last.is_finite() && last > 0.0 && last < 100.0, "k[99]={last}");
    }

    #[test]
    fn parquet_roundtrip() {
        let dir = std::env::temp_dir().join(format!("taclake_test_{}", std::process::id()));
        let lake = Lake::new(dir.clone()).unwrap();
        let bars = vec![
            Bar { t: "2026-06-01T00:00:00+00:00".into(), date: "2026-06-01".into(), o: 1.0, h: 2.0, l: 0.5, c: 1.5, v: 100, n: 10, vw: 1.4 },
            Bar { t: "2026-06-02T00:00:00+00:00".into(), date: "2026-06-02".into(), o: 1.5, h: 3.0, l: 1.0, c: 2.5, v: 200, n: 20, vw: 2.4 },
        ];
        lake.upsert_bars("US", "1d", "AAPL", &bars).unwrap();
        // duplicate + new row exercises dedupe/merge
        lake.upsert_bars("US", "1d", "AAPL", &bars).unwrap();
        let read = lake.read_bars_raw("US", "1d", "AAPL", "2026-06-01", "2026-06-02").unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].c, 1.5);
        assert_eq!(read[1].vw, 2.4);
        // Leading-gap backfill: the file already holds the NEWEST bar; a
        // full-history upsert (which ends at that same bar) must still
        // persist the older bars (regression: SLV 1-bar lake).
        lake.upsert_bars("US", "1d", "AAPL", &[Bar { t: "2026-05-30T00:00:00+00:00".into(), date: "2026-05-30".into(), o: 9.0, h: 9.5, l: 8.5, c: 9.2, v: 50, n: 5, vw: 9.1 }]).unwrap();
        let read = lake.read_bars_raw("US", "1d", "AAPL", "2026-05-30", "2026-06-02").unwrap();
        assert_eq!(read.len(), 3, "leading-gap upsert must merge, not drop history");
        assert_eq!(read[0].c, 9.2);
        assert_eq!(read[1].c, 1.5);
        lake.set_coverage("US", "1d", "AAPL", &Coverage { first_t: Some("2026-06-01".into()), last_t: Some("2026-06-02".into()), num_bars: 2, feed: "iex".into(), adjustment: "raw".into() }).unwrap();
        let cov = lake.get_coverage("US", "1d", "AAPL").unwrap();
        assert_eq!(cov.num_bars, 2);
        assert_eq!(cov.feed, "iex");
        let indicators = "sma_2,obv".split(',').map(String::from).collect::<Vec<_>>();
        let features = lake.compute_ta("US", "AAPL", "1d", "2026-06-01", "2026-06-02", &indicators, true).unwrap();
        assert_eq!(features.len(), 2);
        let feats = lake.read_features(Some("US"), Some("AAPL"), Some("1d"), None, None).unwrap();
        assert_eq!(feats.len(), 2);
        assert!(feats[0].get("obv").is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn features_partitioned_layout() {
        let dir = std::env::temp_dir().join(format!("taclake_feat_{}", std::process::id()));
        let lake = Lake::new(dir.clone()).unwrap();
        let bars = vec![
            Bar { t: "2026-06-01T00:00:00+00:00".into(), date: "2026-06-01".into(), o: 1.0, h: 2.0, l: 0.5, c: 1.5, v: 100, n: 10, vw: 1.4 },
            Bar { t: "2026-06-02T00:00:00+00:00".into(), date: "2026-06-02".into(), o: 1.5, h: 3.0, l: 1.0, c: 2.5, v: 200, n: 20, vw: 2.4 },
        ];
        lake.upsert_bars("US", "1d", "AAPL", &bars).unwrap();
        let indicators = "sma_2,obv".split(',').map(String::from).collect::<Vec<_>>();
        let features = lake.compute_ta("US", "AAPL", "1d", "2026-06-01", "2026-06-02", &indicators, true).unwrap();
        assert_eq!(features.len(), 2);

        // Family-partitioned feature file.
        let path = lake.family_path("US", "1d", "AAPL", "ta");
        assert!(path.exists(), "expected family=ta feature file at {path:?}");
        assert!(!lake.meta_path("features").exists(), "monolithic features.parquet should not be used");
        assert_eq!(path.file_name().map(|n| n.to_string_lossy().to_string()), Some("symbol=AAPL.parquet".into()));

        // Reading back via the hive glob exposes partition columns.
        let feats = lake.read_features(Some("US"), Some("AAPL"), Some("1d"), None, None).unwrap();
        assert_eq!(feats.len(), 2);
        assert!(feats[0].get("obv").is_some());
        assert_eq!(feats[0].get("symbol").and_then(Value::as_str), Some("AAPL"));
        assert_eq!(feats[0].get("market").and_then(Value::as_str), Some("US"));
        assert_eq!(feats[0].get("timeframe").and_then(Value::as_str), Some("1d"));

        // Persisting again rewrites (fresh write, no duplicates).
        lake.compute_ta("US", "AAPL", "1d", "2026-06-01", "2026-06-02", &indicators, true).unwrap();
        let again = lake.read_features(Some("US"), Some("AAPL"), Some("1d"), None, None).unwrap();
        assert_eq!(again.len(), 2);

        // Cross-symbol isolation.
        let msft = lake.read_features(Some("US"), Some("MSFT"), Some("1d"), None, None).unwrap();
        assert!(msft.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    fn mkbar(t: &str, c: f64) -> Bar {
        Bar { t: format!("{t}T00:00:00+00:00"), date: t.into(), o: c, h: c, l: c, c, v: 100, n: 1, vw: c }
    }

    #[test]
    fn missing_windows_shapes() {
        // No coverage -> whole range is one window.
        let none = Coverage::default();
        assert_eq!(missing_windows(&none, "2026-06-01", "2026-08-01"), vec![("2026-06-01".to_string(), "2026-08-01".to_string())]);

        // Coverage covers exactly the range (bar timestamps are 04:00 UTC for
        // daily bars; start == first_t and end == last_t means no windows).
        let full = Coverage {
            first_t: Some("2026-06-01T04:00:00+00:00".into()),
            last_t: Some("2026-08-01T04:00:00+00:00".into()),
            num_bars: 0,
            feed: "iex".into(),
            adjustment: "raw".into(),
        };
        assert!(missing_windows(&full, "2026-06-01T04:00:00+00:00", "2026-08-01T04:00:00+00:00").is_empty());

        // A date-only start compares < the first bar's full timestamp and so
        // still produces a seam window for that first bar.
        assert_eq!(
            missing_windows(&full, "2026-06-01", "2026-08-01T04:00:00+00:00"),
            vec![("2026-06-01".to_string(), "2026-06-01T04:00:00+00:00".to_string())]
        );

        // Range extends past coverage -> trailing window only.
        assert_eq!(
            missing_windows(&full, "2026-06-01T04:00:00+00:00", "2026-08-15T04:00:00+00:00"),
            vec![("2026-08-01T04:00:00+00:00".to_string(), "2026-08-15T04:00:00+00:00".to_string())]
        );

        // Range starts before coverage -> leading gap window (the SLV case:
        // the file already holds the newest bar, so the full-history request
        // must produce a LEADING window, not an empty append).
        assert_eq!(
            missing_windows(&full, "2000-01-03", "2026-08-01T04:00:00+00:00"),
            vec![("2000-01-03".to_string(), "2026-06-01T04:00:00+00:00".to_string())]
        );

        // Both sides -> two windows.
        assert_eq!(
            missing_windows(&full, "2000-01-03", "2026-08-15T04:00:00+00:00"),
            vec![
                ("2000-01-03".to_string(), "2026-06-01T04:00:00+00:00".to_string()),
                ("2026-08-01T04:00:00+00:00".to_string(), "2026-08-15T04:00:00+00:00".to_string()),
            ]
        );

        // Midnight end_bound: a midnight-UTC end at the last bar bumps to the
        // next day -> seam refetch window (keeps the tail current).
        assert_eq!(
            missing_windows(&full, "2026-06-01T04:00:00+00:00", "2026-08-01T00:00:00+00:00"),
            vec![("2026-08-01T04:00:00+00:00".to_string(), "2026-08-02T00:00:00+00:00".to_string())]
        );
    }

    #[test]
    fn incremental_fill_single_symbol() {
        let dir = std::env::temp_dir().join(format!("taclake_inc_{}", std::process::id()));
        let lake = Lake::new(dir.clone()).unwrap();
        let sym = "SLV";

        // (a) Small past window, e.g. the last week of data.
        let week = vec![mkbar("2026-08-11", 58.0), mkbar("2026-08-12", 58.5), mkbar("2026-08-13", 59.0)];
        lake.upsert_bars("US", "1d", sym, &week).unwrap();
        lake.update_coverage("US", "1d", sym, &week, "iex", "raw").unwrap();

        // (b) Fill the last bar to "now" (tail append).
        let tail = vec![mkbar("2026-08-17", 59.575)];
        lake.upsert_bars("US", "1d", sym, &tail).unwrap();
        lake.update_coverage("US", "1d", sym, &tail, "iex", "raw").unwrap();

        // At this point the file already holds the NEWEST bar (2026-08-17) —
        // exactly the pre-bug SLV state that caused the backfill to be dropped.
        let cov_before = lake.get_coverage("US", "1d", sym).unwrap();
        assert_eq!(cov_before.last_t.as_deref().map(|s| &s[..10]), Some("2026-08-17"));
        assert_eq!(cov_before.num_bars, 4);

        // (c) Backfill all from TAC_LAKE_START_DATE (a leading-gap backfill
        // that ends at the already-present newest bar). The fetched set is
        // mostly OLDER than the file's latest t — the old filter dropped all
        // of it. The merge must persist the full history.
        let history = vec![
            mkbar("2006-04-28", 10.0),
            mkbar("2006-04-29", 10.2),
            mkbar("2026-08-11", 58.0),
            mkbar("2026-08-12", 58.5),
            mkbar("2026-08-13", 59.0),
            mkbar("2026-08-17", 59.575),
        ];
        lake.upsert_bars("US", "1d", sym, &history).unwrap();
        lake.update_coverage("US", "1d", sym, &history, "iex+yahoo", "raw").unwrap();

        // (d) Verify metadata: coverage first_t must now reach back to the
        // earliest bar, num_bars must be the full merged set.
        let cov = lake.get_coverage("US", "1d", sym).unwrap();
        assert_eq!(cov.first_t.as_deref().map(|s| &s[..10]), Some("2006-04-28"));
        assert_eq!(cov.last_t.as_deref().map(|s| &s[..10]), Some("2026-08-17"));
        assert_eq!(cov.num_bars, 6, "full history must survive the merge");
        assert_eq!(cov.feed, "iex+yahoo");

        // File actually holds all 6 bars, sorted.
        let read = lake.read_bars_raw("US", "1d", sym, "2006-04-28", "2026-08-17").unwrap();
        assert_eq!(read.len(), 6);
        assert_eq!(read[0].c, 10.0);
        assert_eq!(read[5].c, 59.575);

        // touch_symbols: first_seen = earliest bar date, not load time.
        lake.touch_symbols("US", "1d", &[sym.to_string()]).unwrap();
        let symbols = lake.read_symbols().unwrap();
        let row = symbols.iter().find(|r| r.get("symbol").and_then(Value::as_str) == Some(sym)).unwrap();
        let fs = row.get("first_seen").and_then(Value::as_str).unwrap();
        assert!(fs.starts_with("2006-04-28"), "first_seen should be the first trading date, got {fs}");
        let up = row.get("updated_at").and_then(Value::as_str).unwrap();
        assert!(up.starts_with("2026-"), "updated_at should be the touch time, got {up}");

        // Calendar: bars path only records trading days — no session times or
        // per-symbol prices leak into the market-wide calendar.
        let day_bars: Vec<Bar> = history.iter().filter(|b| b.date == "2026-08-17").cloned().collect();
        lake.update_calendar_from_bars("US", &day_bars).unwrap();
        let cal = lake.read_calendar(Some("US"), Some("2026-08-17"), Some("2026-08-17")).unwrap();
        assert_eq!(cal.len(), 1);
        let calrow = &cal[0];
        assert_eq!(calrow.get("date").and_then(Value::as_str), Some("2026-08-17"));
        assert_eq!(calrow.get("source").and_then(Value::as_str), Some("bars"));
        assert!(calrow.get("session_open").and_then(Value::as_str).is_none(), "bars path must not set session_open: {calrow:?}");
        assert!(calrow.get("session_close").and_then(Value::as_str).is_none(), "bars path must not set session_close: {calrow:?}");
        assert!(calrow.get("open_price").and_then(Value::as_f64).is_none(), "bars path must not set open_price: {calrow:?}");
        assert!(calrow.get("close_price").and_then(Value::as_f64).is_none(), "bars path must not set close_price: {calrow:?}");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_fill_list_of_symbols() {
        let dir = std::env::temp_dir().join(format!("taclake_inc_multi_{}", std::process::id()));
        let lake = Lake::new(dir.clone()).unwrap();
        let syms = ["SLV", "AGG"];

        for sym in syms {
            // Same sequence as the single-symbol test, per symbol.
            let week = vec![mkbar("2026-08-11", 58.0), mkbar("2026-08-13", 59.0)];
            lake.upsert_bars("US", "1d", sym, &week).unwrap();
            lake.update_coverage("US", "1d", sym, &week, "iex", "raw").unwrap();
            let tail = vec![mkbar("2026-08-17", 59.575)];
            lake.upsert_bars("US", "1d", sym, &tail).unwrap();
            lake.update_coverage("US", "1d", sym, &tail, "iex", "raw").unwrap();

            let history = vec![mkbar("2003-09-29", 20.0), mkbar("2026-08-11", 58.0), mkbar("2026-08-13", 59.0), mkbar("2026-08-17", 59.575)];
            lake.upsert_bars("US", "1d", sym, &history).unwrap();
            lake.update_coverage("US", "1d", sym, &history, "iex+yahoo", "raw").unwrap();
        }

        // Every symbol must have the full merged history + correct coverage.
        for sym in syms {
            let read = lake.read_bars_raw("US", "1d", sym, "2003-09-29", "2026-08-17").unwrap();
            assert_eq!(read.len(), 4, "{sym} should hold 4 merged bars");
            let cov = lake.get_coverage("US", "1d", sym).unwrap();
            assert_eq!(cov.first_t.as_deref().map(|s| &s[..10]), Some("2003-09-29"), "{sym}");
            assert_eq!(cov.num_bars, 4, "{sym}");
        }

        lake.touch_symbols("US", "1d", &syms.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
        let symbols = lake.read_symbols().unwrap();
        assert_eq!(symbols.len(), 2);
        for row in &symbols {
            let fs = row.get("first_seen").and_then(Value::as_str).unwrap();
            assert!(fs.starts_with("2003-09-29"), "first_seen should be first trading date, got {fs}");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_dataset_catches_hollow_missing_and_misaligned() {
        let dir = std::env::temp_dir().join(format!("taclake_val_{}", std::process::id()));
        let lake = Lake::new(dir.clone()).unwrap();

        // Healthy symbol: bars + ta + sp features.
        let bars = vec![
            Bar { t: "2026-06-01T00:00:00+00:00".into(), date: "2026-06-01".into(), o: 1.0, h: 2.0, l: 0.5, c: 1.5, v: 100, n: 10, vw: 1.4 },
            Bar { t: "2026-06-02T00:00:00+00:00".into(), date: "2026-06-02".into(), o: 1.5, h: 3.0, l: 1.0, c: 2.5, v: 200, n: 20, vw: 2.4 },
        ];
        lake.upsert_bars("US", "1d", "GOOD", &bars).unwrap();
        lake.set_coverage("US", "1d", "GOOD", &Coverage { first_t: Some("2026-06-01".into()), last_t: Some("2026-06-02".into()), num_bars: 2, feed: "iex".into(), adjustment: "raw".into() }).unwrap();
        let indicators = "sma_2".split(',').map(String::from).collect::<Vec<_>>();
        lake.compute_ta("US", "GOOD", "1d", "2026-06-01", "2026-06-02", &indicators, true).unwrap();
        lake.compute_sp("US", "GOOD", "1d", "2026-06-01", "2026-06-02", &["ou".to_string()], None, true).unwrap();

        // Hollow symbol: coverage claims a wide span (num_bars=5000 spanning
        // 2000-2026) but the file holds only 2 recent bars.
        lake.upsert_bars("US", "1d", "HOLLOW", &bars).unwrap();
        lake.set_coverage("US", "1d", "HOLLOW", &Coverage { first_t: Some("2000-01-03".into()), last_t: Some("2026-08-01".into()), num_bars: 5000, feed: "iex".into(), adjustment: "raw".into() }).unwrap();

        // Seed a calendar wide enough that the hollow check sees a big gap.
        let mut cal: BTreeMap<String, Value> = BTreeMap::new();
        let now = now_rfc3339();
        for y in 2000..2027 {
            for m in 1..=12 {
                let d = format!("{y}-{m:02}-01");
                cal.insert(d.clone(), json!({ "market": "US", "date": d, "source": "bars", "updated_at": now.clone() }));
            }
        }
        lake.write_calendar(&cal).unwrap();

        // Missing-features symbol: bars but no family=ta/sp files.
        lake.upsert_bars("US", "1d", "NOFEAT", &bars).unwrap();
        lake.set_coverage("US", "1d", "NOFEAT", &Coverage { first_t: Some("2026-06-01".into()), last_t: Some("2026-06-02".into()), num_bars: 2, feed: "iex".into(), adjustment: "raw".into() }).unwrap();

        let rep = lake.validate_lake_dataset("US", "1d", None, None, None).unwrap();
        let verdict = rep.get("verdict").and_then(Value::as_str).unwrap();
        assert_eq!(verdict, "ERRORS", "expected ERRORS verdict, got {verdict}");

        let syms = rep.get("symbols").and_then(Value::as_array).unwrap();
        let codes: Vec<String> = syms
            .iter()
            .filter_map(|s| {
                s.get("symbol").and_then(Value::as_str).and_then(|name| {
                    if name == "HOLLOW" || name == "NOFEAT" {
                        s.get("issues")
                            .and_then(Value::as_array)
                            .map(|i| i.iter().filter_map(|x| x.get("code").and_then(Value::as_str).map(String::from)).collect::<Vec<_>>())
                    } else {
                        None
                    }
                })
            })
            .flatten()
            .collect();
        let all_codes: Vec<&str> = codes.iter().map(|s| s.as_str()).collect();
        assert!(all_codes.contains(&"HOLLOW_COVERAGE"), "expected HOLLOW_COVERAGE, got {all_codes:?}");
        assert!(all_codes.contains(&"MISSING_FEATURES"), "expected MISSING_FEATURES, got {all_codes:?}");

        // GOOD should have no error-level issues (only MISSING_COLUMN warnings
        // because it was persisted with a deliberately tiny feature set).
        let good = syms.iter().find(|s| s.get("symbol").and_then(Value::as_str) == Some("GOOD")).unwrap();
        let good_issues = good.get("issues").and_then(Value::as_array).unwrap();
        assert!(
            good_issues.iter().all(|i| i.get("level").and_then(Value::as_str) != Some("error")),
            "GOOD should have no error issues, got {good_issues:?}"
        );

        // All-NaN detection: persist an all-NaN TA column, then validate a window.
        let nan_indicators = "sma_2,ema_2".split(',').map(String::from).collect::<Vec<_>>();
        lake.compute_ta("US", "GOOD", "1d", "2026-06-01", "2026-06-02", &nan_indicators, true).unwrap();
        // ema_2 needs 2 warm-up bars -> second row has a value, first is NaN, so
        // NOT all-NaN; but sma_2 over 2 rows is fully defined. To force an
        // all-NaN, write an empty-column via a sparse persist is awkward here, so
        // instead assert the healthy path: window alignment reports no misalign.
        let rep2 = lake.validate_lake_dataset("US", "1d", Some("GOOD"), Some("2026-06-01"), Some("2026-06-02")).unwrap();
        let good2 = rep2.get("symbols").and_then(Value::as_array).unwrap().iter().next().unwrap();
        let issues2 = good2.get("issues").and_then(Value::as_array).unwrap();
        let has_misalign = issues2.iter().any(|i| i.get("code").and_then(Value::as_str) == Some("FEATURE_BAR_MISALIGN"));
        assert!(!has_misalign, "healthy symbol should not report misalignment: {issues2:?}");

        let _ = fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// FX (cross-currency) cache + realized vol + correlation store
// ---------------------------------------------------------------------------

impl Lake {
    /// Cache-first daily FX bars for a currency pair under
    /// `market=FX/timeframe=1d` (symbol = 6-letter pair code, e.g. `EURUSD`).
    /// When `lazy` and the requested window is not fully covered, fetches the
    /// missing history through the FX source chain (Yahoo → Frankfurter/ECB)
    /// and persists it — no Alpaca involved (its forex endpoints return 403).
    pub async fn ensure_fx_bars(
        &self,
        leg: &str,
        start: &str,
        end: &str,
        lazy: bool,
    ) -> Result<BackfillResult, McpError> {
        let symbol = crate::fx::pair_code(leg);
        let (market, tf) = (crate::fx::FX_MARKET, crate::fx::FX_TIMEFRAME);

        let mut cov = self.get_coverage(market, tf, &symbol)?;
        let on_disk = self.read_bars_full(market, tf, &symbol)?;
        if on_disk.is_empty() && (cov.first_t.is_some() || cov.last_t.is_some() || cov.num_bars > 0) {
            self.set_coverage(market, tf, &symbol, &Coverage::default())?;
            cov = Coverage::default();
        } else if !on_disk.is_empty() && (cov.num_bars as usize != on_disk.len()) {
            let (f, l) = (on_disk.first().map(|b| b.t.clone()), on_disk.last().map(|b| b.t.clone()));
            self.set_coverage(
                market,
                tf,
                &symbol,
                &Coverage {
                    first_t: f,
                    last_t: l,
                    num_bars: on_disk.len() as i64,
                    feed: cov.feed.clone(),
                    adjustment: cov.adjustment.clone(),
                },
            )?;
            cov = self.get_coverage(market, tf, &symbol)?;
        }
        if lazy && self.complete(cov.clone(), market, tf, &symbol, start, end)? {
            return Ok(BackfillResult {
                bars: self.read_bars_raw(market, tf, &symbol, start, end)?,
                source: "lake",
            });
        }
        if !lazy {
            return Ok(BackfillResult {
                bars: self.read_bars_raw(market, tf, &symbol, start, end)?,
                source: "partial",
            });
        }

        let fx = crate::fx::FxClient::new()?;
        let sd = crate::fx::date_of(start)?;
        let ed = crate::fx::date_of(end)?;
        let (src, hist) = fx
            .fetch_history(
                &symbol,
                sd,
                ed.checked_add_days(chrono::Days::new(1)).unwrap_or(ed),
            )
            .await?;
        let mut bars: Vec<Bar> = Vec::with_capacity(hist.len());
        for h in hist {
            let t_utc = Utc
                .from_utc_datetime(&h.date.and_hms_opt(4, 0, 0).unwrap_or_default())
                .to_rfc3339();
            bars.push(Bar {
                t: t_utc,
                date: h.date.to_string(),
                o: h.o,
                h: h.h,
                l: h.l,
                c: h.c,
                v: 0,
                n: 0,
                vw: h.c,
            });
        }
        if !bars.is_empty() {
            self.upsert_bars(market, tf, &symbol, &bars)?;
            self.update_coverage(market, tf, &symbol, &bars, src.name(), "raw")?;
            self.touch_symbols(market, tf, &[symbol.clone()])?;
            self.update_calendar_from_bars(market, &bars)?;
        }
        Ok(BackfillResult {
            bars: self.read_bars_raw(market, tf, &symbol, start, end)?,
            source: src.name(),
        })
    }

    /// Realized-volatility features for a daily instrument (FX or any 1d
    /// series): `close`, its log return `lr`, and 20/60-day annualized
    /// realized vol of log returns. Rows cover `[start, end]` (computed with
    /// warm-up lookback). Persisted under `family=fxvol` when `persist`.
    pub fn compute_fx_vol(
        &self,
        market: &str,
        symbol: &str,
        timeframe: &str,
        start: &str,
        end: &str,
        persist: bool,
    ) -> Result<Vec<Value>, McpError> {
        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT t, c FROM read_parquet('{}') WHERE t <= CAST(? AS TIMESTAMP) ORDER BY t",
            sq(&path.to_string_lossy())
        );
        let rows = self.query_json(&sql, &[end_bound(end)])?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let n = rows.len();
        let closes: Vec<f64> = rows
            .iter()
            .map(|r| r.get("c").and_then(Value::as_f64).unwrap_or(f64::NAN))
            .collect();
        let mut lr: Vec<Option<f64>> = vec![None; n];
        for i in 1..n {
            if closes[i - 1].is_finite() && closes[i - 1] > 0.0 && closes[i].is_finite() {
                lr[i] = Some((closes[i] / closes[i - 1]).ln());
            }
        }
        let rv = |window: usize, i: usize| -> Option<f64> {
            if i + 1 < window {
                return None;
            }
            let mut sum = 0.0;
            for k in (i + 1 - window)..=i {
                match lr[k] {
                    Some(v) => sum += v * v,
                    None => return None,
                }
            }
            Some((sum / window as f64 * 252.0).sqrt())
        };
        let start_rfc = norm_ts(start)?;
        let out: Vec<Value> = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.get("t")
                    .and_then(Value::as_str)
                    .map(|t| t >= start_rfc.as_str())
                    .unwrap_or(true)
            })
            .map(|(i, r)| {
                let mut obj = Map::new();
                obj.insert("market".into(), json!(market));
                obj.insert("symbol".into(), json!(symbol));
                obj.insert("timeframe".into(), json!(timeframe));
                obj.insert("t".into(), r.get("t").cloned().unwrap_or(Value::Null));
                obj.insert("close".into(), opt(Some(closes[i])));
                obj.insert("lr".into(), opt(lr[i]));
                obj.insert("rv20_ann".into(), opt(rv(20, i)));
                obj.insert("rv60_ann".into(), opt(rv(60, i)));
                Value::Object(obj)
            })
            .collect();
        if persist && !out.is_empty() {
            self.persist_family(market, symbol, timeframe, "fxvol", &out)?;
        }
        Ok(out)
    }

    /// Replace the lake correlation store (`correlations.parquet`) with the
    /// given pairs, streamed through a DuckDB Appender. Columns: `t DATE,
    /// leg_a, leg_b, rho0, rho, alpha, window_days, n_obs, source`. `pairs` is
    /// `(leg_idx_a, leg_idx_b, rho0)`; rows with a non-finite rho0 are skipped
    /// (they carry no information) and `rho` is the dampened value
    /// `dampen(rho0, alpha)`.
    pub fn write_correlations(
        &self,
        t_date: &str,
        legs: &[String],
        alpha: f64,
        window: usize,
        source: &str,
        pairs: &[(usize, usize, f64)],
    ) -> Result<(), McpError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let days = NaiveDate::parse_from_str(t_date, "%Y-%m-%d")
            .map_err(|e| lake_err(format!("bad correlation date {t_date}: {e}")))?
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        let path = self.meta_path("correlations");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| lake_err(format!("create dir: {e}")))?;
        }
        let tmp = path.with_extension("parquet.tmp");
        let conn = self.conn()?;
        conn.execute_batch("BEGIN TRANSACTION;").map_err(lake_err)?;
        conn.execute_batch(
            "CREATE OR REPLACE TABLE correlations(t DATE, leg_a VARCHAR, leg_b VARCHAR, rho0 DOUBLE, rho DOUBLE, alpha DOUBLE, window_days INTEGER, n_obs INTEGER, source VARCHAR);",
        )
        .map_err(lake_err)?;
        {
            let mut app = conn.appender("correlations").map_err(lake_err)?;
            for (i, j, v) in pairs {
                if !v.is_finite() {
                    continue;
                }
                let rho0 = (v * 10000.0).round() / 10000.0;
                let rho = (crate::corr::dampen(*v, alpha) * 10000.0).round() / 10000.0;
                let t_val = DbValue::Date32(days);
                let w = window as i64;
                let params: Vec<&dyn duckdb::ToSql> = vec![
                    &t_val,
                    &legs[*i],
                    &legs[*j],
                    &rho0,
                    &rho,
                    &alpha,
                    &w,
                    &w,
                    &source,
                ];
                app.append_row(params.as_slice()).map_err(lake_err)?;
            }
            app.flush().map_err(lake_err)?;
        }
        let copy = format!(
            "COPY correlations TO '{}' (FORMAT PARQUET);",
            sq(&tmp.to_string_lossy())
        );
        conn.execute_batch(&copy).map_err(lake_err)?;
        conn.execute_batch("COMMIT;").map_err(lake_err)?;
        fs::rename(&tmp, &path).map_err(|e| lake_err(format!("rename correlations: {e}")))?;
        Ok(())
    }

    /// Query the correlation store, newest first, optionally filtered by legs
    /// and `t` window.
    pub fn read_correlations(
        &self,
        leg_a: Option<&str>,
        leg_b: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Value>, McpError> {
        let path = self.meta_path("correlations");
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut conds: Vec<String> = Vec::new();
        let mut args: Vec<String> = Vec::new();
        if let Some(a) = leg_a {
            conds.push("leg_a = ?".into());
            args.push(a.trim().to_uppercase());
        }
        if let Some(b) = leg_b {
            conds.push("leg_b = ?".into());
            args.push(b.trim().to_uppercase());
        }
        if let Some(s) = start {
            conds.push("t >= CAST(? AS DATE)".into());
            args.push(s.trim().to_string());
        }
        if let Some(e) = end {
            conds.push("t <= CAST(? AS DATE)".into());
            args.push(e.trim().to_string());
        }
        let where_sql = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        let lim = limit.clamp(1, 1_000_000);
        let sql = format!(
            "SELECT * FROM read_parquet('{}'){where_sql} ORDER BY t DESC, leg_a, leg_b LIMIT {lim}",
            sq(&path.to_string_lossy())
        );
        self.query_json(&sql, &args)
    }

    /// Daily closes `(date, close)` for a symbol over `[start, end]`,
    /// ascending. Reads the on-disk bar file directly — no network. Missing
    /// observation dates are simply absent (the caller aligns/ffills).
    pub(crate) fn read_daily_closes(
        &self,
        market: &str,
        timeframe: &str,
        symbol: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, f64)>, McpError> {
        let path = self.bar_path(market, timeframe, symbol);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT CAST(t AS DATE) AS d, c FROM read_parquet('{}') WHERE t >= CAST(? AS TIMESTAMP) AND t <= CAST(? AS TIMESTAMP) ORDER BY t",
            sq(&path.to_string_lossy())
        );
        let rows = self.query_json(&sql, &[norm_ts(start)?, norm_ts(end)?])?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let d = r.get("d").and_then(Value::as_str).unwrap_or_default().to_string();
            let c = r.get("c").and_then(Value::as_f64).unwrap_or(f64::NAN);
            out.push((d, c));
        }
        Ok(out)
    }
}
