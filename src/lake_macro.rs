//! Synthetic macro symbols in the lake (`market=MACRO`) — addon to
//! [`crate::lake`] implementing the `tac-lake-macro` skill.
//!
//! Bars come from free providers ([`crate::macro_sources`]) and land in
//! `market=MACRO/timeframe=1d/symbol=<SYM>.parquet` with the standard Bar
//! schema (`o=h=l=c=value`, `v=n=0`). A dedicated feature family,
//! `family=macro`, holds freq-aware transforms (`macro_*` columns) computed
//! from those bars; it is reserved exclusively for synthetic macro symbols.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{Datelike, Days, NaiveDate, TimeZone, Utc};
use rmcp::ErrorData as McpError;
use serde_json::{json, Map, Value};

use crate::lake::{end_bound, lake_err, norm_ts, opt_val, Lake};
use crate::macro_sources::{CatalogRow, MacroFetcher};

pub const MACRO_MARKET: &str = "MACRO";
pub const MACRO_TIMEFRAME: &str = "1d";
pub const MACRO_FAMILY: &str = "macro";

/// Feature families computable for a macro symbol. `transform` works at every
/// frequency; `regime` is daily-only; `align` projects the series onto the US
/// trading-day calendar with release-lag awareness.
pub const FAMILIES: [&str; 3] = ["transform", "regime", "align"];

/// Starter catalog seeded into `macro_catalog.parquet` on first use.
fn default_catalog() -> Vec<CatalogRow> {
    let mk = |symbol: &str,
              source: &str,
              source_id: &str,
              title: &str,
              frequency: &str,
              units: &str,
              seasonal_adj: bool,
              release_lag_days: i64| CatalogRow {
        symbol: symbol.into(),
        source: source.into(),
        source_id: source_id.into(),
        title: title.into(),
        frequency: frequency.into(),
        units: units.into(),
        seasonal_adj,
        release_lag_days,
        enabled: true,
    };
    vec![
        mk("FRED_CPIAUCSL", "fred", "CPIAUCSL", "CPI-U, all items, SA", "M", "index", true, 14),
        mk("FRED_FEDFUNDS", "fred", "FEDFUNDS", "Effective fed funds rate (monthly)", "M", "%", false, 7),
        mk("FRED_DGS10", "fred", "DGS10", "10-year Treasury constant maturity", "D", "%", false, 0),
        mk("FRED_DGS2", "fred", "DGS2", "2-year Treasury constant maturity", "D", "%", false, 0),
        mk("FRED_T10Y2Y", "fred", "T10Y2Y", "Term spread 10y minus 2y", "D", "%", false, 0),
        mk("FRED_UNRATE", "fred", "UNRATE", "Unemployment rate", "M", "%", true, 15),
        mk("FRED_PAYEMS", "fred", "PAYEMS", "Nonfarm payrolls", "M", "thousands", true, 15),
        mk("FRED_INDPRO", "fred", "INDPRO", "Industrial production index", "M", "index", true, 15),
        mk("FRED_UMCSENT", "fred", "UMCSENT", "Consumer sentiment", "M", "index", false, 0),
        mk("FRED_GDPC1", "fred", "GDPC1", "Real GDP", "Q", "bn USD", true, 45),
        mk("FRED_WALCL", "fred", "WALCL", "Fed balance sheet assets", "W", "mn USD", false, 4),
        mk("FRED_M2SL", "fred", "M2SL", "M2 money supply", "M", "bn USD", true, 28),
        // NAPM (ISM manufacturing PMI) excluded: ISM removed all ISM series
        // from FRED in June 2016, so the id no longer resolves.
        mk("CBOE_VIX", "cboe", "VIX_History.csv", "CBOE VIX index history", "D", "index", false, 0),
        mk("WB_GDP_US", "worldbank", "US/NY.GDP.MKTP.KD", "US real GDP (constant 2015 USD)", "A", "USD", true, 120),
        mk(
            "OECD_CLI_US",
            "oecd",
            "OECD.SDD.STES,DSD_STES@DF_CLI/USA.M.LI.IX._Z.AA.IX._Z.H",
            "US composite leading indicator, amplitude adjusted",
            "M",
            "index",
            true,
            30,
        ),
        mk(
            "OECD_CPI_US",
            "oecd",
            "OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.CP.IX._Z._Z._Z",
            "US consumer price index, all items",
            "M",
            "index",
            true,
            14,
        ),
        mk(
            "OECD_IPI_US",
            "oecd",
            "OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.PRVM.IX.C.Y._Z",
            "US industrial production index (ex construction), calendar adj",
            "M",
            "index",
            true,
            15,
        ),
    ]
}

const CATALOG_SCHEMA: &str = "(symbol VARCHAR, source VARCHAR, source_id VARCHAR, title VARCHAR, \
frequency VARCHAR, units VARCHAR, seasonal_adj BOOLEAN, release_lag_days BIGINT, enabled BOOLEAN, updated_at TIMESTAMP)";
const CATALOG_INSERT: &str = "INSERT INTO macro_catalog VALUES (CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), \
CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS BOOLEAN), CAST(? AS BIGINT), CAST(? AS BOOLEAN), CAST(? AS TIMESTAMP))";

impl Lake {
    fn macro_catalog_path(&self) -> PathBuf {
        self.meta_path("macro_catalog")
    }

    /// Seed the starter catalog on first use (idempotent).
    pub fn ensure_macro_catalog(&self) -> Result<(), McpError> {
        let path = self.macro_catalog_path();
        if path.exists() {
            return Ok(());
        }
        self.upsert_macro_catalog_rows(default_catalog())
    }

    /// Merge catalog rows by symbol and rewrite the parquet.
    pub fn upsert_macro_catalog_rows(&self, rows: Vec<CatalogRow>) -> Result<(), McpError> {
        let existing = if self.macro_catalog_path().exists() {
            self.table_read("macro_catalog", "", &[])?
        } else {
            Vec::new()
        };
        let mut by_key: BTreeMap<String, Value> = BTreeMap::new();
        for r in existing {
            if let Some(s) = r.get("symbol").and_then(Value::as_str) {
                by_key.insert(s.to_string(), r);
            }
        }
        for row in rows {
            let mut v = row.to_value();
            v["updated_at"] = json!(Utc::now().to_rfc3339());
            by_key.insert(row.symbol.clone(), v);
        }
        let rows: Vec<Vec<Value>> = by_key
            .into_values()
            .map(|v| {
                let m = v.as_object().cloned().unwrap_or_default();
                vec![
                    m.get("symbol").cloned().unwrap_or(Value::Null),
                    m.get("source").cloned().unwrap_or(Value::Null),
                    m.get("source_id").cloned().unwrap_or(Value::Null),
                    m.get("title").cloned().unwrap_or(Value::Null),
                    m.get("frequency").cloned().unwrap_or(Value::Null),
                    m.get("units").cloned().unwrap_or(Value::Null),
                    m.get("seasonal_adj").cloned().unwrap_or(Value::Null),
                    m.get("release_lag_days").cloned().unwrap_or(Value::Null),
                    m.get("enabled").cloned().unwrap_or(Value::Null),
                    m.get("updated_at").cloned().unwrap_or(Value::Null),
                ]
            })
            .collect();
        self.table_write("macro_catalog", CATALOG_SCHEMA, CATALOG_INSERT, &rows)
    }

    /// Read managed macro symbols; `symbol=None` returns all rows.
    pub fn read_macro_catalog(
        &self,
        symbol: Option<&str>,
        enabled_only: bool,
    ) -> Result<Vec<Value>, McpError> {
        self.ensure_macro_catalog()?;
        let mut conds: Vec<String> = Vec::new();
        let mut args: Vec<String> = Vec::new();
        if let Some(s) = symbol {
            conds.push("symbol = ?".to_string());
            args.push(s.to_uppercase());
        }
        if enabled_only {
            conds.push("enabled = TRUE".to_string());
        }
        let where_sql = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        self.table_read("macro_catalog", &where_sql, &args)
    }

    /// Frequency + release lag for a symbol, falling back to inference from
    /// the observation spacing when the catalog row is missing or has no
    /// usable `frequency` (e.g. rows added via put_macro_symbol without one).
    fn macro_symbol_meta(
        &self,
        symbol: &str,
        dates: &[NaiveDate],
    ) -> Result<(String, i64), McpError> {
        if let Some(cr) = self
            .read_macro_catalog(Some(symbol), false)?
            .first()
            .and_then(CatalogRow::from_value)
        {
            let freq = cr.frequency.trim().to_ascii_uppercase();
            return Ok((
                if freq.is_empty() {
                    infer_frequency(dates)
                } else {
                    freq
                },
                cr.release_lag_days,
            ));
        }
        Ok((infer_frequency(dates), 0))
    }

    /// Fetch one catalog symbol from its free provider, persist synthetic
    /// bars under `market=MACRO`, register it in symbols.parquet, and refresh
    /// the `family=macro` features. Returns a per-symbol result entry.
    pub async fn load_macro_symbol(
        &self,
        fetcher: &MacroFetcher,
        row: &CatalogRow,
        start_date: NaiveDate,
        end_date: NaiveDate,
        families: &[String],
    ) -> Result<Value, McpError> {
        let mut entry = json!({ "symbol": row.symbol, "source": row.source });

        let bars = fetcher.fetch_series(row, start_date, end_date).await?;
        entry["fetched"] = json!(bars.len());
        if !bars.is_empty() {
            self.upsert_bars(MACRO_MARKET, MACRO_TIMEFRAME, &row.symbol, &bars)?;
            self.update_coverage(
                MACRO_MARKET,
                MACRO_TIMEFRAME,
                &row.symbol,
                &bars,
                &row.source,
                "none",
            )?;
            self.touch_symbols_as(
                MACRO_MARKET,
                MACRO_TIMEFRAME,
                std::slice::from_ref(&row.symbol),
                "macro",
                Some(false),
            )?;
        }

        let total = self.read_bars_full(MACRO_MARKET, MACRO_TIMEFRAME, &row.symbol)?.len();
        entry["bars_total"] = json!(total);

        match self.compute_macro_features(
            &row.symbol,
            &start_date.format("%Y-%m-%d").to_string(),
            &end_date.format("%Y-%m-%d").to_string(),
            families,
            true,
        ) {
            Ok(feats) => {
                let cols: Vec<String> = feats
                    .first()
                    .and_then(Value::as_object)
                    .map(|m| m.keys().filter(|k| k.starts_with("macro_")).cloned().collect())
                    .unwrap_or_default();
                entry["feature_rows"] = json!(feats.len());
                entry["feature_columns"] = json!(cols);
            }
            Err(e) => {
                entry["features_error"] = json!(e.message);
            }
        }
        Ok(entry)
    }

    /// Compute `macro_*` feature families from the stored bars of a synthetic
    /// symbol and optionally persist them to
    /// `features/market=MACRO/timeframe=1d/family=macro/symbol=<SYM>.parquet`.
    ///
    /// Computation always uses the full bar history (warm-up), while the
    /// returned rows cover `[start, end]`.
    pub fn compute_macro_features(
        &self,
        symbol: &str,
        start: &str,
        end: &str,
        families: &[String],
        persist: bool,
    ) -> Result<Vec<Value>, McpError> {
        let wanted = parse_macro_families(families)?;

        let bars = self.read_bars_full(MACRO_MARKET, MACRO_TIMEFRAME, symbol)?;
        if bars.is_empty() {
            return Ok(Vec::new());
        }
        let native_t: Vec<String> = bars.iter().map(|b| b.t.clone()).collect();
        let dates: Vec<NaiveDate> = bars
            .iter()
            .filter_map(|b| NaiveDate::parse_from_str(&b.date, "%Y-%m-%d").ok())
            .collect();
        if dates.len() != bars.len() {
            return Err(lake_err(format!("{symbol}: unparseable observation dates")));
        }
        let vals: Vec<f64> = bars.iter().map(|b| b.c).collect();
        let (freq, release_lag_days) = self.macro_symbol_meta(symbol, &dates)?;

        // rows keyed by canonical timestamp (04:00Z like every other 1d bar);
        // transform/regime attach to native obs timestamps, align to the
        // daily grid, merging into one row when both hit the same day.
        let mut rows: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
        for t in &native_t {
            rows.entry(t.clone()).or_default();
        }

        let (mom_len, yoy_len, z_len) = macro_periods(&freq);
        if wanted.contains(&"transform") {
            attach(&mut rows, &native_t, "macro_yoy", pct_change(&vals, yoy_len));
            if let Some(m) = mom_len {
                attach(&mut rows, &native_t, "macro_mom", pct_change(&vals, m));
            }
            attach(&mut rows, &native_t, "macro_zscore", rolling_zscore(&vals, z_len));
        }

        if wanted.contains(&"regime") && freq.as_str() == "D" {
            let sma = sma_vec(&vals, 200);
            let flag: Vec<Option<f64>> = vals
                .iter()
                .zip(sma.iter())
                .map(|(v, s)| match (v, s) {
                    (v, Some(s)) if v.is_finite() => Some(if v > s { 1.0 } else { 0.0 }),
                    _ => None,
                })
                .collect();
            attach(&mut rows, &native_t, "macro_above_sma200", flag);
            attach(&mut rows, &native_t, "macro_trend_slope_60", ols_slope_log(&vals, 60));
        }

        if wanted.contains(&"align") {
            let first = dates[0];
            let last = *dates.last().expect("dates non-empty");
            let grid = self.us_calendar_days(first, last)?.unwrap_or_else(|| weekday_grid(first, last));
            let lag = chrono::Duration::days(release_lag_days.max(0));
            let vis: Vec<NaiveDate> = dates.iter().map(|d| *d + lag).collect();

            let mut ts: Vec<String> = Vec::new();
            let mut vv: Vec<Option<f64>> = Vec::new();
            let mut aa: Vec<Option<f64>> = Vec::new();
            let mut idx: i64 = -1;
            for day in grid {
                while ((idx + 1) as usize) < vis.len() && vis[(idx + 1) as usize] <= day {
                    idx += 1;
                }
                if idx < 0 {
                    continue;
                }
                let i = idx as usize;
                ts.push(day_ts(day));
                vv.push(Some(vals[i]));
                aa.push(Some((day - dates[i]).num_days() as f64));
            }
            attach(&mut rows, &ts, "macro_value_ffill", vv);
            attach(&mut rows, &ts, "macro_age_days", aa);
        }

        // Union schema: every row carries every column (Null where a family
        // does not apply), because persist_family derives the schema from the
        // first row alone.
        let mut cols: Vec<String> = rows
            .values()
            .flat_map(|m| m.keys().cloned())
            .collect();
        cols.sort();
        cols.dedup();
        let all: Vec<Value> = rows
            .into_iter()
            .map(|(t, mut m)| {
                for c in &cols {
                    m.entry(c.clone()).or_insert(Value::Null);
                }
                let mut obj = Map::new();
                obj.insert("market".into(), json!(MACRO_MARKET));
                obj.insert("symbol".into(), json!(symbol.to_uppercase()));
                obj.insert("timeframe".into(), json!(MACRO_TIMEFRAME));
                obj.insert("t".into(), json!(t));
                for (k, v) in m {
                    obj.insert(k, v);
                }
                Value::Object(obj)
            })
            .collect();

        if persist && !all.is_empty() {
            self.persist_family(MACRO_MARKET, symbol, MACRO_TIMEFRAME, MACRO_FAMILY, &all)?;
        }

        let ns = norm_ts(start)?;
        let ne = end_bound(end);
        let key = |v: &Value| {
            v.get("t")
                .and_then(Value::as_str)
                .and_then(ts_key)
                .unwrap_or(i64::MIN)
        };
        let lo = ts_key(&ns).unwrap_or(i64::MIN);
        let hi = ts_key(&ne).unwrap_or(i64::MAX);
        Ok(all
            .into_iter()
            .filter(|r| {
                let k = key(r);
                k >= lo && k <= hi
            })
            .collect())
    }

    /// US trading-day calendar restricted to `[first, last]`; `None` when the
    /// calendar is absent/empty for the range (callers fall back to weekdays).
    fn us_calendar_days(
        &self,
        first: NaiveDate,
        last: NaiveDate,
    ) -> Result<Option<Vec<NaiveDate>>, McpError> {
        let fs = first.format("%Y-%m-%d").to_string();
        let ls = last.format("%Y-%m-%d").to_string();
        let rows = self.read_calendar(Some("US"), Some(fs.as_str()), Some(ls.as_str()))?;
        let mut days: Vec<NaiveDate> = rows
            .iter()
            .filter_map(|r| r.get("date").and_then(Value::as_str))
            .filter_map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            .collect();
        days.sort();
        days.dedup();
        Ok(if days.is_empty() { None } else { Some(days) })
    }

    /// Read persisted `family=macro` features for synthetic symbols
    /// (`market=MACRO/timeframe=1d`).
    pub fn read_macro_features(
        &self,
        symbol: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<Vec<Value>, McpError> {
        self.read_features(
            Some(MACRO_MARKET),
            symbol.map(str::to_uppercase).as_deref(),
            Some(MACRO_TIMEFRAME),
            start,
            end,
        )
    }
}

// ---- helpers -----------------------------------------------------------------

/// Resolve comma-separated macro family names; empty input -> all families.
fn parse_macro_families(families: &[String]) -> Result<Vec<&'static str>, McpError> {
    let mut out: Vec<&'static str> = Vec::new();
    for raw in families {
        for item in raw.split(',') {
            let item = item.trim().to_ascii_lowercase();
            if item.is_empty() {
                continue;
            }
            let known = FAMILIES.iter().copied().find(|f| *f == item.as_str());
            let Some(fam) = known else {
                return Err(lake_err(format!(
                    "unsupported macro family: {item} (supported: {})",
                    FAMILIES.join(",")
                )));
            };
            if !out.contains(&fam) {
                out.push(fam);
            }
        }
    }
    if out.is_empty() {
        out.extend(FAMILIES.iter().copied());
    }
    Ok(out)
}

/// Observation-count windows `(mom, yoy, zscore)` per catalog frequency.
/// YoY/MoM must respect the native cadence — 12 monthly observations is a
/// year, 21 daily observations is roughly a month.
pub(crate) fn macro_periods(freq: &str) -> (Option<usize>, usize, usize) {
    match freq.to_ascii_uppercase().as_str() {
        "W" | "WEEKLY" => (Some(4), 52, 104),
        "M" | "MONTHLY" => (Some(1), 12, 24),
        "Q" | "QUARTERLY" => (Some(1), 4, 8),
        "A" | "ANNUAL" => (None, 1, 5),
        _ => (Some(21), 252, 252),
    }
}

/// Guess the cadence from the median gap between observation dates.
pub(crate) fn infer_frequency(dates: &[NaiveDate]) -> String {
    let mut gaps: Vec<i64> = dates
        .windows(2)
        .map(|w| (w[1] - w[0]).num_days())
        .filter(|g| *g > 0)
        .collect();
    if gaps.is_empty() {
        return "D".to_string();
    }
    gaps.sort_unstable();
    let med = gaps[gaps.len() / 2];
    match med {
        0..=2 => "D",
        3..=10 => "W",
        11..=45 => "M",
        46..=120 => "Q",
        _ => "A",
    }
    .to_string()
}

/// Percentage change vs `k` observations back; requires positive levels on
/// both ends (ratio changes across sign are meaningless).
pub(crate) fn pct_change(vals: &[f64], k: usize) -> Vec<Option<f64>> {
    vals.iter()
        .enumerate()
        .map(|(i, v)| {
            if i < k || !v.is_finite() {
                return None;
            }
            let prev = vals[i - k];
            if !prev.is_finite() || prev <= 0.0 || *v <= 0.0 {
                return None;
            }
            Some(v / prev - 1.0)
        })
        .collect()
}

/// Rolling z-score of the level over a trailing window ending at `i`.
pub(crate) fn rolling_zscore(vals: &[f64], w: usize) -> Vec<Option<f64>> {
    vals.iter()
        .enumerate()
        .map(|(i, v)| {
            if i + 1 < w || !v.is_finite() {
                return None;
            }
            let win = &vals[i + 1 - w..=i];
            let n = win.len() as f64;
            let mean = win.iter().sum::<f64>() / n;
            let var = win.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
            let std = var.sqrt();
            if !std.is_finite() || std <= 0.0 {
                return None;
            }
            Some((v - mean) / std)
        })
        .collect()
}

/// OLS slope of log(level) over a trailing window (per-observation drift).
pub(crate) fn ols_slope_log(vals: &[f64], w: usize) -> Vec<Option<f64>> {
    vals.iter()
        .enumerate()
        .map(|(i, _)| {
            if i + 1 < w {
                return None;
            }
            let win: Vec<f64> = vals[i + 1 - w..=i]
                .iter()
                .filter(|x| x.is_finite() && **x > 0.0)
                .map(|x| x.ln())
                .collect();
            if win.len() != w {
                return None;
            }
            let n = w as f64;
            let mean_x = (n - 1.0) / 2.0;
            let mean_y = win.iter().sum::<f64>() / n;
            let mut cov = 0.0;
            let mut var = 0.0;
            for (j, y) in win.iter().enumerate() {
                let dx = j as f64 - mean_x;
                cov += dx * (y - mean_y);
                var += dx * dx;
            }
            if var == 0.0 {
                return None;
            }
            Some(cov / var)
        })
        .collect()
}

pub(crate) fn sma_vec(vals: &[f64], w: usize) -> Vec<Option<f64>> {
    vals.iter()
        .enumerate()
        .map(|(i, _)| {
            if i + 1 < w {
                return None;
            }
            let win = &vals[i + 1 - w..=i];
            if win.iter().any(|x| !x.is_finite()) {
                return None;
            }
            Some(win.iter().sum::<f64>() / w as f64)
        })
        .collect()
}

fn attach(
    rows: &mut BTreeMap<String, Map<String, Value>>,
    ts: &[String],
    col: &str,
    series: Vec<Option<f64>>,
) {
    for (t, v) in ts.iter().zip(series) {
        rows.entry(t.clone()).or_default().insert(col.to_string(), opt_val(v));
    }
}

/// Canonical 1d timestamp (04:00Z) for a calendar day.
pub(crate) fn day_ts(d: NaiveDate) -> String {
    Utc.from_utc_datetime(&d.and_hms_opt(4, 0, 0).unwrap()).to_rfc3339()
}

/// Canonical epoch microseconds for RFC3339 / naive-datetime / date strings.
pub(crate) fn ts_key(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Utc.from_utc_datetime(&ndt).timestamp_micros());
    }
    if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(
            Utc.from_utc_datetime(&nd.and_hms_opt(0, 0, 0)?).timestamp_micros(),
        );
    }
    None
}

fn weekday_grid(first: NaiveDate, last: NaiveDate) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut d = first;
    while d <= last {
        if d.weekday().number_from_monday() <= 5 {
            out.push(d);
        }
        match d.checked_add_days(Days::new(1)) {
            Some(next) => d = next,
            None => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lake::Bar;
    use crate::macro_sources::bar_for;
    use chrono::Months;

    fn temp_lake(tag: &str) -> Lake {
        let dir = std::env::temp_dir().join(format!("taclake_macro_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Lake::new(dir).unwrap()
    }

    fn monthly_bars(start: (i32, u32), months: usize, base: f64) -> Vec<Bar> {
        let mut out = Vec::new();
        let (y0, m0) = start;
        for i in 0..months {
            let total = (y0 as i64) * 12 + (m0 as i64 - 1) + i as i64;
            let y = (total / 12) as i32;
            let m = (total % 12) as u32 + 1;
            let d = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
            out.push(bar_for(d, base + i as f64, base + i as f64, base + i as f64, base + i as f64));
        }
        out
    }

    #[test]
    fn math_helpers_sanity() {
        let v: Vec<f64> = (100..=110).map(|x| x as f64).collect();
        let pc = pct_change(&v, 1);
        assert_eq!(pc[0], None);
        assert!((pc[1].unwrap() - 0.01).abs() < 1e-12);

        let flat = vec![5.0; 30];
        assert!(rolling_zscore(&flat, 20).iter().all(|x| x.is_none()));

        let growth: Vec<f64> = (0..80).map(|i| 100.0 * (1.01_f64).powi(i)).collect();
        let slopes = ols_slope_log(&growth, 60);
        assert!(slopes[..59].iter().all(Option::is_none));
        for s in [slopes[59], slopes[79]] {
            let s = s.expect("full window slope should be defined");
            assert!((s - 1.01_f64.ln()).abs() < 1e-6);
        }

        let sma = sma_vec(&v, 5);
        assert_eq!(sma[4].unwrap(), 102.0);

        assert_eq!(macro_periods("M"), (Some(1), 12, 24));
        assert_eq!(macro_periods("Q"), (Some(1), 4, 8));
        assert_eq!(macro_periods("weekly"), (Some(4), 52, 104));
        assert_eq!(macro_periods("D"), (Some(21), 252, 252));

        let monthly: Vec<NaiveDate> =
            (0..13).map(|i| NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + Months::new(i as u32)).collect();
        assert_eq!(infer_frequency(&monthly), "M");
    }

    #[test]
    fn catalog_seeds_and_filters() {
        let lake = temp_lake("catalog");
        let all = lake.read_macro_catalog(None, false).unwrap();
        assert!(all.len() >= 14);
        assert!(all.iter().any(|r| r.get("symbol").and_then(Value::as_str) == Some("CBOE_VIX")));

        let one = lake.read_macro_catalog(Some("fred_dgs10"), false).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0]["frequency"], "D");

        // Upsert overrides fields for existing symbols.
        let row = CatalogRow {
            symbol: "FRED_DGS10".into(),
            source: "fred".into(),
            source_id: "DGS10".into(),
            title: "custom".into(),
            frequency: "D".into(),
            units: "%".into(),
            seasonal_adj: false,
            release_lag_days: 1,
            enabled: false,
        };
        lake.upsert_macro_catalog_rows(vec![row]).unwrap();
        let disabled = lake.read_macro_catalog(None, true).unwrap();
        assert!(!disabled.iter().any(|r| r.get("symbol").and_then(Value::as_str) == Some("FRED_DGS10")));
    }

    #[test]
    fn transform_family_on_monthly_bars() {
        let lake = temp_lake("transform");
        let sym = "FRED_CPIAUCSL";
        lake.upsert_bars(MACRO_MARKET, MACRO_TIMEFRAME, sym, &monthly_bars((2024, 1), 26, 100.0)).unwrap();

        let feats = lake
            .compute_macro_features(sym, "2024-01-01", "2026-12-31", &["transform".to_string()], true)
            .unwrap();
        assert_eq!(feats.len(), 26);
        let yoy_at = |i: usize| {
            feats[i]
                .get("macro_yoy")
                .and_then(Value::as_f64)
        };
        assert_eq!(yoy_at(11), None);
        let y12 = yoy_at(12).unwrap();
        assert!((y12 - (112.0 / 100.0 - 1.0)).abs() < 1e-9);
        let z_last = feats[25].get("macro_zscore").and_then(Value::as_f64).unwrap();
        assert!(z_last.is_finite());

        let fam_file = lake
            .dir
            .join(format!("features/market={MACRO_MARKET}/timeframe={MACRO_TIMEFRAME}/family={MACRO_FAMILY}/symbol={sym}.parquet"));
        assert!(fam_file.is_file(), "expected {}", fam_file.display());
        let readback = lake.read_macro_features(Some(sym), None, None).unwrap();
        assert_eq!(readback.len(), 26);
        assert!(readback[0].as_object().unwrap().contains_key("macro_yoy"));
    }

    #[test]
    fn empty_catalog_frequency_falls_back_to_inference() {
        // put_macro_symbol rows can carry frequency="" — features must then
        // infer the cadence from the data instead of using daily windows.
        let lake = temp_lake("freq-fallback");
        let sym = "OECD_CPI_TEST";
        lake.upsert_macro_catalog_rows(vec![CatalogRow {
            symbol: sym.into(),
            source: "oecd".into(),
            source_id: "TEST".into(),
            title: String::new(),
            frequency: String::new(),
            units: "index".into(),
            seasonal_adj: false,
            release_lag_days: 14,
            enabled: true,
        }])
        .unwrap();
        lake.upsert_bars(MACRO_MARKET, MACRO_TIMEFRAME, sym, &monthly_bars((2024, 1), 26, 100.0))
            .unwrap();

        let feats = lake
            .compute_macro_features(sym, "2024-01-01", "2026-12-31", &["transform".to_string()], false)
            .unwrap();
        assert_eq!(feats.len(), 26);
        // Monthly windows: first defined yoy at row 12 is 112/100 - 1 (not a
        // 252-row daily window, which would leave every yoy null here).
        let yoy: Vec<Option<f64>> = feats
            .iter()
            .map(|r| r.get("macro_yoy").and_then(Value::as_f64))
            .collect();
        assert_eq!(yoy[..12].iter().filter(|v| v.is_some()).count(), 0);
        assert!((yoy[12].unwrap() - 0.12).abs() < 1e-9);

        // The inferred cadence must not discard the catalog release lag:
        // mid-Feb still shows January's observation (age 44); from Feb 15
        // (obs Feb 1 + 14d) February's value takes over (age 14→15).
        let lagged = lake
            .compute_macro_features(sym, "2024-02-01", "2024-02-29", &["align".to_string()], false)
            .unwrap();
        let row_at = |d: &str| {
            lagged
                .iter()
                .find(|r| r.get("t").and_then(Value::as_str) == Some(&format!("{d}T04:00:00+00:00")))
        };
        assert_eq!(
            row_at("2024-02-14").and_then(|r| r.get("macro_age_days")).and_then(Value::as_f64),
            Some(44.0)
        );
        let feb15 = row_at("2024-02-15").expect("Feb 15 grid day");
        assert_eq!(feb15.get("macro_age_days").and_then(Value::as_f64), Some(14.0));
        assert_eq!(feb15.get("macro_value_ffill").and_then(Value::as_f64), Some(101.0));
    }

    #[test]
    fn align_family_respects_release_lag() {
        let lake = temp_lake("align");
        let sym = "FREQLAG_TEST";
        // Seed a monthly catalog row with a 14-day publication lag.
        lake.upsert_macro_catalog_rows(vec![CatalogRow {
            symbol: sym.into(),
            source: "fred".into(),
            source_id: "TEST".into(),
            title: String::new(),
            frequency: "M".into(),
            units: String::new(),
            seasonal_adj: false,
            release_lag_days: 14,
            enabled: true,
        }])
        .unwrap();

        let bars = vec![
            bar_for(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 100.0, 100.0, 100.0, 100.0),
            bar_for(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(), 101.0, 101.0, 101.0, 101.0),
            bar_for(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(), 102.0, 102.0, 102.0, 102.0),
        ];
        lake.upsert_bars(MACRO_MARKET, MACRO_TIMEFRAME, sym, &bars).unwrap();

        let feats = lake
            .compute_macro_features(sym, "2024-01-01", "2024-03-31", &["align".to_string()], true)
            .unwrap();
        let by_t: std::collections::HashMap<String, f64> = feats
            .iter()
            .filter_map(|r| {
                Some((
                    r.get("t")?.as_str()?.to_string(),
                    r.get("macro_value_ffill")?.as_f64()?,
                ))
            })
            .collect();
        let get = |d: &str| by_t.get(&format!("{d}T04:00:00+00:00"));

        // Before the Jan value becomes visible (Jan 1 + 14d): no row at all.
        assert_eq!(get("2024-01-12"), None);
        // From Jan 15 onward the January value is visible.
        assert_eq!(get("2024-01-15"), Some(&100.0));
        // End of January still shows January's value...
        assert_eq!(get("2024-01-31"), Some(&100.0));
        // ...until February's observation turns visible on Feb 15.
        assert_eq!(get("2024-02-14"), Some(&100.0));
        assert_eq!(get("2024-02-15"), Some(&101.0));

        let ages: std::collections::HashMap<String, f64> = feats
            .iter()
            .filter_map(|r| {
                Some((
                    r.get("t")?.as_str()?.to_string(),
                    r.get("macro_age_days")?.as_f64()?,
                ))
            })
            .collect();
        assert_eq!(ages.get("2024-01-15T04:00:00+00:00"), Some(&14.0));

        // Obs dates that are also trading days merge into one row carrying
        // both align columns and (had we asked) transform columns.
        assert!(by_t.contains_key("2024-02-01T04:00:00+00:00"));
    }

    #[test]
    fn unknown_family_rejected() {
        let lake = temp_lake("badfam");
        let err = lake
            .compute_macro_features("X", "2024-01-01", "2024-02-01", &["nope".to_string()], false)
            .unwrap_err();
        assert!(err.message.contains("unsupported macro family"));
    }
}
