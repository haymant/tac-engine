//! Free macro-data providers normalized into lake `Bar`s for the synthetic
//! `market=MACRO` partition (see the `tac-lake-macro` skill).
//!
//! Sources: FRED (JSON API), CBOE (daily index CSVs), OECD (SDMX CSV),
//! World Bank (JSON API). Every observation becomes a bar stamped at 04:00Z on
//! its native date (`o=h=l=c=value`, `v=n=0`) so MACRO bars share the lake's
//! 1d timestamp conventions and consumers forward-fill themselves.
//!
//! Parsing lives in pure functions over payloads so it is unit-testable
//! without network access.

use std::time::Duration;

use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use rmcp::ErrorData as McpError;
use reqwest::{Client, Method};
use serde_json::{Value, json};

use crate::lake::Bar;

const LAKE_1D_HOUR: u32 = 4;
// api.stlouisfed.org (Akamai) whitelists curl-style User-Agents and
// resets/tarpits requests carrying browser or custom UAs.
const USER_AGENT: &str = "curl/8.18.0";

const FRED_BASE: &str = "https://api.stlouisfed.org/fred/series/observations";
const CBOE_BASE: &str = "https://cdn.cboe.com/api/global/us_indices/daily_prices";
const OECD_BASE: &str = "https://sdmx.oecd.org/public/rest/data";
const WORLD_BANK_BASE: &str = "https://api.worldbank.org/v2/country";

fn macro_err(msg: impl std::fmt::Display) -> McpError {
    McpError::internal_error(format!("macro source error: {msg}"), None)
}

/// One managed macro symbol (a row of `macro_catalog.parquet`).
#[derive(Debug, Clone)]
pub struct CatalogRow {
    pub symbol: String,
    pub source: String,
    pub source_id: String,
    pub title: String,
    pub frequency: String,
    pub units: String,
    pub seasonal_adj: bool,
    pub release_lag_days: i64,
    pub enabled: bool,
}

impl CatalogRow {
    pub fn from_value(v: &Value) -> Option<Self> {
        let s = |k: &str| v.get(k).and_then(Value::as_str).map(String::from);
        Some(Self {
            symbol: s("symbol")?,
            source: s("source")?,
            source_id: s("source_id").unwrap_or_default(),
            title: s("title").unwrap_or_default(),
            frequency: s("frequency").unwrap_or_else(|| "D".into()),
            units: s("units").unwrap_or_default(),
            seasonal_adj: v.get("seasonal_adj").and_then(Value::as_bool).unwrap_or(false),
            release_lag_days: v.get("release_lag_days").and_then(Value::as_i64).unwrap_or(0),
            enabled: v.get("enabled").and_then(Value::as_bool).unwrap_or(true),
        })
    }

    pub fn to_value(&self) -> Value {
        json!({
            "symbol": self.symbol,
            "source": self.source,
            "source_id": self.source_id,
            "title": self.title,
            "frequency": self.frequency,
            "units": self.units,
            "seasonal_adj": self.seasonal_adj,
            "release_lag_days": self.release_lag_days,
            "enabled": self.enabled,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacroSourceKind {
    Fred,
    Cboe,
    Oecd,
    WorldBank,
}

pub fn macro_source_kind(source: &str) -> Result<MacroSourceKind, McpError> {
    match source.to_ascii_lowercase().as_str() {
        "fred" => Ok(MacroSourceKind::Fred),
        "cboe" => Ok(MacroSourceKind::Cboe),
        "oecd" => Ok(MacroSourceKind::Oecd),
        "worldbank" | "world_bank" | "wb" => Ok(MacroSourceKind::WorldBank),
        other => Err(macro_err(format!(
            "unknown macro source '{other}' (supported: fred, cboe, oecd, worldbank)"
        ))),
    }
}

// ---- bar stamping ---------------------------------------------------------

/// Stamp an observation as a lake bar at 04:00Z on its date.
pub fn bar_for(date: NaiveDate, o: f64, h: f64, l: f64, c: f64) -> Bar {
    let ndt = date.and_hms_opt(LAKE_1D_HOUR, 0, 0).unwrap();
    Bar {
        t: Utc.from_utc_datetime(&ndt).to_rfc3339(),
        date: date.format("%Y-%m-%d").to_string(),
        o,
        h,
        l,
        c,
        v: 0,
        n: 0,
        vw: c,
    }
}

/// Flexible observation-date parser across source spellings.
pub fn parse_obs_date(s: &str) -> Option<NaiveDate> {
    let t = s.trim();
    // Quarter spelling, e.g. 2020-Q1 / 2020Q1.
    let up = t.to_ascii_uppercase();
    if let Some(qpos) = up.find('Q') {
        match (
            up[..qpos].trim().trim_end_matches(['-', '/', '.', '_']).parse::<i32>(),
            up[qpos + 1..].trim().parse::<u32>(),
        ) {
            (Ok(year), Ok(q)) if qpos > 0 && (1..=4).contains(&q) => {
                return NaiveDate::from_ymd_opt(year, (q - 1) * 3 + 1, 1);
            }
            _ => {}
        }
        return None;
    }
    if let Ok(d) = NaiveDate::parse_from_str(t, "%Y-%m-%d") {
        return Some(d);
    }
    if let Ok(d) = NaiveDate::parse_from_str(t, "%m/%d/%Y") {
        return Some(d);
    }
    // `YYYY-MM` -> first of the month (3-part dates were handled above).
    let mut parts = t.splitn(3, '-');
    if let (Some(y), Some(m), None) = (parts.next(), parts.next(), parts.next())
        && let (Ok(year), Ok(month)) = (y.parse::<i32>(), m.parse::<u32>())
    {
        return NaiveDate::from_ymd_opt(year, month, 1);
    }
    // Bare `YYYY` -> Dec 31 (annual observations land at year end).
    if let Ok(year) = t.parse::<i32>() {
        return NaiveDate::from_ymd_opt(year, 12, 31);
    }
    None
}

fn sort_dedupe(mut bars: Vec<Bar>) -> Vec<Bar> {
    bars.sort_by(|a, b| a.t.cmp(&b.t));
    bars.dedup_by(|a, b| a.t == b.t);
    bars
}

// ---- parsers --------------------------------------------------------------

/// FRED observations JSON. Missing values arrive as the string `"."` and are
/// skipped (they are NOT zero-filled).
pub fn parse_fred_observations(payload: &str) -> Result<Vec<Bar>, McpError> {
    let v: Value = serde_json::from_str(payload)
        .map_err(|e| macro_err(format!("fred json: {e}")))?;
    let mut out = Vec::new();
    for obs in v.get("observations").and_then(Value::as_array).cloned().unwrap_or_default() {
        let date = obs.get("date").and_then(Value::as_str).unwrap_or_default();
        let val_raw = obs.get("value").and_then(Value::as_str).unwrap_or(".");
        let (Some(d), Ok(val)) = (
            parse_obs_date(date),
            val_raw.trim().parse::<f64>(),
        ) else {
            continue;
        };
        out.push(bar_for(d, val, val, val, val));
    }
    Ok(sort_dedupe(out))
}

/// CBOE daily-index CSV (e.g. VIX_History.csv): DATE,OPEN,HIGH,LOW,CLOSE with
/// `%m/%d/%Y` dates. Real OHLC when present; falls back to close-only.
pub fn parse_cboe_csv(payload: &str) -> Result<Vec<Bar>, McpError> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(payload.as_bytes());
    let headers: Vec<String> = rdr
        .headers()
        .map_err(|e| macro_err(format!("cboe csv header: {e}")))?
        .iter()
        .map(|h| h.trim().to_ascii_uppercase())
        .collect();
    let idx_of = |names: &[&str]| -> Option<usize> {
        headers.iter().position(|h| names.iter().any(|n| h == n))
    };
    let i_date = idx_of(&["DATE"]).unwrap_or(0);
    let i_open = idx_of(&["OPEN"]);
    let i_high = idx_of(&["HIGH"]);
    let i_low = idx_of(&["LOW"]);
    let i_close = idx_of(&["CLOSE", "LAST", "INDEX_VALUE"]);
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| macro_err(format!("cboe csv row: {e}")))?;
        let get = |i: Option<usize>| -> Option<f64> {
            i.and_then(|i| rec.get(i)).and_then(|s| s.trim().parse::<f64>().ok())
        };
        let Some(d) = rec.get(i_date).and_then(parse_obs_date) else {
            continue;
        };
        let Some(close) = get(i_close) else { continue };
        out.push(bar_for(
            d,
            get(i_open).unwrap_or(close),
            get(i_high).unwrap_or(close),
            get(i_low).unwrap_or(close),
            close,
        ));
    }
    Ok(sort_dedupe(out))
}

/// Generic observational CSV (OECD SDMX `format=csvfile` and similar): locate
/// a time column and a numeric observation column by common header names.
pub fn parse_generic_csv(payload: &str) -> Result<Vec<Bar>, McpError> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(payload.as_bytes());
    let headers: Vec<String> = rdr
        .headers()
        .map_err(|e| macro_err(format!("csv header: {e}")))?
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();
    let idx_of = |names: &[&str]| headers.iter().position(|h| names.iter().any(|n| h == n));
    let i_time = idx_of(&["time_period", "time", "ref_period", "period", "date"])
        .or_else(|| idx_of(&["obs_time"]));
    let i_val = idx_of(&["observation_value", "obs_value", "value"])
        .or_else(|| headers.len().checked_sub(1));
    let (Some(i_time), Some(i_val)) = (i_time, i_val) else {
        return Err(macro_err(format!(
            "csv lacks recognizable time/value columns (headers: {})",
            headers.join(",")
        )));
    };
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| macro_err(format!("csv row: {e}")))?;
        let (Some(d), Some(val)) = (
            rec.get(i_time).and_then(parse_obs_date),
            rec.get(i_val).and_then(|s| s.trim().parse::<f64>().ok()),
        ) else {
            continue;
        };
        out.push(bar_for(d, val, val, val, val));
    }
    Ok(sort_dedupe(out))
}

/// World Bank indicator JSON: a two-element array `[meta, [entries]]` where
/// each entry carries `{"date":"2023","value":123.4}` (null = missing).
pub fn parse_world_bank(payload: &str) -> Result<Vec<Bar>, McpError> {
    let v: Value = serde_json::from_str(payload)
        .map_err(|e| macro_err(format!("worldbank json: {e}")))?;
    let entries = v
        .get(1)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for e in entries {
        let val = e.get("value").and_then(Value::as_f64);
        let (Some(val), Some(d)) = (
            val,
            e.get("date").and_then(Value::as_str).and_then(parse_obs_date),
        ) else {
            continue;
        };
        out.push(bar_for(d, val, val, val, val));
    }
    Ok(sort_dedupe(out))
}

// ---- URLs -----------------------------------------------------------------

/// FRED reads its key from the environment at fetch time; it is never logged
/// or persisted.
pub fn fred_api_key() -> Result<String, McpError> {
    std::env::var("FRED_API_KEY")
        .ok()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
        .ok_or_else(|| {
            macro_err(
                "FRED_API_KEY is not set. Export a free key from \
                 https://fred.stlouisfed.org/docs/api/api_key.html to load FRED symbols.",
            )
        })
}

fn url_with(base: &str, params: &[(&str, String)]) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    let qs: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect();
    format!("{base}{sep}{}", qs.join("&"))
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn request_url(row: &CatalogRow, start: NaiveDate, end: NaiveDate) -> Result<String, McpError> {
    match macro_source_kind(&row.source)? {
        MacroSourceKind::Fred => {
            let key = fred_api_key()?;
            Ok(url_with(FRED_BASE, &[
                ("series_id", row.source_id.clone()),
                ("api_key", key),
                ("file_type", "json".into()),
                ("observation_start", start.format("%Y-%m-%d").to_string()),
                ("observation_end", end.format("%Y-%m-%d").to_string()),
            ]))
        }
        MacroSourceKind::Cboe => {
            if row.source_id.is_empty() {
                return Err(macro_err(format!(
                    "{}: cboe symbols need source_id = the CSV filename (e.g. VIX_History.csv)",
                    row.symbol
                )));
            }
            Ok(format!("{CBOE_BASE}/{}", row.source_id.trim_start_matches('/')))
        }
        MacroSourceKind::Oecd => {
            if row.source_id.is_empty() {
                return Err(macro_err(format!(
                    "{}: oecd symbols need source_id = '{{AGENCY}},{{DATAFLOW_ID}}/{{KEY}}' \
                     with an EMPTY version (verified working pattern), e.g. \
                     OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.PRVM.IX.C.Y._Z",
                    row.symbol
                )));
            }
            Ok(url_with(
                &format!("{OECD_BASE}/{}", row.source_id.trim_start_matches('/')),
                &[
                    ("format", "csvfile".into()),
                    ("dimensionAtObservation", "AllDimensions".into()),
                    ("startPeriod", start.format("%Y-%m-%d").to_string()),
                    ("endPeriod", end.format("%Y-%m-%d").to_string()),
                ],
            ))
        }
        MacroSourceKind::WorldBank => {
            let (country, indicator) = row
                .source_id
                .split_once('/')
                .ok_or_else(|| {
                    macro_err(format!(
                        "{}: worldbank symbols need source_id = 'COUNTRY/INDICATOR', e.g. US/NY.GDP.MKTP.KD",
                        row.symbol
                    ))
                })?;
            Ok(url_with(
                &format!("{WORLD_BANK_BASE}/{country}/indicator/{indicator}"),
                &[
                    ("format", "json".into()),
                    ("per_page", "20000".into()),
                    ("date", format!("{}:{}", start.year(), end.year())),
                ],
            ))
        }
    }
}

// ---- fetcher ----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MacroFetcher {
    http: Client,
}

impl MacroFetcher {
    pub fn new() -> Result<Self, McpError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| macro_err(format!("build http client: {e}")))?;
        Ok(Self { http })
    }

    async fn send(&self, method: Method, url: &str, echo_body_snippet: bool) -> Result<String, McpError> {
        let resp = self
            .http
            .request(method, url)
            .send()
            .await
            .map_err(|e| macro_err(format!("request failed: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            // Never echo the full URL back: FRED URLs embed the api key. Other
            // providers are keyless, so a short body snippet aids debugging.
            if echo_body_snippet {
                let mut snippet: String = body.chars().take(240).collect();
                if snippet.contains(char::is_control) {
                    snippet = snippet.split_whitespace().collect::<Vec<_>>().join(" ");
                }
                return Err(macro_err(format!(
                    "HTTP {} from macro provider: {}",
                    status.as_u16(),
                    snippet
                )));
            }
            return Err(macro_err(format!(
                "HTTP {} from macro provider ({} bytes)",
                status.as_u16(),
                body.len()
            )));
        }
        Ok(body)
    }

    /// Fetch one catalog symbol over `[start, end]` (native observation dates)
    /// and return sorted, deduped bars.
    pub async fn fetch_series(
        &self,
        row: &CatalogRow,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<Bar>, McpError> {
        let url = request_url(row, start, end)?;
        let echo_body_snippet = !matches!(macro_source_kind(&row.source)?, MacroSourceKind::Fred);
        let payload = self.send(Method::GET, &url, echo_body_snippet).await?;
        let bars = match macro_source_kind(&row.source)? {
            MacroSourceKind::Fred => parse_fred_observations(&payload)?,
            MacroSourceKind::Cboe => parse_cboe_csv(&payload)?,
            MacroSourceKind::Oecd => parse_generic_csv(&payload)?,
            MacroSourceKind::WorldBank => parse_world_bank(&payload)?,
        };
        Ok(bars
            .into_iter()
            .filter(|b| {
                let d = NaiveDate::parse_from_str(&b.date, "%Y-%m-%d");
                d.map(|d| d >= start && d <= end).unwrap_or(false)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fred_parses_and_skips_missing() {
        let payload = r#"{
          "observations": [
            {"date":"2026-01-01","value":"321.5"},
            {"date":"2026-02-01","value":"."},
            {"date":"2026-03-01","value":"322.1"}
          ]
        }"#;
        let bars = parse_fred_observations(payload).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].date, "2026-01-01");
        assert_eq!(bars[0].c, 321.5);
        assert_eq!(bars[0].o, 321.5);
        assert_eq!(bars[0].v, 0);
        assert!(bars[0].t.ends_with("T04:00:00+00:00"));
    }

    #[test]
    fn cboe_parses_us_dates_and_ohlc() {
        let payload = "DATE,OPEN,HIGH,LOW,CLOSE\n\
                       01/02/2026,17.1,17.8,16.9,17.4\n\
                       badrow\n\
                       01/05/2026,17.4,18.2,17.2,18.0\n";
        let bars = parse_cboe_csv(payload).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[1].date, "2026-01-05");
        assert_eq!((bars[1].o, bars[1].h, bars[1].l, bars[1].c), (17.4, 18.2, 17.2, 18.0));
    }

    #[test]
    fn generic_csv_picks_time_and_value_columns() {
        let payload = "UNIT,MULT,GEO,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n\
                       Index,1.0,USA,2026-01,101.2,A\n\
                       Index,1.0,USA,2026-02,102.7,A\n\
                       Index,1.0,USA,2026-03,,B\n";
        let bars = parse_generic_csv(payload).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[1].date, "2026-02-01");
        assert_eq!(bars[1].c, 102.7);
    }

    #[test]
    fn world_bank_parses_annual_entries() {
        let payload = r#"[
          {"page":1,"pages":1,"total":2},
          [
            {"date":"2023","value":27360.9},
            {"date":"2024","value":null}
          ]
        ]"#;
        let bars = parse_world_bank(payload).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].date, "2023-12-31");
    }

    #[test]
    fn obs_dates_cover_all_spellings() {
        for (s, want) in [
            ("2026-08-21", "2026-08-21"),
            ("2026-08", "2026-08-01"),
            ("08/21/2026", "2026-08-21"),
            ("2026-Q2", "2026-04-01"),
            ("2020q3", "2020-07-01"),
            ("2024", "2024-12-31"),
        ] {
            let got = parse_obs_date(s).map(|d| d.format("%Y-%m-%d").to_string());
            assert_eq!(got.as_deref(), Some(want), "input {s}");
        }
        assert!(parse_obs_date("garbage").is_none());
    }

    #[test]
    fn catalog_rows_round_trip() {
        let v = json!({
            "symbol": "FRED_CPIAUCSL",
            "source": "fred",
            "source_id": "CPIAUCSL",
            "title": "CPI-U SA",
            "frequency": "M",
            "units": "index",
            "seasonal_adj": true,
            "release_lag_days": 14,
            "enabled": true,
        });
        let row = CatalogRow::from_value(&v).unwrap();
        assert_eq!(row.frequency, "M");
        assert_eq!(row.release_lag_days, 14);
        assert_eq!(row.to_value(), v);

        // Defaults: missing optional fields don't kill the row.
        let minimal = json!({ "symbol": "X", "source": "cboe" });
        let row = CatalogRow::from_value(&minimal).unwrap();
        assert!(row.enabled && !row.seasonal_adj);
        assert_eq!(row.release_lag_days, 0);
    }

    #[test]
    fn fred_url_embeds_params_and_requires_key() {
        let row = CatalogRow {
            symbol: "FRED_DGS10".into(),
            source: "fred".into(),
            source_id: "DGS10".into(),
            title: String::new(),
            frequency: "D".into(),
            units: "%".into(),
            seasonal_adj: false,
            release_lag_days: 0,
            enabled: true,
        };
        // Without a key this must fail loudly (never silently hit FRED unauth'd).
        if std::env::var("FRED_API_KEY").is_err() {
            assert!(request_url(&row, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2024, 2, 1).unwrap()).is_err());
        }
        let wb = CatalogRow {
            symbol: "WB_GDP_US".into(),
            source: "worldbank".into(),
            source_id: "US/NY.GDP.MKTP.KD".into(),
            ..row.clone()
        };
        let url = request_url(&wb, NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).unwrap();
        assert!(url.starts_with("https://api.worldbank.org/v2/country/US/indicator/NY.GDP.MKTP.KD"));
        assert!(url.contains("date=2000%3A2026"));
        let oecd = CatalogRow {
            symbol: "OECD_IPI_US".into(),
            source: "oecd".into(),
            source_id: "OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.PRVM.IX.C.Y._Z".into(),
            ..wb.clone()
        };
        let url = request_url(&oecd, NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(), NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).unwrap();
        assert!(url.starts_with("https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI/USA.M.PRVM.IX.C.Y._Z?"));
        assert!(url.contains("format=csvfile"));
        assert!(url.contains("dimensionAtObservation=AllDimensions"));
        assert!(url.contains("startPeriod=2000-01-01"));
    }

    #[tokio::test]
    async fn debug_fred_transport_error_chain() {
        use std::net::ToSocketAddrs;
        let addr: std::net::SocketAddr = ("api.stlouisfed.org", 443)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let mut tcp_ok = 0;
        for _ in 0..5 {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(8)).is_ok() {
                tcp_ok += 1;
            }
        }
        println!("std tcp connect: {tcp_ok}/5 ok to {addr}");
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(8))
            .resolve("api.stlouisfed.org", addr)
            .user_agent(USER_AGENT)
            .build()
            .unwrap();
        for i in 0..3 {
            match http
                .get("https://api.stlouisfed.org/fred/category?category_id=125")
                .send()
                .await
            {
                Ok(r) => println!("reqwest try{i}: HTTP {}", r.status()),
                Err(e) => println!("reqwest try{i}: FAIL {}", e.to_string().chars().take(100).collect::<String>()),
            }
        }
    }
}
