//! Yahoo Finance chart-API client used as a **history fallback** when Alpaca's
//! daily-bar coverage is too short: Alpaca SIP goes back to 2016-01-04 and IEX
//! to 2020-07-27, while Yahoo carries daily bars back to 2000 (or a symbol's
//! IPO) for most US equities/ETFs.
//!
//! Endpoint: `GET https://queryN.finance.yahoo.com/v8/finance/chart/{symbol}`,
//! `interval=1d`, `period1`/`period2` as unix epoch. No auth required, but it
//! rate-limits anonymous clients (HTTP 429) — we retry with backoff across both
//! hosts (`query2`, `query1`).

use std::time::Duration;

use chrono::{DateTime, Days, NaiveDate, TimeZone, Utc};
use reqwest::{Client, StatusCode};
use rmcp::ErrorData as McpError;
use serde_json::Value;

use crate::lake::Bar;

/// Yahoo stamps daily bars at the exchange session open/close in local wall
/// time; we re-stamp them at 04:00Z (midnight ET) so they share the lake's
/// 1d `t` convention with Alpaca daily bars.
const LAKE_1D_HOUR: u32 = 4;

/// Split long ranges into ~5-year requests to stay well under Yahoo's
/// per-response cap and keep retries granular.
const MAX_CHUNK_DAYS: i64 = 1830;

const HOSTS: [&str; 2] = ["query2", "query1"];
const USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0";

#[derive(Debug)]
pub struct YahooBars {
    pub bars: Vec<Bar>,
    /// From `meta.firstTradeDate` (unix epoch) — the earliest date the symbol
    /// traded at all on Yahoo, when the API reports it.
    pub first_trade_date: Option<NaiveDate>,
}

#[derive(Debug, Clone)]
pub struct YahooClient {
    http: Client,
}

fn yahoo_err(msg: impl std::fmt::Display) -> McpError {
    McpError::internal_error(format!("yahoo error: {msg}"), None)
}

fn date_epoch(d: NaiveDate) -> i64 {
    let ndt = d
        .and_hms_opt(LAKE_1D_HOUR, 0, 0)
        .unwrap_or_else(|| d.and_hms_opt(0, 0, 0).unwrap());
    Utc.from_utc_datetime(&ndt).timestamp()
}

impl YahooClient {
    pub fn new() -> Result<Self, McpError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| yahoo_err(format!("build http client: {e}")))?;
        Ok(Self { http })
    }

    /// Daily bars with session dates in `[start, end)` (end exclusive — pass
    /// the day *after* the last session you want). Idempotent, sorted by `t`,
    /// deduped on session date (last bar of a day wins).
    ///
    /// Yahoo serves the full `1d` series in a single response (even a 26-year
    /// range), returning bars from the symbol's `firstTradeDate` when `start`
    /// precedes it, so we try the whole range in one shot first and only chunk
    /// on failure.
    pub async fn daily_bars(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<YahooBars, McpError> {
        if end <= start {
            return Ok(YahooBars {
                bars: Vec::new(),
                first_trade_date: None,
            });
        }
        if let Ok(res) = self.fetch_range(symbol, start, end).await {
            return Ok(self.dedupe(res));
        }
        let mut out: Vec<Bar> = Vec::new();
        let mut first_trade_date: Option<NaiveDate> = None;
        let mut cur = start;
        while cur < end {
            let chunk_end = cur
                .checked_add_days(Days::new(MAX_CHUNK_DAYS as u64))
                .unwrap_or(end)
                .min(end);
            // Skip chunks wholly before the symbol's IPO (Yahoo 400s those).
            if let Some(ftd) = first_trade_date {
                if chunk_end <= ftd {
                    cur = chunk_end;
                    continue;
                }
            }
            let res = self.fetch_range(symbol, cur, chunk_end).await?;
            out.extend(res.bars);
            if first_trade_date.is_none() {
                first_trade_date = res.first_trade_date;
            }
            cur = chunk_end;
            if cur < end {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        Ok(self.dedupe(YahooBars {
            bars: out,
            first_trade_date,
        }))
    }

    fn dedupe(&self, mut res: YahooBars) -> YahooBars {
        res.bars.sort_by(|a, b| a.date.cmp(&b.date));
        res.bars.dedup_by(|a, b| a.date == b.date);
        res.bars.sort_by(|a, b| a.t.cmp(&b.t));
        res
    }

    async fn fetch_range(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<YahooBars, McpError> {
        let period1 = date_epoch(start);
        let period2 = date_epoch(end);
        let mut last_err: Option<String> = None;
        for attempt in 0..3u32 {
            for host in HOSTS {
                let url = format!(
                    "https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}?period1={period1}&period2={period2}&interval=1d"
                );
                let resp = match self.http.get(&url).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                };
                let status = resp.status();
                if status == StatusCode::TOO_MANY_REQUESTS
                    || status == StatusCode::UNAUTHORIZED
                    || status.is_server_error()
                {
                    last_err = Some(format!("HTTP {status}"));
                    continue;
                }
                // HTTP 400 from Yahoo means "No data found" for a range with no
                // trading sessions (e.g. entirely before the symbol's IPO) — an
                // empty result, not an error.
                if status == StatusCode::BAD_REQUEST {
                    return Ok(YahooBars {
                        bars: Vec::new(),
                        first_trade_date: None,
                    });
                }
                if !status.is_success() {
                    last_err = Some(format!("HTTP {status}"));
                    continue;
                }
                let text = match resp.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        last_err = Some(format!("read body: {e}"));
                        continue;
                    }
                };
                let value: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        last_err = Some(format!("parse json: {e}"));
                        continue;
                    }
                };
                return parse_chart(value);
            }
            tokio::time::sleep(Duration::from_millis(600u64 << attempt)).await;
        }
        Err(yahoo_err(format!(
            "fetch {symbol} [{start}, {end}) failed: {}",
            last_err.as_deref().unwrap_or("unknown error")
        )))
    }
}

/// Parse a Yahoo chart response into lake bars. A `chart.error` (e.g. "No data
/// found" for a symbol/range with no history) yields empty bars, not a failure.
fn parse_chart(value: Value) -> Result<YahooBars, McpError> {
    let chart = value
        .get("chart")
        .ok_or_else(|| yahoo_err("missing chart key"))?;
    if let Some(err) = chart.get("error") {
        let msg = err.as_str().unwrap_or_default();
        if !err.is_null() && !msg.is_empty() {
            tracing::debug!("Yahoo chart error: {msg}");
            return Ok(YahooBars {
                bars: Vec::new(),
                first_trade_date: None,
            });
        }
    }
    let result = chart
        .get("result")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .cloned()
        .ok_or_else(|| yahoo_err("no result in chart response"))?;

    let first_trade_date = result
        .get("meta")
        .and_then(|m| m.get("firstTradeDate"))
        .and_then(Value::as_i64)
        .and_then(|s| DateTime::<Utc>::from_timestamp(s, 0))
        .map(|dt| dt.date_naive());

    let ts = result
        .get("timestamp")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let quote = result
        .pointer("/indicators/quote/0")
        .cloned()
        .unwrap_or(Value::Null);

    let mut bars: Vec<Bar> = Vec::new();
    for (i, t) in ts.iter().enumerate() {
        let Some(secs) = t.as_i64() else {
            continue;
        };
        let cell = |k: &str| {
            quote
                .get(k)
                .and_then(Value::as_array)
                .and_then(|a| a.get(i))
                .and_then(Value::as_f64)
        };
        let (Some(o), Some(h), Some(l), Some(c)) =
            (cell("open"), cell("high"), cell("low"), cell("close"))
        else {
            continue;
        };
        let vol = cell("volume").unwrap_or(0.0);
        let Some(dt) = DateTime::<Utc>::from_timestamp(secs, 0) else {
            continue;
        };
        let date = dt.date_naive();
        let t_utc = Utc
            .from_utc_datetime(&date.and_hms_opt(LAKE_1D_HOUR, 0, 0).unwrap())
            .to_rfc3339();
        bars.push(Bar {
            t: t_utc,
            date: date.to_string(),
            o,
            h,
            l,
            c,
            v: vol as i64,
            n: 0,
            vw: c,
        });
    }
    Ok(YahooBars {
        bars,
        first_trade_date,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_chart_ok() {
        let v = json!({
            "chart": {
                "result": [{
                    "meta": { "symbol": "AAPL", "firstTradeDate": 345479400 },
                    "timestamp": [946909800, 946996200, 947082600],
                    "indicators": {
                        "quote": [{
                            "open": [1.0, 1.1, 1.2],
                            "high": [1.5, 1.6, 1.7],
                            "low": [0.9, 1.0, 1.1],
                            "close": [1.4, 1.5, 1.6],
                            "volume": [1000, 2000, 3000]
                        }]
                    }
                }]
            }
        });
        let out = parse_chart(v).unwrap();
        assert_eq!(out.bars.len(), 3);
        assert_eq!(out.first_trade_date, Some(NaiveDate::from_ymd_opt(1980, 12, 12).unwrap()));
        let b = &out.bars[0];
        assert_eq!(b.date, "2000-01-03");
        assert_eq!(b.t, "2000-01-03T04:00:00+00:00");
        assert_eq!(b.c, 1.4);
        assert_eq!(b.v, 1000);
        assert_eq!(b.n, 0); // Yahoo has no trade count
        assert_eq!(b.vw, 1.4);
    }

    #[test]
    fn parse_chart_no_data() {
        let v = json!({ "chart": { "result": [], "error": "No data found" } });
        let out = parse_chart(v).unwrap();
        assert!(out.bars.is_empty());
        assert!(out.first_trade_date.is_none());
    }

    #[test]
    fn parse_chart_skips_null_cells() {
        let v = json!({
            "chart": {
                "result": [{
                    "meta": {},
                    "timestamp": [946909800, 946996200],
                    "indicators": {
                        "quote": [{
                            "open": [null, 1.1],
                            "high": [null, 1.6],
                            "low": [null, 1.0],
                            "close": [null, 1.5],
                            "volume": [null, 2000]
                        }]
                    }
                }]
            }
        });
        let out = parse_chart(v).unwrap();
        assert_eq!(out.bars.len(), 1);
        assert_eq!(out.bars[0].date, "2000-01-04");
    }

    #[test]
    fn date_epoch_uses_0400_utc() {
        // 2000-01-03 at 04:00Z
        assert_eq!(
            date_epoch(NaiveDate::from_ymd_opt(2000, 1, 3).unwrap()),
            946872000
        );
    }
}
