//! Multi-source daily FX history for the lake and for Alpaca-proxy fallbacks.
//!
//! Alpaca's `/v1beta1/forex/*` endpoints return **403 Forbidden** on most
//! plans, so FX data is sourced from free public providers instead, tried in
//! order with automatic failover:
//!
//! 1. **Yahoo Finance** chart API (`{PAIR}=X`, `interval=1d`) — full OHLC
//!    history since 1999 plus live/current session data. Same client as the
//!    equity history fallback (retries/backoff across `query1`/`query2`).
//! 2. **Frankfurter** (`api.frankfurter.app`) — ECB daily reference rates
//!    (mid rates, ~16:00 CET, business days), no API key. All ECB cross rates
//!    via the `from`/`to` pair query.
//!
//! Each source returns an OHLCV-style daily series (`o=h=l=c=rate` for
//! Frankfurter, true OHLC from Yahoo). Legs are stored in the lake under
//! `market=FX / timeframe=1d / symbol={PAIR}` (e.g. `EURUSD`).

use std::fmt::Display;
use std::time::Duration;

use chrono::{DateTime, Days, NaiveDate, Utc};
use reqwest::{Client, StatusCode};
use rmcp::ErrorData as McpError;
use serde_json::Value;

use crate::yahoo::YahooClient;

pub const FX_MARKET: &str = "FX";
pub const FX_TIMEFRAME: &str = "1d";

/// Default FX universe: majors plus a few liquid crosses (matches what the
/// RFQ correlation / FX tabs consume).
pub const DEFAULT_FX_PAIRS: &[&str] = &[
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD", "EURGBP", "EURJPY",
    "GBPJPY", "EURNOK", "USDSEK", "USDMXN", "USDSGD",
];

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0";
/// Frankfurter's ECB reference history goes back to 1999, but its time-series
/// endpoint caps sizable ranges; chunk long backfills like the Yahoo client.
const MAX_CHUNK_DAYS: u64 = 1830;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FxSource {
    Yahoo,
    Frankfurter,
}

impl FxSource {
    pub fn name(self) -> &'static str {
        match self {
            FxSource::Yahoo => "yahoo",
            FxSource::Frankfurter => "frankfurter",
        }
    }
    pub const ALL: [FxSource; 2] = [FxSource::Yahoo, FxSource::Frankfurter];
}

#[derive(Debug, Clone)]
pub struct FxDaily {
    /// Session date (UTC).
    pub date: NaiveDate,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
}

fn fx_err(msg: impl Display) -> McpError {
    McpError::internal_error(format!("fx error: {msg}"), None)
}

/// Normalize a currency pair to the 6-letter lake key: `EUR/USD`, `EUR-USD`,
/// `eurusd` → `EURUSD`.
pub fn pair_code(pair: &str) -> String {
    pair.trim().to_uppercase().replace(['/', '-'], "")
}

/// Yahoo's symbol for an FX pair leg: `EURUSD` → `EURUSD=X`.
pub fn yahoo_symbol(leg: &str) -> String {
    format!("{}=X", pair_code(leg))
}

/// Split `EURUSD` into (`EUR`, `USD`).
fn split_pair(leg: &str) -> Option<(String, String)> {
    let code = pair_code(leg);
    if code.len() != 6 {
        return None;
    }
    Some((code[..3].to_string(), code[3..].to_string()))
}

/// Parses `YYYY-MM-DD` or RFC-3339 to a date part.
pub fn date_of(s: &str) -> Result<NaiveDate, McpError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s.trim()) {
        return Ok(dt.with_timezone(&Utc).date_naive());
    }
    let head = s.trim();
    let head = &head[..10.min(head.len())];
    NaiveDate::parse_from_str(head, "%Y-%m-%d")
        .map_err(|e| fx_err(format!("unrecognized date {head:?}: {e}")))
}

#[derive(Debug, Clone)]
pub struct FxClient {
    http: Client,
    yahoo: YahooClient,
}

impl FxClient {
    pub fn new() -> Result<Self, McpError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| fx_err(format!("build http client: {e}")))?;
        Ok(Self {
            http,
            yahoo: YahooClient::new()?,
        })
    }

    /// Fetch daily FX history for `[start, end)` (end exclusive) trying each
    /// source in [`FxSource::ALL`] order. Returns the first non-empty success
    /// alongside which source supplied it. Errors only when every source fails
    /// or all return empty.
    pub async fn fetch_history(
        &self,
        leg: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<(FxSource, Vec<FxDaily>), McpError> {
        if end <= start {
            return Ok((FxSource::Yahoo, Vec::new()));
        }
        let mut last_err: Option<String> = None;
        for src in FxSource::ALL {
            let res = match src {
                FxSource::Yahoo => self.fetch_yahoo(leg, start, end).await,
                FxSource::Frankfurter => self.fetch_frankfurter(leg, start, end).await,
            };
            match res {
                Ok(series) if !series.is_empty() => return Ok((src, series)),
                Ok(_) => {
                    last_err = Some(format!("{} returned no data", src.name()));
                }
                Err(e) => {
                    tracing::warn!(src = src.name(), leg, %e, "FX source failed; trying next");
                    last_err = Some(e.message.to_string());
                }
            }
        }
        Err(fx_err(format!(
            "no FX source returned data for {leg} [{start}, {end}): {}",
            last_err.as_deref().unwrap_or("unknown error")
        )))
    }

    /// Latest daily bar for a pair (the current session when the market is
    /// open, else the most recent close).
    pub async fn fetch_latest(&self, leg: &str) -> Result<(FxSource, FxDaily), McpError> {
        let end = Utc::now().date_naive() + Days::new(1);
        let start = end - Days::new(7);
        let (src, hist) = self.fetch_history(leg, start, end).await?;
        hist.into_iter().next_back().ok_or_else(|| fx_err("no bars for latest"))
            .map(|b| (src, b))
    }

    async fn fetch_yahoo(
        &self,
        leg: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<FxDaily>, McpError> {
        let yb = self.yahoo.daily_bars(&yahoo_symbol(leg), start, end).await?;
        let mut out = Vec::with_capacity(yb.bars.len());
        for b in yb.bars {
            let Some(date) = NaiveDate::parse_from_str(&b.date, "%Y-%m-%d").ok() else {
                continue;
            };
            out.push(FxDaily {
                date,
                o: b.o,
                h: b.h,
                l: b.l,
                c: b.c,
            });
        }
        out.sort_by(|a, b| a.date.cmp(&b.date));
        Ok(out)
    }

    async fn fetch_frankfurter(
        &self,
        leg: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<FxDaily>, McpError> {
        let (base, quote) = split_pair(leg)
            .ok_or_else(|| fx_err(format!("invalid pair {leg}: expected 6-letter code")))?;
        let mut out: Vec<FxDaily> = Vec::new();
        let mut cur = start;
        while cur < end {
            let chunk_end = cur
                .checked_add_days(Days::new(MAX_CHUNK_DAYS))
                .unwrap_or(end)
                .min(end);
            let url = format!(
                "https://api.frankfurter.app/{}..{}?from={base}&to={quote}",
                cur, chunk_end
            );
            let resp = self.http.get(&url).send().await.map_err(|e| fx_err(e.to_string()))?;
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                tokio::time::sleep(Duration::from_millis(1200)).await;
                continue;
            }
            if !resp.status().is_success() {
                return Err(fx_err(format!("frankfurter HTTP {}", resp.status())));
            }
            let value: Value = resp
                .json()
                .await
                .map_err(|e| fx_err(format!("frankfurter parse: {e}")))?;
            let rates = value
                .get("rates")
                .and_then(Value::as_object)
                .ok_or_else(|| fx_err("frankfurter response missing rates"))?;
            for (date_str, v) in rates {
                let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") else {
                    continue;
                };
                if date < start || date >= end {
                    continue;
                }
                let rate = v
                    .get(&quote)
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NAN);
                if !rate.is_finite() {
                    continue;
                }
                out.push(FxDaily {
                    date,
                    o: rate,
                    h: rate,
                    l: rate,
                    c: rate,
                });
            }
            cur = chunk_end;
            if cur < end {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        out.sort_by(|a, b| a.date.cmp(&b.date));
        out.dedup_by(|a, b| a.date == b.date);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_code_normalizes() {
        assert_eq!(pair_code("EUR/USD"), "EURUSD");
        assert_eq!(pair_code("eur-jpy"), "EURJPY");
        assert_eq!(pair_code(" EURUSD "), "EURUSD");
    }

    #[test]
    fn yahoo_symbol_construction() {
        assert_eq!(yahoo_symbol("EUR/USD"), "EURUSD=X");
        assert_eq!(yahoo_symbol("USDJPY"), "USDJPY=X");
    }

    #[test]
    fn source_names() {
        assert_eq!(FxSource::Yahoo.name(), "yahoo");
        assert_eq!(FxSource::Frankfurter.name(), "frankfurter");
        assert_eq!(FxSource::ALL.len(), 2);
    }

    #[test]
    fn date_of_parses_both_forms() {
        let d = date_of("2024-01-02").unwrap();
        assert_eq!(d.to_string(), "2024-01-02");
        let d2 = date_of("2024-01-02T04:00:00+00:00").unwrap();
        assert_eq!(d2.to_string(), "2024-01-02");
    }

    #[test]
    fn split_pair_requires_six_letters() {
        assert_eq!(split_pair("EURUSD"), Some(("EUR".into(), "USD".into())));
        assert_eq!(split_pair("EUR/USD"), Some(("EUR".into(), "USD".into())));
        assert!(split_pair("XXX").is_none());
    }
}