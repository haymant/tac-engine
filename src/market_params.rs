use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::query::{push_opt, push_opt_bool, push_opt_f64, push_opt_i64};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct NewsParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub sort: Option<String>,
    /// Comma-separated symbols.
    pub symbols: Option<String>,
    pub limit: Option<i64>,
    pub include_content: Option<bool>,
    pub exclude_contentless: Option<bool>,
    pub page_token: Option<String>,
}

impl NewsParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        push_opt(&mut q, "symbols", self.symbols.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt_bool(&mut q, "include_content", self.include_content);
        push_opt_bool(&mut q, "exclude_contentless", self.exclude_contentless);
        push_opt(&mut q, "page_token", self.page_token.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CorporateActionsParams {
    pub symbols: Option<String>,
    pub cusips: Option<String>,
    /// Comma-separated CA types.
    pub types: Option<String>,
    pub region: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub ids: Option<String>,
    pub limit: Option<i64>,
    /// `complete` (default) or `all`.
    pub data_quality: Option<String>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
}

impl CorporateActionsParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "symbols", self.symbols.clone());
        push_opt(&mut q, "cusips", self.cusips.clone());
        push_opt(&mut q, "types", self.types.clone());
        push_opt(&mut q, "region", self.region.clone());
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt(&mut q, "ids", self.ids.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "data_quality", self.data_quality.clone());
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MostActivesParams {
    /// Ranking metric: `volume` (default) or `trades`.
    pub by: Option<String>,
    /// Number of symbols (1–100, default 10).
    pub top: Option<i64>,
}

impl MostActivesParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "by", self.by.clone());
        push_opt_i64(&mut q, "top", self.top);
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MarketMoversParams {
    /// `stocks` or `crypto`.
    pub market_type: String,
    /// Number of gainers/losers (default 10).
    pub top: Option<i64>,
}

impl MarketMoversParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt_i64(&mut q, "top", self.top);
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ForexLatestRatesParams {
    /// Comma-separated pairs, e.g. `USDJPY,EURUSD`.
    pub currency_pairs: String,
}

impl ForexLatestRatesParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        vec![("currency_pairs", self.currency_pairs.clone())]
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ForexRatesParams {
    pub currency_pairs: String,
    /// Snapshot frequency, e.g. `1Min`, `1Hour`, `1Day`.
    pub timeframe: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<i64>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
}

impl ForexRatesParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = vec![("currency_pairs", self.currency_pairs.clone())];
        push_opt(&mut q, "timeframe", self.timeframe.clone());
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        q
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct StockMultiParams {
    /// Comma-separated symbols (required for multi endpoints).
    pub symbols: String,
    pub start: Option<String>,
    pub end: Option<String>,
    /// Bar timeframe, e.g. `1Min`, `5Min`, `1Hour`, `1Day`.
    pub timeframe: Option<String>,
    pub limit: Option<i64>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
    /// `iex`, `sip`, `boats`, `otc`, …
    pub feed: Option<String>,
    pub currency: Option<String>,
    pub asof: Option<String>,
    pub adjustment: Option<String>,
}

impl StockMultiParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = vec![("symbols", self.symbols.clone())];
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt(&mut q, "timeframe", self.timeframe.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        push_opt(&mut q, "feed", self.feed.clone());
        push_opt(&mut q, "currency", self.currency.clone());
        push_opt(&mut q, "asof", self.asof.clone());
        push_opt(&mut q, "adjustment", self.adjustment.clone());
        q
    }

    pub fn to_latest_query(&self) -> Vec<(&str, String)> {
        let mut q = vec![("symbols", self.symbols.clone())];
        push_opt(&mut q, "feed", self.feed.clone());
        push_opt(&mut q, "currency", self.currency.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct StockSymbolParams {
    pub symbol: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub timeframe: Option<String>,
    pub limit: Option<i64>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
    pub feed: Option<String>,
    pub currency: Option<String>,
    pub asof: Option<String>,
    pub adjustment: Option<String>,
}

impl StockSymbolParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt(&mut q, "timeframe", self.timeframe.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        push_opt(&mut q, "feed", self.feed.clone());
        push_opt(&mut q, "currency", self.currency.clone());
        push_opt(&mut q, "asof", self.asof.clone());
        push_opt(&mut q, "adjustment", self.adjustment.clone());
        q
    }

    pub fn to_latest_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "feed", self.feed.clone());
        push_opt(&mut q, "currency", self.currency.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct OptionMultiParams {
    /// Comma-separated option contract symbols.
    pub symbols: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub timeframe: Option<String>,
    pub limit: Option<i64>,
    pub page_token: Option<String>,
    pub sort: Option<String>,
    /// `opra` or `indicative`.
    pub feed: Option<String>,
}

impl OptionMultiParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = vec![("symbols", self.symbols.clone())];
        push_opt(&mut q, "start", self.start.clone());
        push_opt(&mut q, "end", self.end.clone());
        push_opt(&mut q, "timeframe", self.timeframe.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "sort", self.sort.clone());
        push_opt(&mut q, "feed", self.feed.clone());
        q
    }

    pub fn to_latest_query(&self) -> Vec<(&str, String)> {
        let mut q = vec![("symbols", self.symbols.clone())];
        push_opt(&mut q, "feed", self.feed.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct OptionChainParams {
    pub underlying_symbol: String,
    pub feed: Option<String>,
    pub limit: Option<i64>,
    pub updated_since: Option<String>,
    pub page_token: Option<String>,
    /// `call` or `put`.
    #[serde(rename = "type")]
    pub option_type: Option<String>,
    pub strike_price_gte: Option<f64>,
    pub strike_price_lte: Option<f64>,
    pub expiration_date: Option<String>,
    pub expiration_date_gte: Option<String>,
    pub expiration_date_lte: Option<String>,
    pub root_symbol: Option<String>,
}

impl OptionChainParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut q = Vec::new();
        push_opt(&mut q, "feed", self.feed.clone());
        push_opt_i64(&mut q, "limit", self.limit);
        push_opt(&mut q, "updated_since", self.updated_since.clone());
        push_opt(&mut q, "page_token", self.page_token.clone());
        push_opt(&mut q, "type", self.option_type.clone());
        push_opt_f64(&mut q, "strike_price_gte", self.strike_price_gte);
        push_opt_f64(&mut q, "strike_price_lte", self.strike_price_lte);
        push_opt(&mut q, "expiration_date", self.expiration_date.clone());
        push_opt(
            &mut q,
            "expiration_date_gte",
            self.expiration_date_gte.clone(),
        );
        push_opt(
            &mut q,
            "expiration_date_lte",
            self.expiration_date_lte.clone(),
        );
        push_opt(&mut q, "root_symbol", self.root_symbol.clone());
        q
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct OptionMetaParams {
    /// For conditions: `trade` or `quote`.
    pub ticktype: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubscribeMarketStreamParams {
    /// Stream kind: `stocks`, `options`, `news`, or `test`.
    pub stream: String,
    /// Stocks: `iex` (default) or `sip`. Options: `indicative` (default) or `opra`.
    pub feed: Option<String>,
    /// Comma-separated symbols for trades channel (stocks/options). Use `FAKEPACA` with `test`.
    pub trades: Option<String>,
    pub quotes: Option<String>,
    pub bars: Option<String>,
    pub updated_bars: Option<String>,
    pub daily_bars: Option<String>,
    pub statuses: Option<String>,
    pub lulds: Option<String>,
    /// News symbols or `*`.
    pub news: Option<String>,
    /// How long to sample (1–30 seconds, default 5).
    pub duration_secs: Option<i64>,
    /// Max messages to collect (1–200, default 50).
    pub max_messages: Option<i64>,
}

impl SubscribeMarketStreamParams {
    pub fn stream_path(&self) -> Result<String, String> {
        let feed = self.feed.as_deref();
        match self.stream.to_ascii_lowercase().as_str() {
            "test" => Ok("v2/test".into()),
            "stocks" => {
                let f = feed.unwrap_or("iex");
                if !matches!(f, "iex" | "sip") {
                    return Err(format!("stocks feed must be iex or sip, got {f}"));
                }
                Ok(format!("v2/{f}"))
            }
            "options" => {
                let f = feed.unwrap_or("indicative");
                if !matches!(f, "indicative" | "opra") {
                    return Err(format!("options feed must be indicative or opra, got {f}"));
                }
                Ok(format!("v1beta1/{f}"))
            }
            "news" => Ok("v1beta1/news".into()),
            other => Err(format!(
                "stream must be stocks, options, news, or test; got {other}"
            )),
        }
    }

    pub fn subscribe_body(&self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("action".into(), Value::String("subscribe".into()));
        for (key, value) in [
            ("trades", &self.trades),
            ("quotes", &self.quotes),
            ("bars", &self.bars),
            ("updatedBars", &self.updated_bars),
            ("dailyBars", &self.daily_bars),
            ("statuses", &self.statuses),
            ("lulds", &self.lulds),
            ("news", &self.news),
        ] {
            if let Some(csv) = value {
                let symbols: Vec<Value> = csv
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| Value::String(s.to_string()))
                    .collect();
                if !symbols.is_empty() {
                    map.insert(key.into(), Value::Array(symbols));
                }
            }
        }
        // Sensible default for test stream.
        if map.len() == 1 && self.stream.eq_ignore_ascii_case("test") {
            map.insert(
                "quotes".into(),
                json!(["FAKEPACA"]),
            );
            map.insert("bars".into(), json!(["FAKEPACA"]));
        }
        Value::Object(map)
    }
}
