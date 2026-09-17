use rmcp::{
    ErrorData as McpError, handler::server::wrapper::Parameters, tool, tool_router,
};

use crate::alpaca::{is_http_error, json_text};
use crate::fx::{self, FxClient};
use crate::market_params::{
    CorporateActionsParams, ForexLatestRatesParams, ForexRatesParams, MarketMoversParams,
    MostActivesParams, NewsParams, OptionChainParams, OptionMetaParams, OptionMultiParams,
    StockMultiParams, StockSymbolParams, SubscribeMarketStreamParams,
};
use crate::server::TradeacServer;
use crate::stream::sample_market_stream;

/// Alpaca's `/v1beta1/forex/*` endpoints return 403 Forbidden on most plans.
/// Restrict the Yahoo/Frankfurter fallback to real HTTP responses so transport
/// errors still surface the Alpaca error verbatim.
fn is_http_denied(err: &McpError) -> bool {
    if let Some(data) = &err.data {
        if let Some(code) = data.get("http_status").and_then(serde_json::Value::as_u64) {
            return (401..=500).contains(&code);
        }
    }
    err.message.contains("HTTP ")
}

/// Normalize a comma-separated pair list (`EUR/USD,usdjpy`) to lake keys.
fn fx_pair_list(pairs: &str) -> Vec<String> {
    pairs
        .split(',')
        .map(fx::pair_code)
        .filter(|p| !p.is_empty() && p.len() == 6)
        .collect()
}

/// Latest FX quotes via the free source chain, shaped like Alpaca's
/// `{ rates: { "EURUSD": { bid, ask, mid, timestamp } } }` (bid/ask collapse
/// to the mid quote, which all our sources provide).
async fn yahoo_fx_latest(pairs: &[String]) -> Result<serde_json::Value, McpError> {
    let fx = FxClient::new()?;
    let mut rates = serde_json::Map::new();
    for pair in pairs {
        if let Ok((_src, last)) = fx.fetch_latest(pair).await {
            rates.insert(
                pair.clone(),
                serde_json::json!({
                    "bid": last.l, "ask": last.h, "mid": last.c, "timestamp": last.date.to_string(),
                }),
            );
        }
    }
    Ok(serde_json::json!({ "rates": serde_json::Value::Object(rates) }))
}

/// Historical daily FX via the free source chain, shaped like Alpaca's
/// `{ rates: { "EURUSD": [{ t, o, h, l, c }] }, next_page_token: null }`.
async fn yahoo_fx_rates(
    pairs: &[String],
    start: &str,
    end: &str,
) -> Result<serde_json::Value, McpError> {
    let fx = FxClient::new()?;
    let sd = if start.is_empty() {
        chrono::Utc::now().date_naive() - chrono::Days::new(365)
    } else {
        fx::date_of(start)?
    };
    let ed = if end.is_empty() {
        chrono::Utc::now().date_naive()
    } else {
        fx::date_of(end)?
    };
    let mut rates = serde_json::Map::new();
    for pair in pairs {
        if let Ok((_src, hist)) =
            fx.fetch_history(pair, sd, ed.checked_add_days(chrono::Days::new(1)).unwrap_or(ed))
                .await
        {
            let rows: Vec<serde_json::Value> = hist
                .into_iter()
                .map(|b| {
                    serde_json::json!({
                        "t": format!("{}T04:00:00+00:00", b.date),
                        "o": b.o, "h": b.h, "l": b.l, "c": b.c,
                    })
                })
                .collect();
            rates.insert(pair.clone(), serde_json::Value::Array(rows));
        }
    }
    Ok(serde_json::json!({
        "rates": serde_json::Value::Object(rates),
        "next_page_token": null,
    }))
}

#[tool_router(router = market_tools, vis = "pub")]
impl TradeacServer {
    #[tool(description = "Get market news articles (data API). Optional symbols, start/end, limit, include_content.")]
    async fn get_news(
        &self,
        Parameters(params): Parameters<NewsParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/news", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Get corporate actions (data API /v1/corporate-actions). Filter by symbols, types, start/end, data_quality.")]
    async fn get_corporate_actions(
        &self,
        Parameters(params): Parameters<CorporateActionsParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1/corporate-actions", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Screener: most active stocks by volume or trade count.")]
    async fn get_most_actives(
        &self,
        Parameters(params): Parameters<MostActivesParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/screener/stocks/most-actives", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Screener: top market movers. market_type is stocks or crypto.")]
    async fn get_market_movers(
        &self,
        Parameters(params): Parameters<MarketMoversParams>,
    ) -> Result<String, McpError> {
        let path = format!(
            "/v1beta1/screener/{}/movers",
            params.market_type.to_ascii_lowercase()
        );
        let value = self.client.data_get(&path, &params.to_query()).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Latest forex rates for currency_pairs (e.g. USDJPY,EURUSD). Falls back to Yahoo/ECB free sources when Alpaca's forex API is denied (403).")]
    async fn get_forex_latest_rates(
        &self,
        Parameters(params): Parameters<ForexLatestRatesParams>,
    ) -> Result<String, McpError> {
        let value = match self
            .client
            .data_get("/v1beta1/forex/latest/rates", &params.to_query())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                if is_http_denied(&err) {
                    tracing::warn!(%err, "get_forex_latest_rates denied; falling back to free FX sources");
                    match yahoo_fx_latest(&fx_pair_list(&params.currency_pairs)).await {
                        Ok(v) => v,
                        Err(_) => return Err(err),
                    }
                } else {
                    return Err(err);
                }
            }
        };
        Ok(json_text(value))
    }

    #[tool(description = "Historical forex rates for currency_pairs with optional timeframe/start/end. Falls back to Yahoo/ECB free sources when Alpaca's forex API is denied (403).")]
    async fn get_forex_rates(
        &self,
        Parameters(params): Parameters<ForexRatesParams>,
    ) -> Result<String, McpError> {
        let value = match self
            .client
            .data_get("/v1beta1/forex/rates", &params.to_query())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                if is_http_denied(&err) {
                    tracing::warn!(%err, "get_forex_rates denied; falling back to free FX sources");
                    match yahoo_fx_rates(
                        &fx_pair_list(&params.currency_pairs),
                        params.start.as_deref().unwrap_or(""),
                        params.end.as_deref().unwrap_or(""),
                    )
                    .await
                    {
                        Ok(v) => v,
                        Err(_) => return Err(err),
                    }
                } else {
                    return Err(err);
                }
            }
        };
        Ok(json_text(value))
    }

    #[tool(description = "Historical stock bars for symbols (requires timeframe).")]
    async fn get_stock_bars(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = match self
            .client
            .data_get("/v2/stocks/bars", &params.to_query())
            .await
        {
            Err(err) if is_http_error(&err, 403) => {
                // Auto-switch feed when the requested one is not permitted (e.g. no SIP).
                let fallback = if params
                    .feed
                    .as_deref()
                    .map(|f| f.eq_ignore_ascii_case("iex"))
                    .unwrap_or(false)
                {
                    "sip"
                } else {
                    "iex"
                };
                tracing::warn!(%err, "get_stock_bars returned 403; retrying with feed={fallback}");
                let mut retry = params.clone();
                retry.feed = Some(fallback.to_string());
                self.client
                    .data_get("/v2/stocks/bars", &retry.to_query())
                    .await?
            }
            other => other?,
        };
        Ok(json_text(value))
    }

    #[tool(description = "Latest stock minute bars for symbols.")]
    async fn get_stock_latest_bars(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/bars/latest", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical stock quotes for symbols.")]
    async fn get_stock_quotes(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/quotes", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Latest stock quotes (NBBO) for symbols.")]
    async fn get_stock_latest_quotes(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/quotes/latest", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical stock trades for symbols.")]
    async fn get_stock_trades(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/trades", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Latest stock trades for symbols.")]
    async fn get_stock_latest_trades(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/trades/latest", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Stock snapshots (latest trade/quote + bars) for symbols.")]
    async fn get_stock_snapshots(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/snapshots", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical stock auctions for symbols.")]
    async fn get_stock_auctions(
        &self,
        Parameters(params): Parameters<StockMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v2/stocks/auctions", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical stock bars for a single symbol (requires timeframe).")]
    async fn get_stock_bars_single(
        &self,
        Parameters(params): Parameters<StockSymbolParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/stocks/{}/bars", params.symbol);
        let value = self.client.data_get(&path, &params.to_query()).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Stock snapshot for a single symbol.")]
    async fn get_stock_snapshot(
        &self,
        Parameters(params): Parameters<StockSymbolParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/stocks/{}/snapshot", params.symbol);
        let value = self
            .client
            .data_get(&path, &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical option bars for contract symbols (requires timeframe).")]
    async fn get_option_bars(
        &self,
        Parameters(params): Parameters<OptionMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/bars", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Latest option quotes for contract symbols.")]
    async fn get_option_latest_quotes(
        &self,
        Parameters(params): Parameters<OptionMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/quotes/latest", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Historical option trades for contract symbols.")]
    async fn get_option_trades(
        &self,
        Parameters(params): Parameters<OptionMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/trades", &params.to_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Latest option trades for contract symbols.")]
    async fn get_option_latest_trades(
        &self,
        Parameters(params): Parameters<OptionMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/trades/latest", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Option snapshots (trade/quote/greeks) for contract symbols.")]
    async fn get_option_snapshots(
        &self,
        Parameters(params): Parameters<OptionMultiParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/snapshots", &params.to_latest_query())
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Option chain snapshots for an underlying symbol (filters: type, strikes, expiration).")]
    async fn get_option_chain(
        &self,
        Parameters(params): Parameters<OptionChainParams>,
    ) -> Result<String, McpError> {
        let path = format!(
            "/v1beta1/options/snapshots/{}",
            params.underlying_symbol
        );
        let value = self.client.data_get(&path, &params.to_query()).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Option condition code mapping. ticktype: trade or quote.")]
    async fn get_option_meta_conditions(
        &self,
        Parameters(params): Parameters<OptionMetaParams>,
    ) -> Result<String, McpError> {
        let ticktype = params.ticktype.unwrap_or_else(|| "trade".into());
        let path = format!("/v1beta1/options/meta/conditions/{ticktype}");
        let value = self.client.data_get(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Option exchange code mapping.")]
    async fn get_option_meta_exchanges(&self) -> Result<String, McpError> {
        let value = self
            .client
            .data_get("/v1beta1/options/meta/exchanges", &[])
            .await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "Sample a real-time Alpaca market-data WebSocket (stocks/options/news/test). Auth+subscribe, collect messages for duration_secs (1–30), then disconnect. Most plans allow only ONE concurrent stream."
    )]
    async fn subscribe_market_stream(
        &self,
        Parameters(params): Parameters<SubscribeMarketStreamParams>,
    ) -> Result<String, McpError> {
        let path = params.stream_path().map_err(|msg| {
            McpError::invalid_params(msg, None)
        })?;
        let url = format!("{}/{}", self.client.stream_base_url(), path);
        let subscribe = params.subscribe_body();
        if subscribe.as_object().map(|o| o.len() <= 1).unwrap_or(true)
            && !params.stream.eq_ignore_ascii_case("test")
        {
            return Err(McpError::invalid_params(
                "provide at least one channel subscription (trades/quotes/bars/news/…)",
                None,
            ));
        }
        let duration = params.duration_secs.unwrap_or(5).max(1) as u64;
        let max_messages = params.max_messages.unwrap_or(50).max(1) as usize;
        let value = sample_market_stream(
            &url,
            self.client.api_key_id(),
            self.client.api_secret_key(),
            subscribe,
            duration,
            max_messages,
        )
        .await?;
        Ok(json_text(value))
    }
}
