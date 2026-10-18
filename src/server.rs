use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    tool, tool_handler, tool_router,
};

use crate::alpaca::{AlpacaClient, json_text};
use crate::lake::Lake;
use crate::params::{
    CloseAllPositionsParams, ClosePositionParams, CreateWatchlistParams,
    GetAccountActivitiesByTypeParams, GetAssetParams, GetOrderParams, ListAccountActivitiesParams,
    ListAssetsParams, ListOrdersParams, OrderIdParams, PlaceOrderParams, PortfolioHistoryParams,
    PositionSymbolParams, UpdateWatchlistParams, WatchlistAssetParams, WatchlistIdParams,
};

#[derive(Clone)]
pub struct TradeacServer {
    pub(crate) client: AlpacaClient,
    pub(crate) lake: Lake,
    tool_router: ToolRouter<Self>,
}

impl TradeacServer {
    pub fn new(client: AlpacaClient, lake: Lake) -> Self {
        let tool_router = if client.has_credentials() {
            Self::trading_tools() + Self::market_tools() + Self::lake_tools()
        } else {
            tracing::info!("Alpaca credentials absent; account/order tools are disabled");
            Self::market_tools() + Self::lake_tools()
        };
        Self {
            client,
            lake,
            tool_router,
        }
    }
}

#[tool_router(router = trading_tools, vis = "pub")]
impl TradeacServer {
    #[tool(description = "Get Alpaca brokerage account details (buying power, equity, cash, status).")]
    async fn get_account(&self) -> Result<String, McpError> {
        let value = self.client.get("/v2/account", &[]).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "Get account portfolio history (equity, profit/loss over time). Optional filters: period, timeframe, intraday_reporting, pnl_reset, start/end."
    )]
    async fn get_portfolio_history(
        &self,
        Parameters(params): Parameters<PortfolioHistoryParams>,
    ) -> Result<String, McpError> {
        let query = params.to_query();
        let value = self
            .client
            .get("/v2/account/portfolio/history", &query)
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "List all watchlists for the account.")]
    async fn list_watchlists(&self) -> Result<String, McpError> {
        let value = self.client.get("/v2/watchlists", &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Get a watchlist by ID, including its assets.")]
    async fn get_watchlist(
        &self,
        Parameters(params): Parameters<WatchlistIdParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/watchlists/{}", params.watchlist_id);
        let value = self.client.get(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Create a new watchlist with a name and optional initial symbols.")]
    async fn create_watchlist(
        &self,
        Parameters(params): Parameters<CreateWatchlistParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .post("/v2/watchlists", &[], Some(params.to_body()))
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Update a watchlist name and/or replace its symbol list.")]
    async fn update_watchlist(
        &self,
        Parameters(params): Parameters<UpdateWatchlistParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/watchlists/{}", params.watchlist_id);
        let value = self
            .client
            .put(&path, &[], Some(params.to_body()))
            .await?;
        Ok(json_text(value))
    }

    #[tool(description = "Permanently delete a watchlist by ID.")]
    async fn delete_watchlist(
        &self,
        Parameters(params): Parameters<WatchlistIdParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/watchlists/{}", params.watchlist_id);
        let value = self.client.delete(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Append a symbol to an existing watchlist.")]
    async fn add_watchlist_asset(
        &self,
        Parameters(params): Parameters<WatchlistAssetParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/watchlists/{}", params.watchlist_id);
        let body = serde_json::json!({ "symbol": params.symbol });
        let value = self.client.post(&path, &[], Some(body)).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Remove a symbol from a watchlist.")]
    async fn remove_watchlist_asset(
        &self,
        Parameters(params): Parameters<WatchlistAssetParams>,
    ) -> Result<String, McpError> {
        let path = format!(
            "/v2/watchlists/{}/{}",
            params.watchlist_id, params.symbol
        );
        let value = self.client.delete(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "List all open positions for the account.")]
    async fn list_positions(&self) -> Result<String, McpError> {
        let value = self.client.get("/v2/positions", &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Get an open position by symbol or asset ID.")]
    async fn get_position(
        &self,
        Parameters(params): Parameters<PositionSymbolParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/positions/{}", params.symbol_or_asset_id);
        let value = self.client.get(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "Close (liquidate) an open position by symbol or asset ID. Optional qty or percentage."
    )]
    async fn close_position(
        &self,
        Parameters(params): Parameters<ClosePositionParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/positions/{}", params.symbol_or_asset_id);
        let query = params.to_query();
        let value = self.client.delete(&path, &query).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "Close (liquidate) all open positions. Optionally cancel open orders first."
    )]
    async fn close_all_positions(
        &self,
        Parameters(params): Parameters<CloseAllPositionsParams>,
    ) -> Result<String, McpError> {
        let query = params.to_query();
        let value = self.client.delete("/v2/positions", &query).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "Place a new order. Required: symbol, side, type, time_in_force. Provide qty or notional; add limit_price/stop_price as needed for non-market orders."
    )]
    async fn place_order(
        &self,
        Parameters(params): Parameters<PlaceOrderParams>,
    ) -> Result<String, McpError> {
        let value = self
            .client
            .post("/v2/orders", &[], Some(params.to_body()))
            .await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "List orders with optional filters (status, limit, after, until, direction, nested, symbols, side)."
    )]
    async fn list_orders(
        &self,
        Parameters(params): Parameters<ListOrdersParams>,
    ) -> Result<String, McpError> {
        let query = params.to_query();
        let value = self.client.get("/v2/orders", &query).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Get a single order by order ID.")]
    async fn get_order(
        &self,
        Parameters(params): Parameters<GetOrderParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/orders/{}", params.order_id);
        let query = params.to_query();
        let value = self.client.get(&path, &query).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Cancel a single open order by order ID.")]
    async fn cancel_order(
        &self,
        Parameters(params): Parameters<OrderIdParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/orders/{}", params.order_id);
        let value = self.client.delete(&path, &[]).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Cancel all open orders for the account.")]
    async fn cancel_all_orders(&self) -> Result<String, McpError> {
        let value = self.client.delete("/v2/orders", &[]).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "List account activities (fills, dividends, fees, transfers, etc.). Optional filters: activity_types, category, order_id, date, after, until, direction, page_size, page_token."
    )]
    async fn list_account_activities(
        &self,
        Parameters(params): Parameters<ListAccountActivitiesParams>,
    ) -> Result<String, McpError> {
        let query = params.to_query();
        let value = self.client.get("/v2/account/activities", &query).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "List account activities for a single activity_type (e.g. FILL, DIV, FEE, CSD). Supports the same pagination/date filters as list_account_activities."
    )]
    async fn get_account_activities_by_type(
        &self,
        Parameters(params): Parameters<GetAccountActivitiesByTypeParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/account/activities/{}", params.activity_type);
        let query = params.to_query();
        let value = self.client.get(&path, &query).await?;
        Ok(json_text(value))
    }

    #[tool(
        description = "List tradable/data assets. Optional filters: status, asset_class, exchange, attributes."
    )]
    async fn list_assets(
        &self,
        Parameters(params): Parameters<ListAssetsParams>,
    ) -> Result<String, McpError> {
        let query = params.to_query();
        let value = self.client.get("/v2/assets", &query).await?;
        Ok(json_text(value))
    }

    #[tool(description = "Get a single asset by symbol or asset ID.")]
    async fn get_asset(
        &self,
        Parameters(params): Parameters<GetAssetParams>,
    ) -> Result<String, McpError> {
        let path = format!("/v2/assets/{}", params.symbol_or_asset_id);
        let value = self.client.get(&path, &[]).await?;
        Ok(json_text(value))
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "tac-engine",
    version = "0.1.0",
    instructions = "tac-engine MCP server. With Alpaca credentials, it provides trading, market-data, and lake tools; trading tools can mutate the configured account. Without Alpaca credentials, trading tools are omitted, daily stock history falls back to Yahoo Finance, daily FX uses free Yahoo/ECB sources, and Alpaca-only endpoints are unavailable. subscribe_market_stream requires Alpaca credentials."
)]
impl ServerHandler for TradeacServer {}
