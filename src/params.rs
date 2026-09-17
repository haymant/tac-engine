use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::query::{push_opt, push_opt_bool, push_opt_i64};

fn insert_opt(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(v) = value {
        map.insert(key.to_string(), Value::String(v));
    }
}

fn insert_opt_bool(map: &mut Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(v) = value {
        map.insert(key.to_string(), Value::Bool(v));
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PortfolioHistoryParams {
    /// Length of the history window, e.g. `1D`, `1W`, `1M`, `1A`, `all`.
    pub period: Option<String>,
    /// Resolution of time window, e.g. `1Min`, `5Min`, `15Min`, `1H`, `1D`.
    pub timeframe: Option<String>,
    /// Intraday reporting mode, e.g. `market_hours`.
    pub intraday_reporting: Option<String>,
    /// PnL reset mode, e.g. `per_day`.
    pub pnl_reset: Option<String>,
    /// Inclusive start date (`YYYY-MM-DD`) or RFC3339 datetime.
    pub start: Option<String>,
    /// Inclusive end date (`YYYY-MM-DD`) or RFC3339 datetime.
    pub end: Option<String>,
    /// Start date (`YYYY-MM-DD`).
    pub date_start: Option<String>,
    /// End date (`YYYY-MM-DD`).
    pub date_end: Option<String>,
    /// Include extended hours when true.
    pub extended_hours: Option<bool>,
    /// Cashflow types filter.
    pub cashflow_types: Option<String>,
}

impl PortfolioHistoryParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "period", self.period.clone());
        push_opt(&mut query, "timeframe", self.timeframe.clone());
        push_opt(
            &mut query,
            "intraday_reporting",
            self.intraday_reporting.clone(),
        );
        push_opt(&mut query, "pnl_reset", self.pnl_reset.clone());
        push_opt(&mut query, "start", self.start.clone());
        push_opt(&mut query, "end", self.end.clone());
        push_opt(&mut query, "date_start", self.date_start.clone());
        push_opt(&mut query, "date_end", self.date_end.clone());
        push_opt_bool(&mut query, "extended_hours", self.extended_hours);
        push_opt(&mut query, "cashflow_types", self.cashflow_types.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct WatchlistIdParams {
    /// Watchlist UUID.
    pub watchlist_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateWatchlistParams {
    /// User-defined watchlist name (up to 64 characters).
    pub name: String,
    /// Optional initial symbols.
    pub symbols: Option<Vec<String>>,
}

impl CreateWatchlistParams {
    pub fn to_body(&self) -> Value {
        let mut map = Map::new();
        map.insert("name".into(), Value::String(self.name.clone()));
        if let Some(symbols) = &self.symbols {
            map.insert(
                "symbols".into(),
                Value::Array(symbols.iter().cloned().map(Value::String).collect()),
            );
        }
        Value::Object(map)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateWatchlistParams {
    /// Watchlist UUID.
    pub watchlist_id: String,
    /// New watchlist name.
    pub name: Option<String>,
    /// Replacement symbol list (when provided, replaces the full list).
    pub symbols: Option<Vec<String>>,
}

impl UpdateWatchlistParams {
    pub fn to_body(&self) -> Value {
        let mut map = Map::new();
        insert_opt(&mut map, "name", self.name.clone());
        if let Some(symbols) = &self.symbols {
            map.insert(
                "symbols".into(),
                Value::Array(symbols.iter().cloned().map(Value::String).collect()),
            );
        }
        Value::Object(map)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct WatchlistAssetParams {
    /// Watchlist UUID.
    pub watchlist_id: String,
    /// Asset symbol, e.g. `AAPL`.
    pub symbol: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PositionSymbolParams {
    /// Symbol or asset UUID.
    pub symbol_or_asset_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ClosePositionParams {
    /// Symbol or asset UUID.
    pub symbol_or_asset_id: String,
    /// Number of shares to liquidate.
    pub qty: Option<String>,
    /// Percentage of the position to liquidate (0–100).
    pub percentage: Option<String>,
}

impl ClosePositionParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "qty", self.qty.clone());
        push_opt(&mut query, "percentage", self.percentage.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CloseAllPositionsParams {
    /// If true, cancel open orders before closing positions.
    pub cancel_orders: Option<bool>,
}

impl CloseAllPositionsParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt_bool(&mut query, "cancel_orders", self.cancel_orders);
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct PlaceOrderParams {
    /// Asset symbol, e.g. `AAPL`.
    pub symbol: String,
    /// `buy` or `sell`.
    pub side: String,
    /// Order type: `market`, `limit`, `stop`, `stop_limit`, `trailing_stop`.
    #[serde(rename = "type")]
    pub order_type: String,
    /// Time in force: `day`, `gtc`, `opg`, `cls`, `ioc`, `fok`.
    pub time_in_force: String,
    /// Share quantity (mutually exclusive with `notional` for most orders).
    pub qty: Option<String>,
    /// Dollar notional amount.
    pub notional: Option<String>,
    /// Limit price for limit / stop-limit orders.
    pub limit_price: Option<String>,
    /// Stop price for stop / stop-limit orders.
    pub stop_price: Option<String>,
    /// Trail price for trailing stop orders.
    pub trail_price: Option<String>,
    /// Trail percent for trailing stop orders.
    pub trail_percent: Option<String>,
    /// Extended hours flag.
    pub extended_hours: Option<bool>,
    /// Client-assigned order id.
    pub client_order_id: Option<String>,
    /// Order class: `simple`, `bracket`, `oco`, `oto`.
    pub order_class: Option<String>,
    /// Take-profit nested order object.
    pub take_profit: Option<Value>,
    /// Stop-loss nested order object.
    pub stop_loss: Option<Value>,
    /// Position intent, e.g. `buy_to_open`.
    pub position_intent: Option<String>,
}

impl PlaceOrderParams {
    pub fn to_body(&self) -> Value {
        let mut map = Map::new();
        map.insert("symbol".into(), Value::String(self.symbol.clone()));
        map.insert("side".into(), Value::String(self.side.clone()));
        map.insert("type".into(), Value::String(self.order_type.clone()));
        map.insert(
            "time_in_force".into(),
            Value::String(self.time_in_force.clone()),
        );
        insert_opt(&mut map, "qty", self.qty.clone());
        insert_opt(&mut map, "notional", self.notional.clone());
        insert_opt(&mut map, "limit_price", self.limit_price.clone());
        insert_opt(&mut map, "stop_price", self.stop_price.clone());
        insert_opt(&mut map, "trail_price", self.trail_price.clone());
        insert_opt(&mut map, "trail_percent", self.trail_percent.clone());
        insert_opt_bool(&mut map, "extended_hours", self.extended_hours);
        insert_opt(&mut map, "client_order_id", self.client_order_id.clone());
        insert_opt(&mut map, "order_class", self.order_class.clone());
        if let Some(tp) = &self.take_profit {
            map.insert("take_profit".into(), tp.clone());
        }
        if let Some(sl) = &self.stop_loss {
            map.insert("stop_loss".into(), sl.clone());
        }
        insert_opt(&mut map, "position_intent", self.position_intent.clone());
        Value::Object(map)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListOrdersParams {
    /// Order status filter: `open`, `closed`, `all`.
    pub status: Option<String>,
    /// Max number of orders to return (default 50, max 500).
    pub limit: Option<i64>,
    /// Pagination: response should include only orders after this timestamp.
    pub after: Option<String>,
    /// Pagination: response should include only orders until this timestamp.
    pub until: Option<String>,
    /// Chronological direction: `asc` or `desc`.
    pub direction: Option<String>,
    /// Roll up multi-leg orders under `legs`.
    pub nested: Option<bool>,
    /// Comma-separated symbols filter.
    pub symbols: Option<String>,
    /// Side filter: `buy` or `sell`.
    pub side: Option<String>,
}

impl ListOrdersParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "status", self.status.clone());
        push_opt_i64(&mut query, "limit", self.limit);
        push_opt(&mut query, "after", self.after.clone());
        push_opt(&mut query, "until", self.until.clone());
        push_opt(&mut query, "direction", self.direction.clone());
        push_opt_bool(&mut query, "nested", self.nested);
        push_opt(&mut query, "symbols", self.symbols.clone());
        push_opt(&mut query, "side", self.side.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrderIdParams {
    /// Order UUID.
    pub order_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetOrderParams {
    /// Order UUID.
    pub order_id: String,
    /// If true, include nested multi-leg legs.
    pub nested: Option<bool>,
}

impl GetOrderParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt_bool(&mut query, "nested", self.nested);
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListAccountActivitiesParams {
    /// Comma-separated activity types, e.g. `FILL,DIV,FEE`. Mutually exclusive with `category`.
    pub activity_types: Option<String>,
    /// Activity category: `trade_activity` or `non_trade_activity`. Mutually exclusive with `activity_types`.
    pub category: Option<String>,
    /// Filter activities associated with a specific order UUID.
    pub order_id: Option<String>,
    /// Filter by creation date (`YYYY-MM-DD` or RFC3339).
    pub date: Option<String>,
    /// Activities created before this datetime.
    pub until: Option<String>,
    /// Activities created after this datetime.
    pub after: Option<String>,
    /// Chronological order: `asc` or `desc` (default `desc`).
    pub direction: Option<String>,
    /// Page size (1–100, default 100).
    pub page_size: Option<i64>,
    /// Pagination token (ID of the last activity from the previous page).
    pub page_token: Option<String>,
}

impl ListAccountActivitiesParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "activity_types", self.activity_types.clone());
        push_opt(&mut query, "category", self.category.clone());
        push_opt(&mut query, "order_id", self.order_id.clone());
        push_opt(&mut query, "date", self.date.clone());
        push_opt(&mut query, "until", self.until.clone());
        push_opt(&mut query, "after", self.after.clone());
        push_opt(&mut query, "direction", self.direction.clone());
        push_opt_i64(&mut query, "page_size", self.page_size);
        push_opt(&mut query, "page_token", self.page_token.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetAccountActivitiesByTypeParams {
    /// Activity type, e.g. `FILL`, `DIV`, `FEE`, `CSD`, `CSW`.
    pub activity_type: String,
    /// Filter activities associated with a specific order UUID.
    pub order_id: Option<String>,
    /// Filter by creation date (`YYYY-MM-DD` or RFC3339).
    pub date: Option<String>,
    /// Activities created before this datetime.
    pub until: Option<String>,
    /// Activities created after this datetime.
    pub after: Option<String>,
    /// Chronological order: `asc` or `desc` (default `desc`).
    pub direction: Option<String>,
    /// Page size (1–100, default 100).
    pub page_size: Option<i64>,
    /// Pagination token (ID of the last activity from the previous page).
    pub page_token: Option<String>,
}

impl GetAccountActivitiesByTypeParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "order_id", self.order_id.clone());
        push_opt(&mut query, "date", self.date.clone());
        push_opt(&mut query, "until", self.until.clone());
        push_opt(&mut query, "after", self.after.clone());
        push_opt(&mut query, "direction", self.direction.clone());
        push_opt_i64(&mut query, "page_size", self.page_size);
        push_opt(&mut query, "page_token", self.page_token.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListAssetsParams {
    /// Asset status filter, e.g. `active` or `inactive`.
    pub status: Option<String>,
    /// Asset class, e.g. `us_equity`, `crypto`, `us_option` (default `us_equity`).
    pub asset_class: Option<String>,
    /// Exchange filter, e.g. `NASDAQ`, `NYSE`, `CRYPTO`.
    pub exchange: Option<String>,
    /// Comma-separated attributes, e.g. `has_options,ipo,overnight_tradable`.
    pub attributes: Option<String>,
}

impl ListAssetsParams {
    pub fn to_query(&self) -> Vec<(&str, String)> {
        let mut query = Vec::new();
        push_opt(&mut query, "status", self.status.clone());
        push_opt(&mut query, "asset_class", self.asset_class.clone());
        push_opt(&mut query, "exchange", self.exchange.clone());
        push_opt(&mut query, "attributes", self.attributes.clone());
        query
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetAssetParams {
    /// Symbol or asset UUID. For crypto use old symbology (`BTCUSD`) or URL-encoded pairs (`BTC%2FUSDT`).
    pub symbol_or_asset_id: String,
}
