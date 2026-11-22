use std::{fs, path::Path, sync::Arc};

use rmcp::ErrorData as McpError;
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// State loaded only when TAC_DRY_RUN_FIXTURE points at a JSON fixture.
/// Orders are appended to memory with status `dry_run`; no network request and
/// no position mutation can occur in this mode.
#[derive(Clone)]
pub struct FixtureState {
    pub data: Arc<Mutex<Value>>,
}

impl FixtureState {
    pub fn load(path: &Path) -> Result<Self, McpError> {
        let text = fs::read_to_string(path).map_err(|err| {
            McpError::internal_error(format!("failed to read TAC dry-run fixture: {err}"), None)
        })?;
        let value: Value = serde_json::from_str(&text).map_err(|err| {
            McpError::internal_error(format!("failed to parse TAC dry-run fixture: {err}"), None)
        })?;
        if !value.is_object() {
            return Err(McpError::invalid_params(
                "TAC dry-run fixture must be a JSON object",
                None,
            ));
        }
        Ok(Self { data: Arc::new(Mutex::new(value)) })
    }

    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, McpError> {
        let mut state = self.data.lock().await;
        if method == "POST" && path == "/v2/orders" {
            let mut order = body.unwrap_or_else(|| json!({}));
            let object = order.as_object_mut().ok_or_else(|| {
                McpError::invalid_params("dry-run order body must be an object", None)
            })?;
            let id = format!("dry-run-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());
            object.insert("id".into(), Value::String(id));
            object.insert("status".into(), Value::String("dry_run".into()));
            object.insert("dry_run".into(), Value::Bool(true));
            state["orders"].as_array_mut().map(|orders| orders.push(order.clone()));
            return Ok(order);
        }

        if method == "GET" && path == "/v2/account" {
            return fixture_value(&state, "account");
        }
        if method == "GET" && path == "/v2/positions" {
            return fixture_value(&state, "positions");
        }
        if method == "GET" && path == "/v2/orders" {
            return fixture_value(&state, "orders");
        }
        if method == "GET" && path == "/v2/watchlists" {
            return fixture_value(&state, "watchlists");
        }
        if method == "GET" && path.starts_with("/v2/watchlists/") {
            let watchlist_id = path.trim_start_matches("/v2/watchlists/");
            let watchlists = state.get("watchlists").and_then(Value::as_array).cloned().unwrap_or_default();
            return watchlists.into_iter().find(|item| item.get("id").and_then(Value::as_str) == Some(watchlist_id))
                .ok_or_else(|| McpError::internal_error(format!("fixture watchlist not found: {watchlist_id}"), None));
        }
        if method == "POST" && path == "/v2/watchlists" {
            let body = body.unwrap_or_else(|| json!({}));
            let name = body.get("name").and_then(Value::as_str).unwrap_or("Fixture Watchlist").to_string();
            let id = format!("fixture-watchlist-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());
            let assets = body.get("symbols").and_then(Value::as_array).cloned().unwrap_or_default()
                .into_iter().filter_map(|symbol| symbol.as_str().map(|value| json!({ "symbol": value }))).collect::<Vec<_>>();
            let watchlist = json!({ "id": id, "name": name, "assets": assets });
            state["watchlists"].as_array_mut().map(|watchlists| watchlists.push(watchlist.clone()));
            return Ok(watchlist);
        }
        if method == "POST" && path.starts_with("/v2/watchlists/") {
            let watchlist_id = path.trim_start_matches("/v2/watchlists/");
            let symbol = body.as_ref().and_then(|value| value.get("symbol")).and_then(Value::as_str).unwrap_or_default().to_string();
            let watchlists = state["watchlists"].as_array_mut().ok_or_else(|| McpError::internal_error("fixture is missing `watchlists`", None))?;
            let watchlist = watchlists.iter_mut().find(|item| item.get("id").and_then(Value::as_str) == Some(watchlist_id))
                .ok_or_else(|| McpError::internal_error(format!("fixture watchlist not found: {watchlist_id}"), None))?;
            watchlist["assets"].as_array_mut().map(|assets| assets.push(json!({ "symbol": symbol })));
            return Ok(watchlist.clone());
        }
        if method == "DELETE" && path.starts_with("/v2/watchlists/") {
            let suffix = path.trim_start_matches("/v2/watchlists/");
            let mut parts = suffix.splitn(2, '/');
            let watchlist_id = parts.next().unwrap_or_default();
            let symbol = parts.next().unwrap_or_default().to_uppercase();
            let watchlists = state["watchlists"].as_array_mut().ok_or_else(|| McpError::internal_error("fixture is missing `watchlists`", None))?;
            let watchlist = watchlists.iter_mut().find(|item| item.get("id").and_then(Value::as_str) == Some(watchlist_id))
                .ok_or_else(|| McpError::internal_error(format!("fixture watchlist not found: {watchlist_id}"), None))?;
            if let Some(assets) = watchlist["assets"].as_array_mut() {
                assets.retain(|asset| asset.get("symbol").and_then(Value::as_str).map(|value| value.to_uppercase() != symbol).unwrap_or(true));
            }
            return Ok(watchlist.clone());
        }
        if method == "GET" && path.starts_with("/v2/positions/") {
            let symbol = path.trim_start_matches("/v2/positions/");
            let positions = state.get("positions").and_then(Value::as_array).cloned().unwrap_or_default();
            return positions.into_iter().find(|item| item.get("symbol").and_then(Value::as_str) == Some(symbol))
                .ok_or_else(|| McpError::internal_error(format!("fixture position not found: {symbol}"), None));
        }
        if method == "GET" && path.starts_with("/v2/stocks/") {
            let symbol = path.split('/').nth(3).unwrap_or_default();
            let key = if path.ends_with("/snapshot") { "snapshots" } else { "quotes" };
            let value = state.get(key).and_then(|v| v.get(symbol)).cloned().unwrap_or(Value::Null);
            return Ok(value);
        }
        if method == "GET" && path.contains("/news") {
            return fixture_value(&state, "news");
        }
        Err(McpError::internal_error(
            format!("dry-run fixture has no response for GET/{}", path),
            None,
        ))
    }
}

fn fixture_value(state: &Value, key: &str) -> Result<Value, McpError> {
    state.get(key).cloned().ok_or_else(|| {
        McpError::internal_error(format!("dry-run fixture is missing `{key}`"), None)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn place_order_records_without_mutating_positions() {
        let fixture = FixtureState {
            data: Arc::new(Mutex::new(json!({
                "account": {"equity": "100"},
                "positions": [],
                "orders": []
            }))),
        };
        let order = fixture
            .request("POST", "/v2/orders", Some(json!({
                "symbol": "SPY", "side": "buy", "qty": "1"
            })))
            .await
            .unwrap();
        assert_eq!(order["status"], "dry_run");
        assert_eq!(order["dry_run"], true);
        assert_eq!(fixture.request("GET", "/v2/positions", None).await.unwrap(), json!([]));
        assert_eq!(fixture.request("GET", "/v2/orders", None).await.unwrap().as_array().unwrap().len(), 1);
    }
}
