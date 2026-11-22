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
