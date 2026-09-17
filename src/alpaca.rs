use reqwest::{Client, Method, StatusCode};
use rmcp::ErrorData as McpError;
use serde_json::Value;

use crate::config::Config;

#[derive(Clone)]
pub struct AlpacaClient {
    http: Client,
    base_url: String,
    data_base_url: String,
    stream_base_url: String,
    api_key_id: String,
    api_secret_key: String,
}

impl AlpacaClient {
    pub fn new(config: Config) -> Result<Self, McpError> {
        let http = Client::builder()
            .build()
            .map_err(|err| {
            McpError::internal_error(format!("failed to build HTTP client: {err}"), None)
        })?;

        Ok(Self {
            http,
            base_url: config.base_url,
            data_base_url: config.data_base_url,
            stream_base_url: config.stream_base_url,
            api_key_id: config.api_key_id,
            api_secret_key: config.api_secret_key,
        })
    }

    pub fn api_key_id(&self) -> &str {
        &self.api_key_id
    }

    pub fn api_secret_key(&self) -> &str {
        &self.api_secret_key
    }

    pub fn stream_base_url(&self) -> &str {
        &self.stream_base_url
    }

    pub async fn get(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Value, McpError> {
        self.request(&self.base_url, Method::GET, path, query, None)
            .await
    }

    pub async fn data_get(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Value, McpError> {
        self.request(&self.data_base_url, Method::GET, path, query, None)
            .await
    }

    pub async fn post(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value, McpError> {
        self.request(&self.base_url, Method::POST, path, query, body)
            .await
    }

    pub async fn put(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value, McpError> {
        self.request(&self.base_url, Method::PUT, path, query, body)
            .await
    }

    pub async fn delete(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Value, McpError> {
        self.request(&self.base_url, Method::DELETE, path, query, None)
            .await
    }

    async fn request(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value, McpError> {
        let url = format!("{base_url}{path}");
        let mut builder = self
            .http
            .request(method.clone(), &url)
            .header("accept", "application/json")
            .header("APCA-API-KEY-ID", &self.api_key_id)
            .header("APCA-API-SECRET-KEY", &self.api_secret_key);

        if !query.is_empty() {
            builder = builder.query(query);
        }

        if let Some(payload) = body {
            builder = builder
                .header("content-type", "application/json")
                .json(&payload);
        }

        let response = builder.send().await.map_err(|err| {
            McpError::internal_error(
                format!("Alpaca request failed ({method} {path}): {err}"),
                None,
            )
        })?;

        let status = response.status();
        let text = response.text().await.map_err(|err| {
            McpError::internal_error(format!("failed to read Alpaca response body: {err}"), None)
        })?;

        if !status.is_success() {
            return Err(alpaca_http_error(method, path, status, &text));
        }

        if text.trim().is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_str(&text).map_err(|err| {
            McpError::internal_error(
                format!("failed to parse Alpaca JSON ({method} {path}): {err}"),
                Some(Value::String(text)),
            )
        })
    }
}

pub fn json_text(value: Value) -> String {
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
}

fn alpaca_http_error(method: Method, path: &str, status: StatusCode, body: &str) -> McpError {
    let data = serde_json::json!({
        "http_status": status.as_u16(),
        "reason": status.canonical_reason().unwrap_or(""),
        "body": serde_json::from_str::<Value>(body).unwrap_or_else(|_| Value::String(body.to_string())),
    });
    McpError::internal_error(
        format!("Alpaca API error {status} for {method} {path}"),
        Some(data),
    )
}

/// Returns true when `err` is an Alpaca HTTP error with the given status code
/// (e.g. `403 Forbidden` when the requested feed is not permitted on the account).
pub fn is_http_error(err: &McpError, status: u16) -> bool {
    if let Some(data) = &err.data {
        if let Some(code) = data.get("http_status").and_then(Value::as_u64) {
            return code == status as u64;
        }
    }
    err.message.contains(&format!(" {status} "))
}

/// Returns true when `err` is an Alpaca HTTP error whose status is one of
/// `statuses` — e.g. 400/404/422 for "symbol not found", which the daily-bar
/// Yahoo fallback treats as an unknown (non-US) listing rather than a failure.
pub fn is_http_error_in(err: &McpError, statuses: &[u16]) -> bool {
    if let Some(data) = &err.data {
        if let Some(code) = data.get("http_status").and_then(Value::as_u64) {
            return statuses.contains(&(code as u16));
        }
    }
    statuses.iter().any(|s| err.message.contains(&format!(" {s} ")))
}
