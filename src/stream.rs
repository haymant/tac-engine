use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rmcp::ErrorData as McpError;
use serde_json::{Value, json};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

/// Connect to an Alpaca market-data WebSocket, auth, subscribe, and sample messages.
pub async fn sample_market_stream(
    stream_url: &str,
    api_key_id: &str,
    api_secret_key: &str,
    subscribe: Value,
    duration_secs: u64,
    max_messages: usize,
) -> Result<Value, McpError> {
    let duration = Duration::from_secs(duration_secs.clamp(1, 30));
    let max_messages = max_messages.clamp(1, 200);

    let mut request = stream_url.into_client_request().map_err(|err| {
        McpError::internal_error(format!("invalid stream URL {stream_url}: {err}"), None)
    })?;
    request.headers_mut().insert(
        "APCA-API-KEY-ID",
        api_key_id.parse().map_err(|err| {
            McpError::internal_error(format!("invalid API key header: {err}"), None)
        })?,
    );
    request.headers_mut().insert(
        "APCA-API-SECRET-KEY",
        api_secret_key.parse().map_err(|err| {
            McpError::internal_error(format!("invalid API secret header: {err}"), None)
        })?,
    );

    let (ws, _) = connect_async(request).await.map_err(|err| {
        McpError::internal_error(
            format!(
                "failed to connect to market stream {stream_url}: {err}. Note: most plans allow only one concurrent stream connection."
            ),
            None,
        )
    })?;

    let (mut write, mut read) = ws.split();
    let mut messages: Vec<Value> = Vec::new();

    // Welcome (header auth may already be complete).
    push_next(&mut read, &mut messages, Duration::from_secs(5)).await?;

    // Message auth as fallback when headers were ignored.
    let auth = json!({
        "action": "auth",
        "key": api_key_id,
        "secret": api_secret_key,
    });
    write
        .send(Message::Text(auth.to_string().into()))
        .await
        .map_err(|err| McpError::internal_error(format!("failed to send auth: {err}"), None))?;
    push_next(&mut read, &mut messages, Duration::from_secs(5)).await?;
    // Drop benign duplicate-auth noise from the sample payload.
    messages.retain(|msg| {
        !(msg.get("T").and_then(|v| v.as_str()) == Some("error")
            && msg.get("code").and_then(|v| v.as_i64()) == Some(403)
            && msg
                .get("msg")
                .and_then(|v| v.as_str())
                .is_some_and(|m| m.contains("already authenticated")))
    });

    write
        .send(Message::Text(subscribe.to_string().into()))
        .await
        .map_err(|err| {
            McpError::internal_error(format!("failed to send subscribe: {err}"), None)
        })?;

    let deadline = Instant::now() + duration;
    while messages.len() < max_messages {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => append_payload(&mut messages, &text),
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                return Err(McpError::internal_error(
                    format!("stream read error: {err}"),
                    Some(json!({ "messages": messages })),
                ));
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let _ = write.close().await;

    Ok(json!({
        "stream_url": stream_url,
        "subscribe": subscribe,
        "duration_secs": duration.as_secs(),
        "max_messages": max_messages,
        "message_count": messages.len(),
        "messages": messages,
    }))
}

async fn push_next<S>(
    read: &mut S,
    messages: &mut Vec<Value>,
    wait: Duration,
) -> Result<(), McpError>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match timeout(wait, read.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            append_payload(messages, &text);
            Ok(())
        }
        Ok(Some(Ok(other))) => Err(McpError::internal_error(
            format!("unexpected stream frame before ready: {other:?}"),
            None,
        )),
        Ok(Some(Err(err))) => Err(McpError::internal_error(
            format!("stream error before ready: {err}"),
            None,
        )),
        Ok(None) => Err(McpError::internal_error(
            "stream closed before ready",
            None,
        )),
        Err(_) => Err(McpError::internal_error(
            "timed out waiting for stream handshake message",
            None,
        )),
    }
}

fn append_payload(messages: &mut Vec<Value>, text: &str) {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(items)) => messages.extend(items),
        Ok(other) => messages.push(other),
        Err(_) => messages.push(Value::String(text.to_string())),
    }
}
