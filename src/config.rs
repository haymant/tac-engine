use anyhow::{Context, Result, bail};

const DEFAULT_BASE_URL: &str = "https://paper-api.alpaca.markets";
const DEFAULT_DATA_BASE_URL: &str = "https://data.alpaca.markets";
const DEFAULT_STREAM_BASE_URL: &str = "wss://stream.data.alpaca.markets";

#[derive(Debug, Clone)]
pub struct Config {
    pub api_key_id: String,
    pub api_secret_key: String,
    pub base_url: String,
    pub data_base_url: String,
    pub stream_base_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let api_key_id = std::env::var("APCA_API_KEY_ID")
            .context("APCA_API_KEY_ID is required (set in environment or .env)")?;
        let api_secret_key = std::env::var("APCA_API_SECRET_KEY")
            .context("APCA_API_SECRET_KEY is required (set in environment or .env)")?;

        if api_key_id.trim().is_empty() || api_secret_key.trim().is_empty() {
            bail!("APCA_API_KEY_ID and APCA_API_SECRET_KEY must be non-empty");
        }

        let base_url = std::env::var("APCA_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let data_base_url = std::env::var("APCA_DATA_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_DATA_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let stream_base_url = std::env::var("APCA_STREAM_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_STREAM_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            api_key_id,
            api_secret_key,
            base_url,
            data_base_url,
            stream_base_url,
        })
    }
}
