use anyhow::{Result, bail};

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
        let (api_key_id, api_secret_key) = credential_pair(
            std::env::var("APCA_API_KEY_ID").ok(),
            std::env::var("APCA_API_SECRET_KEY").ok(),
        )?;

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

    pub fn has_credentials(&self) -> bool {
        !self.api_key_id.is_empty() && !self.api_secret_key.is_empty()
    }
}

fn normalized_credential(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && !value.to_ascii_lowercase().starts_with("your-"))
}

fn credential_pair(
    api_key_id: Option<String>,
    api_secret_key: Option<String>,
) -> Result<(String, String)> {
    match (
        normalized_credential(api_key_id),
        normalized_credential(api_secret_key),
    ) {
        (Some(api_key_id), Some(api_secret_key)) => Ok((api_key_id, api_secret_key)),
        (None, None) => Ok((String::new(), String::new())),
        _ => bail!(
            "Set both APCA_API_KEY_ID and APCA_API_SECRET_KEY, or leave both unset to use Yahoo fallback mode"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_credentials_enable_fallback_mode() {
        let (key, secret) = credential_pair(None, None).unwrap();
        assert!(key.is_empty());
        assert!(secret.is_empty());
    }

    #[test]
    fn example_placeholders_enable_fallback_mode() {
        let (key, secret) = credential_pair(
            Some("your-alpaca-api-key-id".into()),
            Some("your-alpaca-api-secret-key".into()),
        )
        .unwrap();
        assert!(key.is_empty());
        assert!(secret.is_empty());
    }

    #[test]
    fn partial_credentials_are_rejected() {
        assert!(credential_pair(Some("key".into()), None).is_err());
        assert!(credential_pair(None, Some("secret".into())).is_err());
    }

    #[test]
    fn complete_credentials_are_trimmed() {
        let (key, secret) = credential_pair(Some(" key ".into()), Some(" secret ".into())).unwrap();
        assert_eq!(key, "key");
        assert_eq!(secret, "secret");
    }
}
