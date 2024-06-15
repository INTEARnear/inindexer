use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use near_indexer_primitives::{types::BlockHeight, StreamerMessage};

use crate::message_provider::MessageProvider;

pub struct NeardataServerProvider {
    base_url: String,
    client: reqwest::Client,
}

impl NeardataServerProvider {
    pub fn with_base_url(base_url: String) -> Self {
        Self::with_base_url_and_client(
            base_url,
            reqwest::Client::builder()
                .user_agent(format!(
                    "{} {} {}",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                    if cfg!(test) {
                        "test"
                    } else if cfg!(debug_assertions) {
                        "debug"
                    } else {
                        "release"
                    }
                ))
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        )
    }

    pub fn with_base_url_and_client(base_url: String, client: reqwest::Client) -> Self {
        Self { base_url, client }
    }

    pub fn mainnet() -> Self {
        Self::with_base_url("https://mainnet.neardata.xyz".to_string())
    }

    pub fn testnet() -> Self {
        Self::with_base_url("https://testnet.neardata.xyz".to_string())
    }
}

#[async_trait]
impl MessageProvider for NeardataServerProvider {
    type Error = FastNearDataServerError;

    async fn get_message(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<StreamerMessage>, Self::Error> {
        let response = self
            .client
            .get(&format!("{}/v0/block/{block_height}", self.base_url))
            .send()
            .await?;
        let text = response.text().await?;
        match serde_json::from_str::<Option<StreamerMessage>>(&text) {
            Ok(maybe_block) => Ok(maybe_block),
            Err(err) => Err(FastNearDataServerError::FailedToParse {
                err,
                response: text,
            }),
        }
    }
}

#[derive(Debug)]
pub enum FastNearDataServerError {
    FailedToParse {
        err: serde_json::Error,
        response: String,
    },
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<E> From<E> for FastNearDataServerError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: E) -> Self {
        FastNearDataServerError::Other(Box::new(err))
    }
}

impl Display for FastNearDataServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FastNearDataServerError::FailedToParse { err, response } => {
                write!(f, "Failed to parse response: {err:?}\nResponse: {response}")
            }
            FastNearDataServerError::Other(err) => write!(f, "{err}"),
        }
    }
}
