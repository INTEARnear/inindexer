use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use near_indexer_primitives::{types::BlockHeight, StreamerMessage};

use crate::message_provider::MessageProvider;

pub struct OldNeardataProvider {
    base_url: String,
    client: reqwest::Client,
    optimistic: bool,
}

impl OldNeardataProvider {
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
        Self {
            base_url,
            client,
            optimistic: false,
        }
    }

    pub fn mainnet() -> Self {
        Self::with_base_url("https://mainnet.neardata.xyz".to_string())
    }

    pub fn testnet() -> Self {
        Self::with_base_url("https://testnet.neardata.xyz".to_string())
    }

    pub fn optimistic(mut self) -> Self {
        self.optimistic = true;
        self
    }
}

#[async_trait]
impl MessageProvider for OldNeardataProvider {
    type Error = OldNeardataError;

    async fn get_message(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<StreamerMessage>, Self::Error> {
        let finality = if self.optimistic {
            "block_opt"
        } else {
            "block"
        };
        let response = self
            .client
            .get(format!("{}/v0/{finality}/{block_height}", self.base_url))
            .send()
            .await?;
        let text = response.text().await?;
        match serde_json::from_str::<Option<StreamerMessage>>(&text) {
            Ok(maybe_block) => Ok(maybe_block),
            Err(err) => Err(OldNeardataError::FailedToParse {
                err,
                response: text,
            }),
        }
    }
}

#[derive(Debug)]
pub enum OldNeardataError {
    FailedToParse {
        err: serde_json::Error,
        response: String,
    },
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<E> From<E> for OldNeardataError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: E) -> Self {
        OldNeardataError::Other(Box::new(err))
    }
}

impl Display for OldNeardataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OldNeardataError::FailedToParse { err, response } => {
                write!(f, "Failed to parse response: {err:?}\nResponse: {response}")
            }
            OldNeardataError::Other(err) => write!(f, "{err}"),
        }
    }
}
