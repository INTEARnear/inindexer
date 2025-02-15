use std::{
    fmt::Display,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use async_trait::async_trait;
use fastnear_neardata_fetcher::{fetcher, FetcherConfig};
use fastnear_primitives::types::ChainId;
use near_indexer_primitives::{
    types::BlockHeight, IndexerExecutionOutcomeWithReceipt, IndexerShard, StreamerMessage,
};
use tokio::{
    sync::{self, mpsc},
    task::JoinHandle,
};

use crate::MessageStreamer;

pub struct NeardataProvider {
    config: FetcherConfig,
    client: reqwest::Client,
}

impl NeardataProvider {
    pub fn of_chain_id(chain_id: ChainId) -> Self {
        Self {
            config: FetcherConfig {
                num_threads: std::thread::available_parallelism()
                    .map(|p| p.get() as u64)
                    .unwrap_or(1),
                start_block_height: 0, // will be set later
                chain_id,
                timeout_duration: None,
                retry_duration: None,
                disable_archive_sync: false,
                auth_bearer_token: None,
            },
            client: reqwest::Client::builder()
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
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    pub fn mainnet() -> Self {
        Self::of_chain_id(ChainId::Mainnet)
    }

    pub fn testnet() -> Self {
        Self::of_chain_id(ChainId::Testnet)
    }

    pub fn with_auth_bearer_token(mut self, token: String) -> Self {
        self.config.auth_bearer_token = Some(token);
        self
    }
}

#[async_trait]
impl MessageStreamer for NeardataProvider {
    type Error = NeardataError;

    async fn stream(
        self,
        mut range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            JoinHandle<Result<(), Self::Error>>,
            mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let (tx, rx) = sync::mpsc::channel(100000);
        let start_block_height = if let Some(start_block_height) = range.next() {
            start_block_height
        } else {
            return Ok((
                tokio::task::spawn(async { Ok(()) }),
                tokio::sync::mpsc::channel(0).1,
            ));
        };
        let join_handle = tokio::spawn(async move {
            let (fastnear_tx, mut fastnear_rx) = tokio::sync::mpsc::channel(100000);
            let is_running = Arc::new(AtomicBool::new(true));
            let join_handle = tokio::spawn(fetcher::start_fetcher(
                Some(self.client),
                FetcherConfig {
                    start_block_height,
                    ..self.config
                },
                fastnear_tx,
                Arc::clone(&is_running),
            ));
            let mut last_requested_block_height = start_block_height;
            'recv: while let Some(received_block) = fastnear_rx.recv().await {
                while last_requested_block_height < received_block.block.header.height {
                    if let Some(next_requested_block_height) = range.next() {
                        if last_requested_block_height + 1 != next_requested_block_height {
                            is_running.store(false, std::sync::atomic::Ordering::SeqCst);
                            join_handle.await.unwrap();
                            return Err(
                                NeardataError::OnlySupportsConsecutiveAscendingBlockHeights,
                            );
                        }
                        last_requested_block_height += 1;
                    } else {
                        is_running.store(false, std::sync::atomic::Ordering::SeqCst);
                        break 'recv;
                    }
                }
                let received_block = StreamerMessage {
                    block: received_block.block,
                    shards: received_block
                        .shards
                        .into_iter()
                        .map(|shard| IndexerShard {
                            shard_id: shard.shard_id,
                            chunk: shard.chunk,
                            receipt_execution_outcomes: shard
                                .receipt_execution_outcomes
                                .into_iter()
                                .map(|receipt| IndexerExecutionOutcomeWithReceipt {
                                    execution_outcome: receipt.execution_outcome,
                                    receipt: receipt.receipt,
                                })
                                .collect(),
                            state_changes: shard.state_changes,
                        })
                        .collect(),
                };
                tx.send(received_block)
                    .await
                    .map_err(|e| NeardataError::SendError(Box::new(e)))?;
            }
            join_handle.await.unwrap();
            Ok(())
        });
        Ok((join_handle, rx))
    }
}

#[derive(Debug)]
pub enum NeardataError {
    OnlySupportsConsecutiveAscendingBlockHeights,
    SendError(Box<tokio::sync::mpsc::error::SendError<StreamerMessage>>),
}

impl Display for NeardataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NeardataError::OnlySupportsConsecutiveAscendingBlockHeights => {
                write!(
                    f,
                    "Inindexer only supports consecutive ascending block heights for Neardata streamer. Try OldNeardataProvider instead."
                )
            }
            NeardataError::SendError(err) => write!(f, "Failed to send message: {err}"),
        }
    }
}

impl std::error::Error for NeardataError {}
