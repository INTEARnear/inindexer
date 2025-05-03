use std::{
    fmt::Display,
    sync::{atomic::AtomicBool, Arc},
};

use async_trait::async_trait;
use fastnear_neardata_fetcher::{fetcher, FetcherConfig};
use fastnear_primitives::types::ChainId;
use near_indexer_primitives::{
    types::{BlockHeight, Finality},
    IndexerExecutionOutcomeWithReceipt, IndexerShard, StreamerMessage,
};
use tokio::{
    sync::{self, mpsc},
    task::JoinHandle,
};

use crate::MessageStreamer;

pub struct NeardataProvider {
    config: FetcherConfig,
}

impl NeardataProvider {
    pub fn of_chain_id(chain_id: ChainId) -> Self {
        Self {
            config: FetcherConfig {
                num_threads: std::thread::available_parallelism()
                    .map(|p| p.get() as u64)
                    .unwrap_or(1),
                start_block_height: None, // will be set later
                chain_id,
                timeout_duration: None,
                retry_duration: None,
                disable_archive_sync: false,
                auth_bearer_token: None,
                enable_r2_archive_sync: true,
                end_block_height: None,
                finality: Finality::Final,
                user_agent: Some(format!(
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
                )),
            },
        }
    }

    pub fn mainnet() -> Self {
        Self::of_chain_id(ChainId::Mainnet)
    }

    pub fn testnet() -> Self {
        Self::of_chain_id(ChainId::Testnet)
    }

    pub fn finality(mut self, finality: Finality) -> Self {
        self.config.finality = finality;
        self
    }

    pub fn with_auth_bearer_token(mut self, token: String) -> Self {
        self.config.auth_bearer_token = Some(token);
        self
    }

    /// Start and end block height in this config will be ignored.
    pub fn override_fetcher_config(mut self, config: FetcherConfig) -> Self {
        self.config = config;
        self
    }
}

#[async_trait]
impl MessageStreamer for NeardataProvider {
    type Error = NeardataError;

    async fn stream(
        self,
        first_block_inclusive: BlockHeight,
        last_block_exclusive: Option<BlockHeight>,
    ) -> Result<
        (
            JoinHandle<Result<(), Self::Error>>,
            mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let (tx, rx) = sync::mpsc::channel(100000);
        let join_handle = tokio::spawn(async move {
            let (fastnear_tx, mut fastnear_rx) = tokio::sync::mpsc::channel(100000);
            let is_running = Arc::new(AtomicBool::new(true));
            let join_handle = tokio::spawn(fetcher::start_fetcher(
                FetcherConfig {
                    start_block_height: Some(first_block_inclusive),
                    end_block_height: last_block_exclusive,
                    ..self.config
                },
                fastnear_tx,
                Arc::clone(&is_running),
            ));
            let mut next_block_height = first_block_inclusive;
            while let Some(received_block) = fastnear_rx.recv().await {
                if received_block.block.header.height < next_block_height {
                    panic!(
                        "Received block {} which is before the next expected block {}, aborting",
                        received_block.block.header.height, next_block_height
                    );
                }
                next_block_height = received_block.block.header.height + 1;
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
                if last_block_exclusive
                    .is_some_and(|last_block_exclusive| next_block_height >= last_block_exclusive)
                {
                    is_running.store(false, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
            }
            join_handle.await.unwrap();
            Ok(())
        });
        Ok((join_handle, rx))
    }
}

#[derive(Debug)]
pub enum NeardataError {
    SendError(Box<tokio::sync::mpsc::error::SendError<StreamerMessage>>),
}

impl Display for NeardataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NeardataError::SendError(err) => write!(f, "Failed to send message: {err}"),
        }
    }
}

impl std::error::Error for NeardataError {}
