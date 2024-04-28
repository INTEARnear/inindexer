use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use near_indexer_primitives::{types::BlockHeight, StreamerMessage};

use crate::MessageStreamer;

const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1000;

/// A source of streamer messages, can be used as [`MessageStreamer`].
#[async_trait]
pub trait MessageProvider {
    type Error: Display + Debug + Send + Sync + 'static;

    /// Get streamer message at a given block height.
    ///
    /// Returns `None` if there is no block at this height (a fork occured).
    /// When returned None, you should skip this block and try again with the next block height.
    ///
    /// Returns `Err` if there was an error fetching the message. The recommended behavior is to
    /// retry fetching the message after a short delay, unless the implementation says otherwise.
    async fn get_message(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<StreamerMessage>, Self::Error>;
}

pub struct ProviderStreamer<P: MessageProvider + Send + Sync + 'static> {
    provider: P,
    buffer_size: usize,
}

impl<P: MessageProvider + Send + Sync + 'static> ProviderStreamer<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
        }
    }

    pub fn with_buffer_size(provider: P, buffer_size_messages: usize) -> Self {
        Self {
            provider,
            buffer_size: buffer_size_messages,
        }
    }
}

#[async_trait]
impl<P: MessageProvider + Send + Sync + 'static> MessageStreamer for ProviderStreamer<P> {
    type Error = MessageStreamerError<P::Error>;

    async fn stream(
        mut self,
        range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), Self::Error>>,
            tokio::sync::mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let (tx, rx) = tokio::sync::mpsc::channel(self.buffer_size);
        let join_handle = tokio::spawn(async move {
            'outer: for next_block_height in range {
                let mut retries = 0;
                loop {
                    retries += 1;
                    match self.provider.get_message(next_block_height).await {
                        Ok(Some(block)) => {
                            if tx.send(block).await.is_err() {
                                break 'outer;
                            }
                            break;
                        }
                        Ok(None) => {
                            log::debug!(target: "inindexer::message_provider::detect_forks", "No block found at height {next_block_height}. Skipping this block.");
                            break;
                        }
                        Err(err) => {
                            log::error!(target: "inindexer::message_provider::fetch_failed", "Failed to fetch block {next_block_height} (attempt {retries}): {err}");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            log::info!("Block range ended.");
            Ok(())
        });
        Ok((join_handle, rx))
    }
}

#[async_trait]
impl<P: MessageProvider + Send + Sync + 'static> MessageStreamer for P {
    type Error = MessageStreamerError<P::Error>;

    async fn stream(
        self,
        range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), Self::Error>>,
            tokio::sync::mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        ProviderStreamer::new(self).stream(range).await
    }
}

#[derive(Debug)]
pub enum MessageStreamerError<E> {
    ProviderError(E),
    ChannelSendError(tokio::sync::mpsc::error::SendError<StreamerMessage>),
}

/// A parallel implementation of [`MessageStreamer`] that uses multiple workers to fetch messages.
/// Recommended to use when you need to fetch large amount of blocks historically.
pub struct ParallelProviderStreamer<P: MessageProvider + Send + Sync + 'static> {
    pub workers: usize,
    pub provider: Arc<P>,
}

impl<P: MessageProvider + Send + Sync + 'static> ParallelProviderStreamer<P> {
    pub fn new(provider: P, workers: usize) -> Self {
        Self {
            provider: Arc::new(provider),
            workers,
        }
    }
}

#[async_trait]
impl<P: MessageProvider + Send + Sync + 'static> MessageStreamer for ParallelProviderStreamer<P> {
    type Error = MessageStreamerError<P::Error>;

    async fn stream(
        self,
        range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), Self::Error>>,
            tokio::sync::mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE);
        let (mut tasks_txs, mut tasks_rxs) = (Vec::new(), Vec::new());
        for _ in 0..self.workers {
            let (tasks_tx, tasks_rx) = tokio::sync::mpsc::channel(1);
            tasks_txs.push(tasks_tx);
            tasks_rxs.push(tasks_rx);
        }
        let join_handle = tokio::spawn(async move {
            let current_block_height = Arc::new(AtomicU64::new(0));
            for mut tasks_rx in tasks_rxs {
                let provider = Arc::clone(&self.provider);
                let current_block_height = Arc::clone(&current_block_height);
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(processing_block_height) = tasks_rx.recv().await {
                        let mut retries = 0;
                        loop {
                            retries += 1;
                            match provider.get_message(processing_block_height).await {
                                Ok(Some(block)) => {
                                    while current_block_height.load(Ordering::Relaxed)
                                        != processing_block_height
                                    {
                                        tokio::task::yield_now().await;
                                    }
                                    tx.send(block)
                                        .await
                                        .expect("Channel send failed in parallel streamer.");
                                    current_block_height.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Ok(None) => {
                                    while current_block_height.load(Ordering::Relaxed)
                                        != processing_block_height
                                    {
                                        tokio::task::yield_now().await;
                                    }
                                    current_block_height.fetch_add(1, Ordering::Relaxed);
                                    log::debug!(target: "inindexer::message_provider::detect_forks", "No block found at height {processing_block_height}. Skipping this block.");
                                    break;
                                }
                                Err(err) => {
                                    log::error!(target: "inindexer::message_provider::fetch_failed", "Failed to fetch block {processing_block_height} (attempt {retries}): {err}");
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }
                            }
                        }
                    }
                });
            }

            for (i, next_block_height) in range.enumerate() {
                if current_block_height.load(Ordering::Relaxed) == 0 {
                    current_block_height.store(next_block_height, Ordering::Relaxed);
                }
                tasks_txs[i % tasks_txs.len()]
                    .send(next_block_height)
                    .await
                    .unwrap();
            }

            log::info!("Block range ended.");
            Ok(())
        });
        Ok((join_handle, rx))
    }
}
