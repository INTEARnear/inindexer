use std::fmt::{Debug, Display};

use async_trait::async_trait;
use near_indexer_primitives::{types::BlockHeight, StreamerMessage};
use near_lake_framework::LakeConfigBuilder;

use crate::MessageStreamer;

pub struct LakeStreamer {
    config: LakeConfigBuilder,
    blocks_preload_pool_size: usize,
}

impl LakeStreamer {
    pub fn from_config(config: LakeConfigBuilder, blocks_preload_pool_size: usize) -> Self {
        Self {
            config,
            blocks_preload_pool_size,
        }
    }

    pub fn mainnet() -> Self {
        Self::from_config(LakeConfigBuilder::default().mainnet(), 100)
    }

    pub fn testnet() -> Self {
        Self::from_config(LakeConfigBuilder::default().testnet(), 100)
    }
}

#[async_trait]
impl MessageStreamer for LakeStreamer {
    type Error = LakeError;

    async fn stream(
        self,
        mut range: impl Iterator<Item = BlockHeight> + Send + 'static,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), Self::Error>>,
            tokio::sync::mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let start_block_height = if let Some(start_block_height) = range.next() {
            start_block_height
        } else {
            return Ok((
                tokio::task::spawn(async { Ok(()) }),
                tokio::sync::mpsc::channel(0).1,
            ));
        };
        let (join_handle, mut streamer) = near_lake_framework::streamer(
            self.config
                .blocks_preload_pool_size(self.blocks_preload_pool_size)
                .start_block_height(start_block_height)
                .build()
                .map_err(|err| LakeError::LakeConfigError(Box::new(err)))?,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(self.blocks_preload_pool_size);
        let join_handle_2 = tokio::spawn(async move {
            let mut last_requested_block_height = start_block_height;
            while let Some(received_block) = streamer.recv().await {
                // This part is a bit tricky, it checks if our range is consecutive and ascending, which
                // needs to handle skipped block heights (forks) properly.
                // Assuming receiving `==` is fine because the first block will be the same, and
                // assuming receiving `>` is impossible because lake framework always sends blocks in ascending order.
                while last_requested_block_height < received_block.block.header.height {
                    if let Some(next_requested_block_height) = range.next() {
                        if last_requested_block_height + 1 != next_requested_block_height {
                            return Err(
                                LakeError::LakeOnlySupportsConsecutiveAscendingBlockHeights,
                            );
                        }
                        last_requested_block_height += 1;
                    } else {
                        return Ok(());
                    }
                }
                tx.send(received_block)
                    .await
                    .map_err(|err| LakeError::SendError(err))?;
            }
            log::info!("Block range ended.");
            drop(streamer);
            join_handle
                .await
                .map_err(|err| LakeError::JoinError(err))?
                .map_err(|err| LakeError::LakeError(err.into()))?;
            Ok(())
        });
        Ok((join_handle_2, rx))
    }
}

#[derive(Debug)]
pub enum LakeError {
    LakeOnlySupportsConsecutiveAscendingBlockHeights,
    LakeConfigError(Box<dyn std::error::Error + Send>),
    JoinError(tokio::task::JoinError),
    LakeError(Box<dyn std::error::Error + Send>),
    SendError(tokio::sync::mpsc::error::SendError<StreamerMessage>),
}

impl Display for LakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LakeError::LakeOnlySupportsConsecutiveAscendingBlockHeights => {
                write!(
                    f,
                    "Inindexer only supports consecutive ascending block heights for Lake streamer"
                )
            }
            LakeError::LakeConfigError(err) => write!(f, "Lake config building error: {err}"),
            LakeError::JoinError(err) => write!(f, "Join error: {err}"),
            LakeError::LakeError(err) => write!(f, "Lake error: {err}"),
            LakeError::SendError(err) => write!(f, "Send error: {err}"),
        }
    }
}

impl std::error::Error for LakeError {}
