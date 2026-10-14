mod indexer_state;
#[cfg(all(test, feature = "neardata", feature = "neardata-old"))]
mod indexer_tests;
pub mod message_provider;
pub mod multiindexer;
pub mod near_utils;
#[cfg(feature = "neardata")]
pub mod neardata;
#[cfg(feature = "neardata-old")]
pub mod neardata_old;
#[cfg(feature = "redb")]
pub mod redb_storage;
pub mod storage;
#[cfg(feature = "teardata")]
pub mod teardata;
#[cfg(all(test, feature = "teardata"))]
mod teardata_tests;
pub mod transaction_trees;
#[cfg(all(test, feature = "teardata", feature = "redb"))]
mod transaction_trees_tests;

use std::fmt::Debug;

use async_trait::async_trait;
use indexer_state::{InIndexerError, IndexerState};
pub use near_indexer_primitives;
use near_indexer_primitives::{
    CryptoHash, IndexerChunkView,
    types::{BlockHeight, BlockHeightDelta, ShardId},
    views::{BlockView, ExecutionOutcomeWithIdView, ReceiptView, StateChangeWithCauseView},
};
use near_utils::MAINNET_GENESIS_BLOCK_HEIGHT;
use serde::{Deserialize, Serialize};
use storage::IndexerStorage;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use transaction_trees::{IndexerStorageError, TransactionTree, TransactionTreeStore};

#[async_trait]
pub trait MessageStreamer {
    type Error;

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
    >;
}

#[async_trait]
pub trait Indexer: Send + Sync + 'static {
    type Error: Debug + Send + Sync + 'static;

    /// Runs for every block.
    async fn process_block(&mut self, _block: &StreamerMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every transaction once all the receipts it has led to have been executed, after
    /// the receipts of the block where the last of them was executed.
    async fn process_transaction(
        &mut self,
        _transaction: &TransactionTree,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every receipt, when it's executed, with the tree of its transaction, which has
    /// every receipt of it executed up to the end of this block.
    async fn process_receipt(
        &mut self,
        _receipt: &IndexerExecutionOutcomeWithReceipt,
        _transaction: &TransactionTree,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every block, after all other callbacks are called.
    ///
    /// Indexers that resume with [`BlockRange::AutoContinue`] save the block's height as the last
    /// processed one in the same atomic write as the block's own data, usually the database
    /// transaction committed here. Saved apart from the data, a crash between the two writes
    /// skips the block or processes it twice.
    async fn process_block_end(&mut self, _block: &StreamerMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs when the indexer stops (block range ended or Ctrl+C). Useful if you don't save the
    /// data right away on every receipt, and want to "flush" some of the generated data. This
    /// helps you make sure the program doesn't exit before all data is saved. When you `.await`
    /// a `run_indexer` call, it won't return until `finalize` returns.
    async fn finalize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamerMessage {
    pub block: BlockView,
    pub shards: Vec<IndexerShard>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerShard {
    pub shard_id: ShardId,
    pub chunk: Option<IndexerChunkView>,
    pub receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt>,
    pub state_changes: Vec<StateChangeWithCauseView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: ExecutionOutcomeWithIdView,
    pub receipt: ReceiptView,
    pub tx_hash: CryptoHash,
}

pub async fn run_indexer<
    I: Indexer + Send + Sync + 'static,
    S: MessageStreamer + Send + Sync + 'static,
>(
    indexer: &mut I,
    streamer: S,
    options: IndexerOptions,
) -> Result<(), InIndexerError<S::Error>> {
    let mut indexer_state = IndexerState::new();

    let cancellation_token = CancellationToken::new();
    let mut transaction_trees = TransactionTreeStore::open(options.storage)
        .await
        .map_err(InIndexerError::Storage)?;
    let (start_block_height, end_block_height, mut expected_prev_height) = match options.range {
        BlockRange::Range {
            start_inclusive,
            end_exclusive,
        } => (start_inclusive, end_exclusive, None),
        BlockRange::AutoContinue(auto_continue) => {
            let last_processed_block = transaction_trees
                .last_processed_block()
                .map_err(InIndexerError::Storage)?;
            let start = last_processed_block
                .map_or(auto_continue.start_height_if_none_processed, |height| {
                    height + 1
                });
            let end = match auto_continue.end {
                AutoContinueEnd::Height(height) => Some(height),
                AutoContinueEnd::Count(count) => Some(start + count),
                AutoContinueEnd::Infinite => None,
            };
            (start, end, last_processed_block)
        }
    };

    if options.ctrl_c_handler {
        let cancellation_token_2 = cancellation_token.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            cancellation_token_2.cancel();
        });
    }

    let first_block_to_read = match transaction_trees.last_block() {
        Some(last_block) if last_block >= start_block_height => {
            return Err(InIndexerError::Storage(IndexerStorageError::Ahead {
                last_block,
                next_block: start_block_height,
            }));
        }
        Some(last_block) => {
            expected_prev_height = Some(last_block);
            last_block + 1
        }
        None => {
            let warm_up_start = start_block_height.saturating_sub(options.warm_up_blocks);
            if warm_up_start < start_block_height {
                expected_prev_height = None;
            }
            warm_up_start
        }
    };
    if first_block_to_read < start_block_height {
        log::info!(
            "Reading blocks {first_block_to_read}..{start_block_height} into the transaction trees only"
        );
    }

    let (handle, mut streamer) = streamer
        .stream(first_block_to_read, end_block_height)
        .await
        .map_err(InIndexerError::Streamer)?;

    while let Some(message) = streamer.recv().await {
        let header = &message.block.header;
        // Headers from before prev_height was added to them can't be checked
        if let (Some(expected_prev_height), Some(prev_height)) =
            (expected_prev_height, header.prev_height)
            && prev_height != expected_prev_height
        {
            return Err(InIndexerError::Discontinuity {
                block_height: header.height,
                prev_height,
                expected_prev_height,
            });
        }
        expected_prev_height = Some(header.height);

        transaction_trees.add_block(&message);
        let processed = message.block.header.height >= start_block_height;
        if processed {
            match indexer_state
                .process_block(indexer, &message, &transaction_trees)
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    log::error!(
                        "Error processing block {height}: {e:?}",
                        height = message.block.header.height
                    );
                    if options.stop_on_error {
                        break;
                    }
                }
            }
        }
        // After the block is saved as processed, so that the trees are never ahead of the indexer.
        transaction_trees
            .commit(message.block.header.height, processed)
            .await
            .map_err(InIndexerError::Storage)?;
        if cancellation_token.is_cancelled() {
            log::info!(
                "Received Ctrl+C signal, stopping after block {} is fully processed",
                message.block.header.height
            );
            break;
        }
    }

    drop(streamer);
    indexer_state.on_end(indexer);
    handle
        .await
        .map_err(InIndexerError::Join)?
        .map_err(InIndexerError::Streamer)
}

#[derive(Debug)]
pub struct IndexerOptions {
    /// If true, the indexer will stop if one of indexer's methods returns an error.
    pub stop_on_error: bool,
    /// Blocks range to process. If None, the indexer will process all blocks from the streamer.
    pub range: BlockRange,
    /// If true, the indexer will gracefully stop on Ctrl+C signal, after the block it's processing.
    pub ctrl_c_handler: bool,
    /// Keeps the transaction trees and the last processed block between runs. `None` keeps the
    /// trees only in memory, so they start from the warm-up again, and can't be used with
    /// [`BlockRange::AutoContinue`].
    pub storage: Option<Box<dyn IndexerStorage>>,
    /// When the storage is empty, this many blocks before the first block to process are read into
    /// the transaction trees without calling the indexer, so that receipts of transactions from
    /// just before it have their trees too. A receipt of a transaction from before the blocks the
    /// trees have read panics, since it has no tree.
    pub warm_up_blocks: BlockHeightDelta,
}

/// Every block's `prev_height` is checked against the height of the block streamed before it, so a
/// streamer that skips a block stops the indexer with [`InIndexerError::Discontinuity`].
#[derive(Debug)]
pub enum BlockRange {
    /// Consecutive range of blocks to process. The range can be finite or infinite. If it's finite,
    /// the indexer will stop once the iterator is exhausted.
    Range {
        start_inclusive: BlockHeight,
        end_exclusive: Option<BlockHeight>,
    },
    /// Continues right after the last block committed to [`IndexerOptions::storage`] as processed,
    /// which is committed after `process_block_end`. A crash between the indexer's own writes and
    /// this one makes the block run again, unless the storage commits them together. The first
    /// block's `prev_height` must be that block.
    AutoContinue(AutoContinue),
}

impl IndexerOptions {
    pub fn default_with_range(range: BlockRange) -> Self {
        Self {
            stop_on_error: false,
            range,
            ctrl_c_handler: true,
            storage: None,
            warm_up_blocks: 0,
        }
    }
}

#[derive(Debug)]
pub struct AutoContinue {
    /// Where to start when no block was processed yet. Default is the mainnet genesis block height.
    pub start_height_if_none_processed: BlockHeight,
    /// If set, the indexer will stop processing blocks after this height. If None, the indexer will process
    /// blocks infinitely.
    pub end: AutoContinueEnd,
}

#[derive(Debug)]
pub enum AutoContinueEnd {
    /// The indexer will stop processing blocks after this height.
    Height(BlockHeight),
    /// The indexer will process this many blocks and then stop.
    Count(BlockHeightDelta),
    /// The indexer will process blocks infinitely.
    Infinite,
}

impl Default for AutoContinue {
    fn default() -> Self {
        Self {
            start_height_if_none_processed: MAINNET_GENESIS_BLOCK_HEIGHT,
            end: AutoContinueEnd::Infinite,
        }
    }
}
