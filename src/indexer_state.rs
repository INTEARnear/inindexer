use std::fmt::{Debug, Display};
use std::time::Duration;

use near_indexer_primitives::types::{BlockHeight, BlockHeightDelta};

use crate::{
    Indexer, StreamerMessage,
    transaction_trees::{TransactionTreeStore, TransactionTreesError},
};

const BLOCK_PROCESSING_WARNING_THRESHOLD: Duration = Duration::from_millis(300);
const PERFORMANCE_REPORT_EVERY_BLOCKS: BlockHeightDelta = 5000;

#[derive(Debug)]
pub(crate) struct IndexerState {
    blocks_received: BlockHeightDelta,
    time_spent: Duration,
}

impl IndexerState {
    pub fn new() -> Self {
        Self {
            blocks_received: 0,
            time_spent: Duration::ZERO,
        }
    }

    pub fn on_end<I: Indexer>(&mut self, _indexer: &I) {
        self.report_performance();
    }

    pub fn report_performance(&self) {
        if self.blocks_received == 0 {
            return;
        }
        log::info!(
            target: "inindexer::performance",
            "Processing {} blocks took {:#?} (excluding download), average time per block: {:#?}",
            self.blocks_received,
            self.time_spent,
            self.time_spent / self.blocks_received as u32
        );
    }

    pub(crate) async fn process_block<I: Indexer>(
        &mut self,
        indexer: &mut I,
        message: &StreamerMessage,
        transaction_trees: &TransactionTreeStore,
    ) -> Result<(), I::Error> {
        self.blocks_received += 1;
        if self
            .blocks_received
            .is_multiple_of(PERFORMANCE_REPORT_EVERY_BLOCKS)
        {
            self.report_performance();
        }

        let started = std::time::Instant::now();

        indexer.process_block(message).await?;
        for shard in message.shards.iter() {
            for receipt in shard.receipt_execution_outcomes.iter() {
                let transaction = transaction_trees.tree(&receipt.tx_hash).unwrap_or_else(|| {
                    panic!(
                        "Receipt {} is of transaction {}, which is from before the blocks the transaction trees have read. Increase TransactionTrees::warm_up_blocks",
                        receipt.receipt.receipt_id, receipt.tx_hash
                    )
                });
                indexer
                    .process_receipt(receipt, transaction, message)
                    .await?;
            }
        }
        for transaction in transaction_trees.finished() {
            indexer.process_transaction(transaction, message).await?;
        }
        indexer.process_block_end(message).await?;

        let elapsed = started.elapsed();
        self.time_spent += elapsed;
        log::debug!(target: "inindexer::performance", "Processing block {height} took {elapsed:#?}",
            height = message.block.header.height);
        if elapsed > BLOCK_PROCESSING_WARNING_THRESHOLD {
            log::warn!(target: "inindexer::performance", "Processing block {height} took {elapsed:#?}",
                height = message.block.header.height);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum InIndexerError<E> {
    Streamer(E),
    Join(tokio::task::JoinError),
    Checkpoint(std::io::Error),
    TransactionTrees(TransactionTreesError),
    Discontinuity {
        block_height: BlockHeight,
        prev_height: BlockHeight,
        expected_prev_height: BlockHeight,
    },
}

impl<E: Debug> Display for InIndexerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InIndexerError::Streamer(e) => write!(f, "Streamer error: {e:?}"),
            InIndexerError::Join(e) => write!(f, "Join error: {e:?}"),
            InIndexerError::Checkpoint(e) => write!(f, "Checkpoint file error: {e}"),
            InIndexerError::TransactionTrees(e) => write!(f, "Transaction trees error: {e}"),
            InIndexerError::Discontinuity {
                block_height,
                prev_height,
                expected_prev_height,
            } => write!(
                f,
                "Block {block_height} comes after block {prev_height}, but the last block was {expected_prev_height}"
            ),
        }
    }
}

impl<E: Debug> std::error::Error for InIndexerError<E> {}
