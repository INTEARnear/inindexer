use std::{error::Error, fmt::Debug};

use async_trait::async_trait;
use near_indexer_primitives::{CryptoHash, types::BlockHeight};

use crate::transaction_trees::TransactionTree;

/// Keeps the transaction trees and the last processed block, so that the indexer continues with
/// them after a restart.
#[async_trait]
pub trait IndexerStorage: Debug + Send + Sync {
    /// Reads everything committed so far, once, before the indexer starts.
    async fn load(&mut self) -> Result<SavedState, Box<dyn Error + Send + Sync>>;

    /// Saves the changes of one block in one atomic write, after `process_block_end`.
    async fn commit(&mut self, commit: BlockCommit<'_>)
    -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Default)]
pub struct SavedState {
    pub trees: Vec<TransactionTree>,
    /// The last block committed.
    pub last_block: Option<BlockHeight>,
    /// The last block committed as processed.
    pub last_processed_block: Option<BlockHeight>,
}

#[derive(Debug)]
pub struct BlockCommit<'a> {
    pub block_height: BlockHeight,
    /// Whether the indexer has processed the block (should save height as last processed block), or it was only read into the trees.
    pub processed: bool,
    /// Trees that the block created or changed, and didn't finish.
    pub changed: Vec<&'a TransactionTree>,
    /// Transactions of the trees that the block finished, which are removed.
    pub finished: &'a [CryptoHash],
}
