use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Display,
};

use near_indexer_primitives::{CryptoHash, IndexerTransactionWithOutcome, types::BlockHeight};
use serde::{Deserialize, Serialize};

use crate::{
    IndexerExecutionOutcomeWithReceipt, StreamerMessage,
    near_utils::is_receipt_successful,
    storage::{BlockCommit, IndexerStorage, SavedState},
};

/// A transaction, and the receipts it has led to that have executed so far.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionTree {
    pub transaction: IndexerTransactionWithOutcome,
    /// In the order they were executed.
    pub receipts: Vec<ExecutedReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutedReceipt {
    pub receipt: IndexerExecutionOutcomeWithReceipt,
    /// The block the receipt was executed in.
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
}

impl TransactionTree {
    pub fn receipt(&self, receipt_id: &CryptoHash) -> Option<&ExecutedReceipt> {
        self.receipts
            .iter()
            .find(|receipt| receipt.receipt.receipt.receipt_id == *receipt_id)
    }

    /// The receipt that created this receipt, `None` if the transaction created it.
    pub fn parent(&self, receipt_id: &CryptoHash) -> Option<&ExecutedReceipt> {
        self.receipts.iter().find(|receipt| {
            receipt
                .receipt
                .execution_outcome
                .outcome
                .receipt_ids
                .contains(receipt_id)
        })
    }

    /// The receipt's parent, the parent's parent, and so on, up to the receipt the transaction
    /// created.
    pub fn ancestors(&self, receipt_id: &CryptoHash) -> impl Iterator<Item = &ExecutedReceipt> {
        std::iter::successors(self.parent(receipt_id), |receipt| {
            self.parent(&receipt.receipt.receipt.receipt_id)
        })
    }

    /// Receipts created by the transaction or its receipts that haven't been executed yet.
    pub fn pending_receipt_ids(&self) -> impl Iterator<Item = &CryptoHash> {
        let executed: HashSet<&CryptoHash> = self
            .receipts
            .iter()
            .map(|receipt| &receipt.receipt.receipt.receipt_id)
            .collect();
        std::iter::once(&self.transaction.outcome.execution_outcome.outcome)
            .chain(
                self.receipts
                    .iter()
                    .map(|receipt| &receipt.receipt.execution_outcome.outcome),
            )
            .flat_map(|outcome| outcome.receipt_ids.iter())
            .filter(move |receipt_id| !executed.contains(receipt_id))
    }

    pub fn is_complete(&self) -> bool {
        self.pending_receipt_ids().next().is_none()
    }

    pub fn all_receipts_successful(&self) -> bool {
        self.receipts
            .iter()
            .all(|receipt| is_receipt_successful(&receipt.receipt) == Some(true))
    }
}

pub(crate) struct TransactionTreeStore {
    storage: Option<Box<dyn IndexerStorage>>,
    trees: HashMap<CryptoHash, TransactionTree>,
    /// Trees that the current block changed and didn't finish, in the order it changed them.
    changed: Vec<CryptoHash>,
    /// Trees that the current block finished, in the order it changed them.
    finished: Vec<CryptoHash>,
    last_block: Option<BlockHeight>,
    last_processed_block: Option<BlockHeight>,
}

impl TransactionTreeStore {
    pub(crate) async fn open(
        mut storage: Option<Box<dyn IndexerStorage>>,
    ) -> Result<Self, IndexerStorageError> {
        let saved = match &mut storage {
            Some(storage) => {
                let saved = storage.load().await.map_err(IndexerStorageError::Storage)?;
                log::info!(target: "inindexer::transaction_trees", "Loaded {} transaction trees, last block: {:?}, last processed block: {:?}", saved.trees.len(), saved.last_block, saved.last_processed_block);
                saved
            }
            None => SavedState::default(),
        };
        Ok(Self {
            storage,
            trees: saved
                .trees
                .into_iter()
                .map(|tree| (tree.transaction.transaction.hash, tree))
                .collect(),
            changed: Vec::new(),
            finished: Vec::new(),
            last_block: saved.last_block,
            last_processed_block: saved.last_processed_block,
        })
    }

    pub(crate) fn last_block(&self) -> Option<BlockHeight> {
        self.last_block
    }

    pub(crate) fn last_processed_block(&self) -> Result<Option<BlockHeight>, IndexerStorageError> {
        if self.storage.is_none() {
            return Err(IndexerStorageError::NoStorage);
        }
        Ok(self.last_processed_block)
    }

    pub(crate) fn tree(&self, transaction_hash: &CryptoHash) -> Option<&TransactionTree> {
        self.trees.get(transaction_hash)
    }

    /// Trees that the current block finished: all their receipts have been executed.
    pub(crate) fn finished(&self) -> impl Iterator<Item = &TransactionTree> {
        self.finished.iter().map(|hash| &self.trees[hash])
    }

    /// Adds the block's transactions as new trees, and its receipts to the trees of their
    /// transactions.
    pub(crate) fn add_block(&mut self, block: &StreamerMessage) {
        let header = &block.block.header;
        let mut changed = Vec::new();
        for transaction in block
            .shards
            .iter()
            .filter_map(|shard| shard.chunk.as_ref())
            .flat_map(|chunk| chunk.transactions.iter())
        {
            let hash = transaction.transaction.hash;
            self.trees.insert(
                hash,
                TransactionTree {
                    transaction: transaction.clone(),
                    receipts: Vec::new(),
                },
            );
            changed.push(hash);
        }
        for receipt in block
            .shards
            .iter()
            .flat_map(|shard| shard.receipt_execution_outcomes.iter())
        {
            if let Some(tree) = self.trees.get_mut(&receipt.tx_hash) {
                tree.receipts.push(ExecutedReceipt {
                    receipt: receipt.clone(),
                    block_height: header.height,
                    block_timestamp_nanosec: header.timestamp_nanosec as u128,
                });
                changed.push(receipt.tx_hash);
            }
        }
        let mut seen = HashSet::new();
        changed.retain(|hash| seen.insert(*hash));
        (self.finished, self.changed) = changed
            .into_iter()
            .partition(|hash| self.trees[hash].is_complete());
    }

    /// Removes the trees that the block finished, and commits the block's changes to the storage,
    /// if there is one.
    pub(crate) async fn commit(
        &mut self,
        block_height: BlockHeight,
        processed: bool,
    ) -> Result<(), IndexerStorageError> {
        let finished = std::mem::take(&mut self.finished);
        let changed = std::mem::take(&mut self.changed);
        if let Some(storage) = &mut self.storage {
            storage
                .commit(BlockCommit {
                    block_height,
                    processed,
                    changed: changed.iter().map(|hash| &self.trees[hash]).collect(),
                    finished: &finished,
                })
                .await
                .map_err(IndexerStorageError::Storage)?;
        }
        for hash in finished.iter() {
            self.trees.remove(hash);
        }
        self.last_block = Some(block_height);
        if processed {
            self.last_processed_block = Some(block_height);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum IndexerStorageError {
    Storage(Box<dyn Error + Send + Sync>),
    /// [`crate::BlockRange::AutoContinue`] is used without a storage to keep the last processed
    /// block in.
    NoStorage,
    /// The storage has blocks that the indexer hasn't processed, so their receipts would be in the
    /// trees twice.
    Ahead {
        last_block: BlockHeight,
        next_block: BlockHeight,
    },
}

impl Display for IndexerStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexerStorageError::Storage(err) => write!(f, "Storage error: {err}"),
            IndexerStorageError::NoStorage => write!(
                f,
                "BlockRange::AutoContinue needs IndexerOptions::storage to keep the last processed block in"
            ),
            IndexerStorageError::Ahead {
                last_block,
                next_block,
            } => write!(
                f,
                "The storage has read blocks up to {last_block}, but the indexer continues at block {next_block}. Clear it to start over"
            ),
        }
    }
}

impl Error for IndexerStorageError {}
