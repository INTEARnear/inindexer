use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
    sync::Arc,
};

use near_indexer_primitives::{
    CryptoHash, IndexerTransactionWithOutcome,
    types::{BlockHeight, BlockHeightDelta},
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{
    IndexerExecutionOutcomeWithReceipt, StreamerMessage, near_utils::is_receipt_successful,
};

const TREES: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("trees");
const LAST_BLOCK: TableDefinition<(), BlockHeight> = TableDefinition::new("last_block");

#[derive(Debug, Clone)]
pub struct TransactionTrees {
    pub storage: TransactionTreesStorage,
    /// When the trees start empty, this many blocks before the first block to process are read
    /// into them without calling the indexer, so that receipts of transactions from just before
    /// it have their trees too. A receipt of a transaction from before the blocks the trees have
    /// read panics, since it has no tree.
    pub warm_up_blocks: BlockHeightDelta,
}

impl Default for TransactionTrees {
    fn default() -> Self {
        Self {
            storage: TransactionTreesStorage::Memory,
            warm_up_blocks: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionTreesStorage {
    /// The trees are gone when the indexer stops, so they start from the warm-up again when it
    /// restarts.
    Memory,
    /// A redb file, created if it doesn't exist, so that the trees continue with
    /// [`crate::BlockRange::AutoContinue`]. Only one process can have it open.
    Redb(PathBuf),
}

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

/// The trees, and the file they're committed to after every block, if they're kept in one.
pub(crate) struct TransactionTreeStore {
    options: TransactionTrees,
    database: Option<Arc<Database>>,
    trees: HashMap<CryptoHash, TransactionTree>,
    /// Trees that the current block changed and didn't finish, in the order it changed them.
    changed: Vec<CryptoHash>,
    /// Trees that the current block finished, in the order it changed them.
    finished: Vec<CryptoHash>,
    /// The last block whose changes are committed.
    last_block: Option<BlockHeight>,
}

impl TransactionTreeStore {
    pub(crate) async fn open(options: TransactionTrees) -> Result<Self, TransactionTreesError> {
        let mut store = Self {
            options,
            database: None,
            trees: HashMap::new(),
            changed: Vec::new(),
            finished: Vec::new(),
            last_block: None,
        };
        let TransactionTreesStorage::Redb(path) = &store.options.storage else {
            return Ok(store);
        };
        let path = path.clone();
        let (database, last_block, stored_trees) =
            tokio::task::spawn_blocking(move || -> Result<_, redb::Error> {
                let database = Database::create(path)?;
                let write = database.begin_write()?;
                write.open_table(TREES)?;
                write.open_table(LAST_BLOCK)?;
                write.commit()?;
                let read = database.begin_read()?;
                let last_block = read
                    .open_table(LAST_BLOCK)?
                    .get(())?
                    .map(|height| height.value());
                let stored_trees = read
                    .open_table(TREES)?
                    .iter()?
                    .map(|row| row.map(|(_, tree)| tree.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()?;
                drop(read);
                Ok((database, last_block, stored_trees))
            })
            .await
            .expect("Opening transaction trees panicked")
            .map_err(TransactionTreesError::Database)?;
        for stored_tree in stored_trees {
            let tree: TransactionTree = serde_json::from_slice(&stored_tree)
                .map_err(TransactionTreesError::FailedToParse)?;
            store.trees.insert(tree.transaction.transaction.hash, tree);
        }
        store.database = Some(Arc::new(database));
        store.last_block = last_block;
        log::info!(target: "inindexer::transaction_trees", "Loaded {} transaction trees, last block: {last_block:?}", store.trees.len());
        Ok(store)
    }

    pub(crate) fn options(&self) -> &TransactionTrees {
        &self.options
    }

    pub(crate) fn last_block(&self) -> Option<BlockHeight> {
        self.last_block
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

    /// Removes the trees that the block finished, and writes the block's changes to the file, if
    /// there is one.
    pub(crate) async fn commit(
        &mut self,
        block_height: BlockHeight,
    ) -> Result<(), TransactionTreesError> {
        let finished = std::mem::take(&mut self.finished);
        for hash in finished.iter() {
            self.trees.remove(hash);
        }
        let changed = std::mem::take(&mut self.changed);
        if let Some(database) = &self.database {
            let changed_trees: Vec<_> = changed
                .iter()
                .filter_map(|hash| self.trees.get(hash))
                .map(|tree| {
                    (
                        tree.transaction.transaction.hash.0,
                        serde_json::to_vec(tree).expect("Transaction trees are serializable"),
                    )
                })
                .collect();
            let database = Arc::clone(database);
            tokio::task::spawn_blocking(move || -> Result<(), redb::Error> {
                let write = database.begin_write()?;
                {
                    let mut trees = write.open_table(TREES)?;
                    for hash in finished.iter() {
                        trees.remove(hash.0)?;
                    }
                    for (hash, tree) in changed_trees.iter() {
                        trees.insert(hash, tree.as_slice())?;
                    }
                    write.open_table(LAST_BLOCK)?.insert((), block_height)?;
                }
                write.commit()?;
                Ok(())
            })
            .await
            .expect("Committing transaction trees panicked")
            .map_err(TransactionTreesError::Database)?;
        }
        self.last_block = Some(block_height);
        Ok(())
    }
}

#[derive(Debug)]
pub enum TransactionTreesError {
    Database(redb::Error),
    FailedToParse(serde_json::Error),
    /// The file has blocks that the indexer hasn't processed, so their receipts would be in the
    /// trees twice.
    Ahead {
        last_block: BlockHeight,
        next_block: BlockHeight,
    },
}

impl Display for TransactionTreesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionTreesError::Database(err) => write!(f, "Database error: {err}"),
            TransactionTreesError::FailedToParse(err) => {
                write!(f, "Failed to parse a transaction tree: {err}")
            }
            TransactionTreesError::Ahead {
                last_block,
                next_block,
            } => write!(
                f,
                "The transaction trees file has blocks up to {last_block}, but the indexer continues at block {next_block}. Delete the file to start new trees"
            ),
        }
    }
}

impl std::error::Error for TransactionTreesError {}
