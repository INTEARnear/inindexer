use std::fmt::{Debug, Display};
use std::{collections::HashMap, time::Duration};

use dashmap::DashMap;
use near_indexer_primitives::types::BlockHeightDelta;
use near_indexer_primitives::{
    CryptoHash, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
};

use crate::{BlockProcessingOptions, CompletedTransaction, Indexer};

const BLOCK_PROCESSING_WARNING_THRESHOLD: Duration = Duration::from_millis(300);
const PERFORMANCE_REPORT_EVERY_BLOCKS: BlockHeightDelta = 5000;

#[derive(Debug)]
pub(crate) struct IndexerState {
    pending_transactions: DashMap<CryptoHash, IncompleteTransaction>,
    receipt_id_to_transaction: DashMap<CryptoHash, CryptoHash>,
    blocks_received: BlockHeightDelta,
    time_spent: Duration,
}

#[derive(Debug)]
struct IncompleteTransaction {
    transaction: IndexerTransactionWithOutcome,
    receipts: HashMap<CryptoHash, Option<IndexerExecutionOutcomeWithReceipt>>,
}

impl TryFrom<&IncompleteTransaction> for CompletedTransaction {
    type Error = &'static str;

    fn try_from(value: &IncompleteTransaction) -> Result<Self, Self::Error> {
        let receipts = value
            .receipts
            .values()
            .map(|receipt| {
                receipt.clone().ok_or("Missing receipt").map(|receipt| {
                    IndexerExecutionOutcomeWithReceipt {
                        execution_outcome: receipt.execution_outcome,
                        receipt: receipt.receipt,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            transaction: value.transaction.clone(),
            receipts,
        })
    }
}

impl IndexerState {
    pub fn new() -> Self {
        Self {
            pending_transactions: DashMap::new(),
            receipt_id_to_transaction: DashMap::new(),
            blocks_received: 0,
            time_spent: Duration::ZERO,
        }
    }

    pub fn on_start<I: Indexer>(&mut self, _indexer: &I) {}

    pub fn on_end<I: Indexer>(&mut self, _indexer: &I) {
        self.report_performance();
    }

    pub fn report_performance(&self) {
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
        indexer: &I,
        message: &StreamerMessage,
        options: &BlockProcessingOptions,
    ) -> Result<(), I::Error> {
        self.blocks_received += 1;
        if self.blocks_received % PERFORMANCE_REPORT_EVERY_BLOCKS == 0 {
            self.report_performance();
        }

        let started = std::time::Instant::now();

        if options.handle_by_indexer {
            indexer.process_block(message).await?;
        }

        for chunk in message
            .shards
            .iter()
            .filter_map(|shard| shard.chunk.as_ref())
        {
            for transaction in chunk.transactions.iter() {
                if options.preprocess {
                    for receipt_id in transaction
                        .outcome
                        .execution_outcome
                        .outcome
                        .receipt_ids
                        .iter()
                    {
                        self.receipt_id_to_transaction
                            .insert(*receipt_id, transaction.transaction.hash);
                    }
                    self.pending_transactions.insert(
                        transaction.transaction.hash,
                        IncompleteTransaction {
                            transaction: transaction.clone(),
                            receipts: HashMap::from_iter(
                                transaction
                                    .outcome
                                    .execution_outcome
                                    .outcome
                                    .receipt_ids
                                    .iter()
                                    .map(|receipt_id| (*receipt_id, None)),
                            ),
                        },
                    );
                }

                if options.handle_by_indexer {
                    indexer.process_transaction(transaction).await?;
                }
            }
        }

        for shard in message.shards.iter() {
            for receipt in shard.receipt_execution_outcomes.iter() {
                if let Some((tx_id, _)) = self
                    .receipt_id_to_transaction
                    .remove(&receipt.receipt.receipt_id)
                {
                    if options.preprocess {
                        if let Some(mut incomplete_transaction) =
                            self.pending_transactions.get_mut(&tx_id)
                        {
                            incomplete_transaction
                                .receipts
                                .insert(receipt.receipt.receipt_id, Some(receipt.clone()));
                            for new_receipt_id in
                                receipt.execution_outcome.outcome.receipt_ids.iter()
                            {
                                self.receipt_id_to_transaction
                                    .insert(*new_receipt_id, tx_id);
                                incomplete_transaction
                                    .receipts
                                    .insert(*new_receipt_id, None);
                            }

                            if let Ok(completed_transaction) =
                                CompletedTransaction::try_from(&*incomplete_transaction)
                            {
                                self.pending_transactions.remove(&tx_id);
                                if options.handle_by_indexer {
                                    indexer.on_transaction(&completed_transaction).await?;
                                }
                            }
                        }
                    }
                }

                if options.handle_by_indexer {
                    indexer.process_receipt(receipt).await?;
                }
            }
        }

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
    PostProcessor(Box<dyn std::error::Error>),
}

impl<E: Debug> Display for InIndexerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InIndexerError::Streamer(e) => write!(f, "Streamer error: {e:?}"),
            InIndexerError::Join(e) => write!(f, "Join error: {e:?}"),
            InIndexerError::PostProcessor(e) => write!(f, "Post processor error: {e:?}"),
        }
    }
}

impl<E: Debug> std::error::Error for InIndexerError<E> {}