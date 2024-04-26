//! # InIndexer
//! InIndexer is a library that simplifies building near indexers.
//!
//! ## Features
//!
//! - Different sources of near data: [neardata-server](https://github.com/fastnear/neardata-server) (implemented),
//!   [AWS Lake](https://docs.near.org/concepts/advanced/near-lake-framework) (only consecutive ascending ranges
//!   are supported), local file storage for backfilling (planned), you can add your own sources by implementing
//!   [`MessageStreamer`] or [`message_provider::MessageProvider`] trait.
//! - Simple indexer interface: you only need to implement [`Indexer`] trait and handle receipts, blocks,
//!   transactions, or transactions with all receipts included, at a cost of some preprocessing overhead.
//! - Retries, performance warnings, skipped blocks handling, and other features are built-in, so you can focus on
//!   your indexer logic.

#[cfg(feature = "fastnear-data-server")]
pub mod fastnear_data_server;
#[cfg(feature = "lake")]
pub mod lake;
#[cfg(feature = "message-provider")]
pub mod message_provider;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    time::Duration,
};

use async_trait::async_trait;
pub use near_indexer_primitives;
use near_indexer_primitives::{
    types::BlockHeight, views::ExecutionStatusView, CryptoHash, IndexerExecutionOutcomeWithReceipt,
    IndexerTransactionWithOutcome, StreamerMessage,
};
use tokio::{sync::mpsc, task::JoinHandle};

#[async_trait]
pub trait MessageStreamer {
    type Error;

    async fn stream(
        self,
        range: impl Iterator<Item = BlockHeight> + Send + 'static,
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
    type Error: Display + Debug + Send + Sync + 'static;

    async fn process_block(&self, _block: &StreamerMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn process_transaction(
        &self,
        _transaction: &IndexerTransactionWithOutcome,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn process_receipt(
        &self,
        _receipt: &IndexerExecutionOutcomeWithReceipt,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn on_transaction(&self, transaction: &CompletedTransaction) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct CompletedTransaction {
    pub transaction: IndexerTransactionWithOutcome,
    pub receipts: Vec<IndexerExecutionOutcomeWithReceipt>,
}

impl CompletedTransaction {
    pub fn all_receipts_successful(&self) -> bool {
        self.receipts.iter().all(|receipt| {
            matches!(
                receipt.execution_outcome.outcome.status,
                ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_)
            )
        })
    }
}

pub async fn run_indexer<
    I: Indexer + Send + Sync + 'static,
    S: MessageStreamer + Send + Sync + 'static,
>(
    indexer: I,
    streamer: S,
    range: impl Iterator<Item = BlockHeight> + Send + 'static,
    stop_on_error: bool,
) -> Result<(), InIndexerError<S::Error>> {
    let (handle, mut streamer) = streamer
        .stream(range)
        .await
        .map_err(InIndexerError::StreamerError)?;
    let mut indexer_state = IndexerState::new();
    while let Some(message) = streamer.recv().await {
        match indexer_state.process_block(&indexer, &message).await {
            Ok(()) => {}
            Err(e) => {
                log::error!(
                    "Error processing block {height}: {e}",
                    height = message.block.header.height
                );
                if stop_on_error {
                    break;
                } else {
                    continue;
                }
            }
        }
    }
    Ok(handle
        .await
        .map_err(InIndexerError::JoinError)?
        .map_err(InIndexerError::StreamerError)?)
}

#[derive(Debug)]
struct IndexerState {
    pending_transactions: HashMap<CryptoHash, IncompleteTransaction>,
    receipt_id_to_transaction: HashMap<CryptoHash, CryptoHash>,
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
            .iter()
            .map(|(_receipt_id, receipt)| {
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
            pending_transactions: HashMap::new(),
            receipt_id_to_transaction: HashMap::new(),
        }
    }

    async fn process_block<I: Indexer>(
        &mut self,
        indexer: &I,
        message: &StreamerMessage,
    ) -> Result<(), I::Error> {
        const BLOCK_PROCESSING_WARNING_THRESHOLD: Duration = Duration::from_millis(300);

        let started = std::time::Instant::now();

        indexer.process_block(message).await?;

        for chunk in message
            .shards
            .iter()
            .filter_map(|shard| shard.chunk.as_ref())
        {
            for transaction in chunk.transactions.iter() {
                for receipt_id in transaction
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .iter()
                {
                    self.receipt_id_to_transaction
                        .insert(receipt_id.clone(), transaction.transaction.hash);
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
                                .map(|receipt_id| (receipt_id.clone(), None)),
                        ),
                    },
                );

                indexer.process_transaction(&transaction).await?;
            }
        }

        for shard in message.shards.iter() {
            for receipt in shard.receipt_execution_outcomes.iter() {
                if let Some(tx_id) = self
                    .receipt_id_to_transaction
                    .remove(&receipt.receipt.receipt_id)
                {
                    if let Some(incomplete_transaction) = self.pending_transactions.get_mut(&tx_id)
                    {
                        incomplete_transaction
                            .receipts
                            .insert(receipt.receipt.receipt_id, Some(receipt.clone()));
                        for new_receipt_id in receipt.execution_outcome.outcome.receipt_ids.iter() {
                            self.receipt_id_to_transaction
                                .insert(new_receipt_id.clone(), tx_id);
                            incomplete_transaction
                                .receipts
                                .insert(new_receipt_id.clone(), None);
                        }

                        if let Ok(completed_transaction) =
                            CompletedTransaction::try_from(&*incomplete_transaction)
                        {
                            self.pending_transactions.remove(&tx_id);
                            indexer.on_transaction(&completed_transaction).await?;
                        }
                    }
                }

                indexer.process_receipt(&receipt).await?;
            }
        }

        let elapsed = started.elapsed();
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
    StreamerError(E),
    JoinError(tokio::task::JoinError),
}

impl<E: Debug> Display for InIndexerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InIndexerError::StreamerError(e) => write!(f, "Streamer error: {e:?}"),
            InIndexerError::JoinError(e) => write!(f, "Join error: {e:?}"),
        }
    }
}

impl<E: Debug> std::error::Error for InIndexerError<E> {}

pub const GENESIS_BLOCK_HEIGHT: BlockHeight = 9820210;
