mod indexer_state;
#[cfg(test)]
mod indexer_tests;
pub mod message_provider;
pub mod multiindexer;
pub mod near_utils;
pub mod neardata;
pub mod neardata_old;

use std::{collections::HashMap, fmt::Debug, ops::Range, path::PathBuf};

use async_trait::async_trait;
use indexer_state::{InIndexerError, IndexerState};
pub use near_indexer_primitives;
use near_indexer_primitives::{
    types::{BlockHeight, BlockHeightDelta},
    views::ExecutionStatusView,
    CryptoHash, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
};
use near_utils::{is_receipt_successful, MAINNET_GENESIS_BLOCK_HEIGHT};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

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

    /// Runs for every transaction when it starts (not when it's complete, see `on_transaction`).
    async fn process_transaction(
        &mut self,
        _transaction: &IndexerTransactionWithOutcome,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every receipt, when it's executed.
    async fn process_receipt(
        &mut self,
        _receipt: &IndexerExecutionOutcomeWithReceipt,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every transaction, when it's fully complete and all its receipts are executed.
    /// Requires preprocessing enabled in `IndexerOptions`.
    async fn on_transaction(
        &mut self,
        _transaction: &CompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every receipt, when it's executed. Also contains information about the parent
    /// transaction and previous receipts. Requires preprocessing enabled in `IndexerOptions`.
    async fn on_receipt(
        &mut self,
        _receipt: &TransactionReceipt,
        _transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Runs for every block, after all on_transaction, on_receipt, and other callbacks are
    /// called.
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompleteTransaction {
    pub transaction: IndexerTransactionWithOutcome,
    pub receipts: Vec<TransactionReceipt>,
}

impl CompleteTransaction {
    pub fn all_receipts_successful(&self) -> bool {
        self.receipts.iter().all(|receipt| {
            matches!(
                receipt.receipt.execution_outcome.outcome.status,
                ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_)
            )
        })
    }
}

#[derive(Debug)]
pub struct IncompleteTransaction {
    pub transaction: IndexerTransactionWithOutcome,
    /// Receipts with None are created by a transaction or another receipt, but are not yet available.
    ///
    /// This map does not contain all receipts of a transaction, since this on_receipt is called
    /// before the transaction is fully complete, so there's no way to know how many receipts there will be.
    ///
    /// During on_receipt, the receipt you're processing is None in this map.
    pub receipts: HashMap<CryptoHash, Option<TransactionReceipt>>,
}

impl TryFrom<&IncompleteTransaction> for CompleteTransaction {
    type Error = &'static str;

    fn try_from(value: &IncompleteTransaction) -> Result<Self, Self::Error> {
        let receipts = value
            .receipts
            .values()
            .map(|receipt| receipt.clone().ok_or("Missing receipt"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            transaction: value.transaction.clone(),
            receipts,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionReceipt {
    pub receipt: IndexerExecutionOutcomeWithReceipt,
    pub block_height: BlockHeight,
    #[serde(with = "near_utils::dec_format")]
    pub block_timestamp_nanosec: u128,
}

impl TransactionReceipt {
    pub fn is_successful(&self, if_unknown: bool) -> bool {
        is_receipt_successful(&self.receipt).unwrap_or(if_unknown)
    }
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
    let ((start_block_height, end_block_height), post_processor): (
        _,
        Option<Box<dyn PostProcessor>>,
    ) = match options.range {
        BlockRange::Range {
            start_inclusive: start,
            end_exclusive: end,
        } => ((start, end), None),
        BlockRange::AutoContinue(auto_continue) => {
            let range = auto_continue.range().await;
            (
                (
                    range.start,
                    if range.end == BlockHeight::MAX {
                        None
                    } else {
                        Some(range.end)
                    },
                ),
                Some(Box::new(auto_continue)),
            )
        }
    };

    let (prefetch_blocks, postfetch_blocks) =
        if let Some(preprocess_transactions) = &options.preprocess_transactions {
            (
                preprocess_transactions.prefetch_blocks,
                preprocess_transactions.postfetch_blocks,
            )
        } else {
            (0, 0)
        };

    let prefetch_range = ((start_block_height - prefetch_blocks as BlockHeightDelta)
        .max(options.genesis_block_height))..start_block_height;
    let postfetch_range = end_block_height.unwrap_or(BlockHeight::MAX)
        ..(end_block_height.map_or(BlockHeight::MAX, |x| {
            x + postfetch_blocks as BlockHeightDelta
        }));

    let start_block_height_with_prefetch =
        start_block_height.saturating_sub(prefetch_blocks as BlockHeightDelta);
    let end_block_height_with_postfetch =
        end_block_height.map(|x| x + postfetch_blocks as BlockHeightDelta);

    if options.ctrl_c_handler {
        let cancellation_token_2 = cancellation_token.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            cancellation_token_2.cancel();
        });
    }

    if !prefetch_range.is_empty() {
        log::info!(
            "Prefetching transactions from blocks {} to {}. Only transactions that are completed from {} onwards will be processed",
            prefetch_range.start,
            prefetch_range.end - 1,
            start_block_height
        );
    }

    let (handle, mut streamer) = streamer
        .stream(
            start_block_height_with_prefetch,
            end_block_height_with_postfetch,
        )
        .await
        .map_err(InIndexerError::Streamer)?;

    indexer_state.on_start(indexer);

    let mut has_sent_postfetch_message = false;
    while let Some(message) = streamer.recv().await {
        if postfetch_range.contains(&message.block.header.height) && !has_sent_postfetch_message {
            has_sent_postfetch_message = true;
            log::info!("Stopped processing new transactions, waiting for receipts of the current transactions to complete");
        }
        let processing_options = BlockProcessingOptions {
            height: message.block.header.height,
            preprocess: options.preprocess_transactions.is_some(),
            preprocess_new_transactions: !postfetch_range.contains(&message.block.header.height),
            handle_raw_events: !postfetch_range.contains(&message.block.header.height)
                && !prefetch_range.contains(&message.block.header.height),
            handle_preprocessed_transactions_by_indexer: !prefetch_range
                .contains(&message.block.header.height),
        };

        if message.block.header.height == start_block_height {
            log::info!("Prefetched all blocks successfully, starting to process new blocks");
        }

        match indexer_state
            .process_block(indexer, &message, &processing_options)
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
                } else {
                    continue;
                }
            }
        }
        if !prefetch_range.contains(&message.block.header.height) {
            if let Some(post_processor) = &post_processor {
                post_processor
                    .after_block(
                        &message,
                        processing_options,
                        postfetch_range.contains(&message.block.header.height),
                    )
                    .await
                    .map_err(InIndexerError::PostProcessor)?;
            }
        }
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
    /// If enabled, the indexer will preprocess transactions and receipts, so you can access them in the
    /// [`Indexer::on_transaction`] and [`Indexer::on_receipt`] methods. If you don't need this, set to
    /// false to save some memory. If disabled, you can still access transactions and receipts in the
    /// [`Indexer::process_transaction`] and [`Indexer::process_receipt`] methods, but `on_transaction`
    /// and `on_receipt` will not be called.
    pub preprocess_transactions: Option<PreprocessTransactionsSettings>,
    /// Genesis block height, used to limit [`PreprocessTransactionsSettings::prefetch_blocks`] so the indexer
    /// doesn't try to query blocks lower than genesis. Default is [`MAINNET_GENESIS_BLOCK_HEIGHT`]
    pub genesis_block_height: BlockHeight,
    /// If true, the indexer will gracefully stop on Ctrl+C signal, avoiding double processing of transactions.
    /// Transactions that have started, but not finished processing, will be processed again on the next run.
    pub ctrl_c_handler: bool,
}

pub enum BlockRange {
    /// Consecutive range of blocks to process. The range can be finite or infinite. If it's finite,
    /// the indexer will stop once the iterator is exhausted.
    Range {
        start_inclusive: BlockHeight,
        end_exclusive: Option<BlockHeight>,
    },
    /// If set, the indexer will save the last processed block height to the file and continue from it
    /// on the next run. If the indexer was forcibly stopped in the middle of processing a block, it will
    /// start from the beginning of the block, potentially processing some transactions twice.
    /// Doesn't work with [`postfetch_blocks`](PreprocessTransactionsSettings::postfetch_blocks) option
    /// because this stream is infinite.
    AutoContinue(AutoContinue),
}

impl Debug for BlockRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockRange::Range {
                start_inclusive: start,
                end_exclusive: end,
            } => write!(f, "Range {{ start: {}, end: {:?} }}", start, end),
            BlockRange::AutoContinue(_) => write!(f, "AutoContinue"),
        }
    }
}

#[derive(Debug)]
pub struct PreprocessTransactionsSettings {
    /// When transactions are preprocessed, the indexer needs to get a transaction and wait for all receipts
    /// to be available. In case some transactions were received during the previous run, and they are not available
    /// now, this parameter sets how many blocks the indexer will prefetch to get all previous transactions.
    /// Default is 100.
    pub prefetch_blocks: usize,
    /// If your block iterator is finite, you can set this parameter to load some blocks after the stream is
    /// exhausted, so that if a transaction was initiated within the iterable range, but receipts are available later,
    /// the indexer will still process them, but it won't touch transaction that were initiated after the last block
    /// in the iterable range. Default is 100.
    pub postfetch_blocks: usize,
}

impl Default for PreprocessTransactionsSettings {
    fn default() -> Self {
        Self {
            prefetch_blocks: 100,
            postfetch_blocks: 100,
        }
    }
}

impl IndexerOptions {
    pub fn default_with_range(range: BlockRange) -> Self {
        Self {
            stop_on_error: false,
            range,
            preprocess_transactions: None,
            genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
            ctrl_c_handler: true,
        }
    }
}

pub struct AutoContinue {
    /// Path to the file where the last processed block height will be saved. It's a simple text file with
    /// a single number.
    pub save_location: Box<dyn SaveLocation>,
    /// If the save file does not exist, the indexer will start from this height. Default is the mainnet
    /// genesis block height.
    pub start_height_if_does_not_exist: BlockHeight,
    /// If set, the indexer will stop processing blocks after this height. If None, the indexer will process
    /// blocks infinitely.
    pub end: AutoContinueEnd,
}

pub enum AutoContinueEnd {
    /// The indexer will stop processing blocks after this height.
    Height(BlockHeight),
    /// The indexer will process this many blocks and then stop.
    Count(BlockHeightDelta),
    /// The indexer will process blocks infinitely.
    Infinite,
}

impl AutoContinue {
    pub async fn get_start_block(&self) -> BlockHeight {
        self.save_location
            .load()
            .await
            .unwrap_or(self.start_height_if_does_not_exist)
    }

    pub async fn range(&self) -> Range<BlockHeight> {
        let start = self.get_start_block().await;
        let end = match self.end {
            AutoContinueEnd::Height(height) => height,
            AutoContinueEnd::Count(count) => start + count,
            AutoContinueEnd::Infinite => BlockHeight::MAX,
        };
        start..end
    }
}

impl Default for AutoContinue {
    fn default() -> Self {
        Self {
            save_location: Box::new(PathBuf::from("last-processed-block.txt")),
            start_height_if_does_not_exist: MAINNET_GENESIS_BLOCK_HEIGHT,
            end: AutoContinueEnd::Infinite,
        }
    }
}

#[async_trait]
pub trait SaveLocation: Send + Sync {
    async fn load(&self) -> Option<BlockHeight>;

    async fn save(&self, height: BlockHeight) -> Result<(), Box<dyn std::error::Error>>;
}

#[async_trait]
impl<T> SaveLocation for T
where
    for<'a> &'a T: Into<PathBuf>,
    T: Send + Sync,
{
    async fn load(&self) -> Option<BlockHeight> {
        let path = self.into();
        if !tokio::fs::try_exists(&path)
            .await
            .expect("Failed to check if save file exists")
        {
            return None;
        }
        let contents = tokio::fs::read_to_string(&path)
            .await
            .expect("Failed to read save file");
        Some(
            contents
                .trim()
                .replace([',', ' ', '.'], "")
                .parse()
                .expect("Failed to parse save file contents"),
        )
    }

    async fn save(&self, height: BlockHeight) -> Result<(), Box<dyn std::error::Error>> {
        tokio::fs::write(self.into(), height.to_string()).await?;
        Ok(())
    }
}

#[async_trait]
pub trait PostProcessor: Send {
    async fn after_block(
        &self,
        block: &StreamerMessage,
        options: BlockProcessingOptions,
        stopping: bool,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[async_trait]
impl PostProcessor for AutoContinue {
    async fn after_block(
        &self,
        block: &StreamerMessage,
        _options: BlockProcessingOptions,
        stopping: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // This check is not actually needed, since AutoContinue is an infinite stream, but just in case
        // someone decides to use this as a postprocessor with a finite stream, it won't save postfetch
        // blocks
        if !stopping {
            // +1 because processing will start from the next block inclusive
            self.save_location.save(block.block.header.height + 1).await
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct BlockProcessingOptions {
    /// Block height to process.
    pub height: BlockHeight,
    /// If true, the indexer will preprocess this transaction. Only works if
    /// [`IndexerOptions::preprocess_transactions`] is enabled.
    pub preprocess: bool,
    /// If [`preprocess`](BlockProcessingOptions::preprocess) is true, but this is false, the indexer will no
    /// longer insert new transactions into the indexer state, but will still check if new receipts belong to
    /// a transaction that was saved while this flag was `true`.
    pub preprocess_new_transactions: bool,
    /// If true, the indexer will handle this block (methods `process_block`, `process_transaction`, `process_receipt`)
    pub handle_raw_events: bool,
    /// If true, the indexer will handle preprocessed transactions that completed this block (methods `on_transaction`,
    /// `on_receipt`)
    pub handle_preprocessed_transactions_by_indexer: bool,
}
