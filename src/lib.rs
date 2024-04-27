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
//!   transactions, or transactions with all receipts included, at a cost of some preprocessing overhead (around 1-2ms
//!   in release mode with 80-100 TPS on Slime's PC, this can be disabled in [`IndexerOptions::preprocess_transactions`]).
//! - Retries, performance warnings, skipped blocks handling, and other features are built-in, so you can focus on
//!   your indexer logic.
//! - Auto-Continue: the indexer will save the last processed block height to the file and continue from it
//!   on the next run. Includes a Ctrl+C handler for graceful shutdown.
//! - Some helper functions and types for working with logs, balances, and other commonly used functionality in
//!   [`indexer_utils`].
//!
//! This crate only works with tokio runtime.

#[cfg(feature = "fastnear-data-server")]
pub mod fastnear_data_server;
mod indexer_state;
pub mod indexer_utils;
#[cfg(feature = "lake")]
pub mod lake;
#[cfg(feature = "message-provider")]
pub mod message_provider;

use std::{
    fmt::{Debug, Display},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use indexer_state::{InIndexerError, IndexerState};
use indexer_utils::MAINNET_GENESIS_BLOCK_HEIGHT;
pub use near_indexer_primitives;
use near_indexer_primitives::{
    types::{BlockHeight, BlockHeightDelta},
    views::ExecutionStatusView,
    IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
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

    async fn on_transaction(&self, _transaction: &CompletedTransaction) -> Result<(), Self::Error> {
        Ok(())
    }
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
    options: IndexerOptions,
) -> Result<(), InIndexerError<S::Error>> {
    let mut indexer_state = IndexerState::new();

    let (mut range, mut ctrl_c_channel, post_processor): (_, _, Option<Box<dyn PostProcessor>>) =
        match options.range {
            BlockIterator::Iterator(range) => (range, None, None),
            BlockIterator::AutoContinue(auto_continue) if auto_continue.ctrl_c_handler => (
                Box::new(auto_continue.get_start_block().await..)
                    as Box<dyn Iterator<Item = BlockHeight> + Send>,
                Some(mpsc::channel::<()>(1)),
                Some(Box::new(auto_continue)),
            ),
            BlockIterator::AutoContinue(auto_continue) => (
                Box::new(auto_continue.get_start_block().await..)
                    as Box<dyn Iterator<Item = BlockHeight> + Send>,
                None,
                Some(Box::new(auto_continue)),
            ),
            BlockIterator::Custom(range, post_processor) => (range, None, Some(post_processor)),
        };

    let start_block_height = if let Some(first) = range.next() {
        first
    } else {
        return Ok(());
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

    let current_block = Arc::new(AtomicU64::new(0));
    let current_block_2 = Arc::clone(&current_block);
    let is_stopping = Arc::new(AtomicBool::new(false));
    let is_stopping_2 = Arc::clone(&is_stopping);

    let prefetch_range = (start_block_height.max(options.genesis_block_height)
        - prefetch_blocks as BlockHeightDelta)..start_block_height;
    let first_block_to_process = std::iter::once(start_block_height);
    let postfetch_iter = std::iter::repeat_with(move || {
        if !is_stopping_2.swap(true, Ordering::Relaxed) {
            log::info!("Stopped processing new transactions, waiting for receipts of the current transactions to complete");
        }
        current_block_2.fetch_add(1, Ordering::Relaxed) + 1
    })
    .take(postfetch_blocks);

    let range = prefetch_range
        .clone()
        .chain(first_block_to_process)
        .chain(range.inspect(move |x| {
            current_block.store(*x, Ordering::Relaxed);
        }))
        .chain(postfetch_iter);

    let (handle, mut streamer) = streamer
        .stream(range)
        .await
        .map_err(InIndexerError::Streamer)?;
    if let Some(ctrl_c_channel) = &mut ctrl_c_channel {
        let ctrl_c_channel = ctrl_c_channel.0.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = ctrl_c_channel.send(()).await;
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

    indexer_state.on_start(&indexer);

    while let Some(message) = streamer.recv().await {
        let is_stopping = is_stopping.load(Ordering::Relaxed);
        let processing_options = BlockProcessingOptions {
            height: message.block.header.height,
            preprocess: options.preprocess_transactions.is_some(),
            preprocess_insert_new: !is_stopping,
            handle_by_indexer: !prefetch_range.contains(&message.block.header.height),
        };

        if message.block.header.height == start_block_height {
            log::info!("Prefetched all blocks successfully, starting to process new blocks");
        }

        match indexer_state
            .process_block(&indexer, &message, &processing_options)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                log::error!(
                    "Error processing block {height}: {e}",
                    height = message.block.header.height
                );
                if options.stop_on_error {
                    break;
                } else {
                    continue;
                }
            }
        }
        if let Some(post_processor) = &post_processor {
            post_processor
                .after_block(&message, processing_options, is_stopping)
                .await
                .map_err(InIndexerError::PostProcessor)?;
        }
        if let Some(ctrl_c_channel) = &mut ctrl_c_channel {
            if ctrl_c_channel.1.try_recv().is_ok() {
                log::info!(
                    "Received Ctrl+C signal, stopping after block {} is fully processed",
                    message.block.header.height
                );
                break;
            }
        }
    }

    drop(streamer);
    indexer_state.on_end(&indexer);
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
    pub range: BlockIterator,
    /// If enabled, the indexer will preprocess transactions and receipts, so you can access them in the
    /// [`Indexer::on_transaction`] method. If you don't need this, set to false to save some memory. If disabled,
    /// you can still access transactions and receipts in the [`Indexer::process_transaction`] and
    /// [`Indexer::process_receipt`] methods, but `on_transaction` will not be called.
    pub preprocess_transactions: Option<PreprocessTransactionsSettings>,
    /// Genesis block height, used to limit [`PreprocessTransactionsSettings::prefetch_blocks`] so the indexer
    /// doesn't try to query blocks lower than genesis. Default is [`MAINNET_GENESIS_BLOCK_HEIGHT`]
    pub genesis_block_height: BlockHeight,
}

pub enum BlockIterator {
    /// Custom range or iterator of blocks to process. The iterator can be finite or infinite. If it's finite,
    /// the indexer will stop once the iterator is exhausted.
    Iterator(Box<dyn Iterator<Item = BlockHeight> + Send>),
    /// If set, the indexer will save the last processed block height to the file and continue from it
    /// on the next run. If the indexer was forcibly stopped in the middle of processing a block, it will
    /// start from the beginning of the block, potentially processing some transactions twice.
    /// Doesn't work with [`postfetch_blocks`](PreprocessTransactionsSettings::postfetch_blocks) option
    /// because this stream is infinite.
    AutoContinue(AutoContinue),
    Custom(
        Box<dyn Iterator<Item = BlockHeight> + Send>,
        Box<dyn PostProcessor>,
    ),
}

impl BlockIterator {
    pub fn iterator(range: impl Iterator<Item = BlockHeight> + Send + 'static) -> Self {
        BlockIterator::Iterator(Box::new(range))
    }
}

impl Debug for BlockIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockIterator::Iterator(_) => write!(f, "Iterator"),
            BlockIterator::AutoContinue(auto_continue) => {
                write!(f, "AutoContinue({:?})", auto_continue)
            }
            BlockIterator::Custom(_, _) => write!(f, "Custom"),
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

impl Default for IndexerOptions {
    fn default() -> Self {
        Self {
            stop_on_error: false,
            range: BlockIterator::Iterator(Box::new(std::iter::once_with(|| {
                panic!("Range is not set in IndexerOptions")
            }))),
            preprocess_transactions: Some(PreprocessTransactionsSettings::default()),
            genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
        }
    }
}

#[derive(Debug)]
pub struct AutoContinue {
    /// Path to the file where the last processed block height will be saved. It's a simple text file with
    /// a single number.
    pub save_file: PathBuf,
    /// If the save file does not exist, the indexer will start from this height. Default is the mainnet
    /// genesis block height.
    pub start_height_if_does_not_exist: BlockHeight,
    /// If true, the indexer will gracefully stop on Ctrl+C signal, avoiding double processing of transactions.
    /// Transactions that have started, but not finished processing, will be processed again on the next run.
    pub ctrl_c_handler: bool,
}

impl AutoContinue {
    pub async fn get_start_block(&self) -> BlockHeight {
        if !tokio::fs::try_exists(&self.save_file)
            .await
            .expect("Failed to check if save file exists")
        {
            log::info!(
                "Save file does not exist, starting from block {}",
                self.start_height_if_does_not_exist
            );
            return self.start_height_if_does_not_exist;
        }
        let contents = tokio::fs::read_to_string(&self.save_file)
            .await
            .expect("Failed to read save file");
        contents
            .trim()
            .replace([',', ' ', '.'], "")
            .parse()
            .expect("Failed to parse save file contents")
    }
}

impl Default for AutoContinue {
    fn default() -> Self {
        Self {
            save_file: PathBuf::from("last-processed-block.txt"),
            start_height_if_does_not_exist: MAINNET_GENESIS_BLOCK_HEIGHT,
            ctrl_c_handler: true,
        }
    }
}

#[async_trait]
pub trait PostProcessor {
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
        // This is not actually needed, since AutoContinue is an infinite stream, but just in case
        // someone decides to use this as a postprocessor with a finite stream, it won't save postfetch
        // blocks
        if !stopping {
            Ok(tokio::fs::write(&self.save_file, block.block.header.height.to_string()).await?)
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
    pub preprocess_insert_new: bool,
    /// If true, the indexer will handle this block. If false, the indexer will only use this block
    /// to preprocess transactions.
    pub handle_by_indexer: bool,
}
