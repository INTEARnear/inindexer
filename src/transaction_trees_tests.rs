use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use async_trait::async_trait;
use near_indexer_primitives::{CryptoHash, types::BlockHeight};
use serde::Deserialize;
use temp_file::TempFile;

use crate::{
    AutoContinue, AutoContinueEnd, BlockRange, Indexer, IndexerExecutionOutcomeWithReceipt,
    IndexerOptions, StreamerMessage,
    indexer_state::InIndexerError,
    redb_storage::RedbStorage,
    run_indexer,
    storage::IndexerStorage,
    teardata::{TeardataError, TeardataProvider},
    transaction_trees::{IndexerStorageError, TransactionTree, TransactionTreeStore},
};

/// Checks every tree it gets, and logs them to compare runs.
#[derive(Default)]
struct TreeIndexer {
    log: Vec<(BlockHeight, String)>,
    heights: Vec<BlockHeight>,
    /// Transactions in the blocks the indexer processed, with their block.
    started: HashMap<CryptoHash, BlockHeight>,
    finished: HashSet<CryptoHash>,
    deepest_receipt: usize,
    /// Panics after this block is processed, like a crash after the indexer saved it.
    crash_after: Option<BlockHeight>,
}

#[async_trait]
impl Indexer for TreeIndexer {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        let height = block.block.header.height;
        self.heights.push(height);
        for chunk in block.shards.iter().filter_map(|shard| shard.chunk.as_ref()) {
            for transaction in chunk.transactions.iter() {
                self.started.insert(transaction.transaction.hash, height);
            }
        }
        Ok(())
    }

    async fn process_receipt(
        &mut self,
        receipt: &IndexerExecutionOutcomeWithReceipt,
        transaction: &TransactionTree,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let receipt_id = receipt.receipt.receipt_id;
        assert_eq!(transaction.transaction.transaction.hash, receipt.tx_hash);
        assert_eq!(
            transaction.receipt(&receipt_id).unwrap().block_height,
            block.block.header.height
        );
        let parent = transaction.parent(&receipt_id);
        let created_by = match parent {
            Some(parent) => &parent.receipt.execution_outcome.outcome,
            None => &transaction.transaction.outcome.execution_outcome.outcome,
        };
        assert!(created_by.receipt_ids.contains(&receipt_id));
        self.deepest_receipt = self
            .deepest_receipt
            .max(transaction.ancestors(&receipt_id).count());
        self.log.push((
            block.block.header.height,
            format!(
                "Receipt {receipt_id} of {}, parent {:?}, {} executed, {} pending",
                receipt.tx_hash,
                parent.map(|parent| parent.receipt.receipt.receipt_id),
                transaction.receipts.len(),
                transaction.pending_receipt_ids().count(),
            ),
        ));
        Ok(())
    }

    async fn process_transaction(
        &mut self,
        transaction: &TransactionTree,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let hash = transaction.transaction.transaction.hash;
        assert!(transaction.is_complete());
        assert!(self.finished.insert(hash), "{hash} processed twice");
        self.log.push((
            block.block.header.height,
            format!(
                "Transaction {hash} with {} receipts",
                transaction.receipts.len()
            ),
        ));
        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        let height = block.block.header.height;
        if self.crash_after == Some(height) {
            panic!("Crashing after block {height}");
        }
        Ok(())
    }
}

/// Enough for every receipt in the ranges below to have its transaction's tree.
const WARM_UP_BLOCKS: BlockHeight = 500;

#[derive(Deserialize)]
struct Status {
    final_height: BlockHeight,
}

/// Recent blocks, which teardata has in archives, and won't remove soon.
async fn recent_range(length: BlockHeight) -> Range<BlockHeight> {
    let body = reqwest::get("https://mainnet-teardata.intea.rs/v0/status")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let status: Status = serde_json::from_str(&body).unwrap();
    let start = status.final_height - 3000;
    start..start + length
}

async fn run(
    indexer: &mut TreeIndexer,
    range: BlockRange,
    storage: Option<Box<dyn IndexerStorage>>,
) -> Result<(), InIndexerError<TeardataError>> {
    run_indexer(
        indexer,
        TeardataProvider::mainnet(),
        IndexerOptions {
            stop_on_error: true,
            range,
            ctrl_c_handler: false,
            storage,
            warm_up_blocks: WARM_UP_BLOCKS,
        },
    )
    .await
}

fn in_file(file: &TempFile) -> Option<Box<dyn IndexerStorage>> {
    Some(Box::new(RedbStorage::new(file.path())))
}

/// Runs the range in a task that panics after `crash_after` is processed.
async fn crash(
    range: BlockRange,
    storage: Option<Box<dyn IndexerStorage>>,
    crash_after: BlockHeight,
) {
    let crashing_run = tokio::spawn(async move {
        let mut crashing = TreeIndexer {
            crash_after: Some(crash_after),
            ..Default::default()
        };
        run(&mut crashing, range, storage).await
    })
    .await;
    assert!(crashing_run.unwrap_err().is_panic());
}

fn auto_continue(start: BlockHeight, end: BlockHeight) -> BlockRange {
    BlockRange::AutoContinue(AutoContinue {
        start_height_if_none_processed: start,
        end: AutoContinueEnd::Height(end),
    })
}

/// Every transaction in the range, except for the last `settle_blocks` blocks, has to be
/// processed.
async fn check_trees_finish(range: Range<BlockHeight>, settle_blocks: BlockHeight) {
    let mut indexer = TreeIndexer::default();
    run(
        &mut indexer,
        BlockRange::Range {
            start_inclusive: range.start,
            end_exclusive: Some(range.end),
        },
        None,
    )
    .await
    .unwrap();

    let unfinished: Vec<_> = indexer
        .started
        .iter()
        .filter(|(hash, height)| {
            **height < range.end - settle_blocks && !indexer.finished.contains(hash)
        })
        .collect();
    assert!(unfinished.is_empty(), "{unfinished:?}");
    assert!(indexer.finished.len() > 100, "{}", indexer.finished.len());
    assert!(indexer.deepest_receipt >= 2, "{}", indexer.deepest_receipt);
}

#[tokio::test]
async fn trees_finish_on_early_mainnet() {
    check_trees_finish(50_000_000..50_001_500, 500).await;
}

#[tokio::test]
async fn trees_finish_on_recent_mainnet() {
    check_trees_finish(recent_range(1500).await, 500).await;
}

#[tokio::test]
#[should_panic(expected = "Increase IndexerOptions::warm_up_blocks")]
async fn receipt_without_tree_panics() {
    let range = recent_range(10).await;
    run_indexer(
        &mut TreeIndexer::default(),
        TeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: range.start,
            end_exclusive: Some(range.end),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn trees_continue_after_restarts_and_crashes() {
    let range = recent_range(600).await;
    let mut uninterrupted = TreeIndexer::default();
    run(
        &mut uninterrupted,
        BlockRange::Range {
            start_inclusive: range.start,
            end_exclusive: Some(range.end),
        },
        None,
    )
    .await
    .unwrap();
    let middle_index = uninterrupted.heights.len() / 2;
    let middle = uninterrupted.heights[middle_index];

    // Stopped in the middle, then continued from the last block the storage has as processed.
    let trees_file = temp_file::empty();
    let mut restarted = TreeIndexer::default();
    for end in [middle, range.end] {
        run(
            &mut restarted,
            auto_continue(range.start, end),
            in_file(&trees_file),
        )
        .await
        .unwrap();
    }
    assert_eq!(restarted.log, uninterrupted.log);

    // Crashed after the middle block was processed, before the storage committed it, so it runs
    // again.
    let trees_file = temp_file::empty();
    crash(
        auto_continue(range.start, range.end),
        in_file(&trees_file),
        middle,
    )
    .await;
    let trees_after_crash = TransactionTreeStore::open(in_file(&trees_file))
        .await
        .unwrap();
    let before_middle = uninterrupted.heights[middle_index - 1];
    assert_eq!(trees_after_crash.last_block(), Some(before_middle));
    assert_eq!(
        trees_after_crash.last_processed_block().unwrap(),
        Some(before_middle)
    );
    drop(trees_after_crash);
    let mut recovered = TreeIndexer::default();
    run(
        &mut recovered,
        auto_continue(range.start, range.end),
        in_file(&trees_file),
    )
    .await
    .unwrap();
    let from_middle: Vec<_> = uninterrupted
        .log
        .iter()
        .filter(|(height, _)| *height >= middle)
        .cloned()
        .collect();
    assert_eq!(recovered.log, from_middle);

    // The storage is now at the end of the range, so starting from the middle again would give the
    // indexer receipts twice.
    let ahead = run(
        &mut TreeIndexer::default(),
        BlockRange::Range {
            start_inclusive: middle,
            end_exclusive: Some(range.end),
        },
        in_file(&trees_file),
    )
    .await;
    let last_height = *uninterrupted.heights.last().unwrap();
    assert!(
        matches!(
            ahead,
            Err(InIndexerError::Storage(IndexerStorageError::Ahead { last_block, .. }))
                if last_block == last_height
        ),
        "{ahead:?}"
    );
}

#[tokio::test]
async fn auto_continue_needs_storage() {
    let result = run(
        &mut TreeIndexer::default(),
        auto_continue(50_000_000, 50_000_010),
        None,
    )
    .await;
    assert!(
        matches!(
            result,
            Err(InIndexerError::Storage(IndexerStorageError::NoStorage))
        ),
        "{result:?}"
    );
}
