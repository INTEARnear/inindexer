use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use async_trait::async_trait;
use near_indexer_primitives::{CryptoHash, types::BlockHeight};
use serde::Deserialize;
use temp_file::TempFile;

use crate::{
    AutoContinue, AutoContinueEnd, BlockRange, Checkpoint, Indexer,
    IndexerExecutionOutcomeWithReceipt, IndexerOptions, StreamerMessage,
    indexer_state::InIndexerError,
    run_indexer,
    teardata::{TeardataError, TeardataProvider},
    transaction_trees::{
        TransactionTree, TransactionTreeStore, TransactionTrees, TransactionTreesError,
        TransactionTreesStorage,
    },
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
    transaction_trees: TransactionTrees,
) -> Result<(), InIndexerError<TeardataError>> {
    run_indexer(
        indexer,
        TeardataProvider::mainnet(),
        IndexerOptions {
            stop_on_error: true,
            range,
            ctrl_c_handler: false,
            transaction_trees,
        },
    )
    .await
}

fn in_memory() -> TransactionTrees {
    TransactionTrees {
        storage: TransactionTreesStorage::Memory,
        warm_up_blocks: WARM_UP_BLOCKS,
    }
}

fn in_file(file: &TempFile) -> TransactionTrees {
    TransactionTrees {
        storage: TransactionTreesStorage::Redb(file.path().into()),
        warm_up_blocks: WARM_UP_BLOCKS,
    }
}

fn auto_continue(checkpoint: Checkpoint, start: BlockHeight, end: BlockHeight) -> BlockRange {
    BlockRange::AutoContinue(AutoContinue {
        checkpoint,
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
        in_memory(),
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
#[should_panic(expected = "Increase TransactionTrees::warm_up_blocks")]
async fn receipt_without_tree_panics() {
    let range = recent_range(10).await;
    run(
        &mut TreeIndexer::default(),
        BlockRange::Range {
            start_inclusive: range.start,
            end_exclusive: Some(range.end),
        },
        TransactionTrees::default(),
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
        in_memory(),
    )
    .await
    .unwrap();
    let middle_index = uninterrupted.heights.len() / 2;
    let middle = uninterrupted.heights[middle_index];

    // Stopped in the middle, then continued from the checkpoint file.
    let trees_file = temp_file::empty();
    let checkpoint_file = temp_file::empty();
    std::fs::remove_file(checkpoint_file.path()).unwrap();
    let mut restarted = TreeIndexer::default();
    for end in [middle, range.end] {
        run(
            &mut restarted,
            auto_continue(
                Checkpoint::File(checkpoint_file.path().into()),
                range.start,
                end,
            ),
            in_file(&trees_file),
        )
        .await
        .unwrap();
    }
    assert_eq!(restarted.log, uninterrupted.log);

    // Crashed after the indexer saved the middle block, before the trees were committed.
    let trees_file = temp_file::empty();
    let crashing_run = tokio::spawn({
        let transaction_trees = in_file(&trees_file);
        async move {
            let mut crashing = TreeIndexer {
                crash_after: Some(middle),
                ..Default::default()
            };
            run(
                &mut crashing,
                BlockRange::Range {
                    start_inclusive: range.start,
                    end_exclusive: Some(range.end),
                },
                transaction_trees,
            )
            .await
        }
    })
    .await;
    assert!(crashing_run.unwrap_err().is_panic());
    let trees_after_crash = TransactionTreeStore::open(in_file(&trees_file))
        .await
        .unwrap();
    assert_eq!(
        trees_after_crash.last_block(),
        Some(uninterrupted.heights[middle_index - 1])
    );
    drop(trees_after_crash);

    let mut recovered = TreeIndexer::default();
    run(
        &mut recovered,
        auto_continue(
            Checkpoint::SavedByIndexer {
                last_processed_block: Some(middle),
            },
            range.start,
            range.end,
        ),
        in_file(&trees_file),
    )
    .await
    .unwrap();
    let after_crash: Vec<_> = uninterrupted
        .log
        .into_iter()
        .filter(|(height, _)| *height > middle)
        .collect();
    assert_eq!(recovered.log, after_crash);

    // The trees are now at the end of the range, so continuing from the middle again would give
    // the indexer receipts twice.
    let ahead = run(
        &mut TreeIndexer::default(),
        auto_continue(
            Checkpoint::SavedByIndexer {
                last_processed_block: Some(middle),
            },
            range.start,
            range.end,
        ),
        in_file(&trees_file),
    )
    .await;
    let last_height = *uninterrupted.heights.last().unwrap();
    assert!(
        matches!(
            ahead,
            Err(InIndexerError::TransactionTrees(TransactionTreesError::Ahead { last_block, .. }))
                if last_block == last_height
        ),
        "{ahead:?}"
    );
}
