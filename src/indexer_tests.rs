use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, ops::Range, path::PathBuf};

use crate::neardata::NeardataProvider;
use crate::{
    message_provider::ParallelProviderStreamer, near_utils::MAINNET_GENESIS_BLOCK_HEIGHT,
    neardata_old::OldNeardataProvider, AutoContinue, AutoContinueEnd, BlockRange,
    CompleteTransaction, IndexerOptions, PreprocessTransactionsSettings,
};
use async_trait::async_trait;
use near_indexer_primitives::types::Finality;
use near_indexer_primitives::{
    types::{BlockHeight, BlockHeightDelta},
    CryptoHash, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
};

use crate::multiindexer::{ChainIndexers, ParallelJoinIndexers};
use crate::{run_indexer, Indexer};

#[tokio::test]
async fn neardata_old_provider() {
    const RANGE: Range<BlockHeight> =
        MAINNET_GENESIS_BLOCK_HEIGHT..(MAINNET_GENESIS_BLOCK_HEIGHT + 10);

    #[derive(Default)]
    struct TestIndexer {
        blocks_processed: BlockHeightDelta,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(RANGE.contains(&block.block.header.height));
            self.blocks_processed += 1;
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: RANGE.start,
            end_exclusive: Some(RANGE.end),
        }),
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4)
}

#[tokio::test]
async fn neardata_provider() {
    const RANGE: Range<BlockHeight> =
        MAINNET_GENESIS_BLOCK_HEIGHT..(MAINNET_GENESIS_BLOCK_HEIGHT + 10);

    #[derive(Default)]
    struct TestIndexer {
        blocks_processed: BlockHeightDelta,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(RANGE.contains(&block.block.header.height));
            self.blocks_processed += 1;
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: RANGE.start,
            end_exclusive: Some(RANGE.end),
        }),
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4)
}

#[tokio::test]
async fn neardata_optimistic_provider() {
    const RANGE: Range<BlockHeight> =
        MAINNET_GENESIS_BLOCK_HEIGHT..(MAINNET_GENESIS_BLOCK_HEIGHT + 10);

    #[derive(Default)]
    struct TestIndexer {
        blocks_processed: BlockHeightDelta,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(RANGE.contains(&block.block.header.height));
            self.blocks_processed += 1;
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet().finality(Finality::DoomSlug),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: RANGE.start,
            end_exclusive: Some(RANGE.end),
        }),
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4) // I guess old optimistic blocks are purged, so idk how to test
}

#[tokio::test]
async fn parallel_provider_with_correct_order() {
    const RANGE: Range<BlockHeight> =
        MAINNET_GENESIS_BLOCK_HEIGHT..(MAINNET_GENESIS_BLOCK_HEIGHT + 10);

    #[derive(Default)]
    struct TestIndexer {
        blocks_processed: BlockHeightDelta,
        previous_block: Option<BlockHeight>,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(RANGE.contains(&block.block.header.height));
            self.blocks_processed += 1;
            if let Some(previous_block) = self.previous_block {
                assert!(previous_block < block.block.header.height);
            }
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();

    run_indexer(
        &mut indexer,
        ParallelProviderStreamer::new(OldNeardataProvider::mainnet(), 3),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: RANGE.start,
            end_exclusive: Some(RANGE.end),
        }),
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4)
}

#[tokio::test]
async fn auto_continue() {
    #[derive(Default)]
    struct TestIndexer {
        last_block_height: BlockHeight,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(block.block.header.height > self.last_block_height);
            self.last_block_height = block.block.header.height;
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();
    let save_file = temp_file::with_contents(MAINNET_GENESIS_BLOCK_HEIGHT.to_string().as_bytes());
    let save_path = save_file.path();

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::AutoContinue(AutoContinue {
            save_location: Box::new(PathBuf::from(save_path)),
            start_height_if_does_not_exist: MAINNET_GENESIS_BLOCK_HEIGHT,
            end: AutoContinueEnd::Count(5),
        })),
    )
    .await
    .unwrap();

    assert!(indexer.last_block_height > MAINNET_GENESIS_BLOCK_HEIGHT);

    let current_height = indexer.last_block_height;

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::AutoContinue(AutoContinue {
            save_location: Box::new(PathBuf::from(save_path)),
            start_height_if_does_not_exist: MAINNET_GENESIS_BLOCK_HEIGHT,
            end: AutoContinueEnd::Count(5),
        })),
    )
    .await
    .unwrap();

    assert!(indexer.last_block_height > current_height);
}

#[tokio::test]
async fn prefetch_and_postfetch_dont_process_blocks() {
    const RANGE: Range<BlockHeight> =
        (MAINNET_GENESIS_BLOCK_HEIGHT + 20)..(MAINNET_GENESIS_BLOCK_HEIGHT + 20 + 5);

    #[derive(Default)]
    struct TestIndexer {
        blocks_processed: BlockHeightDelta,
        tx_id_to_block_height: HashMap<CryptoHash, BlockHeight>,
        receipt_id_to_block_height: HashMap<CryptoHash, BlockHeight>,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
            assert!(RANGE.contains(&block.block.header.height));
            self.blocks_processed += 1;
            for shard in block.shards.iter() {
                if let Some(chunk) = shard.chunk.as_ref() {
                    for transaction in chunk.transactions.iter() {
                        self.tx_id_to_block_height
                            .insert(transaction.transaction.hash, block.block.header.height);
                        for receipt_id in transaction
                            .outcome
                            .execution_outcome
                            .outcome
                            .receipt_ids
                            .iter()
                        {
                            self.receipt_id_to_block_height
                                .insert(*receipt_id, block.block.header.height);
                        }
                    }
                }
                for receipt in shard.receipt_execution_outcomes.iter() {
                    self.receipt_id_to_block_height
                        .insert(receipt.receipt.receipt_id, block.block.header.height);
                }
            }
            Ok(())
        }

        async fn process_transaction(
            &mut self,
            transaction: &IndexerTransactionWithOutcome,
            _block: &StreamerMessage,
        ) -> Result<(), Self::Error> {
            let block_height = self
                .tx_id_to_block_height
                .get(&transaction.transaction.hash)
                .unwrap();
            assert!(RANGE.contains(block_height));
            Ok(())
        }

        async fn process_receipt(
            &mut self,
            receipt: &IndexerExecutionOutcomeWithReceipt,
            _block: &StreamerMessage,
        ) -> Result<(), Self::Error> {
            let block_height = self
                .receipt_id_to_block_height
                .get(&receipt.receipt.receipt_id)
                .unwrap();
            assert!(RANGE.contains(block_height));
            Ok(())
        }

        async fn on_transaction(
            &mut self,
            transaction: &CompleteTransaction,
            _block: &StreamerMessage,
        ) -> Result<(), Self::Error> {
            let block_height = self
                .tx_id_to_block_height
                .get(&transaction.transaction.transaction.hash)
                .unwrap();
            assert!(RANGE.contains(block_height));
            Ok(())
        }
    }

    let mut indexer = TestIndexer::default();

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 5,
                postfetch_blocks: 5,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: RANGE.start,
                end_exclusive: Some(RANGE.end),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4)
}

#[tokio::test]
async fn preprocessing_should_supply_completed_transaction() {
    struct TestIndexer {
        found: bool,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn on_transaction(
            &mut self,
            transaction: &CompleteTransaction,
            _block: &StreamerMessage,
        ) -> Result<(), Self::Error> {
            if transaction.transaction.transaction.hash
                == "Dvx5xxjrMfKXRUuRBmTizvQf7qA3U2w5Dq7peCFL41tT"
                    .parse()
                    .unwrap()
            {
                assert_eq!(transaction.receipts.len(), 10);
                self.found = true;
            }
            Ok(())
        }
    }

    let mut indexer = TestIndexer { found: false };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 5,
                postfetch_blocks: 5,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 116_917_957,
                end_exclusive: Some(116_917_962),
            })
        },
    )
    .await
    .unwrap();

    assert!(indexer.found);
}

#[derive(Debug, Clone)]
struct CountingIndexer {
    counter: Arc<AtomicU32>,
    sleep_ms: u64,
}

impl CountingIndexer {
    fn new(counter: Arc<AtomicU32>, sleep_ms: u64) -> Self {
        Self { counter, sleep_ms }
    }
}

#[async_trait]
impl Indexer for CountingIndexer {
    type Error = String;

    async fn finalize(&mut self) -> Result<(), Self::Error> {
        if self.sleep_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.sleep_ms)).await;
        }
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_sequential_multi_indexer() {
    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));

    let indexer1 = CountingIndexer::new(counter1.clone(), 50);
    let indexer2 = CountingIndexer::new(counter2.clone(), 50);

    let mut multi_indexer = indexer1.chain(indexer2);

    let start = std::time::Instant::now();
    multi_indexer.finalize().await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(counter1.load(Ordering::SeqCst), 1);
    assert_eq!(counter2.load(Ordering::SeqCst), 1);
    assert!(
        elapsed.as_millis() >= 100,
        "Sequential execution should take at least 100ms"
    );
}

#[tokio::test]
async fn test_parallel_multi_indexer() {
    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));

    let indexer1 = CountingIndexer::new(counter1.clone(), 50);
    let indexer2 = CountingIndexer::new(counter2.clone(), 50);

    let mut parallel_indexer = indexer1.parallel_join(indexer2);

    let start = std::time::Instant::now();
    parallel_indexer.finalize().await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(counter1.load(Ordering::SeqCst), 1);
    assert_eq!(counter2.load(Ordering::SeqCst), 1);
    assert!(
        elapsed.as_millis() < 100,
        "Parallel execution should take less than 100ms"
    );
}

#[tokio::test]
async fn test_chaining_multiple_indexers() {
    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));
    let counter3 = Arc::new(AtomicU32::new(0));

    let indexer1 = CountingIndexer::new(counter1.clone(), 0);
    let indexer2 = CountingIndexer::new(counter2.clone(), 0);
    let indexer3 = CountingIndexer::new(counter3.clone(), 0);

    let mut sequential = indexer1.chain(indexer2.clone()).chain(indexer3.clone());
    sequential.finalize().await.unwrap();

    assert_eq!(counter1.load(Ordering::SeqCst), 1);
    assert_eq!(counter2.load(Ordering::SeqCst), 1);
    assert_eq!(counter3.load(Ordering::SeqCst), 1);

    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));
    let counter3 = Arc::new(AtomicU32::new(0));

    let indexer1 = CountingIndexer::new(counter1.clone(), 0);
    let indexer2 = CountingIndexer::new(counter2.clone(), 0);
    let indexer3 = CountingIndexer::new(counter3.clone(), 0);

    let mut parallel = indexer1.parallel_join(indexer2).parallel_join(indexer3);
    parallel.finalize().await.unwrap();

    assert_eq!(counter1.load(Ordering::SeqCst), 1);
    assert_eq!(counter2.load(Ordering::SeqCst), 1);
    assert_eq!(counter3.load(Ordering::SeqCst), 1);
}
