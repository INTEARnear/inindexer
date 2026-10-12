use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::neardata::{NeardataError, NeardataProvider};
use crate::{
    AutoContinue, AutoContinueEnd, BlockRange, Checkpoint, IndexerExecutionOutcomeWithReceipt,
    IndexerOptions, MessageStreamer, StreamerMessage, indexer_state::InIndexerError,
    message_provider::ParallelProviderStreamer, near_utils::MAINNET_GENESIS_BLOCK_HEIGHT,
    neardata_old::OldNeardataProvider,
};
use async_trait::async_trait;
use near_indexer_primitives::types::Finality;
use near_indexer_primitives::{
    CryptoHash,
    types::{BlockHeight, BlockHeightDelta},
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::multiindexer::{ChainIndexers, ParallelJoinIndexers};
use crate::{Indexer, run_indexer};

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
        OldNeardataProvider::mainnet_fastnear(),
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
        ParallelProviderStreamer::new(OldNeardataProvider::mainnet_fastnear(), 3),
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

    async fn run_once(indexer: &mut TestIndexer, save_path: &std::path::Path) {
        run_indexer(
            indexer,
            NeardataProvider::mainnet(),
            IndexerOptions::default_with_range(BlockRange::AutoContinue(AutoContinue {
                checkpoint: Checkpoint::File(save_path.to_path_buf()),
                start_height_if_none_processed: MAINNET_GENESIS_BLOCK_HEIGHT,
                end: AutoContinueEnd::Count(20),
            })),
        )
        .await
        .unwrap();
    }

    let save_path = std::env::temp_dir().join(format!(
        "inindexer-auto-continue-{}.txt",
        std::process::id()
    ));
    // TestIndexer fails on a block that isn't after the last one, so a second run that repeats
    // blocks fails too
    let mut indexer = TestIndexer::default();

    run_once(&mut indexer, &save_path).await;
    let first_run_last_block = indexer.last_block_height;
    assert!(first_run_last_block >= MAINNET_GENESIS_BLOCK_HEIGHT);
    assert_eq!(
        std::fs::read_to_string(&save_path).unwrap(),
        (first_run_last_block + 1).to_string()
    );

    run_once(&mut indexer, &save_path).await;
    assert!(indexer.last_block_height > first_run_last_block);

    std::fs::remove_file(save_path).unwrap();
}

#[tokio::test]
async fn receipts_have_tx_hashes() {
    struct TestIndexer {
        receipts_of_transaction: usize,
    }

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;

        async fn process_receipt(
            &mut self,
            receipt: &IndexerExecutionOutcomeWithReceipt,
            _block: &StreamerMessage,
        ) -> Result<(), Self::Error> {
            let transaction_hash: CryptoHash = "Dvx5xxjrMfKXRUuRBmTizvQf7qA3U2w5Dq7peCFL41tT"
                .parse()
                .unwrap();
            if receipt.tx_hash == transaction_hash {
                self.receipts_of_transaction += 1;
            }
            Ok(())
        }
    }

    let mut indexer = TestIndexer {
        receipts_of_transaction: 0,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: 116_917_952,
            end_exclusive: Some(116_917_967),
        }),
    )
    .await
    .unwrap();

    assert_eq!(indexer.receipts_of_transaction, 10);
}

#[tokio::test]
async fn skipped_block_stops_indexer() {
    /// Drops the second block of the stream
    struct SkippingStreamer(NeardataProvider);

    #[async_trait]
    impl MessageStreamer for SkippingStreamer {
        type Error = NeardataError;

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
        > {
            let (inner_handle, mut inner_rx) = self
                .0
                .stream(first_block_inclusive, last_block_exclusive)
                .await?;
            let (tx, rx) = mpsc::channel(100);
            let handle = tokio::spawn(async move {
                let mut received = 0;
                while let Some(message) = inner_rx.recv().await {
                    received += 1;
                    if received != 2 && tx.send(message).await.is_err() {
                        break;
                    }
                }
                drop(inner_rx);
                inner_handle.await.unwrap()
            });
            Ok((handle, rx))
        }
    }

    struct TestIndexer;

    #[async_trait]
    impl Indexer for TestIndexer {
        type Error = String;
    }

    let result = run_indexer(
        &mut TestIndexer,
        SkippingStreamer(NeardataProvider::mainnet()),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: 116_917_957,
            end_exclusive: Some(116_917_967),
        }),
    )
    .await;

    assert!(matches!(result, Err(InIndexerError::Discontinuity { .. })));
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
