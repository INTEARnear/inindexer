#[cfg(not(feature = "neardata-server"))]
compile_error!("Use `cargo test --all-features` to run tests. If you want to skip AWS Lake test, run with `cargo test --features neardata-server`");

use std::{collections::HashMap, ops::Range, path::PathBuf};

#[cfg(feature = "lake")]
use crate::lake::LakeStreamer;
use crate::{
    message_provider::ParallelProviderStreamer, near_utils::MAINNET_GENESIS_BLOCK_HEIGHT,
    neardata_server::NeardataServerProvider, AutoContinue, AutoContinueEnd, BlockIterator,
    CompleteTransaction, IndexerOptions, PreprocessTransactionsSettings,
};
use async_trait::async_trait;
use near_indexer_primitives::{
    types::{BlockHeight, BlockHeightDelta},
    CryptoHash, IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
};

use crate::{run_indexer, Indexer};

#[tokio::test]
async fn neardata_server_provider() {
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
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(RANGE),
            preprocess_transactions: None,
            ..Default::default()
        },
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
        NeardataServerProvider::mainnet().optimistic(),
        IndexerOptions {
            range: BlockIterator::iterator(RANGE),
            preprocess_transactions: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4) // I guess old optimistic blocks are purged, so idk how to test
}

#[cfg(feature = "lake")]
#[tokio::test]
async fn lake_provider() {
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
        LakeStreamer::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(RANGE),
            preprocess_transactions: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(indexer.blocks_processed, 4)
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
        ParallelProviderStreamer::new(NeardataServerProvider::mainnet(), 3),
        IndexerOptions {
            range: BlockIterator::iterator(RANGE),
            preprocess_transactions: None,
            ..Default::default()
        },
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
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::AutoContinue(AutoContinue {
                save_location: Box::new(PathBuf::from(save_path)),
                end: AutoContinueEnd::Count(5),
                ..Default::default()
            }),
            preprocess_transactions: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(indexer.last_block_height > MAINNET_GENESIS_BLOCK_HEIGHT);

    let current_height = indexer.last_block_height;

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::AutoContinue(AutoContinue {
                save_location: Box::new(PathBuf::from(save_path)),
                end: AutoContinueEnd::Count(5),
                ..Default::default()
            }),
            preprocess_transactions: None,
            ..Default::default()
        },
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
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(RANGE),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 5,
                postfetch_blocks: 5,
            }),
            ..Default::default()
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
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(116_917_957..=116_917_962),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 5,
                postfetch_blocks: 5,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(indexer.found);
}
