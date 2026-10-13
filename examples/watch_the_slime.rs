//! This example demonstrates how to watch slimedragon.near's transactions

use async_trait::async_trait;
use inindexer::{
    BlockRange, Indexer, IndexerOptions, StreamerMessage,
    neardata_old::OldNeardataProvider,
    run_indexer,
    transaction_trees::{TransactionTree, TransactionTrees},
};
use near_indexer_primitives::types::AccountId;

struct WatcherIndexer {
    tracked_account: AccountId,
}

#[async_trait]
impl Indexer for WatcherIndexer {
    type Error = String;

    async fn process_transaction(
        &mut self,
        transaction: &TransactionTree,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        // Note: this is a simple example, which doesn't handle DELEGATE actions
        let transaction = &transaction.transaction.transaction;
        if transaction.signer_id == self.tracked_account {
            log::info!(
                "Found transaction: https://pikespeak.ai/transaction-viewer/{}",
                transaction.hash
            );
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()?;

    let mut indexer = WatcherIndexer {
        tracked_account: "slimedragon.near".parse()?,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet_fastnear(),
        IndexerOptions {
            // Receipts in the first blocks can be of transactions from blocks before them
            transaction_trees: TransactionTrees {
                warm_up_blocks: 100,
                ..Default::default()
            },
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 112_037_807,
                // A few more blocks for the transactions' receipts to be executed
                end_exclusive: Some(112_037_815),
            })
        },
    )
    .await?;

    Ok(())
}
