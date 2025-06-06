//! This example demonstrates how to watch slimedragon.near's transactions

use async_trait::async_trait;
use inindexer::{
    neardata_old::OldNeardataProvider, run_indexer, BlockRange, CompleteTransaction, Indexer,
    IndexerOptions,
};
use near_indexer_primitives::{types::AccountId, StreamerMessage};

struct WatcherIndexer {
    tracked_account: AccountId,
}

#[async_trait]
impl Indexer for WatcherIndexer {
    type Error = String;

    async fn on_transaction(
        &mut self,
        transaction: &CompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        // Note: this is a simple example, which doesn't handle DELEGATE actions
        if transaction.transaction.transaction.signer_id == self.tracked_account {
            log::info!(
                "Found transaction: https://pikespeak.ai/transaction-viewer/{}",
                transaction.transaction.transaction.hash
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
        OldNeardataProvider::mainnet(),
        IndexerOptions::default_with_range(BlockRange::Range {
            start_inclusive: 112_037_807,
            end_exclusive: Some(112_037_810),
        }),
    )
    .await?;

    Ok(())
}
