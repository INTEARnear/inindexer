//! This example demonstrates how to watch slimedragon.near's transactions

use async_trait::async_trait;
use inindexer::{
    fastnear_data_server::FastNearDataServerProvider, run_indexer, CompletedTransaction, Indexer,
};
use near_indexer_primitives::types::AccountId;

struct WatcherIndexer {
    tracked_account: AccountId,
}

#[async_trait]
impl Indexer for WatcherIndexer {
    type Error = String;

    async fn on_transaction(&self, transaction: &CompletedTransaction) -> Result<(), Self::Error> {
        // Note: this is a simple example, which doesn't handle DELEGATE actions
        if transaction.transaction.transaction.signer_id == self.tracked_account {
            log::info!(
                "Found transaction: https://pikespeak.ai/transaction-viewer/{:?}",
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

    let indexer = WatcherIndexer {
        tracked_account: "slimedragon.near".parse()?,
    };

    run_indexer(
        indexer,
        FastNearDataServerProvider::mainnet(),
        112_037_807..112_037_811,
        true,
    )
    .await?;

    Ok(())
}
