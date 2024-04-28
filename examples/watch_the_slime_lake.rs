//! This example demonstrates how to watch slimedragon.near's transactions.
//!
//! Before running this example, make sure you have the environment set up with AWS credentials:
//! https://docs.near.org/build/data-infrastructure/lake-framework/near-lake-state-changes-indexer

use async_trait::async_trait;
use inindexer::{
    lake::LakeStreamer, run_indexer, BlockIterator, CompletedTransaction, Indexer, IndexerOptions,
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
        transaction: &CompletedTransaction,
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
        .with_module_level("aws_config", log::LevelFilter::Warn)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()?;

    let mut indexer = WatcherIndexer {
        tracked_account: "slimedragon.near".parse()?,
    };

    run_indexer(
        &mut indexer,
        LakeStreamer::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(112_037_807..=112_037_810),
            ..Default::default()
        },
    )
    .await?;

    Ok(())
}
