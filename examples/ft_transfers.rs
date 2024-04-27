//! This example demonstrates how to use more features in IndexerOptions,
//! and build an indexer that watches for specific on-chain events.

use std::path::PathBuf;

use async_trait::async_trait;
use inindexer::{
    fastnear_data_server::FastNearDataServerProvider,
    indexer_utils::{
        deserialize_json_log, EventLogData, FtTransferLog, MAINNET_GENESIS_BLOCK_HEIGHT,
    },
    run_indexer, AutoContinue, BlockIterator, Indexer, IndexerOptions,
};
use near_indexer_primitives::views::ExecutionStatusView;

struct FtTransferIndexer;

#[async_trait]
impl Indexer for FtTransferIndexer {
    type Error = String;

    // We're not interested in transactions overall, so even if some receipt after the transfer fails,
    // it doesn't matter, as long as the transfer was successful
    async fn process_receipt(
        &self,
        receipt: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> Result<(), Self::Error> {
        if let ExecutionStatusView::Failure(_) = receipt.execution_outcome.outcome.status {
            return Ok(());
        }
        // This is not the most efficient way to filter tokens, ideally you would want to store
        // all FT contracts in an in-memory Set, and not process receipts for other contracts.
        // Also, instead of making 2 attempts to deserialize every log, checking `.contains("nep141")`
        // is a dirty but efficient way to discard 99% of irrelevant logs
        let token_id = &receipt.receipt.receiver_id;
        let mut transfers = Vec::new();
        for log in receipt.execution_outcome.outcome.logs.iter() {
            if let Ok(transfer_log) = deserialize_json_log::<EventLogData<FtTransferLog>>(log) {
                if transfer_log.validate() {
                    transfers.push(transfer_log.data);
                }
            } else if let Ok(transfer_log) = FtTransferLog::deserialize_tkn_farm_log(log) {
                transfers.push(transfer_log);
            }
        }
        for transfer in transfers {
            // Format: Sender --> Receiver: Amount Token, transaction link
            log::info!(
                "{} --> {}: {} {}, https://nearblocks.io?query={}",
                transfer.old_owner_id,
                transfer.new_owner_id,
                transfer.amount,
                token_id,
                receipt.receipt.receipt_id,
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

    let indexer = FtTransferIndexer;

    run_indexer(
        indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::AutoContinue(AutoContinue {
                save_file: PathBuf::from("example_ft_trasnfers_last_block.txt"),
                start_height_if_does_not_exist: 114_625_946,
                ctrl_c_handler: true,
            }),
            stop_on_error: false,
            preprocess_transactions: None,
            genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
        },
    )
    .await?;

    Ok(())
}
