//! This example demonstrates how to use more features in IndexerOptions,
//! and build an indexer that watches for specific on-chain events.

use async_trait::async_trait;
use inindexer::{
    AutoContinue, BlockRange, Checkpoint, Indexer, IndexerExecutionOutcomeWithReceipt,
    IndexerOptions, StreamerMessage,
    near_utils::{EventLogData, FtTransferLog},
    run_indexer,
    teardata::TeardataProvider,
    transaction_trees::{TransactionTree, TransactionTrees},
};
use near_indexer_primitives::views::ExecutionStatusView;

struct FtTransferIndexer;

#[async_trait]
impl Indexer for FtTransferIndexer {
    type Error = String;

    // We're not interested in transactions overall, so even if some receipt after the transfer fails,
    // it doesn't matter, as long as the transfer was successful
    async fn process_receipt(
        &mut self,
        receipt: &IndexerExecutionOutcomeWithReceipt,
        _transaction: &TransactionTree,
        _block: &StreamerMessage,
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
            if let Ok(transfer_log) = EventLogData::<FtTransferLog>::deserialize(log) {
                if transfer_log.validate() {
                    transfers.extend(transfer_log.data.0);
                }
            } else if let Ok(transfer_log) = FtTransferLog::deserialize_tkn_farm_log(log) {
                transfers.extend(transfer_log.0);
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

    let mut indexer = FtTransferIndexer;

    run_indexer(
        &mut indexer,
        TeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockRange::AutoContinue(AutoContinue {
                checkpoint: Checkpoint::File("example_ft_trasnfers_last_block.txt".into()),
                start_height_if_none_processed: 139770436,
                end: inindexer::AutoContinueEnd::Infinite,
            }),
            stop_on_error: false,
            ctrl_c_handler: true,
            // Receipts in the first blocks can be of transactions from blocks before them
            transaction_trees: TransactionTrees {
                warm_up_blocks: 100,
                ..Default::default()
            },
        },
    )
    .await?;

    Ok(())
}
