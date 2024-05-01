//! This example downloads blocks locally, so that you can process them later.
//! This is a simple example, but would be nice to rewrite it with a more efficient
//! data structure, such as https://github.com/fastnear/fast-near/blob/main/scripts/load-raw-near-lake.js,
//! this would also make it compatible with other indexing software.
//!
//! Also, downloading blocks in batches would be much faster, but inindexer doesn't support this yet.
//! If you want to download a lot of blocks and speed matters, you can use download method directly
//! and asynchronously: FastNearDataServerProvider::get_message(BlockHeight)

use std::path::PathBuf;

use async_trait::async_trait;
use inindexer::{
    fastnear_data_server::FastNearDataServerProvider,
    indexer_utils::{MAINNET_GENESIS_BLOCK_HEIGHT, TESTNET_GENESIS_BLOCK_HEIGHT},
    run_indexer, AutoContinue, BlockIterator, Indexer, IndexerOptions,
};
use near_indexer_primitives::{types::BlockHeight, StreamerMessage};

struct DownloadIndexer {
    path: PathBuf,
}

#[async_trait]
impl Indexer for DownloadIndexer {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        let height = block.block.header.height;
        let stringified = height.to_string();
        let (folder1, folder2, file) = (
            &format!("{:0>1$}", &stringified[..(stringified.len() - 6)], 6),
            &stringified[(stringified.len() - 6)..(stringified.len() - 3)],
            &(stringified.to_owned() + ".json"),
        );
        let block_file = self.path.join(folder1).join(folder2).join(file);
        tokio::fs::create_dir_all(block_file.parent().unwrap())
            .await
            .map_err(|e| e.to_string())?;
        tokio::fs::write(
            block_file,
            serde_json::to_vec(block).map_err(|e| e.to_string())?,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

enum StartBlockHeight {
    Genesis,
    BlockHeight(BlockHeight),
}

enum EndBlockHeight {
    Infinity,
    AutoContinue,
    BlockHeight(BlockHeight),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()?;

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        eprintln!("Usage: {} <path> <mainnet | testnet> <start_block_height | genesis> <end_block_height | infinity | autocontinue>", args[0]);
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    let network = match &args[2][..] {
        "mainnet" => "mainnet",
        "testnet" => "testnet",
        _ => {
            eprintln!("Invalid network: {}", args[2]);
            std::process::exit(1);
        }
    };
    let start_block = match &args[3][..] {
        "genesis" => StartBlockHeight::Genesis,
        _ => StartBlockHeight::BlockHeight(args[3].parse().expect("Invalid start block height")),
    };
    let end_block = match &args[4][..] {
        "infinity" => EndBlockHeight::Infinity,
        "autocontinue" => EndBlockHeight::AutoContinue,
        _ => EndBlockHeight::BlockHeight(args[4].parse().expect("Invalid end block height")),
    };
    let start_block_height = match network {
        "mainnet" => MAINNET_GENESIS_BLOCK_HEIGHT,
        "testnet" => TESTNET_GENESIS_BLOCK_HEIGHT,
        _ => unreachable!(),
    };
    let range = match (start_block, end_block) {
        (StartBlockHeight::Genesis, EndBlockHeight::Infinity) => {
            BlockIterator::iterator(start_block_height..)
        }
        (StartBlockHeight::Genesis, EndBlockHeight::AutoContinue) => {
            BlockIterator::AutoContinue(AutoContinue {
                start_height_if_does_not_exist: start_block_height,
                save_location: Box::new(path.join("last_block.txt")),
                ctrl_c_handler: true,
                end: inindexer::AutoContinueEnd::Infinite,
            })
        }
        (StartBlockHeight::Genesis, EndBlockHeight::BlockHeight(end_block_height)) => {
            BlockIterator::iterator(start_block_height..=end_block_height)
        }
        (StartBlockHeight::BlockHeight(start_block_height), EndBlockHeight::Infinity) => {
            BlockIterator::iterator(start_block_height..)
        }
        (StartBlockHeight::BlockHeight(start_block_height), EndBlockHeight::AutoContinue) => {
            BlockIterator::AutoContinue(AutoContinue {
                start_height_if_does_not_exist: start_block_height,
                save_location: Box::new(path.join("last_block.txt")),
                ctrl_c_handler: true,
                end: inindexer::AutoContinueEnd::Infinite,
            })
        }
        (
            StartBlockHeight::BlockHeight(start_block_height),
            EndBlockHeight::BlockHeight(end_block_height),
        ) => BlockIterator::iterator(start_block_height..=end_block_height),
    };

    let mut indexer = DownloadIndexer { path };

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range,
            stop_on_error: true,
            preprocess_transactions: None,
            genesis_block_height: MAINNET_GENESIS_BLOCK_HEIGHT,
        },
    )
    .await?;

    Ok(())
}
