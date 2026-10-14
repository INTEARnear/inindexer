use std::ops::Range;

use async_trait::async_trait;
use near_indexer_primitives::types::BlockHeight;
use serde::Deserialize;

use crate::{
    BlockRange, Indexer, IndexerOptions, StreamerMessage, run_indexer, teardata::TeardataProvider,
};

#[derive(Deserialize)]
struct Status {
    first_block_height: BlockHeight,
    final_height: BlockHeight,
}

#[derive(Default)]
struct CollectingIndexer {
    heights: Vec<BlockHeight>,
}

#[async_trait]
impl Indexer for CollectingIndexer {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.heights.push(block.block.header.height);
        Ok(())
    }
}

async fn get_json<T: serde::de::DeserializeOwned>(path: &str) -> T {
    let body = reqwest::get(format!("https://testnet-teardata.intea.rs{path}"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    serde_json::from_str(&body).unwrap()
}

async fn status() -> Status {
    get_json("/v0/status").await
}

/// Blocks before the range that the transaction trees read first, so the stream starts this many
/// blocks before the range.
const WARM_UP_BLOCKS: BlockHeight = 100;

/// The provider must yield exactly the heights for which `/v0/block` has a block, in order.
async fn check_range(range: Range<BlockHeight>) {
    let mut indexer = CollectingIndexer::default();
    run_indexer(
        &mut indexer,
        TeardataProvider::testnet(),
        IndexerOptions {
            warm_up_blocks: WARM_UP_BLOCKS,
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: range.start,
                end_exclusive: Some(range.end),
            })
        },
    )
    .await
    .unwrap();

    let mut expected = Vec::new();
    for height in range {
        let block: Option<serde_json::Value> = get_json(&format!("/v0/block/{height}")).await;
        if block.is_some() {
            expected.push(height);
        }
    }
    assert!(!expected.is_empty());
    assert_eq!(indexer.heights, expected);
}

#[tokio::test]
async fn teardata_provider_archives() {
    let first = status().await.first_block_height;
    // Unaligned start and end, so partial archives at both edges are covered.
    check_range(first + WARM_UP_BLOCKS + 3..first + WARM_UP_BLOCKS + 37).await;
}

#[tokio::test]
async fn teardata_provider_tip() {
    let final_height = status().await.final_height;
    // Ends past the final head, so the provider has to wait for upcoming blocks.
    check_range(final_height - 3..final_height + 4).await;
}
