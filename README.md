![crates-io](https://img.shields.io/crates/v/inindexer.svg) [![Workflow Status](https://github.com/INTEARnear/inindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/INTEARnear/inindexer/actions?query=workflow%3A%22main%22)

# Indexer
InIndexer is a NEAR indexer framework.

## Features

- Different sources of near data: [neardata-server](https://github.com/fastnear/neardata-server) (implemented),
  [AWS Lake](https://docs.near.org/concepts/advanced/near-lake-framework) (only consecutive ascending ranges
  are supported), local file storage for backfilling (planned), you can add your own sources by implementing
  `MessageStreamer` or `message_provider::MessageProvider` trait.
- Simple indexer interface: you only need to implement `Indexer` trait and handle receipts, blocks,
  transactions, or transactions with all receipts included, at a cost of some preprocessing overhead (around 1-2ms
  in release mode with 80-100 TPS on Slime's PC, this can be disabled in `IndexerOptions::preprocess_transactions`).
- Retries, performance warnings, skipped blocks handling, and other features are built-in, so you can focus on
  your indexer logic.
- Auto-Continue: the indexer will save the last processed block height to the file and continue from it
  on the next run. Includes a Ctrl+C handler for graceful shutdown.
- Some helper functions and types for working with logs, balances, and other commonly used functionality in
  `indexer_utils`.

This crate only works with tokio runtime.

If you want to see some examples, check minimal examples in [examples/](examples/) or real indexers used in Intear infrastructure ([nft-indexer](https://github.com/INTEARnear/nft-indexer)).

License: MIT OR Apache-2.0
