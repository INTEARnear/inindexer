[![crates-io](https://img.shields.io/crates/v/inindexer.svg)](https://crates.io/crates/inindexer) [![Workflow Status](https://github.com/INTEARnear/inindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/INTEARnear/inindexer/actions?query=workflow%3A%22main%22)

# Indexer
InIndexer is a NEAR indexer framework.

## Features

- Different sources of near data, each behind a cargo feature (none are enabled by default):
  - `neardata`: `NeardataProvider`, [neardata](https://github.com/fastnear/neardata-server) through FastNEAR's fetcher.
  - `neardata-old`: `OldNeardataProvider`, neardata or any neardata-compatible server, one block per request.
  - `teardata`: `TeardataProvider`, same as neardata but by Intear.

  You can add your own sources by implementing `MessageStreamer` or `message_provider::MessageProvider` trait.
- Simple indexer interface: you only need to implement `Indexer` trait and handle receipts, blocks,
  transactions, or transactions with all receipts included, at a cost of some preprocessing overhead (around 1-2ms
  in release mode with 80-100 TPS on Slime's PC, this can be disabled in `IndexerOptions::preprocess_transactions`).
- Retries, performance warnings, skipped blocks handling, and other features are built-in, so you can focus on
  your indexer logic.
- Auto-Continue: the indexer will save the last processed block height to the file and continue from it
  on the next run. Includes a Ctrl+C handler for graceful shutdown.
- Some helper functions and types for working with logs, balances, and other commonly used functionality in
  `near_utils`.

This crate only works with tokio runtime.

If you want to see some examples, check minimal examples in [examples/](examples/) or real indexers used in Intear infrastructure ([nft-indexer](https://github.com/INTEARnear/nft-indexer), [potlock-indexer](https://github.com/INTEARnear/potlock-indexer), [trade-indexer](https://github.com/INTEARnear/trade-indexer), [new-token-indexer](https://github.com/INTEARnear/new-token-indexer), [intear-oracle indexer](https://github.com/INTEARnear/oracle/tree/main/crates/indexer)). By the way, some of these repositories are libraries, so if you want the same functionality but with a different event handler, you can use them in your code by specifying them as git dependencies.

To run multiple indexers at once without making a new request for each indexer, use `MultiIndexer`, with `MapErrorIndexer` if your indexers have different error types.

If you use neardata or teardata, you can enable optimistic block retrieval if you don't care about finality and need to minimize latency, just call `.optimistic()` (OldNeardataProvider) and `.finality(Finality::DoomSlug)` (NeardataProvider, TeardataProvider) on the data provider.
