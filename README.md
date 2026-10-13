[![crates-io](https://img.shields.io/crates/v/inindexer.svg)](https://crates.io/crates/inindexer) [![Workflow Status](https://github.com/INTEARnear/inindexer/actions/workflows/rust.yml/badge.svg)](https://github.com/INTEARnear/inindexer/actions?query=workflow%3A%22main%22)

# Indexer
InIndexer is a NEAR indexer framework.

## Features

- Different sources of near data, each behind a cargo feature (none are enabled by default):
  - `neardata`: `NeardataProvider`, [neardata](https://github.com/fastnear/neardata-server) through FastNEAR's fetcher.
  - `neardata-old`: `OldNeardataProvider`, neardata or any neardata-compatible server, one block per request.
  - `teardata`: `TeardataProvider`, same as neardata but by Intear.

  You can add your own sources by implementing `MessageStreamer` or `message_provider::MessageProvider` trait.
- Simple indexer interface: you only need to implement `Indexer` trait and handle receipts, blocks, or
  transactions. Every receipt comes with the hash of its transaction (`tx_hash`), taken from neardata / teardata.
- Retries, performance warnings, skipped blocks handling, and other features are built-in, so you can focus on
  your indexer logic.
- Auto-Continue: `BlockRange::AutoContinue` continues right after the last processed block. Either the indexer
  saves it in the same write as its data (`Checkpoint::SavedByIndexer`, e.g. in the database transaction it
  commits in `process_block_end`), or inindexer saves it in a file after each block (`Checkpoint::File`).
  Includes a Ctrl+C handler for graceful shutdown.
- Skipped block detection: every block's `prev_height` is checked against the block before it, so a gap in the
  stream stops the indexer instead of silently losing data.
- Transaction trees: every transaction is kept with the receipts it has led to until all of them have been
  executed, so `process_receipt` gets each receipt with its transaction's tree (to find the receipt that created
  it, or who started a chain of calls), and `process_transaction` gets each transaction with all of its receipts.
  The trees are in memory by default, or in a [redb](https://github.com/cberner/redb) file
  (`TransactionTreesStorage::File`) that is committed after each block is saved as processed, so they continue
  with Auto-Continue, through Ctrl+C and crashes. When the trees start anywhere but genesis, set
  `TransactionTrees::warm_up_blocks` so that receipts of transactions from before the first block have their
  trees: a receipt without one panics.
- Some helper functions and types for working with logs, balances, and other commonly used functionality in
  `near_utils`.

This crate only works with tokio runtime.

If you want to see some examples, check minimal examples in [examples/](examples/) or real indexers used in Intear infrastructure ([nft-indexer](https://github.com/INTEARnear/nft-indexer), [potlock-indexer](https://github.com/INTEARnear/potlock-indexer), [trade-indexer](https://github.com/INTEARnear/trade-indexer), [new-token-indexer](https://github.com/INTEARnear/new-token-indexer), [intear-oracle indexer](https://github.com/INTEARnear/oracle/tree/main/crates/indexer)). By the way, some of these repositories are libraries, so if you want the same functionality but with a different event handler, you can use them in your code by specifying them as git dependencies.

To run multiple indexers at once without making a new request for each indexer, use `MultiIndexer`, with `MapErrorIndexer` if your indexers have different error types.

If you use neardata or teardata, you can enable optimistic block retrieval if you don't care about finality and need to minimize latency, just call `.optimistic()` (OldNeardataProvider) and `.finality(Finality::DoomSlug)` (NeardataProvider, TeardataProvider) on the data provider.
