use std::{fmt::Display, io::Read, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use near_indexer_primitives::{StreamerMessage, types::BlockHeight};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::MessageStreamer;

const BLOCKS_PER_ARCHIVE: BlockHeight = 10;
const ARCHIVE_SYNC_THRESHOLD: BlockHeight = BLOCKS_PER_ARCHIVE * 2;
const TIP_LOOKAHEAD: usize = 3;
/// After this many single blocks, the server status is checked again in case the consumer fell
/// far enough behind for archives to be faster.
const TIP_BATCH: BlockHeight = 1000;
const RETRY_DURATION: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TeardataProvider {
    base_url: String,
    client: reqwest::Client,
    optimistic: bool,
    archive_workers: usize,
}

impl TeardataProvider {
    pub fn mainnet() -> Self {
        Self {
            base_url: "https://mainnet-teardata.intea.rs".to_string(),
            client: reqwest::Client::new(),
            optimistic: false,
            archive_workers: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        }
    }

    pub fn testnet() -> Self {
        Self {
            base_url: "https://testnet-teardata.intea.rs".to_string(),
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .unwrap(),
            optimistic: false,
            archive_workers: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn optimistic(mut self) -> Self {
        self.optimistic = true;
        self
    }

    /// Number of archives downloaded in parallel while catching up.
    pub fn archive_workers(mut self, archive_workers: usize) -> Self {
        self.archive_workers = archive_workers;
        self
    }
}

#[derive(Deserialize)]
struct Status {
    last_sealed_archive: Option<BlockHeight>,
}

#[derive(Deserialize)]
struct BlockErrorResponse {
    #[serde(rename = "type")]
    error_type: String,
}

#[async_trait]
impl MessageStreamer for TeardataProvider {
    type Error = TeardataError;

    async fn stream(
        self,
        first_block_inclusive: BlockHeight,
        last_block_exclusive: Option<BlockHeight>,
    ) -> Result<
        (
            JoinHandle<Result<(), Self::Error>>,
            mpsc::Receiver<StreamerMessage>,
        ),
        Self::Error,
    > {
        let (tx, rx) = mpsc::channel(100);
        let join_handle = tokio::spawn(async move {
            let end = last_block_exclusive.unwrap_or(BlockHeight::MAX);
            let mut next = first_block_inclusive;
            while next < end {
                let status: Status = self.get_json("/v0/status").await?;
                let sealed_end = status
                    .last_sealed_archive
                    .map_or(0, |last| last + BLOCKS_PER_ARCHIVE);
                let (batch_end, sent) = if sealed_end >= next + ARCHIVE_SYNC_THRESHOLD {
                    let batch_end = sealed_end.min(end);
                    log::debug!(target: "inindexer::teardata", "Fetching archives for blocks {next}..{batch_end}");
                    let mut archives = stream::iter(
                        (next / BLOCKS_PER_ARCHIVE * BLOCKS_PER_ARCHIVE..batch_end)
                            .step_by(BLOCKS_PER_ARCHIVE as usize),
                    )
                    .map(|start| self.fetch_archive(start))
                    .buffered(self.archive_workers);
                    let mut sent = true;
                    'archives: while let Some(blocks) = archives.next().await {
                        for block in blocks? {
                            let height = block.block.header.height;
                            if height < next || height >= batch_end {
                                continue;
                            }
                            if tx.send(block).await.is_err() {
                                sent = false;
                                break 'archives;
                            }
                        }
                    }
                    (batch_end, sent)
                } else {
                    let batch_end = next.saturating_add(TIP_BATCH).min(end);
                    let mut blocks = stream::iter(next..batch_end)
                        .map(|height| self.fetch_block(height))
                        .buffered(TIP_LOOKAHEAD);
                    let mut sent = true;
                    while let Some(block) = blocks.next().await {
                        if let Some(block) = block?
                            && tx.send(block).await.is_err()
                        {
                            sent = false;
                            break;
                        }
                    }
                    (batch_end, sent)
                };
                if !sent {
                    // The receiver was dropped, nobody needs more blocks.
                    return Ok(());
                }
                next = batch_end;
            }
            log::info!("Block range ended.");
            Ok(())
        });
        Ok((join_handle, rx))
    }
}

impl TeardataProvider {
    async fn fetch_block(
        &self,
        height: BlockHeight,
    ) -> Result<Option<StreamerMessage>, TeardataError> {
        let route = if self.optimistic {
            "block_opt"
        } else {
            "block"
        };
        self.get_json(&format!("/v0/{route}/{height}")).await
    }

    async fn fetch_archive(
        &self,
        start: BlockHeight,
    ) -> Result<Vec<StreamerMessage>, TeardataError> {
        let archive = self.get_bytes(&format!("/v0/archive/{start}")).await?;
        tokio::task::spawn_blocking(move || parse_archive(&archive))
            .await
            .expect("Archive parsing panicked")
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, TeardataError> {
        let body = self.get_bytes(path).await?;
        serde_json::from_slice(&body).map_err(|err| TeardataError::FailedToParse {
            err,
            response: String::from_utf8_lossy(&body).into_owned(),
        })
    }

    /// Retries network errors, 5xx and blocks that are not produced yet. Other errors (like a
    /// height before the server's first block) are returned.
    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, TeardataError> {
        let url = format!("{}{path}", self.base_url);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let error = match self.client.get(&url).send().await {
                Ok(response) if response.status().is_success() => match response.bytes().await {
                    Ok(body) => return Ok(body.to_vec()),
                    Err(err) => err.to_string(),
                },
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    let not_produced_yet = status == StatusCode::NOT_FOUND
                        && serde_json::from_str::<BlockErrorResponse>(&body)
                            .is_ok_and(|error| error.error_type == "BLOCK_DOES_NOT_EXIST");
                    if !status.is_server_error() && !not_produced_yet {
                        return Err(TeardataError::Server { url, status, body });
                    }
                    format!("{status}: {body}")
                }
                Err(err) => err.to_string(),
            };
            log::warn!(target: "inindexer::teardata::fetch_failed", "Failed to fetch {url} (attempt {attempt}): {error}");
            tokio::time::sleep(RETRY_DURATION).await;
        }
    }
}

/// A tar of `{height:012}.json` blocks compressed with zstd, in height order.
fn parse_archive(archive: &[u8]) -> Result<Vec<StreamerMessage>, TeardataError> {
    let tar = zstd::decode_all(archive).map_err(TeardataError::Archive)?;
    let mut blocks = Vec::new();
    for entry in tar::Archive::new(tar.as_slice())
        .entries()
        .map_err(TeardataError::Archive)?
    {
        let mut content = Vec::new();
        entry
            .map_err(TeardataError::Archive)?
            .read_to_end(&mut content)
            .map_err(TeardataError::Archive)?;
        let block: StreamerMessage =
            serde_json::from_slice(&content).map_err(|err| TeardataError::FailedToParse {
                err,
                response: String::from_utf8_lossy(&content).into_owned(),
            })?;
        blocks.push(block);
    }
    blocks.sort_by_key(|block| block.block.header.height);
    Ok(blocks)
}

#[derive(Debug)]
pub enum TeardataError {
    Server {
        url: String,
        status: StatusCode,
        body: String,
    },
    FailedToParse {
        err: serde_json::Error,
        response: String,
    },
    Archive(std::io::Error),
}

impl Display for TeardataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TeardataError::Server { url, status, body } => {
                write!(f, "Request to {url} failed with {status}: {body}")
            }
            TeardataError::FailedToParse { err, response } => {
                write!(f, "Failed to parse response: {err:?}\nResponse: {response}")
            }
            TeardataError::Archive(err) => write!(f, "Failed to read archive: {err}"),
        }
    }
}

impl std::error::Error for TeardataError {}
