use std::{error::Error, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use near_indexer_primitives::types::BlockHeight;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::storage::{BlockCommit, IndexerStorage, SavedState};

const TREES: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("trees");
const LAST_BLOCK: TableDefinition<(), BlockHeight> = TableDefinition::new("last_block");
const LAST_PROCESSED_BLOCK: TableDefinition<(), BlockHeight> =
    TableDefinition::new("last_processed_block");

/// Transaction trees and the last processed block in a redb file, created if it doesn't exist. Only one process can have it
/// open.
#[derive(Debug)]
pub struct RedbStorage {
    path: PathBuf,
    database: Option<Arc<Database>>,
}

impl RedbStorage {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            database: None,
        }
    }
}

#[async_trait]
impl IndexerStorage for RedbStorage {
    async fn load(&mut self) -> Result<SavedState, Box<dyn Error + Send + Sync>> {
        let path = self.path.clone();
        let (database, last_block, last_processed_block, stored_trees) =
            tokio::task::spawn_blocking(move || -> Result<_, redb::Error> {
                let database = Database::create(path)?;
                let write = database.begin_write()?;
                write.open_table(TREES)?;
                write.open_table(LAST_BLOCK)?;
                write.open_table(LAST_PROCESSED_BLOCK)?;
                write.commit()?;
                let read = database.begin_read()?;
                let last_block = read
                    .open_table(LAST_BLOCK)?
                    .get(())?
                    .map(|height| height.value());
                let last_processed_block = read
                    .open_table(LAST_PROCESSED_BLOCK)?
                    .get(())?
                    .map(|height| height.value());
                let stored_trees = read
                    .open_table(TREES)?
                    .iter()?
                    .map(|row| row.map(|(_, tree)| tree.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()?;
                drop(read);
                Ok((database, last_block, last_processed_block, stored_trees))
            })
            .await
            .expect("Opening transaction trees panicked")?;
        self.database = Some(Arc::new(database));
        Ok(SavedState {
            trees: stored_trees
                .iter()
                .map(|tree| serde_json::from_slice(tree))
                .collect::<Result<_, _>>()?,
            last_block,
            last_processed_block,
        })
    }

    async fn commit(
        &mut self,
        commit: BlockCommit<'_>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let database = Arc::clone(
            self.database
                .as_ref()
                .expect("Transaction trees are loaded before they're committed"),
        );
        let changed: Vec<_> = commit
            .changed
            .iter()
            .map(|tree| {
                (
                    tree.transaction.transaction.hash.0,
                    serde_json::to_vec(tree).expect("Transaction trees are serializable"),
                )
            })
            .collect();
        let finished = commit.finished.to_vec();
        let BlockCommit {
            block_height,
            processed,
            ..
        } = commit;
        tokio::task::spawn_blocking(move || -> Result<(), redb::Error> {
            let write = database.begin_write()?;
            {
                let mut trees = write.open_table(TREES)?;
                for hash in finished.iter() {
                    trees.remove(hash.0)?;
                }
                for (hash, tree) in changed.iter() {
                    trees.insert(hash, tree.as_slice())?;
                }
                write.open_table(LAST_BLOCK)?.insert((), block_height)?;
                if processed {
                    write
                        .open_table(LAST_PROCESSED_BLOCK)?
                        .insert((), block_height)?;
                }
            }
            write.commit()?;
            Ok(())
        })
        .await
        .expect("Committing transaction trees panicked")?;
        Ok(())
    }
}
