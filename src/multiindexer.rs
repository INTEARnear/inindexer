use std::{any::Any, fmt::Debug};

use crate::{CompleteTransaction, IncompleteTransaction, Indexer, TransactionReceipt};

use async_trait::async_trait;
use near_indexer_primitives::{
    IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome, StreamerMessage,
};

/// A multi-indexer that can be used to combine multiple indexers into one.
///
/// This indexer will call all the indexers in the order they were added.
/// The only restriction is that the indexers must have the same error type.
/// You can use [`MapErrorIndexer`] to convert errors to a common type.
///
/// # Example
///
/// ```rust
/// # use inindexer::near_indexer_primitives::StreamerMessage;
/// # use inindexer::Indexer;
/// # use async_trait::async_trait;
/// use inindexer::multiindexer::{MultiIndexer, ChainIndexers};
///
/// struct MyIndexer;
/// struct MyOtherIndexer;
///
/// #[async_trait]
/// impl Indexer for MyIndexer {
///     type Error = String;
///
///     async fn process_block(&mut self, _block: &StreamerMessage) -> Result<(), Self::Error> {
///         Ok(())
///     }
/// }
///
/// #[async_trait]
/// impl Indexer for MyOtherIndexer {
///     type Error = String;
///
///     async fn process_block(&mut self, _block: &StreamerMessage) -> Result<(), Self::Error> {
///         Ok(())
///     }
/// }
///
/// fn main() {
///     let indexer1 = MyIndexer;
///     let indexer2 = MyOtherIndexer;
///     let multi_indexer = indexer1.chain(indexer2);
///     let indexer3 = MyIndexer;
///     let indexer4 = MyOtherIndexer;
///     let multi_indexer = multi_indexer
///         .chain(indexer3)
///         .chain(indexer4);
///     assert_eq!(multi_indexer.indexers().len(), 4);
///     // Now multi_indexer is one indexer that will call all the indexers in order
/// }
pub struct MultiIndexer<E: Debug + Send + Sync + 'static>(Vec<Box<dyn Indexer<Error = E>>>);

impl<E: Debug + Send + Sync + 'static> MultiIndexer<E> {
    pub fn indexers(&self) -> &Vec<Box<dyn Indexer<Error = E>>> {
        &self.0
    }

    pub fn indexers_mut(&mut self) -> &mut Vec<Box<dyn Indexer<Error = E>>> {
        &mut self.0
    }
}

#[async_trait]
impl<E: Debug + Send + Sync + 'static> Indexer for MultiIndexer<E> {
    type Error = E;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.process_block(block).await?;
        }
        Ok(())
    }

    async fn process_transaction(
        &mut self,
        transaction: &IndexerTransactionWithOutcome,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.process_transaction(transaction, block).await?;
        }
        Ok(())
    }

    async fn process_receipt(
        &mut self,
        receipt: &IndexerExecutionOutcomeWithReceipt,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.process_receipt(receipt, block).await?;
        }
        Ok(())
    }

    async fn on_transaction(
        &mut self,
        transaction: &CompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.on_transaction(transaction, block).await?;
        }
        Ok(())
    }

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.on_receipt(receipt, tx, block).await?;
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<(), Self::Error> {
        for indexer in self.indexers_mut() {
            indexer.finalize().await?;
        }
        Ok(())
    }
}

pub trait ChainIndexers<E: Debug + Send + Sync + 'static> {
    fn chain(self, other: impl Indexer<Error = E>) -> MultiIndexer<E>;
}

impl<E: Debug + Send + Sync + 'static, I: Indexer<Error = E>> ChainIndexers<E> for I {
    fn chain(self, other: impl Indexer<Error = E>) -> MultiIndexer<E>
    where
        Self: Any + Send + Sync + 'static,
    {
        let this = Box::new(self) as Box<dyn Any>;
        if this.is::<MultiIndexer<E>>() {
            let mut multi = (this as Box<dyn Any>)
                .downcast::<MultiIndexer<E>>()
                .unwrap();
            multi.indexers_mut().push(Box::new(other));
            *multi
        } else {
            MultiIndexer(vec![this.downcast::<I>().unwrap(), Box::new(other)])
        }
    }
}

/// A helper indexer that maps errors from one type to another.
/// This can be used to convert errors from one type to another so that they can be used in a multi-indexer.
///
/// # Example
///
/// ```rust
/// # use inindexer::near_indexer_primitives::StreamerMessage;
/// # use inindexer::Indexer;
/// # use async_trait::async_trait;
/// use inindexer::multiindexer::MapError;
///
/// struct MyIndexer;
///
/// #[async_trait]
/// impl Indexer for MyIndexer {
///     type Error = String;
///
///     async fn process_block(&mut self, _block: &StreamerMessage) -> Result<(), Self::Error> {
///        Err("error".to_string())
///     }
/// }
///
/// #[derive(Debug)]
/// struct AnotherError(String);
///
/// fn main() {
///     let indexer = MyIndexer;
///     let mapped_indexer = indexer.map_error(|e| AnotherError(e));
///     // Now mapped_indexer has type Error = AnotherError
/// }
pub struct MapErrorIndexer<
    E: Debug + Send + Sync + 'static,
    E2: Debug + Send + Sync + 'static,
    I: Indexer<Error = E>,
    F: Fn(E) -> E2 + Send + Sync + 'static,
> {
    indexer: I,
    map: F,
}

impl<
        E: Debug + Send + Sync + 'static,
        E2: Debug + Send + Sync + 'static,
        I: Indexer<Error = E>,
        F: Fn(E) -> E2 + Send + Sync + 'static,
    > MapErrorIndexer<E, E2, I, F>
{
    pub fn new(indexer: I, map: F) -> Self {
        Self { indexer, map }
    }
}

#[async_trait]
impl<
        E: Debug + Send + Sync + 'static,
        E2: Debug + Send + Sync + 'static,
        I: Indexer<Error = E>,
        F: Fn(E) -> E2 + Send + Sync + 'static,
    > Indexer for MapErrorIndexer<E, E2, I, F>
{
    type Error = E2;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.indexer.process_block(block).await.map_err(&self.map)
    }

    async fn process_transaction(
        &mut self,
        transaction: &IndexerTransactionWithOutcome,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        self.indexer
            .process_transaction(transaction, block)
            .await
            .map_err(&self.map)
    }

    async fn process_receipt(
        &mut self,
        receipt: &IndexerExecutionOutcomeWithReceipt,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        self.indexer
            .process_receipt(receipt, block)
            .await
            .map_err(&self.map)
    }

    async fn on_transaction(
        &mut self,
        transaction: &CompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        self.indexer
            .on_transaction(transaction, block)
            .await
            .map_err(&self.map)
    }

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        self.indexer
            .on_receipt(receipt, tx, block)
            .await
            .map_err(&self.map)
    }
}

pub trait MapError {
    fn map_error<
        E: Debug + Send + Sync + 'static,
        E2: Debug + Send + Sync + 'static,
        F: Fn(E) -> E2 + Send + Sync + 'static,
    >(
        self,
        map: F,
    ) -> MapErrorIndexer<E, E2, Self, F>
    where
        Self: Indexer<Error = E> + Sized;
}

impl<I: Indexer> MapError for I {
    fn map_error<
        E: Debug + Send + Sync + 'static,
        E2: Debug + Send + Sync + 'static,
        F: Fn(E) -> E2 + Send + Sync + 'static,
    >(
        self,
        map: F,
    ) -> MapErrorIndexer<E, E2, Self, F>
    where
        Self: Indexer<Error = E> + Sized,
    {
        MapErrorIndexer::new(self, map)
    }
}
