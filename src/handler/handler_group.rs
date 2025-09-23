use super::errors::EventHandlingError;
use crate::models::{Event, RawEvent};
use futures::future::BoxFuture;
use sqlx::PgTransaction;

pub trait TakeTx<'tx> {
    // Must be &mut to allow boxing
    // Put the transaction in an Option!
    fn take(&mut self) -> PgTransaction<'tx>;
}

pub trait IntoTx<'tx, E: Event> {
    fn to_tx(
        &self,
        tx: PgTransaction<'tx>,
    ) -> Box<dyn TxHandler<'tx, E>>;
}

// Transaction handler - something that implements handler and to yield its transaction
pub trait TxHandler<'tx, E: Event>: Handler<'tx, E> + TakeTx<'tx> {}

pub trait Handler<'tx, E>: IntoTx<'tx, E> + Send + Sync
where
    E: Event,
{
    fn handle(
        &mut self,
        input: &E,
    ) -> BoxFuture<'tx, Result<(), EventHandlingError>>;
}

pub struct Group<'tx, E: Event> {
    handlers: Vec<Box<dyn Handler<'tx, E>>>,
}

impl<'tx, E: Event> Group<'tx, E> {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    pub fn register<H>(
        mut self,
        handler: H,
    ) -> Self
    where
        H: Handler<'tx, E> + 'static,
    {
        self.handlers.push(Box::new(handler));
        self
    }

    pub async fn handle(
        &'tx self,
        event: RawEvent,
        tx: sqlx::PgTransaction<'tx>,
    ) -> (PgTransaction<'tx>, Result<(), EventHandlingError>) {
        let typed = match serde_json::from_value::<E>(event.payload) {
            Ok(typed) => typed,
            Err(err) => {
                return (
                    tx,
                    Err(EventHandlingError::DeserializationError(err)),
                );
            }
        };

        let mut token = Some(tx);
        let mut result = Ok(());

        for handler in &self.handlers {
            let tx = token.take().expect("Tx token should be Some");
            let mut tx_handler = handler.to_tx(tx);
            match tx_handler.handle(&typed).await {
                Ok(()) => {
                    token = Some(tx_handler.take());
                }
                Err(err) => {
                    // Put transaction back and stop processing
                    token = Some(tx_handler.take());
                    result = Err(err);
                    break;
                }
            }
        }

        let tx = token.take().expect("Tx token should be Some");
        (tx, result)
    }
}

pub trait HandlerGroup<'a> {
    fn handle_tx<'tx>(
        &'tx self,
        event: RawEvent,
        tx: sqlx::PgTransaction<'tx>,
    ) -> BoxFuture<
        'tx,
        (sqlx::PgTransaction<'tx>, Result<(), EventHandlingError>),
    >
    where
        'tx: 'a;

    fn handle(
        &self,
        event: RawEvent,
    ) -> BoxFuture<'_, Result<(), EventHandlingError>>;

    fn event_hash(&self) -> i32;

    fn event_name(&self) -> &'static str;
}

/*
*
* where
    H: EventHandler + Send + Sync,
    H: for<'tx> FromOther<'tx>,
    for<'tx> <H as FromOther<'tx>>::TxType:
        TxEventHandler<'tx, Input = H::Input> + Send,
    for<'tx> <H as FromOther<'tx>>::TxType: Into<sqlx::PgTransaction<'tx>>,
*/

impl<'a, E: Event> HandlerGroup<'a> for Group<'a, E> {
    fn handle_tx<'tx>(
        &'tx self,
        event: RawEvent,
        tx: sqlx::PgTransaction<'tx>,
    ) -> BoxFuture<
        'tx,
        (sqlx::PgTransaction<'tx>, Result<(), EventHandlingError>),
    >
    where
        'tx: 'a,
    {
        Box::pin(async move { self.handle(event, tx).await })
    }

    fn handle(
        &self,
        _: RawEvent,
    ) -> BoxFuture<'_, Result<(), EventHandlingError>> {
        Box::pin(async move { todo!() })
    }

    fn event_hash(&self) -> i32 {
        E::HASH
    }

    fn event_name(&self) -> &'static str {
        E::NAME
    }
}
