use super::errors::EventHandlingError;
use super::{EventHandler, TxEventHandler};
use crate::{
    chainable::FromOther,
    models::{Event, RawEvent},
};
use futures::future::BoxFuture;

pub struct Group<H: EventHandler> {
    handlers: Vec<H>,
}

impl<H: EventHandler> Group<H> {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    pub fn register(
        mut self,
        handler: H,
    ) -> Self {
        self.handlers.push(handler);
        self
    }
}

pub trait HandlerGroup {
    fn handle(
        &mut self,
        payload: RawEvent,
    ) -> BoxFuture<'_, Result<(), EventHandlingError>>;
    fn handle_tx<'tx>(
        &'tx self,
        event: RawEvent,
        tx: sqlx::PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (sqlx::PgTransaction<'tx>, Result<(), EventHandlingError>)>;
    fn event_hash(&self) -> i32;
    fn event_name(&self) -> &'static str;
}

impl<H> HandlerGroup for Group<H>
where
    H: EventHandler + Send + Sync,
    H: for<'tx> FromOther<'tx>,
    for<'tx> <H as FromOther<'tx>>::TxType:
        TxEventHandler<'tx, Input = H::Input> + Send,
    for<'tx> <H as FromOther<'tx>>::TxType: Into<sqlx::PgTransaction<'tx>>,
{
    fn handle(
        &mut self,
        event: RawEvent,
    ) -> BoxFuture<'_, Result<(), EventHandlingError>> {
        Box::pin(async move {
            let typed = serde_json::from_value::<H::Input>(event.payload)?;

            for handler in &mut self.handlers {
                handler.handle(&typed).await?;
            }
            Ok(())
        })
    }

    fn handle_tx<'tx>(
        &'tx self,
        event: RawEvent,
        tx: sqlx::PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (sqlx::PgTransaction<'tx>, Result<(), EventHandlingError>)>
    {
        Box::pin(async move {
            let typed = match serde_json::from_value::<H::Input>(event.payload) {
                Ok(typed) => typed,
                Err(err) => return (tx, Err(EventHandlingError::DeserializationError(err))),
            };
            let mut token = Some(tx);
            let mut result = Ok(());

            for handler in &self.handlers {
                let tx = token.take().expect("Tx token should be Some");
                let mut tx_handler = handler.from(tx);
                match tx_handler.handle(&typed).await {
                    Ok(()) => {
                        token = Some(tx_handler.into());
                    }
                    Err(err) => {
                        // Put transaction back and stop processing
                        token = Some(tx_handler.into());
                        result = Err(err);
                        break;
                    }
                }
            }

            let tx = token.take().expect("Tx token should be Some");
            (tx, result)
        })
    }

    fn event_hash(&self) -> i32 {
        <H as EventHandler>::Input::HASH
    }

    fn event_name(&self) -> &'static str {
        <H as EventHandler>::Input::NAME
    }
}
