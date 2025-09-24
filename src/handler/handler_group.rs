use super::errors::EventHandlingError;
use crate::handler::event_handler::EventHandler;
use crate::models::{Event, RawEvent};
use futures::future::BoxFuture;
use sqlx::PgTransaction;

pub struct Group<E: Event> {
    handlers: Vec<Box<dyn EventHandler<E>>>,
}

impl<E: Event + Clone> Group<E> {
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
        H: EventHandler<E> + 'static,
    {
        self.handlers.push(Box::new(handler));
        self
    }
}

pub trait HandlerGroup: Send + Sync {
    fn handle<'tx>(
        &'tx self,
        event: RawEvent,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), EventHandlingError>)>;

    fn event_hash(&self) -> i32;

    fn event_name(&self) -> &'static str;
}

impl<E: Event + Clone + 'static> HandlerGroup for Group<E> {
    fn handle<'tx>(
        &'tx self,
        event: RawEvent,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), EventHandlingError>)>
    {
        Box::pin(async move {
            let typed: E = match serde_json::from_value(event.payload) {
                Ok(typed) => typed,
                Err(err) => {
                    return (
                        tx,
                        Err(EventHandlingError::DeserializationError(err)),
                    );
                }
            };

            let mut current_tx = tx;
            let mut result = Ok(());

            for handler in &self.handlers {
                let (returned_tx, handler_result) =
                    handler.handle(typed.clone(), current_tx).await;
                current_tx = returned_tx;

                if let Err(err) = handler_result {
                    result = Err(err);
                    break; // Stop on first error
                }
            }

            (current_tx, result)
        })
    }

    fn event_hash(&self) -> i32 {
        E::HASH
    }

    fn event_name(&self) -> &'static str {
        E::NAME
    }
}
