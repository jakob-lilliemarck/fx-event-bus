use super::errors::EventHandlingError;
use crate::handler::event_handler::EventHandler;
use crate::models::{Event, RawEvent};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::any::Any;

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
        &mut self,
        handler: H,
    ) where
        H: EventHandler<E> + 'static,
    {
        self.handlers.push(Box::new(handler));
    }
}

pub trait HandlerGroup: Send + Sync + Any {
    fn handle<'tx>(
        &'tx self,
        event: RawEvent,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), EventHandlingError>)>;
}

impl<E: Event + Clone + 'static> HandlerGroup for Group<E> {
    fn handle<'tx>(
        &'tx self,
        event: RawEvent,
        polled_at: DateTime<Utc>,
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
                    handler.handle(typed.clone(), polled_at, current_tx).await;
                current_tx = returned_tx;

                if let Err(err) = handler_result {
                    result = Err(err);
                    break; // Stop on first error
                }
            }

            (current_tx, result)
        })
    }
}
