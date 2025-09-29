use super::errors::EventHandlingError;
use crate::handler::event_handler::EventHandler;
use crate::models::{Event, RawEvent};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::any::Any;

// Type-erased trait for handlers that can return different error types
trait ErasedEventHandler<E: Event>: Send + Sync {
    fn handle_erased<'a>(
        &'a self,
        input: E,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>;
}

// Blanket implementation that converts any EventHandler to ErasedEventHandler
impl<E: Event, H: EventHandler<E>> ErasedEventHandler<E> for H {
    fn handle_erased<'a>(
        &'a self,
        input: E,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>
    {
        let fut = self.handle(input, polled_at, tx);
        Box::pin(async move {
            let (tx, result) = fut.await;
            let converted_result = result
                .map_err(|err| EventHandlingError::HandlerError(Box::new(err)));
            (tx, converted_result)
        })
    }
}

pub struct Group<E: Event> {
    handlers: Vec<Box<dyn ErasedEventHandler<E>>>,
}

impl<E: Event + Clone> Group<E> {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    #[tracing::instrument(skip(self, handler), level = "debug")]
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
        event: &RawEvent,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), EventHandlingError>)>;
}

impl<E: Event + Clone + 'static> HandlerGroup for Group<E> {
    fn handle<'tx>(
        &'tx self,
        event: &RawEvent,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), EventHandlingError>)>
    {
        let event = event.clone();
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
                let (returned_tx, handler_result) = handler
                    .handle_erased(typed.clone(), polled_at, current_tx)
                    .await;
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
