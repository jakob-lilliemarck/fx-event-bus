use crate::handler::event_handler::EventHandler;
use crate::models::{Event, RawEvent};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::any::Any;
use std::sync::Arc;

// Type-erased trait for handlers that can return different error types
trait ErasedEventHandler<E: Event>: Send + Sync {
    fn handle_erased<'a>(
        &'a self,
        input: Arc<E>,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), String>)>;
}

// Blanket implementation that converts any EventHandler to ErasedEventHandler
impl<E: Event, H: EventHandler<E>> ErasedEventHandler<E> for H {
    fn handle_erased<'a>(
        &'a self,
        input: Arc<E>,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), String>)> {
        let fut = self.handle(input, polled_at, tx);
        Box::pin(async move {
            let (tx, result) = fut.await;
            // map error to string directly.
            let converted_result = result.map_err(|err| err.to_string());
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
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), String>)>;
}

impl<E: Event + Clone + Sync + 'static> HandlerGroup for Group<E> {
    fn handle<'tx>(
        &'tx self,
        event: &RawEvent,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'tx>,
    ) -> BoxFuture<'tx, (PgTransaction<'tx>, Result<(), String>)> {
        // Only clone the payload for deserialization
        let payload = event.payload.clone();
        Box::pin(async move {
            let typed: E = match serde_json::from_value(payload) {
                Ok(typed) => typed,
                Err(err) => {
                    return (tx, Err(err.to_string()));
                }
            };

            // Wrap in Arc for cheap sharing across handlers
            let typed = Arc::new(typed);
            let mut current_tx = tx;
            let mut result = Ok(());

            for handler in &self.handlers {
                let (returned_tx, handler_result) = handler
                    .handle_erased(Arc::clone(&typed), polled_at, current_tx)
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
