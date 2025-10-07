use crate::models::Event;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::sync::Arc;

/// Handles events of a specific type.
///
/// Implement this trait to process events. Handlers receive events
/// wrapped in `Arc` for efficient sharing across multiple handlers.
///
/// # Example
///
/// ```rust
/// use fx_event_bus::{Handler, Event};
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
/// use chrono::{DateTime, Utc};
/// use sqlx::PgTransaction;
/// use futures::future::BoxFuture;
/// use thiserror::Error;
///
/// #[derive(Serialize, Deserialize, Clone)]
/// struct OrderCreated { order_id: u64 }
///
/// impl Event for OrderCreated {
///     const NAME: &'static str = "OrderCreated";
/// }
///
/// #[derive(Error, Debug)]
/// #[error("Order processing failed")]
/// struct OrderError;
///
/// struct OrderHandler;
///
/// impl Handler<OrderCreated> for OrderHandler {
///     type Error = OrderError;
///
///     fn handle<'a>(
///         &'a self,
///         event: Arc<OrderCreated>,
///         polled_at: DateTime<Utc>,
///         tx: PgTransaction<'a>,
///     ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), Self::Error>)> {
///         Box::pin(async move {
///             // Process the order...
///             println!("Processing order {}", event.order_id);
///             (tx, Ok(()))
///         })
///     }
/// }
/// ```
pub trait Handler<E: Event>: Send + Sync {
    /// Error type returned by this handler
    type Error: std::error::Error + Send + Sync + 'static;

    /// Process an event within a database transaction.
    ///
    /// # Arguments
    ///
    /// * `input` - The event to process (wrapped in Arc for efficiency)
    /// * `polled_at` - When the event was picked up for processing
    /// * `tx` - Database transaction to use for any operations
    ///
    /// # Returns
    ///
    /// Returns the transaction and either success or an error.
    /// On error, the event will be retried according to retry configuration.
    fn handle<'a>(
        &'a self,
        input: Arc<E>,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), Self::Error>)>;
}
