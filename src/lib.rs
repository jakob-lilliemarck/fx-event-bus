//! A reliable event bus for monolithic Rust applications.
//!
//! Built on PostgreSQL for durability and ACID guarantees, with support for
//! event publishing, consumption, retry logic with exponential backoff, and
//! dead letter queues for failed events.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use fx_event_bus::*;
//! use serde::{Serialize, Deserialize};
//! use std::time::Duration;
//! use std::sync::Arc;
//! use chrono::{DateTime, Utc};
//! use sqlx::PgTransaction;
//! use futures::future::BoxFuture;
//! use thiserror::Error;
//!
//! // 1. Define your event
//! #[derive(Serialize, Deserialize, Clone)]
//! struct OrderCreated { order_id: u64 }
//!
//! impl Event for OrderCreated {
//!     const NAME: &'static str = "OrderCreated";
//! }
//!
//! #[derive(Error, Debug)]
//! #[error("Order processing failed: {0}")]
//! struct OrderError(String);
//!
//! // 2. Create a handler
//! struct OrderHandler;
//! impl Handler<OrderCreated> for OrderHandler {
//!     type Error = OrderError;
//!
//!     fn handle<'a>(
//!         &'a self,
//!         event: Arc<OrderCreated>,
//!         polled_at: DateTime<Utc>,
//!         tx: PgTransaction<'a>,
//!     ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), Self::Error>)> {
//!         Box::pin(async move {
//!             // Handle the order creation
//!             println!("Order {} created!", event.order_id);
//!             (tx, Ok(()))
//!         })
//!     }
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let pool = sqlx::PgPool::connect("postgresql://localhost/test").await?;
//! # let mut tx = pool.begin().await?;
//!
//! // 3. Set up the event bus
//! let mut registry = EventHandlerRegistry::new();
//! registry.with_handler::<OrderCreated, _>(OrderHandler);
//!
//! let listener = Listener::new(pool.clone(), registry)
//!     .with_max_attempts(3)
//!     .with_retry_duration(Duration::from_secs(30));
//!
//! // 4. Publish events
//! let mut publisher = Publisher::new(tx);
//! publisher.publish(OrderCreated { order_id: 123 }).await?;
//! let tx: PgTransaction<'_> = publisher.into();
//! tx.commit().await?;
//!
//! // 5. Start processing (in a real app)
//! // listener.listen(None).await?;
//! # Ok(())
//! # }
//! ```

mod handler;
mod listener;
mod migrations;
mod models;
mod publisher;

#[cfg(any(test, feature = "test-tools"))]
pub mod test_tools;

pub use handler::{EventHandlerRegistry, Handler};
pub use listener::Listener;
pub use migrations::run_migrations;
pub use models::Event;
pub use publisher::{Publisher, PublisherError};
