//! A reliable, high-performance event bus for Rust applications.
//!
//! Built on PostgreSQL for durability and ACID guarantees, with support for
//! event publishing, consumption, retry logic with exponential backoff, and
//! dead letter queues for failed events.
//!
//! # Quick Start
//!
//! ```rust
//! use fx_event_bus::*;
//!
//! // 1. Define your event
//! #[derive(Serialize, Deserialize, Clone)]
//! struct OrderCreated { order_id: u64 }
//!
//! impl Event for OrderCreated {
//!     const NAME: &'static str = "OrderCreated";
//! }
//!
//! // 2. Create a handler
//! struct OrderHandler;
//! impl EventHandler<OrderCreated> for OrderHandler {
//!     // ... handle implementation
//! }
//!
//! // 3. Set up the event bus
//! let mut registry = EventHandlerRegistry::new();
//! registry.with_handler::<OrderCreated, _>(OrderHandler);
//!
//! let listener = Listener::new(pool, registry)
//!     .with_max_attempts(3)
//!     .with_retry_duration(Duration::from_secs(30));
//!
//! // 4. Publish events
//! let mut publisher = Publisher::new(tx);
//! publisher.publish(OrderCreated { order_id: 123 }).await?;
//!
//! // 5. Start processing
//! listener.listen(None).await?;
//! ```

mod handler;
mod listener;
mod migrations;
mod models;
mod publisher;

#[cfg(test)]
mod test_tools;

pub use handler::{Handler, EventHandlerRegistry};
pub use listener::Listener;
pub use migrations::run_migrations;
pub use models::{Event, RawEvent};
pub use publisher::{Publisher, PublisherError};
