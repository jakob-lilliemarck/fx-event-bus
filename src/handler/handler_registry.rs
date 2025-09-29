use super::handler_group::{Group, HandlerGroup};
use crate::{Event, EventHandler, models::RawEvent};
use chrono::{DateTime, Utc};
use sqlx::PgTransaction;
use std::{any::Any, collections::HashMap};

/// Registry for event handlers.
///
/// Manages handlers for different event types and dispatches
/// events to the appropriate handlers based on event type hash.
pub struct EventHandlerRegistry {
    // Map from event hash to handler group
    handlers: HashMap<i32, Box<dyn HandlerGroup>>,
}

impl EventHandlerRegistry {
    /// Creates a new empty handler registry.
    #[tracing::instrument(level = "debug")]
    pub fn new() -> EventHandlerRegistry {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Registers a handler for a specific event type.
    ///
    /// Multiple handlers can be registered for the same event type.
    /// They will be executed sequentially.
    ///
    /// # Arguments
    ///
    /// * `handler` - Handler implementing `EventHandler<E>`
    ///
    /// # Example
    ///
    /// ```rust
    /// let mut registry = EventHandlerRegistry::new();
    /// registry.with_handler::<OrderCreated, _>(OrderHandler);
    /// registry.with_handler::<OrderCreated, _>(EmailHandler);
    /// ```
    #[tracing::instrument(
        skip(self, handler),
        fields(
            event_name = E::NAME,
            event_hash = E::HASH
        ),
        level = "debug"
    )]
    pub fn with_handler<E, H>(
        &mut self,
        handler: H,
    ) where
        E: Event + Clone,
        H: EventHandler<E> + 'static,
    {
        // Get or create the group
        let group = self
            .handlers
            .entry(E::HASH)
            .or_insert(Box::new(Group::<E>::new()));

        // Convert to &mut dyn Any in order to be able to downcast
        let any_ref = group.as_mut() as &mut (dyn Any + '_);

        // Downcast the trait object back to concrete type
        let group = any_ref
            .downcast_mut::<Group<E>>()
            .expect("Could not downcast to group. This indicates a hash collision between event types");

        group.register(handler);
    }

    #[tracing::instrument(
        skip(self, event, tx),
        fields(
            event_id = %event.id,
            event_name = event.name,
            event_hash = event.hash,
            polled_at = %polled_at
        )
    )]
    pub async fn handle<'tx>(
        &'tx self,
        event: &RawEvent,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'tx>,
    ) -> (PgTransaction<'tx>, Result<(), String>) {
        match self.handlers.get(&event.hash) {
            Some(group) => group.handle(event, polled_at, tx).await,
            None => (tx, Ok(())),
        }
    }
}
