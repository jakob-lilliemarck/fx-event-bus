use super::{EventHandlingError, HandlerGroup};
use crate::models::RawEvent;
use sqlx::PgTransaction;
use std::collections::HashMap;

#[derive(thiserror::Error, Debug)]
pub enum EventRegistryError {
    #[error(
        "Hash collision detected: hash {hash} used by both '{existing}' and '{current}'"
    )]
    HashCollision {
        hash: i32,
        current: String,
        existing: String,
    },
}

pub struct EventHandlerRegistry<'a> {
    handlers: HashMap<i32, Box<dyn HandlerGroup<'a> + Send + Sync>>,
}

impl<'a> EventHandlerRegistry<'a> {
    pub fn new() -> EventHandlerRegistry<'a> {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register<H>(
        mut self,
        group: H,
    ) -> Result<Self, EventRegistryError>
    where
        H: HandlerGroup<'a> + Send + Sync + 'static,
    {
        // Check for hash collision
        self.check_for_collision::<H>(&group)?;

        // Register the handler
        self.handlers.insert(group.event_hash(), Box::new(group));

        Ok(self)
    }

    fn check_for_collision<H: HandlerGroup<'a>>(
        &self,
        handler: &H,
    ) -> Result<(), EventRegistryError> {
        let hash = handler.event_hash();
        let name = handler.event_name();

        if let Some(existing) = self.handlers.get(&hash) {
            if existing.event_name() != name {
                return Err(EventRegistryError::HashCollision {
                    hash: hash,
                    current: name.to_owned(),
                    existing: existing.event_name().to_owned(),
                });
            }
        }

        Ok(())
    }

    /// Routes the event to its handler group by hash and executes it.
    pub async fn handle(
        &mut self,
        event: RawEvent,
    ) -> Result<(), EventHandlingError> {
        match self.handlers.get_mut(&event.hash) {
            Some(group) => group.handle(event).await,
            None => Err(EventHandlingError::BusinessLogicError(format!(
                "No handler group registered for event hash {}",
                event.hash
            ))),
        }
    }

    pub async fn handle_tx<'tx>(
        &'a self,
        event: RawEvent,
        tx: PgTransaction<'tx>,
    ) -> (sqlx::PgTransaction<'tx>, Result<(), EventHandlingError>)
    where
        'a: 'tx,
        'tx: 'a,
    {
        match self.handlers.get(&event.hash) {
            Some(group) => group.handle_tx(event, tx).await,
            None => (tx, Ok(())),
        }
    }
}
