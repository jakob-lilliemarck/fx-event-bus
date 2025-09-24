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

pub struct EventHandlerRegistry {
    handlers: HashMap<i32, Box<dyn HandlerGroup>>,
}

impl EventHandlerRegistry {
    pub fn new() -> EventHandlerRegistry {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register<H>(
        mut self,
        group: H,
    ) -> Result<Self, EventRegistryError>
    where
        H: HandlerGroup + 'static,
    {
        // Check for hash collision
        self.check_for_collision::<H>(&group)?;

        // Register the handler
        self.handlers.insert(group.event_hash(), Box::new(group));

        Ok(self)
    }

    fn check_for_collision<H: HandlerGroup>(
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

    pub async fn handle<'tx>(
        &'tx self,
        event: RawEvent,
        tx: PgTransaction<'tx>,
    ) -> (PgTransaction<'tx>, Result<(), EventHandlingError>) {
        match self.handlers.get(&event.hash) {
            Some(group) => group.handle(event, tx).await,
            None => (tx, Ok(())),
        }
    }
}
