use super::{EventHandlingError, HandlerGroup};
use crate::{chainable::FromOther, models::RawEvent};
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
    handlers: HashMap<i32, Box<dyn HandlerGroup + Send + Sync>>,
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
        H: HandlerGroup + Send + Sync + 'static,
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
}

pub struct TxEventHandlerRegistry<'tx> {
    handlers: &'tx HashMap<i32, Box<dyn HandlerGroup + Send + Sync>>,
    /// Can hold a transaction for handling events.
    tx: Option<PgTransaction<'tx>>,
}

impl<'tx> Into<PgTransaction<'tx>> for TxEventHandlerRegistry<'tx> {
    fn into(mut self) -> PgTransaction<'tx> {
        if let Some(tx) = self.tx.take() {
            tx
        } else {
            // FIXME!
            // I do not think this could logically ever happen? But consider using a generic to cofirm saftey through the typesystem?
            panic!(
                "PgTransaction could not be retrieved from TxEventHandlerRegistry"
            );
        }
    }
}

impl<'tx> FromOther<'tx> for EventHandlerRegistry {
    type TxType = TxEventHandlerRegistry<'tx>;

    fn from(
        &'tx self,
        other: impl Into<PgTransaction<'tx>>,
    ) -> Self::TxType {
        TxEventHandlerRegistry {
            handlers: &self.handlers,
            tx: Some(other.into()),
        }
    }
}

impl<'tx> TxEventHandlerRegistry<'tx> {
    pub async fn handle(
        &mut self,
        event: RawEvent,
    ) -> Result<(), EventHandlingError> {
        match self.handlers.get(&event.hash) {
            Some(group) => {
                if let Some(tx) = self.tx.take() {
                    let (tx, result) = group.handle_tx(event, tx).await;
                    self.tx = Some(tx);
                    result
                } else {
                    panic!("This should never happen")
                }
            }
            None => Err(EventHandlingError::BusinessLogicError(format!(
                "No handler group registered for event hash {}",
                event.hash
            ))),
        }
    }
}
