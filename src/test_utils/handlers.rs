use super::events::TestEvent;
use crate::{
    Event, EventHandler, EventHandlingError, FromOther, TxEventHandler,
};
use sqlx::PgTransaction;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NilTx;

// ============================================================
// Shared handler state
// ============================================================
#[derive(Clone)]
pub struct SharedHandlerState {
    pub seen: Vec<(i32, String, bool)>,
    fail: Option<i32>,
}

impl SharedHandlerState {
    pub fn new(fail: Option<i32>) -> Self {
        Self {
            seen: Vec::new(),
            fail: fail,
        }
    }

    pub fn seen(
        &mut self,
        id: i32,
        name: String,
        success: bool,
    ) {
        self.seen.push((id, name, success));
    }

    pub fn count(&self) -> usize {
        self.seen.len()
    }
}

// ============================================================
// Alpha
// ============================================================
pub struct HandlerAlpha<E> {
    executor: E,
    state: Arc<Mutex<SharedHandlerState>>,
}

impl HandlerAlpha<NilTx> {
    pub fn new(state: &Arc<Mutex<SharedHandlerState>>) -> Self {
        Self {
            executor: NilTx,
            state: state.clone(),
        }
    }
}

impl<'tx> Into<PgTransaction<'tx>> for HandlerAlpha<PgTransaction<'tx>> {
    fn into(self) -> PgTransaction<'tx> {
        self.executor
    }
}

impl<'tx, E> FromOther<'tx> for HandlerAlpha<E> {
    type TxType = HandlerAlpha<PgTransaction<'tx>>;

    fn from(
        &self,
        other: impl Into<PgTransaction<'tx>>,
    ) -> HandlerAlpha<PgTransaction<'tx>> {
        HandlerAlpha {
            executor: other.into(),
            state: self.state.clone(),
        }
    }
}

impl<E> EventHandler for HandlerAlpha<E>
where
    Self: Send,
{
    type Input = TestEvent;

    async fn handle(
        &mut self,
        input: &Self::Input,
    ) -> Result<(), EventHandlingError> {
        handle_alpha(self, input).await
    }
}

// FIXME: I would like to get rid of this!
impl<'tx, E> TxEventHandler<'tx> for HandlerAlpha<E>
where
    Self: Send,
{
    type Input = TestEvent;

    async fn handle(
        &mut self,
        input: &Self::Input,
    ) -> Result<(), EventHandlingError> {
        handle_alpha(self, input).await
    }
}

async fn handle_alpha<E>(
    handler: &mut HandlerAlpha<E>,
    _: &TestEvent,
) -> Result<(), EventHandlingError>
where
    HandlerAlpha<E>: Send,
{
    let hash = <<HandlerAlpha<E> as EventHandler>::Input as Event>::HASH;
    let name = <<HandlerAlpha<E> as EventHandler>::Input as Event>::NAME;
    let mut lock = handler.state.lock().await;
    if let Some(fail_hash) = lock.fail {
        if fail_hash == hash {
            lock.seen(hash, name.to_string(), false);
            return Err(EventHandlingError::BusinessLogicError(
                "whatever".to_string(),
            ));
        }
    }
    lock.seen(hash, name.to_string(), true);
    Ok(())
}

// ============================================================
// Beta
// ============================================================
pub struct HandlerBeta<E> {
    executor: E,
    state: Arc<Mutex<SharedHandlerState>>,
}

impl HandlerBeta<NilTx> {
    pub fn new(state: &Arc<Mutex<SharedHandlerState>>) -> Self {
        Self {
            executor: NilTx,
            state: state.clone(),
        }
    }
}

impl<'tx> Into<PgTransaction<'tx>> for HandlerBeta<PgTransaction<'tx>> {
    fn into(self) -> PgTransaction<'tx> {
        self.executor
    }
}

impl<'tx, E> FromOther<'tx> for HandlerBeta<E> {
    type TxType = HandlerBeta<PgTransaction<'tx>>;

    fn from(
        &self,
        other: impl Into<PgTransaction<'tx>>,
    ) -> HandlerBeta<PgTransaction<'tx>> {
        HandlerBeta {
            executor: other.into(),
            state: self.state.clone(),
        }
    }
}

impl<E> EventHandler for HandlerBeta<E>
where
    Self: Send,
{
    type Input = TestEvent;

    async fn handle(
        &mut self,
        input: &Self::Input,
    ) -> Result<(), EventHandlingError> {
        handle_beta(self, input).await
    }
}

// FIXME: I would like to get rid of this!
impl<'tx, E> TxEventHandler<'tx> for HandlerBeta<E>
where
    Self: Send,
{
    type Input = TestEvent;

    async fn handle(
        &mut self,
        input: &Self::Input,
    ) -> Result<(), EventHandlingError> {
        handle_beta(self, input).await
    }
}

async fn handle_beta<E>(
    handler: &mut HandlerBeta<E>,
    _: &TestEvent,
) -> Result<(), EventHandlingError>
where
    HandlerBeta<E>: Send,
{
    let hash = <<HandlerBeta<E> as EventHandler>::Input as Event>::HASH;
    let name = <<HandlerBeta<E> as EventHandler>::Input as Event>::NAME;
    let mut lock = handler.state.lock().await;
    if let Some(fail_hash) = lock.fail {
        if fail_hash == hash {
            lock.seen(hash, name.to_string(), false);
            return Err(EventHandlingError::BusinessLogicError(
                "whatever".to_string(),
            ));
        }
    }
    lock.seen(hash, name.to_string(), true);
    Ok(())
}
