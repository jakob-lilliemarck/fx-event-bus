use crate::{
    EventHandler, EventHandlingError, models::Event, test_utils::TestEvent,
};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::sync::Arc;
use tokio::sync::Mutex;

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
pub struct HandlerAlpha {
    state: Arc<Mutex<SharedHandlerState>>,
}

impl HandlerAlpha {
    pub fn new(state: &Arc<Mutex<SharedHandlerState>>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}
impl EventHandler<TestEvent> for HandlerAlpha {
    fn handle<'a>(
        &'a self,
        _input: TestEvent,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>
    {
        Box::pin(async move {
            let hash = TestEvent::HASH;
            let name = TestEvent::NAME;
            let mut lock = self.state.lock().await;
            if let Some(fail_hash) = lock.fail {
                if fail_hash == hash {
                    lock.seen(hash, name.to_string(), false);
                    return (
                        tx,
                        Err(EventHandlingError::BusinessLogicError(
                            "whatever".to_string(),
                        )),
                    );
                }
            }
            lock.seen(hash, name.to_string(), true);
            (tx, Ok(()))
        })
    }
}

// ============================================================
// Beta
// ============================================================
pub struct HandlerBeta {
    state: Arc<Mutex<SharedHandlerState>>,
}

impl HandlerBeta {
    pub fn new(state: &Arc<Mutex<SharedHandlerState>>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl EventHandler<TestEvent> for HandlerBeta {
    fn handle<'a>(
        &'a self,
        _input: TestEvent,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>
    {
        Box::pin(async move {
            let hash = TestEvent::HASH;
            let name = TestEvent::NAME;
            let mut lock = self.state.lock().await;
            if let Some(fail_hash) = lock.fail {
                if fail_hash == hash {
                    lock.seen(hash, name.to_string(), false);
                    return (
                        tx,
                        Err(EventHandlingError::BusinessLogicError(
                            "whatever".to_string(),
                        )),
                    );
                }
            }
            lock.seen(hash, name.to_string(), true);
            (tx, Ok(()))
        })
    }
}
