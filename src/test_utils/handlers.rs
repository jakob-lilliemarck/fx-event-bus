use crate::{
    EventHandler, EventHandlingError, listener::listener::Listener,
    models::Event, test_utils::TestEvent,
};
use futures::future::BoxFuture;
use sqlx::{PgPool, PgTransaction};
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

// ============================================================
// Gamma
// ============================================================
pub struct HandlerGamma {
    pool: PgPool,
}

impl HandlerGamma {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl EventHandler<TestEvent> for HandlerGamma {
    fn handle<'a>(
        &'a self,
        _input: TestEvent,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>
    {
        let pool = self.pool.clone();
        Box::pin(async move {
            let mut inner_tx =
                pool.begin().await.expect("Could not start transaction");

            match Listener::acknowledge(&mut inner_tx).await {
                Ok(Some(event)) => {
                    panic!("Expected not to find event. Found {:?}", event.id);
                }
                Err(err) => {
                    panic!("Unexpected error: {:?}", err)
                }
                Ok(None) => {
                    tracing::info!("No event found");
                }
            }

            inner_tx.commit().await.expect("Failed to commit");

            (tx, Ok(()))
        })
    }
}
