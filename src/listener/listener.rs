use crate::EventHandlerRegistry;
use sqlx::PgPool;
use std::time::Duration;

pub struct Listener {
    /// Database pool
    pub(super) pool: PgPool,
    /// The handler registry to handle events with
    pub(super) registry: EventHandlerRegistry,
    /// The maximum number of retries before moving to DLQ
    pub(super) max_attempts: i32,
    /// The base duration used to compute exponential backoff
    pub(super) retry_duration: Duration,
    /// The number of events processed by this listener since start
    pub(super) count: usize,
}

impl Listener {
    pub fn new(
        pool: PgPool,
        registry: EventHandlerRegistry,
    ) -> Self {
        Listener {
            pool,
            registry,
            max_attempts: 3,
            retry_duration: Duration::from_millis(15_000),
            count: 0,
        }
    }

    pub fn with_max_attempts(
        mut self,
        max_attempts: u16,
    ) -> Self {
        self.max_attempts = max_attempts as i32;
        self
    }

    pub fn with_retry_duration(
        mut self,
        retry_duration: Duration,
    ) -> Self {
        self.retry_duration = retry_duration;
        self
    }
}
