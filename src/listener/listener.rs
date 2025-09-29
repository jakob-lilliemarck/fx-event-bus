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
    #[tracing::instrument(
        skip(pool, registry),
        fields(max_attempts = 3, retry_duration_ms = 15_000),
        level = "debug"
    )]
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

    #[tracing::instrument(
        skip(self),
        fields(max_attempts = max_attempts),
        level = "debug"
    )]
    pub fn with_max_attempts(
        mut self,
        max_attempts: u16,
    ) -> Self {
        self.max_attempts = max_attempts as i32;
        self
    }

    #[tracing::instrument(
        skip(self),
        fields(retry_duration_ms = retry_duration.as_millis()),
        level = "debug"
    )]
    pub fn with_retry_duration(
        mut self,
        retry_duration: Duration,
    ) -> Self {
        self.retry_duration = retry_duration;
        self
    }
}
