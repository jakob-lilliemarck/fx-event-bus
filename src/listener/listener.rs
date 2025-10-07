use crate::EventHandlerRegistry;
use sqlx::PgPool;
use std::time::Duration;

/// Processes events from the event bus.
///
/// Listens for events, handles them with registered handlers,
/// and manages retry logic with exponential backoff.
pub struct Listener {
    // Database connection pool
    pub(super) pool: PgPool,
    // Registered event handlers
    pub(super) registry: EventHandlerRegistry,
    // Maximum retry attempts before DLQ
    pub(super) max_attempts: i32,
    // Base duration for exponential backoff
    pub(super) retry_duration: Duration,
    // Events processed since start
    pub(super) count: usize,
}

impl Listener {
    /// Creates a new listener with default configuration.
    ///
    /// Default settings: 3 max attempts, 15 second base retry duration.
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    /// * `registry` - Registry containing event handlers
    #[tracing::instrument(
        skip(pool, registry),
        fields(max_attempts = 3, retry_duration_ms = 15_000),
        level = "debug"
    )]
    pub fn new(pool: PgPool, registry: EventHandlerRegistry) -> Self {
        Listener {
            pool,
            registry,
            max_attempts: 3,
            retry_duration: Duration::from_millis(15_000),
            count: 0,
        }
    }

    /// Sets maximum retry attempts before moving events to dead letter queue.
    ///
    /// # Arguments
    ///
    /// * `max_attempts` - Maximum number of processing attempts (1-65535)
    #[tracing::instrument(
        skip(self),
        fields(max_attempts = max_attempts),
        level = "debug"
    )]
    pub fn with_max_attempts(mut self, max_attempts: u16) -> Self {
        self.max_attempts = max_attempts as i32;
        self
    }

    /// Sets base duration for exponential backoff retry delays.
    ///
    /// Actual delay = base_duration * 2^(attempt - 1)
    ///
    /// # Arguments
    ///
    /// * `retry_duration` - Base duration for retry calculations
    #[tracing::instrument(
        skip(self),
        fields(retry_duration_ms = retry_duration.as_millis()),
        level = "debug"
    )]
    pub fn with_retry_duration(mut self, retry_duration: Duration) -> Self {
        self.retry_duration = retry_duration;
        self
    }
}
