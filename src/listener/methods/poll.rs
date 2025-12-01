use super::super::Listener;
use chrono::{DateTime, Utc};
use uuid::Uuid;

impl Listener {
    /// Polls for and processes a single event.
    ///
    /// Prioritizes unacknowledged events over retry events. Handles the event
    /// with registered handlers and manages success/failure reporting.
    ///
    /// # Arguments
    ///
    /// * `now` - Current timestamp for processing
    ///
    /// # Returns
    ///
    /// Returns the UUID of the processed event, or `None` if no events were available.
    ///
    /// # Errors
    ///
    /// Returns database errors if polling or transaction operations fail.
    #[tracing::instrument(
        skip(self),
        fields(timestamp = %now),
        level = "debug",
        err
    )]
    pub async fn poll(&self, now: DateTime<Utc>) -> Result<Option<Uuid>, sqlx::Error> {
        // begin a transaction
        let mut tx = self.pool.begin().await?;

        // Priority: new events (unacknowledged) before retries
        // This ensures fresh events are processed before failed attempts
        let event = match Self::poll_unacknowledged(&mut tx, now).await? {
            None => match Self::poll_retryable(&mut tx, now).await? {
                None => {
                    tracing::debug!("Poll found no events");
                    return Ok(None);
                }
                Some(event) => event,
            },
            Some(event) => event,
        };
        tracing::debug!(event = %event.id, attempted = %event.attempted, "Poll found event");

        let event_id = event.id;

        // keep the event attempted for later use
        let attempted = event.attempted as u32;

        // use the transaction registry to handle the event
        let (mut tx, result) = self.registry.handle(&event, now, tx).await;

        if let Err(error_str) = result {
            // if the handling failed, fail the event
            self.report_failure(&mut tx, event_id, attempted + 1, now, error_str)
                .await?;
        } else {
            // otherwise succeed the event
            self.report_success(&mut tx, event_id, now).await?;
        }
        // commit the transaction
        tx.commit().await?;
        return Ok(Some(event_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventHandlerRegistry;
    use crate::test_tools::{
        FailingHandler, SucceedingHandler, is_acknowledged, is_dead, is_failed, is_succeeded,
        is_unacknowledged,
    };
    use crate::test_tools::{TestEvent, init_tracing};
    use sqlx::PgTransaction;
    use std::time::Duration;

    // Also assert it returns true on successful handling
    #[sqlx::test(migrations = "./migrations")]
    async fn it_prioritizes_unacknowledged_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let _event_1 = publisher.publish(TestEvent::default()).await?;
        let event_2 = publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        // acknowledge the first event
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        // Report the the attempt to handle the acked event as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;
        tx.commit().await?;

        // Poll for the next event at the time when the retryable event is ready
        let polled_event_id = listener
            .poll(now + duration)
            .await?
            .expect("Expected poll to return an event");

        // Expect the polled_event_id to be the ID of event_2, as unacknowledged events should be prioritized before retryables
        assert!(polled_event_id == event_2.id);
        Ok(())
    }

    // Test that it returns events to retry when there are failed attempts and no unacknowledged events
    #[sqlx::test(migrations = "./migrations")]
    async fn it_retries_failed_attempts(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        // acknowledge the event
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        // Report the the attempt to handle the acked event as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;
        tx.commit().await?;

        // Poll for the next event at the time when the retryable event is ready
        let polled_event_id = listener
            .poll(now + duration)
            .await?
            .expect("Expected poll to return an event");

        // Expect the polled event ID to be the event_id of the failed attempt
        assert!(polled_event_id == acked_event.id);
        Ok(())
    }

    // Test that it retries when there are no unacknowledged events
    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_nothing_was_handled(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let listener =
            Listener::new(pool.clone(), EventHandlerRegistry::new()).with_max_attempts(2);

        let polled_event_id = listener.poll(now).await?;

        assert!(polled_event_id.is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_creates_failed_attempt_on_failed_handling(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        let duration = Duration::from_secs(15);
        let mut registry = EventHandlerRegistry::new();

        // Register the failing handler
        registry.with_handler(FailingHandler);

        let listener = Listener::new(pool.clone(), registry)
            .with_max_attempts(2)
            .with_retry_duration(duration);

        // Poll for the next event at the time when the retryable event is ready
        listener
            .poll(now)
            .await?
            .expect("Expected poll to return an event");

        // Expect the event to be acknowledged and the attempt to be failed
        assert!(is_failed(&pool, published_event.id).await?);
        assert!(is_acknowledged(&pool, published_event.id).await?);

        assert!(!is_unacknowledged(&pool, published_event.id).await?);
        assert!(!is_succeeded(&pool, published_event.id).await?);
        assert!(!is_dead(&pool, published_event.id).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_creates_success_attempt_on_successful_handling(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        let duration = Duration::from_secs(15);
        let mut registry = EventHandlerRegistry::new();

        // Register the succeeding handler
        registry.with_handler(SucceedingHandler);

        let listener = Listener::new(pool.clone(), registry)
            .with_max_attempts(2)
            .with_retry_duration(duration);

        // Poll for the next event at the time when the retryable event is ready
        listener
            .poll(now)
            .await?
            .expect("Expected poll to return an event");

        // Expect the event to be acknowledged and the attempt to be failed
        assert!(is_succeeded(&pool, published_event.id).await?);
        assert!(is_acknowledged(&pool, published_event.id).await?);

        assert!(!is_unacknowledged(&pool, published_event.id).await?);
        assert!(!is_failed(&pool, published_event.id).await?);
        assert!(!is_dead(&pool, published_event.id).await?);

        Ok(())
    }
}
