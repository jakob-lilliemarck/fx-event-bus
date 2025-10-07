use super::super::Listener;
use crate::models::RawEvent;
use chrono::{DateTime, Utc};

impl Listener {
    /// Polls for retryable events that are ready for another processing attempt.
    ///
    /// Uses FOR UPDATE SKIP LOCKED to atomically claim retryable events
    /// while maintaining FIFO ordering and avoiding contention between
    /// concurrent listeners.
    ///
    /// # Arguments
    ///
    /// * `tx` - Database transaction to use for the operation
    /// * `now` - Current timestamp to check against retry deadlines
    ///
    /// # Returns
    ///
    /// Returns the next retryable event if one is available and ready,
    /// or `None` if no events are ready for retry.
    ///
    /// # Errors
    ///
    /// Returns database errors if the query or transaction operations fail.
    #[tracing::instrument(
        skip(tx),
        fields(now = %now),
        err
    )]
    pub async fn poll_retryable<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEvent>, sqlx::Error> {
        let retry = sqlx::query_as!(
            RawEvent,
            r#"
            UPDATE fx_event_bus.attempts_failed f
            SET attempted_at = $1
            FROM fx_event_bus.events_acknowledged e
            WHERE f.event_id = e.id
              AND f.id = (
                SELECT af.id
                FROM fx_event_bus.attempts_failed af
                WHERE af.try_earliest <= $1
                  AND af.attempted_at IS NULL
                ORDER BY af.try_earliest ASC, af.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
              )
            RETURNING e.id, e.name, e.hash, e.payload, f.attempted
            "#,
            now,
        )
        .fetch_optional(&mut **tx)
        .await?;

        Ok(retry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventHandlerRegistry;
    use crate::test_tools::{TestEvent, get_failed_attempts, init_tracing};
    use std::time::Duration;

    // Test that it returns event where try_earliest is in the past
    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_ready_event(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        // Report the attempt as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;

        let retried_event = Listener::poll_retryable(&mut tx, now + duration)
            .await?
            .expect("Expected retry to return an event");
        tx.commit().await?;

        let failed_attempts = get_failed_attempts(&pool).await?;
        assert!(retried_event.id == published_event.id);
        assert!(failed_attempts == 1);
        Ok(())
    }

    // Test that it skips events where attempted_at is not null
    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_ready_events_in_fifo_order(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let events = 3;

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);

        // Publish events and collect their ids
        let mut published_event_ids = Vec::with_capacity(events);
        for _ in 0..events {
            publisher
                .publish(TestEvent::default())
                .await
                .map(|event| published_event_ids.push(event.id))?;
        }

        let mut tx: sqlx::PgTransaction = publisher.into();

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);

        // Ack all events and collect the ids in the order they were acked
        let mut acked_event_ids = Vec::with_capacity(events);
        for i in 0..events {
            let acked_event = Listener::poll_unacknowledged(&mut tx, now)
                .await?
                .expect("Expecgted acknowledge to return an event");
            acked_event_ids.push(acked_event.id);
            // Report the attempt as failed
            listener
                .report_failure(
                    &mut tx,
                    acked_event.id,
                    1,
                    now + duration * i as u32,
                    "error".to_string(),
                )
                .await?;
        }

        let mut retried_event_ids = Vec::with_capacity(events);
        for _ in 0..events {
            // Poll for retryable event using the time when the event will be ready
            let retried_event = Listener::poll_retryable(&mut tx, now + duration * events as u32)
                .await?
                .expect("Expected retry to return an event");
            retried_event_ids.push(retried_event.id);
        }
        tx.commit().await?;

        // Since we increase the attempted_at in iteration, we expect retried events to be returned in the order they were attempted
        assert_eq!(published_event_ids, acked_event_ids);
        assert_eq!(published_event_ids, retried_event_ids);

        Ok(())
    }

    // Test that it skips event where try_earliest is in the future
    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_locked_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        let ready_at = now + duration;
        // Report the attempt as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;

        // Poll for retryable event using the time when the event will be ready
        let retryable_event = Listener::poll_retryable(&mut tx, ready_at)
            .await?
            .expect("Expected retry to return an event");

        // Try to poll for the retryable using the same transaction
        let second_retryable = Listener::poll_retryable(&mut tx, ready_at).await?;

        // Try to poll for the retryable again using another transaction
        let tx_2 = pool.begin().await?;
        let third_retryable = Listener::poll_retryable(&mut tx, ready_at).await?;
        tx_2.commit().await?;

        tx.commit().await?;

        assert!(retryable_event.id == published_event.id);
        assert!(second_retryable.is_none());
        assert!(third_retryable.is_none());
        Ok(())
    }

    // Test that it skips events where attempted_at is not null
    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_claimed_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        // Report the attempt as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;

        // Poll for retryable event using the time when the event will be ready
        let retryable_event = Listener::poll_retryable(&mut tx, now + duration)
            .await?
            .expect("Expected retry to return an event");
        tx.commit().await?;

        // After commit the retryable event is no longer locked, but
        // is still skipped since it has been retried (attempted_at is not null)
        let mut tx = pool.begin().await?;
        let second_retryable = Listener::poll_retryable(&mut tx, now + duration).await?;
        tx.commit().await?;

        assert!(retryable_event.id == published_event.id);
        assert!(second_retryable.is_none());
        Ok(())
    }

    // Test that it skips event where it is locked
    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_non_ready_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected acknowledge to return an event");

        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2)
            .with_retry_duration(duration);
        // Report the attempt as failed
        listener
            .report_failure(&mut tx, acked_event.id, 1, now, "error".to_string())
            .await?;

        // Poll for retryable event using now without forwarding time
        let retryable_event = Listener::poll_retryable(&mut tx, now).await?;

        tx.commit().await?;

        assert!(retryable_event.is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_no_event_is_available(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let mut tx = pool.begin().await?;

        let retryable_event = Listener::poll_retryable(&mut tx, now).await?;

        tx.commit().await?;

        assert!(retryable_event.is_none());
        Ok(())
    }
}
