use super::super::Listener;
use crate::listener::poll_control::PollControlStream;
use chrono::Utc;
use futures::StreamExt;
use sqlx::postgres::PgListener;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

const EVENTS_CHANNEL: &str = "fx_event_bus";

/// Result of a polling operation.
///
/// Sent through the channel to report polling results.
pub struct Handled {
    /// ID of the event that was handled, if any
    pub id: Option<Uuid>,
    /// Total events processed by this listener since start
    pub count: usize,
}

impl Listener {
    #[tracing::instrument(
        skip(self, tx),
        fields(
            max_attempts = self.max_attempts,
            retry_duration_ms = self.retry_duration.as_millis(),
            has_channel = tx.is_some()
        ),
        err
    )]
    /// Starts the event listener loop.
    ///
    /// Continuously polls for events using PostgreSQL LISTEN/NOTIFY
    /// with intelligent backoff. Processes events until the loop is stopped.
    ///
    /// # Arguments
    ///
    /// * `tx` - Optional channel to send polling results to
    ///
    /// # Errors
    ///
    /// Returns database errors if connection or polling fails.
    pub async fn listen(
        &mut self,
        tx: Option<Sender<Handled>>,
    ) -> Result<(), sqlx::Error> {
        let mut control = PollControlStream::new(
            Duration::from_millis(500), // FIXME: make configurable
            Duration::from_millis(2_500), // FIXME: make configurable
        );

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(EVENTS_CHANNEL).await?;
        let pg_stream = listener.into_stream();

        control.with_pg_stream(pg_stream);

        while let Some(result) = control.next().await {
            if let Err(err) = result {
                tracing::warn!(message="The control stream returned an error", error=?err)
            }
            match self.poll(Utc::now()).await {
                Ok(handled) => {
                    // Reset failed polling attempts on success
                    control.reset_failed_attempts();

                    // If an event was handled, override the next wait
                    if let Some(_) = handled {
                        self.count += 1;
                        control.set_poll();
                    }

                    // If a channel is provided, inform of the result of the poll
                    if let Some(tx) = &tx {
                        if let Err(_) = tx
                            .send(Handled {
                                id: handled,
                                count: self.count,
                            })
                            .await
                        {
                            tracing::warn!(
                                "Listener failed to broadcast the poll result"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        message = "Polling for events returned an error",
                        error = ?err
                    );
                    control.increment_failed_attempts();
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_tools::{
        FailingHandler, SucceedingHandler, TestEvent, get_failed_attempts,
        get_succeeded_attempts, init_tracing, run_until,
    };
    use crate::{EventHandlerRegistry, Publisher, listener::Listener};
    use chrono::Utc;
    use sqlx::PgTransaction;
    use std::time::Duration;
    use uuid::Uuid;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_handles_an_unacknowledged_event(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let tx = pool.begin().await?;

        let mut publisher = Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;
        let tx: PgTransaction<'_> = publisher.into();

        tx.commit().await?;

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(SucceedingHandler);

        let listener = Listener::new(pool.clone(), registry);
        run_until(listener, 1).await?;

        let failed_attempts = get_failed_attempts(&pool).await?;
        assert!(failed_attempts == 0);

        let succeeded_attempts = get_succeeded_attempts(&pool).await?;
        assert!(succeeded_attempts == 1);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_retries_failed_attempts(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let tx = pool.begin().await?;

        let mut publisher = Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;
        let tx: PgTransaction<'_> = publisher.into();

        tx.commit().await?;

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(FailingHandler);

        let listener = Listener::new(pool.clone(), registry);
        run_until(listener, 1).await?;

        let failed_attempts = get_failed_attempts(&pool).await?;
        assert!(failed_attempts == 1);

        let succeeded = get_succeeded_attempts(&pool).await?;
        assert!(succeeded == 0);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_handles_unacknowledged_events_before_retrying_failed_attempts(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();
        let tx = pool.begin().await?;

        // Publish two events
        let mut publisher = Publisher::new(tx);
        let event_1 = publisher.publish(TestEvent::default()).await?;
        let event_2 = publisher.publish(TestEvent::default()).await?;

        let mut tx: PgTransaction<'_> = publisher.into();

        // Acknowledge one event
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected an event to be returned");

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(SucceedingHandler);
        let listener = Listener::new(pool.clone(), registry)
            .with_max_attempts(2)
            .with_retry_duration(Duration::from_millis(5));

        // Report a failed attempt for the acknoledged event
        listener
            .report_failure(
                &mut tx,
                acked_event.id,
                1,
                now,
                "error".to_string(),
            )
            .await?;
        tx.commit().await?;

        let handled_ids = run_until(listener, 2)
            .await?
            .iter()
            .filter_map(|handled| handled.id)
            .collect::<Vec<Uuid>>();

        // Unacknowledged events are processed before retries
        // event_1 was acknowledged and failed (becomes a retry)
        // event_2 remains unacknowledged
        // So event_2 should be processed first, then event_1 (retry)
        assert_eq!(handled_ids[0], event_2.id); // Unacknowledged event processed first
        assert_eq!(handled_ids[1], event_1.id); // Failed event retried second

        let failed_attempts = get_failed_attempts(&pool).await?;
        assert!(failed_attempts == 1);

        let succeeded = get_succeeded_attempts(&pool).await?;
        assert!(succeeded == 2);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_fails_the_attempt_if_one_handler_fails(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let tx = pool.begin().await?;

        let mut publisher = Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;
        let tx: PgTransaction<'_> = publisher.into();

        tx.commit().await?;

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(SucceedingHandler);
        registry.with_handler(FailingHandler);
        let listener = Listener::new(pool.clone(), registry);

        run_until(listener, 1).await?;

        let failed_attempts = get_failed_attempts(&pool).await?;
        assert!(failed_attempts == 1);

        let succeeded = get_succeeded_attempts(&pool).await?;
        assert!(succeeded == 0);

        Ok(())
    }
}
