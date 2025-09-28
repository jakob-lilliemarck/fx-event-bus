use super::poll_control::PollControlStream;
use crate::{EventHandlerRegistry, RawEvent};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use sqlx::{PgPool, postgres::PgListener};
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

const EVENTS_CHANNEL: &str = "fx_event_bus";

pub struct Listener {
    pool: PgPool,
    registry: EventHandlerRegistry,
    max_attempts: i32,
    retry_duration: Duration,
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

    // Uses PgNotify to listen for events and processes them
    // Sends notifications via the provided channel when events are processed
    pub async fn listen(
        &mut self,
        tx: Option<mpsc::Sender<()>>,
    ) -> Result<(), super::ListenerError> {
        let mut control = PollControlStream::new(
            Duration::from_millis(500),
            Duration::from_millis(2_500),
        );

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(EVENTS_CHANNEL).await?;
        let pg_stream = listener.into_stream();

        control.with_pg_stream(pg_stream);

        while let Some(result) = control.next().await {
            if let Err(err) = result {
                tracing::warn!(message="The control stream returned an error", error=?err)
            }

            match self.poll().await {
                Ok(handled) => {
                    // Reset failed attempts on success
                    control.reset_failed_attempts();
                    // If an event was handled, override the next wait
                    if handled {
                        control.set_poll();
                        // If a channel is provided, send a message to it
                        if let Some(tx) = &tx {
                            if let Err(_) = tx.send(()).await {
                                // Channel closed, stop processing
                                break;
                            }
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

    pub async fn poll(&self) -> Result<bool, super::ListenerError> {
        // Get the current time
        let polled_at = Utc::now();

        // begin a transaction
        let mut tx = self.pool.begin().await?;

        // Try to get an event to handle
        // Try unacknowledged events first, and retry secondary
        let event = match Self::acknowledge(&mut tx).await? {
            None => match Self::retry(&mut tx, polled_at).await? {
                None => return Ok(false), // No events to handle
                Some(event) => event,
            },
            Some(event) => event,
        };

        // keep the event id for later use
        let event_id = event.id;

        // keep the event attempted for later use
        let attempted = event.attempted as u32;

        // use the transaction registry to handle the event
        let (mut tx, result) = self.registry.handle(event, polled_at, tx).await;

        if let Err(error) = result {
            // if the handling failed, fail the event
            self.report_failure(
                &mut tx,
                event_id,
                attempted + 1,
                polled_at,
                error.to_string(),
            )
            .await?;
        } else {
            // otherwise succeed the event
            self.report_success(&mut tx, event_id, polled_at).await?;
        }
        // commit the transaction
        tx.commit().await?;
        return Ok(true);
    }

    // uses FOR UPDATE SKIP LOCKED to acknowledge and return the next event
    // process the event using the same transaction to ensure consistency
    pub async fn acknowledge<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>
    ) -> Result<Option<RawEvent>, super::ListenerError> {
        let acknowledged = sqlx::query_as!(
            RawEvent,
            r#"
            WITH next_event AS (
                DELETE FROM fx_event_bus.events_unacknowledged
                WHERE id = (
                    SELECT id
                    FROM fx_event_bus.events_unacknowledged
                    ORDER BY published_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
            )
            INSERT INTO fx_event_bus.events_acknowledged
            SELECT
                id,
                name,
                hash,
                payload,
                published_at,
                $1 as acknowledged_at
            FROM next_event
            RETURNING
                id,
                name,
                hash,
                payload,
                0::INTEGER "attempted!:i32"; -- always zero attempts when polling from unacknowledged
            "#,
            Utc::now(),
        )
        .fetch_optional(&mut **tx)
        .await?;
        Ok(acknowledged)
    }

    // uses FOR UPDATE SKIP LOCKED to update the attempted_at column and return the next event to retry
    // process the event using the same transaction to ensure consistency
    async fn retry<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEvent>, super::ListenerError> {
        let retry = sqlx::query_as!(
            RawEvent,
            r#"
            WITH claimed AS (
                UPDATE fx_event_bus.attempts_failed
                SET attempted_at = $1
                WHERE id = (
                    SELECT id
                    FROM fx_event_bus.attempts_failed
                    WHERE
                        try_earliest <= $1
                        AND attempted_at IS NULL
                    ORDER BY try_earliest ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING event_id, attempted
            )
            SELECT
                e.id,
                e.name,
                e.hash,
                e.payload,
                c.attempted
            FROM fx_event_bus.events_acknowledged e
            JOIN claimed c ON e.id = c.event_id
            "#,
            now
        )
        .fetch_optional(&mut **tx)
        .await?;

        Ok(retry)
    }

    async fn report_success<'tx>(
        &self,
        tx: &mut sqlx::PgTransaction<'tx>,
        event_id: Uuid,
        attempted_at: DateTime<Utc>,
    ) -> Result<(), super::ListenerError> {
        sqlx::query!(
            r#"
            INSERT INTO fx_event_bus.attempts_succeeded (
                event_id,
                attempted_at
            ) VALUES (
                $1,
                $2
            )
            "#,
            event_id,
            attempted_at
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn report_failure<'tx>(
        &self,
        tx: &mut sqlx::PgTransaction<'tx>,
        event_id: Uuid,
        attempted: u32,
        attempted_at: DateTime<Utc>,
        error: String,
    ) -> Result<(), super::ListenerError> {
        let now = Utc::now();

        if attempted as i32 >= self.max_attempts {
            sqlx::query!(
                r#"
                WITH deleted_attempts AS (
                    DELETE FROM fx_event_bus.attempts_failed
                    WHERE event_id = $1
                    RETURNING
                        attempted_at,
                        error,
                        attempted
                )
                INSERT INTO fx_event_bus.attempts_dead (
                    event_id,
                    attempted_at,
                    dead_at,
                    errors
                )
                SELECT
                    $1,
                    array_agg(attempted_at ORDER BY attempted) || ARRAY[$2::TIMESTAMPTZ],
                    $3,
                    array_agg(error ORDER BY attempted) || ARRAY[$4]
                FROM deleted_attempts
                "#,
                event_id,
                attempted_at,
                now,
                error
            )
            .execute(&mut **tx)
            .await?;
        } else {
            // Insert retry attempt
            let try_earliest =
                attempted_at + self.retry_duration * 2_u32.pow(attempted - 1);

            sqlx::query!(
                r#"
                INSERT INTO fx_event_bus.attempts_failed (
                    id,
                    event_id,
                    try_earliest,
                    attempted,
                    attempted_at,
                    error
                ) VALUES ($1, $2, $3, $4, NULL, $5)
                "#,
                Uuid::now_v7(),
                event_id,
                try_earliest,
                attempted as i32,
                error
            )
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::test_utils::{
        HandlerAlpha, HandlerBeta, HandlerGamma, Runner, SharedHandlerState,
        TestEvent, get_event_failed,
    };
    use sqlx::PgTransaction;
    use std::sync::Arc;
    use std::sync::Once;
    use tokio::sync::Mutex;
    use tracing::Level;
    use tracing_subscriber;

    static INIT: Once = Once::new();

    fn init_tracing() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(Level::DEBUG)
                .init();
        });
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_handles_a_single_event(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();

        // Pass none to not fail
        let state = Arc::new(Mutex::new(SharedHandlerState::new(None)));

        let handler_alpha = HandlerAlpha::new(&state);

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(handler_alpha);

        let listener = Listener::new(pool.clone(), registry);

        let mut runner = Runner::new();
        runner.run(listener).await;

        let tx = pool.begin().await?;

        let mut publisher = crate::Publisher::new(tx);

        publisher
            .publish(TestEvent {
                a_string: "a_string".to_string(),
                a_number: 42,
                a_bool: true,
            })
            .await?;

        let tx: PgTransaction<'_> = publisher.into();

        tx.commit().await?;

        runner.wait_until(1).await;
        runner.cancel();
        runner.join().await?;

        let lock = state.lock().await;
        assert_eq!(lock.count(), 1);
        assert_eq!(
            lock.seen,
            &[(TestEvent::HASH, TestEvent::NAME.to_string(), true)]
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_does_not_acknowledge_on_failure(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();

        // Pass none to not fail
        let state = Arc::new(Mutex::new(SharedHandlerState::new(Some(
            TestEvent::HASH,
        ))));

        let handler_alpha = HandlerAlpha::new(&state);

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(handler_alpha);

        let listener = Listener::new(pool.clone(), registry);

        let mut runner = Runner::new();
        runner.run(listener).await;

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published = publisher
            .publish(TestEvent {
                a_string: "a_string".to_string(),
                a_number: 42,
                a_bool: true,
            })
            .await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        runner.wait_until(1).await;
        runner.cancel();
        runner.join().await?;

        let lock = state.lock().await;
        assert_eq!(lock.count(), 1);
        assert_eq!(
            lock.seen,
            &[(TestEvent::HASH, TestEvent::NAME.to_string(), false)]
        );

        // Assert that the event was acknowledged and moved to failed
        let event = get_event_failed(&pool, published.id).await?;
        assert!(event.len() == 1);
        assert_eq!(event[0].id, published.id);
        assert_eq!(event[0].name, published.name);
        assert_eq!(event[0].hash, published.hash);
        assert_eq!(event[0].payload, published.payload);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_handles_an_event_with_multiple_handlers(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();

        // Pass none to not fail
        let state = Arc::new(Mutex::new(SharedHandlerState::new(Some(
            TestEvent::HASH,
        ))));

        let handler_alpha = HandlerAlpha::new(&state);
        let handler_beta = HandlerBeta::new(&state);

        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(handler_alpha);
        registry.with_handler(handler_beta);

        let listener = Listener::new(pool.clone(), registry);

        let mut runner = Runner::new();
        runner.run(listener).await;

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published = publisher
            .publish(TestEvent {
                a_string: "a_string".to_string(),
                a_number: 42,
                a_bool: true,
            })
            .await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        runner.wait_until(1).await;
        runner.cancel();
        runner.join().await?;

        let lock = state.lock().await;
        assert_eq!(lock.count(), 1);
        assert_eq!(
            lock.seen,
            &[(TestEvent::HASH, TestEvent::NAME.to_string(), false)]
        );

        // Assert that the event was acknowledged and moved to failed
        let event = get_event_failed(&pool, published.id).await?;
        assert!(event.len() == 1);
        assert_eq!(event[0].id, published.id);
        assert_eq!(event[0].name, published.name);
        assert_eq!(event[0].hash, published.hash);
        assert_eq!(event[0].payload, published.payload);
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_locked_rows_during_poll(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher
            .publish(TestEvent {
                a_string: "a_string".to_string(),
                a_number: 42,
                a_bool: true,
            })
            .await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        let handler_gamma = HandlerGamma::new(pool.clone());
        let mut registry = EventHandlerRegistry::new();
        registry.with_handler(handler_gamma);

        let listener = Listener::new(pool.clone(), registry);
        listener.poll().await?;

        Ok(())
    }
}
