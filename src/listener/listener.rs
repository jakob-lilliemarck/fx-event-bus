use crate::{
    EventHandlerRegistry, RawEvent,
    models::{EventResult, EventStatus},
};
use chrono::Utc;
use futures::StreamExt;
use sqlx::{PgPool, postgres::PgListener};
use tokio::sync::mpsc;
use uuid::Uuid;

const EVENTS_CHANNEL: &str = "fx_event_bus";

pub struct Listener {
    pool: PgPool,
    registry: EventHandlerRegistry,
}

impl Listener {
    pub fn new(
        pool: PgPool,
        registry: EventHandlerRegistry,
    ) -> Self {
        Listener { pool, registry }
    }

    // Uses PgNotify to listen for events and calls the provided callback
    // The callback is called for each notification received
    pub async fn listen(
        &mut self,
        tx: Option<mpsc::Sender<()>>,
    ) -> Result<(), super::ListenerError> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(EVENTS_CHANNEL).await?;

        let mut stream = listener.into_stream();
        while let Some(notification_result) = stream.next().await {
            match notification_result {
                Ok(_) => {
                    self.poll().await?;
                    // If a channel is provided, send a message to it
                    if let Some(tx) = &tx {
                        if let Err(err) = tx.send(()).await {
                            tracing::error!(
                                "Failed to send event to listener: {}",
                                err
                            );
                        }
                    }
                }
                Err(e) => {
                    return Err(super::ListenerError::DatabaseError(e));
                }
            }
        }

        Ok(())
    }

    pub async fn poll(&self) -> Result<(), super::ListenerError> {
        // begin a transaction
        let mut tx = self.pool.begin().await?;
        // use the transaction to call acknowledge
        match Self::acknowledge(&mut tx).await? {
            None => return Ok(()),
            Some(event) => {
                // keep the event id for later use
                let id = event.id;
                // use the transaction registry to handle the event
                let (mut tx, result) = self.registry.handle(event, tx).await;

                if let Err(error) = result {
                    // if the handling failed, fail the event
                    Self::insert_result(
                        &mut tx,
                        id,
                        EventResult::Failed,
                        Some(error.to_string()),
                    )
                    .await?;
                } else {
                    // otherwise succeed the event
                    Self::insert_result(
                        &mut tx,
                        id,
                        EventResult::Succeeded,
                        None,
                    )
                    .await?;
                }
                // commit the transaction
                tx.commit().await?;
            }
        }

        Ok(())
    }

    async fn insert_result<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>,
        event_id: Uuid,
        result: EventResult,
        error_message: Option<String>,
    ) -> Result<(), super::ListenerError> {
        sqlx::query!(
            r#"
            INSERT INTO fx_event_bus.results (
                id,
                event_id,
                event_status,
                status,
                processed_at,
                error_message
            ) VALUES (
                $1,
                $2,
                $3::fx_event_bus.event_status,
                $4::fx_event_bus.event_result,
                $5,
                $6
            )
            "#,
            Uuid::now_v7(),
            event_id,
            EventStatus::Acknowledged as EventStatus,
            result as EventResult,
            Utc::now(),
            error_message
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    // uses FOR UPDATE SKIP LOCKED to acknowledge and return the next event
    // process the event using the same transaction to ensure consistency
    async fn acknowledge<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>
    ) -> Result<Option<RawEvent>, super::ListenerError> {
        let acknowledged = sqlx::query_as!(
            RawEvent,
            r#"
                UPDATE fx_event_bus.events
                SET
                    acknowledged_at = $1,
                    status = 'acknowledged'
                FROM (
                    SELECT id
                    FROM fx_event_bus.events_unacknowledged
                    ORDER BY published_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ) next
                WHERE events.id = next.id AND events.status = 'unacknowledged'
                RETURNING
                    events.id,
                    events.name,
                    events.hash,
                    events.payload
            "#,
            Utc::now(),
        )
        .fetch_optional(&mut **tx)
        .await?;
        Ok(acknowledged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::test_utils::{
        HandlerAlpha, HandlerBeta, Runner, SharedHandlerState, TestEvent,
        get_event_failed,
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
        runner.run(listener);

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
        runner.run(listener);

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
        assert_eq!(event.id, published.id);
        assert_eq!(event.name, published.name);
        assert_eq!(event.hash, published.hash);
        assert_eq!(event.payload, published.payload);
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
        runner.run(listener);

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
        assert_eq!(event.id, published.id);
        assert_eq!(event.name, published.name);
        assert_eq!(event.hash, published.hash);
        assert_eq!(event.payload, published.payload);
        Ok(())
    }
}
