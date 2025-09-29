use crate::Listener;
use crate::models::RawEvent;
use chrono::{DateTime, Utc};

impl Listener {
    // uses FOR UPDATE SKIP LOCKED to acknowledge and return the next event
    // process the event using the same transaction to ensure consistency
    #[tracing::instrument(
        skip(tx),
        fields(now = %now),
        err
    )]
    pub(super) async fn poll_unacknowledged<'tx>(
        tx: &mut sqlx::PgTransaction<'tx>,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEvent>, sqlx::Error> {
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
            now,
        )
        .fetch_optional(&mut **tx)
        .await?;
        Ok(acknowledged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tools::{
        TestEvent, init_tracing, is_acknowledged, is_dead, is_failed,
        is_succeeded, is_unacknowledged,
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_acknowledged_events(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;
        let mut tx: sqlx::PgTransaction = publisher.into();

        let acked_event = Listener::poll_unacknowledged(&mut tx, Utc::now())
            .await?
            .expect("Expected an event to be returned");
        tx.commit().await?;

        // Assert the event is now acknowledged
        assert!(published_event.id == acked_event.id);
        // It is acknowledged
        assert!(is_acknowledged(&pool, acked_event.id).await?);
        // It is not any of the other states
        assert!(!is_unacknowledged(&pool, acked_event.id).await?);
        assert!(!is_succeeded(&pool, acked_event.id).await?);
        assert!(!is_failed(&pool, acked_event.id).await?);
        assert!(!is_dead(&pool, acked_event.id).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_acknowledges_events_in_fifo_order(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();

        let events = 3;
        let mut event_ids = Vec::new();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        for _ in 0..events {
            publisher.publish(TestEvent::default()).await.map(|event| {
                event_ids.push(event.id);
            })?;
        }
        let mut tx: sqlx::PgTransaction = publisher.into();

        // Create a buffer to collect event ids in the order they were acked
        let mut acked_ids = Vec::new();
        for _ in 0..events {
            let event =
                Listener::poll_unacknowledged(&mut tx, Utc::now()).await?;
            acked_ids.push(event.expect("Event not found").id)
        }
        tx.commit().await?;

        // Assert the order the events were processed
        assert_eq!(acked_ids, event_ids);
        for id in event_ids {
            // It is acknowledged
            assert!(is_acknowledged(&pool, id).await?);
            // It is not any of the other states
            assert!(!is_unacknowledged(&pool, id).await?);
            assert!(!is_succeeded(&pool, id).await?);
            assert!(!is_failed(&pool, id).await?);
            assert!(!is_dead(&pool, id).await?);
        }
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_skips_locked_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        init_tracing();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        let published_event = publisher.publish(TestEvent::default()).await?;
        let mut tx: sqlx::PgTransaction = publisher.into();

        let acked_event = Listener::poll_unacknowledged(&mut tx, Utc::now())
            .await?
            .expect("Expected an event to be returned");

        // Try to ack again using the same transaction
        let second_ack =
            Listener::poll_unacknowledged(&mut tx, Utc::now()).await?;

        // Try to ack again using another transaction
        let mut tx_2 = pool.begin().await?;
        let third_ack =
            Listener::poll_unacknowledged(&mut tx_2, Utc::now()).await?;
        tx_2.commit().await?;

        // Then commit the original transaction
        tx.commit().await?;

        // Assert that the event was acked only once
        assert!(published_event.id == acked_event.id);
        assert!(second_ack.is_none());
        assert!(third_ack.is_none());
        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_returns_none_when_no_event_is_available(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let mut tx = pool.begin().await?;
        let event = Listener::poll_unacknowledged(&mut tx, Utc::now()).await?;
        tx.commit().await?;

        // assert the returned event was None
        assert!(event.is_none());
        Ok(())
    }
}
