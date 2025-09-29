use crate::Listener;
use chrono::{DateTime, Utc};
use uuid::Uuid;

impl Listener {
    #[tracing::instrument(
        skip(self),
        fields(
            attempted_at = %attempted_at,
            attempted = attempted,
            retry_duration_ms = self.retry_duration.as_millis()
        ),
        level = "debug"
    )]
    fn try_earliest(
        &self,
        attempted_at: DateTime<Utc>,
        attempted: u32,
    ) -> DateTime<Utc> {
        attempted_at + self.retry_duration * 2_u32.pow(attempted - 1)
    }

    #[tracing::instrument(
        skip(self, tx),
        fields(
            event_id = %event_id,
            attempted = attempted,
            attempted_at = %attempted_at,
            error = error_str,
            max_attempts = self.max_attempts
        ),
        err
    )]
    pub(super) async fn report_failure<'tx>(
        &self,
        tx: &mut sqlx::PgTransaction<'tx>,
        event_id: Uuid,
        attempted: u32,
        attempted_at: DateTime<Utc>,
        error_str: String,
    ) -> Result<(), sqlx::Error> {
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
                error_str
            )
            .execute(&mut **tx)
            .await?;
        } else {
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
                self.try_earliest(attempted_at, attempted),
                attempted as i32,
                error_str
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
    use crate::EventHandlerRegistry;
    use crate::test_tools::{
        TestEvent, init_tracing, is_acknowledged, is_dead, is_failed,
        is_succeeded, is_unacknowledged,
    };
    use chrono::TimeZone;
    use std::time::Duration;

    #[sqlx::test(migrations = "./migrations")]
    async fn it_creates_a_failed_attempt_when_max_attempts_is_not_reached(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;

        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2);

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected an event to be returned");

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

        // It is acknowledged and failed
        assert!(is_acknowledged(&pool, acked_event.id).await?);
        assert!(is_failed(&pool, acked_event.id).await?);
        // Its not any of the other states
        assert!(!is_dead(&pool, acked_event.id).await?);
        assert!(!is_succeeded(&pool, acked_event.id).await?);
        assert!(!is_unacknowledged(&pool, acked_event.id).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_creates_a_dlq_record_when_max_attempts_is_reached(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;

        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(1);

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected an event to be returned");

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

        // It is acknowledged and failed
        assert!(is_acknowledged(&pool, acked_event.id).await?);
        assert!(is_dead(&pool, acked_event.id).await?);
        // Its not any of the other states
        assert!(!is_failed(&pool, acked_event.id).await?);
        assert!(!is_succeeded(&pool, acked_event.id).await?);
        assert!(!is_unacknowledged(&pool, acked_event.id).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_cleans_up_failed_attempts_when_moving_to_dlq(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc::now();

        let tx = pool.begin().await?;
        let mut publisher = crate::Publisher::new(tx);
        publisher.publish(TestEvent::default()).await?;

        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(2);

        let mut tx: sqlx::PgTransaction = publisher.into();
        let acked_event = Listener::poll_unacknowledged(&mut tx, now)
            .await?
            .expect("Expected an event to be returned");

        // First attempt shall fail
        listener
            .report_failure(
                &mut tx,
                acked_event.id,
                1,
                now,
                "error".to_string(),
            )
            .await?;

        // Second attempt shall move to DLQ
        listener
            .report_failure(
                &mut tx,
                acked_event.id,
                2,
                now,
                "error".to_string(),
            )
            .await?;
        tx.commit().await?;

        // It is acknowledged and dead
        assert!(is_acknowledged(&pool, acked_event.id).await?);
        assert!(is_dead(&pool, acked_event.id).await?);
        // Its not any of the other states
        assert!(!is_failed(&pool, acked_event.id).await?);
        assert!(!is_succeeded(&pool, acked_event.id).await?);
        assert!(!is_unacknowledged(&pool, acked_event.id).await?);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_computes_try_earliest_with_exponential_backoff(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        init_tracing();
        let now = Utc.with_ymd_and_hms(2025, 09, 29, 12, 0, 0).unwrap();
        let attempts = 3;
        let duration = Duration::from_secs(15);
        let listener = Listener::new(pool.clone(), EventHandlerRegistry::new())
            .with_max_attempts(attempts)
            .with_retry_duration(duration);

        let expected =
            &[now + duration, now + duration * 2, now + duration * 4];
        for i in 1..=attempts as usize {
            let actual = listener.try_earliest(now, i as u32);
            assert_eq!(actual, expected[i - 1], "Failed at attempt {}", i);
        }

        Ok(())
    }
}
