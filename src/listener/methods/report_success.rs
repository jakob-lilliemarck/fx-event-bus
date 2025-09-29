use crate::Listener;
use chrono::{DateTime, Utc};
use uuid::Uuid;

impl Listener {
    #[tracing::instrument(
        skip(self, tx),
        fields(
            event_id = %event_id,
            attempted_at = %attempted_at
        ),
        err
    )]
    pub(super) async fn report_success<'tx>(
        &self,
        tx: &mut sqlx::PgTransaction<'tx>,
        event_id: Uuid,
        attempted_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        // FIXME: reporting success currently does not clean up failures
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventHandlerRegistry;
    use crate::test_tools::{
        TestEvent, init_tracing, is_acknowledged, is_dead, is_failed,
        is_succeeded, is_unacknowledged,
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_creates_a_successful_attempt(
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
            .report_success(&mut tx, acked_event.id, now)
            .await?;
        tx.commit().await?;

        // It is acknowledged and succeeded
        assert!(is_acknowledged(&pool, acked_event.id).await?);
        assert!(is_succeeded(&pool, acked_event.id).await?);
        // Its not any of the other states
        assert!(!is_dead(&pool, acked_event.id).await?);
        assert!(!is_failed(&pool, acked_event.id).await?);
        assert!(!is_unacknowledged(&pool, acked_event.id).await?);

        Ok(())
    }
}
