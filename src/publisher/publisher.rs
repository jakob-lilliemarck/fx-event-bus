use crate::{Event, models::RawEvent};
use chrono::Utc;
use sqlx::PgTransaction;
use uuid::Uuid;

/// Publishes events to the event bus.
///
/// Created from a database transaction to ensure events are published
/// atomically with other database operations.
pub struct Publisher<'tx> {
    tx: PgTransaction<'tx>,
}

impl<'tx> Publisher<'tx> {
    /// Creates a new publisher from a database transaction.
    ///
    /// # Arguments
    ///
    /// * `tx` - Database transaction to publish events within
    #[tracing::instrument(skip(tx), level = "debug")]
    pub fn new(tx: PgTransaction<'tx>) -> Self {
        Self { tx }
    }
}

impl<'tx> Into<PgTransaction<'tx>> for Publisher<'tx> {
    fn into(self) -> PgTransaction<'tx> {
        self.tx
    }
}

impl<'tx> Publisher<'tx> {
    /// Publishes multiple events efficiently in a single database operation.
    ///
    /// # Arguments
    ///
    /// * `events` - Slice of events to publish
    ///
    /// # Errors
    ///
    /// Returns `PublisherError` if serialization fails or database operation fails.
    #[tracing::instrument(
        skip(self, events),
        fields(
            event_name = E::NAME,
            event_hash = E::HASH,
            count = events.len()
        ),
        err
    )]
    pub async fn publish_many<E: Event>(
        &mut self,
        events: &[E],
    ) -> Result<(), super::PublisherError> {
        if events.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO fx_event_bus.events_unacknowledged (
                id, name, hash, payload, published_at
            ) VALUES ",
        );

        let mut first = true;
        // Stream serialization: serialize each event as needed rather than
        // pre-allocating all payloads, reducing peak memory usage
        for event in events {
            let payload = serde_json::to_value(event).map_err(|error| {
                super::PublisherError::SerializationError {
                    hash: E::HASH,
                    name: E::NAME.to_string(),
                    source: error,
                }
            })?;

            if first {
                first = false;
            } else {
                query_builder.push(", ");
            }

            query_builder
                .push("(")
                .push_bind(Uuid::now_v7())
                .push(", ")
                .push_bind(E::NAME)
                .push(", ")
                .push_bind(E::HASH)
                .push(", ")
                .push_bind(payload)
                .push(", ")
                .push_bind(&now)
                .push(")");
        }

        query_builder.build().execute(&mut *self.tx).await.map_err(
            |error| super::PublisherError::DatabaseError {
                hash: E::HASH,
                name: E::NAME.to_string(),
                source: error,
            },
        )?;

        Ok(())
    }

    /// Publishes a single event and returns the stored event data.
    ///
    /// # Arguments
    ///
    /// * `event` - Event to publish
    ///
    /// # Returns
    ///
    /// Returns the `RawEvent` as stored in the database, including the assigned ID.
    ///
    /// # Errors
    ///
    /// Returns `PublisherError` if serialization fails or database operation fails.
    #[tracing::instrument(
        skip(self, event),
        fields(
            event_name = E::NAME,
            event_hash = E::HASH
        ),
        err
    )]
    pub async fn publish<E: Event>(
        &mut self,
        event: E,
    ) -> Result<RawEvent, super::PublisherError> {
        // Serialize the event
        let payload = serde_json::to_value(&event).map_err(|error| {
            super::PublisherError::SerializationError {
                hash: E::HASH,
                name: E::NAME.to_string(),
                source: error,
            }
        })?;

        let now = Utc::now();

        // Publish the event
        let published = sqlx::query_as!(
            RawEvent,
            r#"
            INSERT INTO fx_event_bus.events_unacknowledged (
                id,
                name,
                hash,
                payload,
                published_at
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING
                id,
                name,
                hash,
                payload,
                0::INTEGER "attempted!:i32"
            "#,
            Uuid::now_v7(),
            E::NAME,
            E::HASH,
            payload,
            now
        )
        .fetch_one(&mut *self.tx)
        .await
        .map_err(|error| super::PublisherError::DatabaseError {
            hash: E::HASH,
            name: E::NAME.to_string(),
            source: error,
        })?;

        Ok(published)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        Event,
        test_tools::{TestEvent, get_unacknowledged_events},
    };

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_single_events(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let tx = pool.begin().await?;
        let mut publisher = Publisher::new(tx);

        let published = publisher
            .publish(TestEvent {
                message: "testing testing".to_string(),
                value: 42,
            })
            .await?;

        assert_eq!(published.hash, TestEvent::HASH);
        assert_eq!(published.name, TestEvent::NAME);
        assert_eq!(
            published.payload,
            serde_json::json!({
                "message": "testing testing",
                "value": 42
            })
        );

        Ok(())
    }
    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_many_events(
        pool: sqlx::PgPool
    ) -> anyhow::Result<()> {
        let tx = pool.begin().await?;
        let mut publisher = Publisher::new(tx);
        let events: Vec<TestEvent> = (0..100)
            .map(|i| TestEvent {
                message: "testing testing".to_string(),
                value: i as i32,
            })
            .collect();
        publisher.publish_many(&events).await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        let unacknowledged_events = get_unacknowledged_events(&pool).await?;
        assert_eq!(unacknowledged_events, 100);

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_handles_empty_arrays(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let tx = pool.begin().await?;
        let mut publisher = Publisher::new(tx);
        let events: Vec<TestEvent> = Vec::new();
        publisher.publish_many(&events).await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;

        let unacknowledged_events = get_unacknowledged_events(&pool).await?;
        assert_eq!(unacknowledged_events, 0);

        Ok(())
    }
}
