use crate::{Event, FromOther, RawEvent};
use chrono::Utc;
use sqlx::PgTransaction;
use uuid::Uuid;

pub struct Publisher<'tx> {
    tx: PgTransaction<'tx>,
}

impl<'tx> Publisher<'tx> {
    /// Create a new Publisher from a database transaction.
    pub fn new(tx: PgTransaction<'tx>) -> Self {
        Self { tx }
    }
}

impl<'tx> Into<PgTransaction<'tx>> for Publisher<'tx> {
    fn into(self) -> PgTransaction<'tx> {
        self.tx
    }
}

impl<'tx> FromOther<'tx> for Publisher<'tx> {
    type TxType = Publisher<'tx>;

    fn from(
        &self,
        other: impl Into<PgTransaction<'tx>>,
    ) -> Self::TxType {
        Self::new(other.into())
    }
}

impl<'tx> Publisher<'tx> {
    pub async fn publish_many<E: Event>(
        &mut self,
        events: &[E],
    ) -> Result<(), super::PublisherError> {
        let len = events.len();

        if len == 0 {
            return Ok(());
        }

        let now = Utc::now();

        let payloads = events
            .iter()
            .map(|event| {
                serde_json::to_value(event).map_err(|error| {
                    super::PublisherError::SerializationError {
                        hash: E::HASH,
                        name: E::NAME.to_string(),
                        source: error,
                    }
                })
            })
            .collect::<Result<Vec<serde_json::Value>, super::PublisherError>>(
            )?;

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO fx_event_bus.events_unacknowledged (
                id,
                name,
                hash,
                payload,
                published_at
            ) ",
        );

        query_builder.push_values(payloads, |mut b, payload| {
            b.push_bind(Uuid::now_v7())
                .push_bind(E::NAME)
                .push_bind(E::HASH)
                .push_bind(payload)
                .push_bind(&now);
        });

        query_builder.build().execute(&mut *self.tx).await.map_err(
            |error| super::PublisherError::DatabaseError {
                hash: E::HASH,
                name: E::NAME.to_string(),
                source: error,
            },
        )?;

        Ok(())
    }

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
            Utc::now()
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
    use crate::Event;
    use serde::{Deserialize, Serialize};

    // Test event for our tests
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        message: String,
        value: i32,
    }

    impl Event for TestEvent {
        const NAME: &'static str = "test_event";
        const HASH: i32 = 12345;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn it_publishes_evens(pool: sqlx::PgPool) -> anyhow::Result<()> {
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
}
