use crate::models::EventStatus;
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
    pub async fn publish<T>(
        &mut self,
        event: T,
    ) -> Result<RawEvent, super::PublisherError>
    where
        T: Event,
    {
        // Serialize the event
        let payload = serde_json::to_value(&event).map_err(|error| {
            super::PublisherError::SerializationError {
                hash: T::HASH,
                name: T::HASH.to_string(),
                source: error,
            }
        })?;

        // Publish the event
        let published = sqlx::query_as!(
            RawEvent,
            r#"
                INSERT INTO fx_event_bus.events (
                    id,
                    name,
                    hash,
                    status,
                    payload,
                    published_at
                )
                VALUES ($1, $2, $3, $4::fx_event_bus.event_status, $5, $6)
                RETURNING id, name, hash, payload
            "#,
            Uuid::now_v7(),
            T::NAME,
            T::HASH,
            EventStatus::Unacknowledged as EventStatus,
            payload,
            Utc::now()
        )
        .fetch_one(&mut *self.tx)
        .await
        .map_err(|error| super::PublisherError::DatabaseError {
            hash: T::HASH,
            name: T::NAME.to_string(),
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
    #[derive(Debug, Serialize, Deserialize)]
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
}
