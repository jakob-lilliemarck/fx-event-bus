use super::EventHandlingError;
use crate::models::Event;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;

pub trait EventHandler<E: Event>: Send + Sync {
    fn handle<'a>(
        &'a self,
        input: E,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), EventHandlingError>)>;
}
