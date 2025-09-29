use crate::models::Event;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use sqlx::PgTransaction;
use std::sync::Arc;

pub trait EventHandler<E: Event>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn handle<'a>(
        &'a self,
        input: Arc<E>,
        polled_at: DateTime<Utc>,
        tx: PgTransaction<'a>,
    ) -> BoxFuture<'a, (PgTransaction<'a>, Result<(), Self::Error>)>;
}
