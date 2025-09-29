use std::sync::Once;

use crate::{
    Event,
    listener::listener::{Handled, Listener},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static INIT: Once = Once::new();

pub fn init_tracing() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    });
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TestEvent;

impl Event for TestEvent {
    const NAME: &'static str = "TestEvent";
}

pub async fn is_acknowledged(
    pool: &sqlx::PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM fx_event_bus.events_acknowledged
            WHERE id = $1
        )
        "#,
        event_id
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

pub async fn is_unacknowledged(
    pool: &sqlx::PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM fx_event_bus.events_unacknowledged
            WHERE id = $1
        )
        "#,
        event_id
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

pub async fn is_failed(
    pool: &sqlx::PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM fx_event_bus.attempts_failed
            WHERE event_id = $1
        )
        "#,
        event_id
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

pub struct FailedAttempt {
    pub id: Uuid,
    pub event_id: Uuid,
    pub try_earliest: DateTime<Utc>,
    pub attempted: i32,
    pub attempted_at: Option<DateTime<Utc>>,
    pub error: String,
}

pub async fn get_failed_attempts(
    pool: &sqlx::PgPool
) -> Result<Vec<FailedAttempt>, sqlx::Error> {
    let failed_attempts = sqlx::query_as!(
        FailedAttempt,
        r#"
        SELECT
            id,
            event_id,
            try_earliest,
            attempted,
            attempted_at,
            error
        FROM fx_event_bus.attempts_failed
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(failed_attempts)
}

pub async fn is_succeeded(
    pool: &sqlx::PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM fx_event_bus.attempts_succeeded
            WHERE event_id = $1
        )
        "#,
        event_id
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

pub async fn is_dead(
    pool: &sqlx::PgPool,
    event_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM fx_event_bus.attempts_dead
            WHERE event_id = $1
        )
        "#,
        event_id
    )
    .fetch_one(pool)
    .await?;

    Ok(exists.unwrap_or(false))
}

pub async fn run_until(
    mut listener: Listener,
    until: usize,
) -> anyhow::Result<Vec<Handled>> {
    let (tx, mut rx) = mpsc::channel::<Handled>(100);
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        tokio::select! {
            result = listener.listen(Some(tx)) => {
                result?;
                Ok(())
            }
            _ = cancel_clone.cancelled() => {
                // Listener was cancelled
                Ok(())
            }
        }
    });

    let mut buf = Vec::with_capacity(until);
    while let Some(handled) = rx.recv().await {
        let count = handled.count;
        buf.push(handled);
        if count >= until {
            cancel.cancel();
            break;
        }
    }

    handle.await??;
    Ok(buf)
}
