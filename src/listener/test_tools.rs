use crate::EventHandler;
use crate::listener::methods::listen::Handled;
use crate::{Event, listener::listener::Listener};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Once;
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

pub async fn get_failed_attempts(
    pool: &sqlx::PgPool
) -> Result<i64, sqlx::Error> {
    let failed_attempts = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) "count!"
        FROM fx_event_bus.attempts_failed
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(failed_attempts)
}

pub async fn get_succeeded_attempts(
    pool: &sqlx::PgPool
) -> Result<i64, sqlx::Error> {
    let failed_attempts = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) "count!"
        FROM fx_event_bus.attempts_succeeded
        "#,
    )
    .fetch_one(pool)
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

#[derive(Deserialize, Serialize, Clone)]
pub struct TestEvent;

impl Event for TestEvent {
    const NAME: &'static str = "TestEvent";
}

pub struct FailingHandler;

#[derive(Debug, thiserror::Error)]
#[error("Test error: {message}")]
pub struct TestError {
    message: String,
}

impl EventHandler<TestEvent> for FailingHandler {
    type Error = TestError;

    fn handle<'a>(
        &'a self,
        _: TestEvent,
        _: DateTime<Utc>,
        tx: sqlx::PgTransaction<'a>,
    ) -> futures::future::BoxFuture<
        'a,
        (sqlx::PgTransaction<'a>, Result<(), Self::Error>),
    > {
        Box::pin(async move {
            (
                tx,
                Err(TestError {
                    message: "error".to_string(),
                }),
            )
        })
    }
}

pub struct SucceedingHandler;

impl EventHandler<TestEvent> for SucceedingHandler {
    type Error = std::convert::Infallible;

    fn handle<'a>(
        &'a self,
        _: TestEvent,
        _: DateTime<Utc>,
        tx: sqlx::PgTransaction<'a>,
    ) -> futures::future::BoxFuture<
        'a,
        (sqlx::PgTransaction<'a>, Result<(), Self::Error>),
    > {
        Box::pin(async move { (tx, Ok(())) })
    }
}
