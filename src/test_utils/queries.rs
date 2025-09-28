use sqlx::PgPool;
use uuid::Uuid;

use crate::RawEvent;

pub async fn get_event_failed(
    pool: &sqlx::PgPool,
    id: Uuid,
) -> Result<Vec<RawEvent>, sqlx::Error> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
        WITH failed AS (
            SELECT
                event_id,
                attempted
            FROM fx_event_bus.attempts_failed
            WHERE event_id = $1
        )
        SELECT
            id,
            name,
            hash,
            payload,
            attempted
        FROM fx_event_bus.events_acknowledged
        JOIN failed ON failed.event_id = fx_event_bus.events_acknowledged.id
        WHERE id = event_id
        "#,
        id,
    )
    .fetch_all(pool)
    .await?;
    Ok(event)
}

pub async fn get_event_acknowledged(
    pool: &PgPool,
    id: Uuid,
) -> Result<RawEvent, sqlx::Error> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
            SELECT
                id,
                name,
                hash,
                payload,
                0 "attempted!:i32"
            FROM
                fx_event_bus.events_acknowledged
            WHERE
                id = $1
        "#,
        id,
    )
    .fetch_one(pool)
    .await?;
    Ok(event)
}

pub async fn get_event_unacknowledged(
    pool: &PgPool,
    id: Uuid,
) -> Result<RawEvent, sqlx::Error> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
            SELECT
                id,
                name,
                hash,
                payload,
                0 "attempted!:i32"
            FROM
                fx_event_bus.events_unacknowledged
            WHERE
                id = $1
        "#,
        id,
    )
    .fetch_one(pool)
    .await?;
    Ok(event)
}
