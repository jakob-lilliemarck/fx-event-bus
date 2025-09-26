use sqlx::PgPool;
use uuid::Uuid;

use crate::RawEvent;

pub async fn get_event_failed(
    pool: &sqlx::PgPool,
    id: Uuid,
) -> Result<RawEvent, sqlx::Error> {
    let event = sqlx::query_as!(
        RawEvent,
        r#"
        SELECT
            id,
            name,
            hash,
            payload
        FROM fx_event_bus.events_acknowledged
        WHERE id = (
            SELECT event_id
            FROM fx_event_bus.results_failed
            WHERE event_id = $1
            LIMIT 1
        );
        "#,
        id,
    )
    .fetch_one(pool)
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
                payload
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
                payload
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
