use sqlx::{Acquire, Postgres};

// Embed the migrations directory at compile time
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Runs database migrations for the fx-event-bus schema.
///
/// Creates the 'fx_event_bus' schema if it doesn't exist and runs all
/// embedded migrations within that schema.
///
/// # Arguments
///
/// * `conn` - Database connection or connection pool
///
/// # Errors
///
/// Returns `sqlx::Error` if schema creation or migration execution fails.
pub async fn run_migrations<'a, A>(conn: A) -> Result<(), sqlx::Error>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut tx = conn.begin().await?;
    // Ensure the 'fx_event_bus' schema exists
    sqlx::query("CREATE SCHEMA IF NOT EXISTS fx_event_bus;")
        .execute(&mut *tx)
        .await?;

    // Temporarily set search_path for this transaction
    sqlx::query("SET LOCAL search_path TO fx_event_bus;")
        .execute(&mut *tx)
        .await?;

    // Run migrations within the 'fx_event_bus' schema
    MIGRATOR.run(&mut *tx).await?;

    tx.commit().await?;

    Ok(())
}
