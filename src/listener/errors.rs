#[derive(thiserror::Error, Debug)]
pub enum ListenerError {
    #[error("The event could not be written to the database: {0}")]
    DatabaseError(#[from] sqlx::Error),
}
