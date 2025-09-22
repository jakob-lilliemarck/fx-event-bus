#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("The event payload could not be serialized to JSON")]
    SerializationError {
        hash: i32,
        name: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("The event could not be written to the database")]
    DatabaseError {
        hash: i32,
        name: String,
        #[source]
        source: sqlx::Error,
    },
}
