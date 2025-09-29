#[derive(thiserror::Error, Debug)]
pub enum EventHandlingError {
    #[error("Failed to deserialize event: {0}")]
    DeserializationError(serde_json::Error),
    #[error("Handler error: {0}")]
    HandlerError(Box<dyn std::error::Error + Send + Sync>),
}
