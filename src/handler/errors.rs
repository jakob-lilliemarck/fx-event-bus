#[derive(thiserror::Error, Debug)]
pub enum EventHandlingError {
    #[error("Failed to deserialize event: {0}")]
    DeserializationError(serde_json::Error),
    #[error("Handler error: {0}")]
    HandlerError(Box<dyn std::error::Error + Send + Sync>),
}

// Manual From implementation for serde_json::Error
impl From<serde_json::Error> for EventHandlingError {
    fn from(error: serde_json::Error) -> Self {
        Self::DeserializationError(error)
    }
}

// Specific implementation for boxed errors
impl From<Box<dyn std::error::Error + Send + Sync>> for EventHandlingError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::HandlerError(error)
    }
}

impl EventHandlingError {
    /// Helper function to create a HandlerError from any error type
    /// This automatically boxes the error for you
    pub fn handler<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::HandlerError(Box::new(error))
    }
}
