#[derive(thiserror::Error, Debug)]
pub enum EventHandlingError {
    #[error("Failed to deserialize event: {0}")]
    DeserializationError(#[from] serde_json::Error),

    #[error("Business logic error: {0}")]
    BusinessLogicError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),
}
