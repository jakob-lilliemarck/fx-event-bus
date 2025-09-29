use const_fnv1a_hash::fnv1a_hash_str_32;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

/// Trait for events that can be published and handled by the event bus.
///
/// Implement this trait on your event types to make them processable.
///
/// # Example
/// 
/// ```rust
/// #[derive(Serialize, Deserialize, Clone)]
/// struct OrderCreated {
///     order_id: u64,
///     customer_name: String,
/// }
/// 
/// impl Event for OrderCreated {
///     const NAME: &'static str = "OrderCreated";
/// }
/// ```
pub trait Event: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// Unique name for this event type
    const NAME: &'static str;
    /// Compile-time hash of the event name for fast lookups
    const HASH: i32 = fnv1a_hash_str_32(Self::NAME) as i32;
}

/// Raw event data as stored in the database.
/// 
/// Contains the event metadata and JSON payload. Used internally
/// by handlers - you typically don't create these directly.
#[derive(Debug, Clone)]
pub struct RawEvent {
    /// Unique event identifier
    pub id: Uuid,
    /// Event type name
    pub name: String,
    /// Hash of event name for lookup performance
    pub hash: i32,
    /// Serialized event data
    pub payload: serde_json::Value,
    /// Number of processing attempts
    pub attempted: i32,
}
