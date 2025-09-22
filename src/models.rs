use const_fnv1a_hash::fnv1a_hash_str_32;
use sqlx::Type;
use uuid::Uuid;

#[derive(Debug, Type)]
#[sqlx(type_name = "fx_event_bus.event_status", rename_all = "lowercase")]
pub enum EventStatus {
    Unacknowledged,
    Acknowledged,
    Failed,
}

/// Event trait for type-safe event publishing and deserialization
pub trait Event:
    serde::Serialize + serde::de::DeserializeOwned + Send + 'static
{
    /// The event name for this event type
    const NAME: &'static str;
    /// Compile-time hash of the event name
    const HASH: i32 = fnv1a_hash_str_32(Self::NAME) as i32;
}

#[derive(Debug, Clone)]
pub struct RawEvent {
    /// Event ID
    pub id: Uuid,
    /// Event name
    pub name: String,
    /// Event hash for fast lookup
    pub hash: i32,
    /// JSON payload
    pub payload: serde_json::Value,
}
