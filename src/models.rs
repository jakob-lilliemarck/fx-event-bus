use const_fnv1a_hash::fnv1a_hash_str_32;
use sqlx::Type;
use uuid::Uuid;

#[derive(Debug, Type)]
#[sqlx(type_name = "fx_event_bus.event_status", rename_all = "lowercase")]
pub enum EventStatus {
    Unacknowledged,
    Acknowledged,
}

#[derive(Debug, Type)]
#[sqlx(type_name = "fx_event_bus.event_result", rename_all = "lowercase")]
pub enum EventResult {
    Succeeded,
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
    pub id: Uuid,
    pub name: String,
    pub hash: i32,
    pub payload: serde_json::Value,
}
