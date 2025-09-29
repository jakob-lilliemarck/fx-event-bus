use const_fnv1a_hash::fnv1a_hash_str_32;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

/// Event trait for type-safe event publishing and deserialization
pub trait Event: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// The event name for this event type
    const NAME: &'static str;
    /// Compile-time hash of the event name, used to improve lookup speed
    const HASH: i32 = fnv1a_hash_str_32(Self::NAME) as i32;
}

#[derive(Debug, Clone)]
pub struct RawEvent {
    pub id: Uuid,
    pub name: String,
    pub hash: i32,
    pub payload: serde_json::Value,
    pub attempted: i32,
}
