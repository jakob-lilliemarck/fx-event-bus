use crate::Event;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct TestEvent {
    pub a_string: String,
    pub a_number: i32,
    pub a_bool: bool,
}

impl Event for TestEvent {
    const NAME: &'static str = "test";
}
