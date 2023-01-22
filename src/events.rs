use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct Event {
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub meta: Meta,
}

#[derive(Clone, Debug, Default)]
pub struct Meta {
    pub pod_name: String,
    pub namespace: String,
    pub container_name: String,
    pub pod_id: String,
}

impl Event {
    pub fn new(message: String, meta: Meta) -> Self {
        Event {
            message,
            timestamp: Utc::now(),
            meta,
        }
    }
}
