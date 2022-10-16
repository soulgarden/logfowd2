use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
pub struct Index {
    index: IndexBody,
}

impl Index {
    pub fn new() -> Self {
        Self {
            index: IndexBody::new(),
        }
    }
}

#[derive(Serialize)]
pub struct IndexBody {
    #[serde(rename(serialize = "_id"))]
    id: String,
}

impl IndexBody {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct FieldsBody {
    message: String,
    #[serde(rename(serialize = "@timestamp"))]
    timestamp: DateTime<Utc>,
    pod_name: String,
    namespace: String,
    container_name: String,
    pod_id: String,
}

impl FieldsBody {
    pub fn new(
        message: String,
        timestamp: DateTime<Utc>,
        pod_name: String,
        namespace: String,
        container_name: String,
        pod_id: String,
    ) -> Self {
        Self {
            message,
            timestamp,
            pod_name,
            namespace,
            container_name,
            pod_id,
        }
    }
}
