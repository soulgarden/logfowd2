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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_index_new() {
        let index = Index::new();

        // Should be serializable
        let json = serde_json::to_string(&index).unwrap();
        assert!(json.contains("index"));
        assert!(json.contains("_id"));
    }

    #[test]
    fn test_index_body_new() {
        let body = IndexBody::new();

        // Should have a UUID
        assert!(!body.id.is_empty());
        assert!(body.id.len() > 10); // UUID should be reasonably long

        // UUID should be parseable
        let uuid_result = Uuid::parse_str(&body.id);
        assert!(
            uuid_result.is_ok(),
            "Generated ID should be valid UUID: {}",
            body.id
        );
    }

    #[test]
    fn test_index_body_unique_ids() {
        let body1 = IndexBody::new();
        let body2 = IndexBody::new();

        // IDs should be unique
        assert_ne!(body1.id, body2.id);
    }

    #[test]
    fn test_index_serialization() {
        let index = Index::new();
        let json = serde_json::to_string(&index).unwrap();

        // Parse back to verify structure
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed["index"]["_id"].is_string());
        assert!(!parsed["index"]["_id"].as_str().unwrap().is_empty());
    }

    #[test]
    fn test_fields_body_new() {
        let timestamp = Utc::now();
        let fields = FieldsBody::new(
            "test message".to_string(),
            timestamp,
            "test-pod".to_string(),
            "test-namespace".to_string(),
            "test-container".to_string(),
            "test-pod-id".to_string(),
        );

        assert_eq!(fields.message, "test message");
        assert_eq!(fields.timestamp, timestamp);
        assert_eq!(fields.pod_name, "test-pod");
        assert_eq!(fields.namespace, "test-namespace");
        assert_eq!(fields.container_name, "test-container");
        assert_eq!(fields.pod_id, "test-pod-id");
    }

    #[test]
    fn test_fields_body_serialization() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
        let fields = FieldsBody::new(
            "test message".to_string(),
            timestamp,
            "test-pod".to_string(),
            "test-namespace".to_string(),
            "test-container".to_string(),
            "test-pod-id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify all fields are present and correct
        assert_eq!(parsed["message"].as_str().unwrap(), "test message");
        assert_eq!(parsed["pod_name"].as_str().unwrap(), "test-pod");
        assert_eq!(parsed["namespace"].as_str().unwrap(), "test-namespace");
        assert_eq!(parsed["container_name"].as_str().unwrap(), "test-container");
        assert_eq!(parsed["pod_id"].as_str().unwrap(), "test-pod-id");

        // Verify timestamp format and field name
        assert!(parsed["@timestamp"].is_string());
        let timestamp_str = parsed["@timestamp"].as_str().unwrap();
        assert!(timestamp_str.contains("2023-01-01T12:00:00"));
    }

    #[test]
    fn test_timestamp_serialization_format() {
        let timestamp = Utc.with_ymd_and_hms(2023, 12, 25, 15, 30, 45).unwrap();
        let fields = FieldsBody::new(
            "timestamp test".to_string(),
            timestamp,
            "pod".to_string(),
            "namespace".to_string(),
            "container".to_string(),
            "pod-id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();

        // Should contain proper ISO 8601 format
        assert!(json.contains("2023-12-25T15:30:45"));
        assert!(json.contains("@timestamp"));
    }

    #[test]
    fn test_special_characters_in_message() {
        let fields = FieldsBody::new(
            r#"Message with "quotes" and \backslashes/ and 特殊字符"#.to_string(),
            Utc::now(),
            "pod".to_string(),
            "namespace".to_string(),
            "container".to_string(),
            "pod-id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // JSON should properly escape special characters
        assert_eq!(
            parsed["message"].as_str().unwrap(),
            r#"Message with "quotes" and \backslashes/ and 特殊字符"#
        );
    }

    #[test]
    fn test_empty_fields() {
        let fields = FieldsBody::new(
            String::new(),
            Utc::now(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Empty strings should serialize properly
        assert_eq!(parsed["message"].as_str().unwrap(), "");
        assert_eq!(parsed["pod_name"].as_str().unwrap(), "");
        assert_eq!(parsed["namespace"].as_str().unwrap(), "");
        assert_eq!(parsed["container_name"].as_str().unwrap(), "");
        assert_eq!(parsed["pod_id"].as_str().unwrap(), "");
    }

    #[test]
    fn test_long_message() {
        let long_message = "A".repeat(10000); // 10KB message
        let fields = FieldsBody::new(
            long_message.clone(),
            Utc::now(),
            "pod".to_string(),
            "namespace".to_string(),
            "container".to_string(),
            "pod-id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Long message should serialize without issues
        assert_eq!(parsed["message"].as_str().unwrap(), long_message);
        assert!(json.len() > 10000);
    }

    #[test]
    fn test_kubernetes_valid_names() {
        // Test with valid Kubernetes naming patterns
        let fields = FieldsBody::new(
            "log message".to_string(),
            Utc::now(),
            "my-app-deployment-7d4c8b9f8d-xk2pq".to_string(),
            "production-env".to_string(),
            "nginx-sidecar".to_string(),
            "abc123-def456-ghi789".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(
            parsed["pod_name"].as_str().unwrap(),
            "my-app-deployment-7d4c8b9f8d-xk2pq"
        );
        assert_eq!(parsed["namespace"].as_str().unwrap(), "production-env");
        assert_eq!(parsed["container_name"].as_str().unwrap(), "nginx-sidecar");
        assert_eq!(parsed["pod_id"].as_str().unwrap(), "abc123-def456-ghi789");
    }

    #[test]
    fn test_unicode_content() {
        let fields = FieldsBody::new(
            "日本語のログメッセージ 🚀".to_string(),
            Utc::now(),
            "测试-pod".to_string(),
            "пространство-имён".to_string(),
            "контейнер".to_string(),
            "pod-идентификатор".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Unicode should be preserved correctly
        assert_eq!(
            parsed["message"].as_str().unwrap(),
            "日本語のログメッセージ 🚀"
        );
        assert_eq!(parsed["pod_name"].as_str().unwrap(), "测试-pod");
        assert_eq!(parsed["namespace"].as_str().unwrap(), "пространство-имён");
        assert_eq!(parsed["container_name"].as_str().unwrap(), "контейнер");
        assert_eq!(parsed["pod_id"].as_str().unwrap(), "pod-идентификатор");
    }

    #[test]
    fn test_json_size_reasonable() {
        let fields = FieldsBody::new(
            "normal message".to_string(),
            Utc::now(),
            "pod-name".to_string(),
            "namespace".to_string(),
            "container".to_string(),
            "pod-id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();

        // JSON should be reasonably sized (not bloated)
        assert!(json.len() < 500); // Should be under 500 bytes for typical usage
        assert!(json.len() > 50); // Should have some content
    }

    #[test]
    fn test_complete_elasticsearch_bulk_format() {
        // Test how Index and FieldsBody would work together in ES bulk format
        let index = Index::new();
        let fields = FieldsBody::new(
            "test log entry".to_string(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
            "test-pod".to_string(),
            "test-ns".to_string(),
            "test-container".to_string(),
            "test-pod-id".to_string(),
        );

        let index_json = serde_json::to_string(&index).unwrap();
        let fields_json = serde_json::to_string(&fields).unwrap();

        // Simulate ES bulk format (index line + document line)
        let bulk_entry = format!("{}\n{}", index_json, fields_json);

        // Should be valid bulk format
        let lines: Vec<&str> = bulk_entry.lines().collect();
        assert_eq!(lines.len(), 2);

        // First line should be index metadata
        assert!(lines[0].contains("index"));
        assert!(lines[0].contains("_id"));

        // Second line should be document
        assert!(lines[1].contains("test log entry"));
        assert!(lines[1].contains("@timestamp"));
        assert!(lines[1].contains("test-pod"));
    }

    #[test]
    fn test_field_rename_serialization() {
        let fields = FieldsBody::new(
            "message".to_string(),
            Utc::now(),
            "pod".to_string(),
            "ns".to_string(),
            "container".to_string(),
            "id".to_string(),
        );

        let json = serde_json::to_string(&fields).unwrap();

        // Ensure the timestamp field is renamed correctly
        assert!(json.contains("@timestamp"));
        // Note: serde renames but original field name might still appear in some contexts

        // Ensure the ID field is renamed correctly in IndexBody
        let index = Index::new();
        let index_json = serde_json::to_string(&index).unwrap();
        assert!(index_json.contains("_id"));

        // Verify the actual field names in parsed JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("@timestamp").is_some());
        assert!(parsed.get("timestamp").is_none()); // Original field name should not exist

        let index_parsed: serde_json::Value = serde_json::from_str(&index_json).unwrap();
        assert!(index_parsed["index"]["_id"].is_string());
        assert!(index_parsed["index"].get("id").is_none()); // Original field name should not exist
    }
}
