use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub meta: Meta,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn create_test_meta() -> Meta {
        Meta {
            pod_name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            container_name: "test-container".to_string(),
            pod_id: "test-pod-id-12345".to_string(),
        }
    }

    #[test]
    fn test_event_new() {
        let meta = create_test_meta();
        let message = "Test log message".to_string();
        let event = Event::new(message.clone(), meta.clone());

        assert_eq!(event.message, message);
        assert_eq!(event.meta, meta);
        // Timestamp should be very recent (within last second)
        let now = Utc::now();
        assert!(now.signed_duration_since(event.timestamp).num_seconds() < 1);
    }

    #[test]
    fn test_meta_default() {
        let meta = Meta::default();

        assert_eq!(meta.pod_name, "");
        assert_eq!(meta.namespace, "");
        assert_eq!(meta.container_name, "");
        assert_eq!(meta.pod_id, "");
    }

    #[test]
    fn test_meta_creation() {
        let meta = Meta {
            pod_name: "my-pod".to_string(),
            namespace: "production".to_string(),
            container_name: "web-server".to_string(),
            pod_id: "abc123-def456-ghi789".to_string(),
        };

        assert_eq!(meta.pod_name, "my-pod");
        assert_eq!(meta.namespace, "production");
        assert_eq!(meta.container_name, "web-server");
        assert_eq!(meta.pod_id, "abc123-def456-ghi789");
    }

    #[test]
    fn test_event_cloning() {
        let meta = create_test_meta();
        let event = Event::new("Original message".to_string(), meta);
        let cloned_event = event.clone();

        assert_eq!(event.message, cloned_event.message);
        assert_eq!(event.timestamp, cloned_event.timestamp);
        assert_eq!(event.meta, cloned_event.meta);
    }

    #[test]
    fn test_meta_cloning() {
        let meta = create_test_meta();
        let cloned_meta = meta.clone();

        assert_eq!(meta.pod_name, cloned_meta.pod_name);
        assert_eq!(meta.namespace, cloned_meta.namespace);
        assert_eq!(meta.container_name, cloned_meta.container_name);
        assert_eq!(meta.pod_id, cloned_meta.pod_id);
    }

    #[test]
    fn test_event_serialization() {
        let meta = create_test_meta();
        let event = Event::new("Serialization test message".to_string(), meta);

        // Test serialization to JSON
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("Serialization test message"));
        assert!(json.contains("test-pod"));
        assert!(json.contains("test-namespace"));
        assert!(json.contains("test-container"));
        assert!(json.contains("test-pod-id-12345"));

        // Test deserialization from JSON
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.message, deserialized.message);
        assert_eq!(event.timestamp, deserialized.timestamp);
        assert_eq!(event.meta, deserialized.meta);
    }

    #[test]
    fn test_meta_serialization() {
        let meta = create_test_meta();

        // Test serialization to JSON
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("test-pod"));
        assert!(json.contains("test-namespace"));
        assert!(json.contains("test-container"));
        assert!(json.contains("test-pod-id-12345"));

        // Test deserialization from JSON
        let deserialized: Meta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, deserialized);
    }

    #[test]
    fn test_event_with_empty_fields() {
        let meta = Meta::default();
        let event = Event::new("".to_string(), meta);

        assert_eq!(event.message, "");
        assert_eq!(event.meta.pod_name, "");
        assert_eq!(event.meta.namespace, "");
        assert_eq!(event.meta.container_name, "");
        assert_eq!(event.meta.pod_id, "");
    }

    #[test]
    fn test_event_with_unicode_content() {
        let meta = Meta {
            pod_name: "ποδ-тест".to_string(),
            namespace: "测试命名空间".to_string(),
            container_name: "コンテナ".to_string(),
            pod_id: "🚀-pod-id".to_string(),
        };
        let message = "Unicode log: 日本語 русский ελληνικά 中文 🔥".to_string();
        let event = Event::new(message.clone(), meta.clone());

        assert_eq!(event.message, message);
        assert_eq!(event.meta, meta);

        // Test serialization with unicode
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.message, deserialized.message);
        assert_eq!(event.meta, deserialized.meta);
    }

    #[test]
    fn test_event_with_special_characters() {
        let meta = Meta {
            pod_name: "pod-with-\"quotes\"".to_string(),
            namespace: "namespace\\with\\backslashes".to_string(),
            container_name: "container\nwith\nnewlines".to_string(),
            pod_id: "pod\twith\ttabs".to_string(),
        };
        let message = "Log with special chars: \"quotes\" \\ newlines\n and tabs\t".to_string();
        let event = Event::new(message.clone(), meta.clone());

        // Test serialization handles special characters correctly
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.message, deserialized.message);
        assert_eq!(event.meta, deserialized.meta);
    }

    #[test]
    fn test_event_debug_formatting() {
        let meta = create_test_meta();
        let event = Event::new("Debug test message".to_string(), meta);

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Debug test message"));
        assert!(debug_str.contains("test-pod"));
        assert!(debug_str.contains("test-namespace"));
        assert!(debug_str.contains("test-container"));
        assert!(debug_str.contains("test-pod-id-12345"));
    }

    #[test]
    fn test_meta_debug_formatting() {
        let meta = create_test_meta();

        let debug_str = format!("{:?}", meta);
        assert!(debug_str.contains("pod_name: \"test-pod\""));
        assert!(debug_str.contains("namespace: \"test-namespace\""));
        assert!(debug_str.contains("container_name: \"test-container\""));
        assert!(debug_str.contains("pod_id: \"test-pod-id-12345\""));
    }

    #[test]
    fn test_event_timestamp_precision() {
        let meta = create_test_meta();
        let event1 = Event::new("First message".to_string(), meta.clone());
        let event2 = Event::new("Second message".to_string(), meta);

        // Events created in quick succession should have different timestamps
        // (or at least the same timestamp if created too quickly)
        assert!(event2.timestamp >= event1.timestamp);
    }

    #[test]
    fn test_event_with_multiline_message() {
        let meta = create_test_meta();
        let message = "Line 1\nLine 2\nLine 3\n\nLine 5 after empty line".to_string();
        let event = Event::new(message.clone(), meta);

        assert_eq!(event.message, message);

        // Test serialization preserves multiline content
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.message, deserialized.message);
    }

    #[test]
    fn test_event_equality() {
        let meta = create_test_meta();
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();

        let event1 = Event {
            message: "Test message".to_string(),
            timestamp,
            meta: meta.clone(),
        };

        let event2 = Event {
            message: "Test message".to_string(),
            timestamp,
            meta: meta.clone(),
        };

        let event3 = Event {
            message: "Different message".to_string(),
            timestamp,
            meta,
        };

        assert_eq!(event1, event2);
        assert_ne!(event1, event3);
    }

    #[test]
    fn test_meta_equality() {
        let meta1 = create_test_meta();
        let meta2 = create_test_meta();
        let meta3 = Meta {
            pod_name: "different-pod".to_string(),
            namespace: "test-namespace".to_string(),
            container_name: "test-container".to_string(),
            pod_id: "test-pod-id-12345".to_string(),
        };

        assert_eq!(meta1, meta2);
        assert_ne!(meta1, meta3);
    }
}
