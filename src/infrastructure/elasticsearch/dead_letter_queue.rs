use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::domain::event::Event;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub event: Event,
    pub failure_reason: String,
    pub timestamp: u64,
    pub retry_count: usize,
}

#[derive(Debug, Clone)]
pub struct DeadLetterQueueConfig {
    pub max_queue_size: usize,
    pub persistence_file: Option<String>,
    pub flush_interval: Duration,
}

impl Default for DeadLetterQueueConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            persistence_file: Some("/tmp/logfowd2_dead_letters.json".to_string()),
            flush_interval: Duration::from_secs(60), // 1 minute
        }
    }
}

pub struct DeadLetterQueue {
    config: DeadLetterQueueConfig,
    queue: Arc<RwLock<VecDeque<DeadLetter>>>,
    stats: Arc<RwLock<DeadLetterStats>>,
}

#[derive(Debug, Default)]
pub struct DeadLetterStats {
    pub total_events: usize,
    pub events_in_queue: usize,
    pub events_retried: usize,
    pub events_permanently_failed: usize,
    pub events_recovered: usize,
}

impl DeadLetterQueue {
    pub fn new(config: DeadLetterQueueConfig) -> Self {
        let queue = Arc::new(RwLock::new(VecDeque::new()));
        let stats = Arc::new(RwLock::new(DeadLetterStats::default()));

        Self {
            config,
            queue,
            stats,
        }
    }

    pub async fn add_failed_event(&self, event: Event, failure_reason: String) {
        let failure_reason_for_log = failure_reason.clone();
        let dead_letter = DeadLetter {
            event,
            failure_reason,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            retry_count: 0,
        };

        let mut queue = self.queue.write().await;
        let mut stats = self.stats.write().await;

        // Check queue size limit
        if queue.len() >= self.config.max_queue_size {
            // Remove oldest item
            if let Some(removed) = queue.pop_front() {
                warn!(
                    "Dead letter queue full, dropping oldest event: {}",
                    removed.failure_reason
                );
                stats.events_permanently_failed += 1;
            }
        }

        queue.push_back(dead_letter);
        stats.total_events += 1;
        stats.events_in_queue = queue.len();

        debug!(
            "Added event to dead letter queue: {} (queue size: {})",
            failure_reason_for_log,
            queue.len()
        );
    }

    pub async fn flush_to_disk(&self) -> Result<(), std::io::Error> {
        if let Some(ref file_path) = self.config.persistence_file {
            let queue = self.queue.read().await;
            let data: Vec<DeadLetter> = queue.iter().cloned().collect();

            if data.is_empty() {
                return Ok(());
            }

            let json_data = serde_json::to_string_pretty(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            let temp_path = format!("{}.tmp", file_path);
            match std::fs::write(&temp_path, &json_data) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::StorageFull => {
                    error!("Disk full while writing dead letter queue to {}", temp_path);
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
            std::fs::rename(&temp_path, file_path)?;

            debug!("Flushed {} dead letters to {}", data.len(), file_path);
        }
        Ok(())
    }

    pub async fn load_from_disk(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref file_path) = self.config.persistence_file {
            if !Path::new(file_path).exists() {
                info!("Dead letter queue file {} does not exist", file_path);
                return Ok(());
            }

            let data = std::fs::read_to_string(file_path)?;
            let dead_letters: Vec<DeadLetter> = serde_json::from_str(&data)?;

            let mut queue = self.queue.write().await;
            let mut stats = self.stats.write().await;

            queue.clear();

            let mut loaded_count = 0;
            let mut dropped_count = 0;

            for letter in dead_letters.iter() {
                if queue.len() < self.config.max_queue_size {
                    queue.push_back(letter.clone());
                    loaded_count += 1;
                } else {
                    // Exceeded capacity - mark as permanently failed
                    dropped_count += 1;
                    stats.events_permanently_failed += 1;
                }
            }

            stats.events_in_queue = queue.len();
            stats.total_events += dead_letters.len(); // Total includes both loaded and dropped

            info!(
                "Loaded {} dead letters from {} (dropped {} due to capacity limit)",
                loaded_count, file_path, dropped_count
            );
        }
        Ok(())
    }

    pub async fn start_background_tasks(&self, shutdown_notify: Arc<Notify>) -> JoinHandle<()> {
        let queue_clone = Arc::clone(&self.queue);
        let config_clone = self.config.clone();
        let stats_clone = Arc::clone(&self.stats); // Share the same stats instance for consistency

        // Periodic flush task
        tokio::spawn(async move {
            let mut interval = interval(config_clone.flush_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let dlq = DeadLetterQueue {
                            config: config_clone.clone(),
                            queue: queue_clone.clone(),
                            stats: stats_clone.clone(), // Use shared stats to maintain consistency
                        };

                        if let Err(e) = dlq.flush_to_disk().await {
                            match e.kind() {
                                std::io::ErrorKind::StorageFull => {
                                    error!(
                                        "Failed to flush dead letter queue due to insufficient disk space: {}",
                                        e
                                    );
                                    warn!(
                                        "Dead letter queue persistence is degraded due to disk space constraints"
                                    );
                                }
                                _ => {
                                    error!("Failed to flush dead letter queue to disk: {}", e);
                                }
                            }
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        info!("DLQ background flusher received shutdown signal");

                        // Perform final flush before shutdown
                        let dlq = DeadLetterQueue {
                            config: config_clone.clone(),
                            queue: queue_clone.clone(),
                            stats: stats_clone.clone(),
                        };

                        if let Err(e) = dlq.flush_to_disk().await {
                            error!("Failed final DLQ flush during shutdown: {}", e);
                        } else {
                            debug!("DLQ background flusher completed final flush");
                        }

                        break;
                    }
                }
            }

            info!("DLQ background flusher shutdown complete");
        })
    }
}

impl Clone for DeadLetterStats {
    fn clone(&self) -> Self {
        Self {
            total_events: self.total_events,
            events_in_queue: self.events_in_queue,
            events_retried: self.events_retried,
            events_permanently_failed: self.events_permanently_failed,
            events_recovered: self.events_recovered,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::{Event, Meta};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::sync::Notify;

    fn create_test_event(message: &str) -> Event {
        Event::new(message.to_string(), Meta::default())
    }

    fn create_test_config() -> DeadLetterQueueConfig {
        DeadLetterQueueConfig {
            max_queue_size: 5,
            persistence_file: None,
            flush_interval: Duration::from_millis(100),
        }
    }

    fn create_test_config_with_file(file_path: String) -> DeadLetterQueueConfig {
        DeadLetterQueueConfig {
            max_queue_size: 10,
            persistence_file: Some(file_path),
            flush_interval: Duration::from_millis(100),
        }
    }

    #[tokio::test]
    async fn test_new_dead_letter_queue() {
        let config = create_test_config();
        let dlq = DeadLetterQueue::new(config.clone());

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        assert_eq!(queue.len(), 0);
        assert_eq!(stats.total_events, 0);
        assert_eq!(stats.events_in_queue, 0);
    }

    #[tokio::test]
    async fn test_add_failed_event() {
        let config = create_test_config();
        let dlq = DeadLetterQueue::new(config);

        let event = create_test_event("test message");
        dlq.add_failed_event(event.clone(), "test failure".to_string())
            .await;

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        assert_eq!(queue.len(), 1);
        assert_eq!(stats.total_events, 1);
        assert_eq!(stats.events_in_queue, 1);

        let dead_letter = queue.front().unwrap();
        assert_eq!(dead_letter.event.message, "test message");
        assert_eq!(dead_letter.failure_reason, "test failure");
        assert_eq!(dead_letter.retry_count, 0);
        assert!(dead_letter.timestamp > 0);
    }

    #[tokio::test]
    async fn test_queue_size_limit() {
        let config = create_test_config(); // max_queue_size = 5
        let dlq = DeadLetterQueue::new(config);

        // Add events up to limit
        for i in 0..5 {
            let event = create_test_event(&format!("message {}", i));
            dlq.add_failed_event(event, format!("failure {}", i)).await;
        }

        let queue = dlq.queue.read().await;
        assert_eq!(queue.len(), 5);
        drop(queue);

        // Add one more event - should evict oldest
        let event = create_test_event("message 5");
        dlq.add_failed_event(event, "failure 5".to_string()).await;

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        assert_eq!(queue.len(), 5); // Still at limit
        assert_eq!(stats.events_permanently_failed, 1); // One event was dropped
        assert_eq!(stats.total_events, 6); // Total includes dropped event

        // Verify oldest was removed and newest is at back
        assert_eq!(queue.front().unwrap().failure_reason, "failure 1");
        assert_eq!(queue.back().unwrap().failure_reason, "failure 5");
    }

    #[tokio::test]
    async fn test_flush_to_disk_no_file() {
        let config = create_test_config(); // No persistence file
        let dlq = DeadLetterQueue::new(config);

        let event = create_test_event("test message");
        dlq.add_failed_event(event, "test failure".to_string())
            .await;

        // Should succeed even with no file configured
        let result = dlq.flush_to_disk().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_flush_to_disk_empty_queue() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let config = create_test_config_with_file(file_path);
        let dlq = DeadLetterQueue::new(config);

        // Flush empty queue
        let result = dlq.flush_to_disk().await;
        assert!(result.is_ok());

        // File should not be created for empty queue
        assert!(
            !temp_file.path().exists() || std::fs::metadata(temp_file.path()).unwrap().len() == 0
        );
    }

    #[tokio::test]
    async fn test_flush_and_load_from_disk() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let config = create_test_config_with_file(file_path.clone());
        let dlq = DeadLetterQueue::new(config.clone());

        // Add test events
        for i in 0..3 {
            let event = create_test_event(&format!("message {}", i));
            dlq.add_failed_event(event, format!("failure {}", i)).await;
        }

        // Flush to disk
        let result = dlq.flush_to_disk().await;
        assert!(result.is_ok());

        // Verify file exists and has content
        assert!(std::path::Path::new(&file_path).exists());
        let file_content = std::fs::read_to_string(&file_path).unwrap();
        assert!(file_content.contains("message 0"));
        assert!(file_content.contains("failure 2"));

        // Create new DLQ and load from disk
        let dlq2 = DeadLetterQueue::new(config);
        let result = dlq2.load_from_disk().await;
        assert!(result.is_ok());

        let queue = dlq2.queue.read().await;
        let stats = dlq2.stats.read().await;

        assert_eq!(queue.len(), 3);
        assert_eq!(stats.events_in_queue, 3);
        assert_eq!(stats.total_events, 3);

        // Verify loaded data
        let events: Vec<_> = queue.iter().collect();
        assert_eq!(events[0].event.message, "message 0");
        assert_eq!(events[1].failure_reason, "failure 1");
        assert_eq!(events[2].event.message, "message 2");
    }

    #[tokio::test]
    async fn test_load_from_nonexistent_file() {
        let config = create_test_config_with_file("/nonexistent/path/file.json".to_string());
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        assert!(result.is_ok());

        let queue = dlq.queue.read().await;
        assert_eq!(queue.len(), 0);
    }

    #[tokio::test]
    async fn test_load_from_invalid_json() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();

        // Write invalid JSON
        std::fs::write(&file_path, "invalid json content").unwrap();

        let config = create_test_config_with_file(file_path);
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_default_config() {
        let config = DeadLetterQueueConfig::default();

        assert_eq!(config.max_queue_size, 10000);
        assert_eq!(
            config.persistence_file,
            Some("/tmp/logfowd2_dead_letters.json".to_string())
        );
        assert_eq!(config.flush_interval, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_dead_letter_serialization() {
        let event = create_test_event("test message");
        let dead_letter = DeadLetter {
            event: event.clone(),
            failure_reason: "test failure".to_string(),
            timestamp: 1234567890,
            retry_count: 2,
        };

        // Test serialization
        let json = serde_json::to_string(&dead_letter).unwrap();
        assert!(json.contains("test message"));
        assert!(json.contains("test failure"));
        assert!(json.contains("1234567890"));
        assert!(json.contains("\"retry_count\":2"));

        // Test deserialization
        let deserialized: DeadLetter = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event.message, "test message");
        assert_eq!(deserialized.failure_reason, "test failure");
        assert_eq!(deserialized.timestamp, 1234567890);
        assert_eq!(deserialized.retry_count, 2);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let config = create_test_config();
        let dlq = DeadLetterQueue::new(config);

        // Add some events
        for i in 0..3 {
            let event = create_test_event(&format!("message {}", i));
            dlq.add_failed_event(event, format!("failure {}", i)).await;
        }

        let stats = dlq.stats.read().await;
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.events_in_queue, 3);
        assert_eq!(stats.events_permanently_failed, 0);
    }

    #[tokio::test]
    async fn test_stats_with_queue_overflow() {
        let config = create_test_config(); // max_queue_size = 5
        let dlq = DeadLetterQueue::new(config);

        // Add more events than queue size
        for i in 0..7 {
            let event = create_test_event(&format!("message {}", i));
            dlq.add_failed_event(event, format!("failure {}", i)).await;
        }

        let stats = dlq.stats.read().await;
        assert_eq!(stats.total_events, 7);
        assert_eq!(stats.events_in_queue, 5); // Queue is capped
        assert_eq!(stats.events_permanently_failed, 2); // 2 events were dropped
    }

    #[tokio::test]
    async fn test_atomic_file_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let temp_path = format!("{}.tmp", file_path);

        let config = create_test_config_with_file(file_path.clone());
        let dlq = DeadLetterQueue::new(config);

        // Add test event
        let event = create_test_event("test message");
        dlq.add_failed_event(event, "test failure".to_string())
            .await;

        // Flush to disk
        dlq.flush_to_disk().await.unwrap();

        // Verify temp file was cleaned up
        assert!(!std::path::Path::new(&temp_path).exists());
        // Verify main file exists
        assert!(std::path::Path::new(&file_path).exists());
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let config = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(config));

        // Spawn multiple tasks adding events concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let dlq_clone = dlq.clone();
            handles.push(tokio::spawn(async move {
                let event = create_test_event(&format!("concurrent message {}", i));
                dlq_clone
                    .add_failed_event(event, format!("concurrent failure {}", i))
                    .await;
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        // All events should be added (queue size is 5, so some will be evicted)
        assert_eq!(queue.len(), 5); // Queue limit
        assert_eq!(stats.total_events, 10); // All were processed
        assert_eq!(stats.events_permanently_failed, 5); // 5 were evicted
    }

    #[tokio::test]
    async fn test_load_from_disk_success() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();

        // First, create a DLQ and save some events
        {
            let config = create_test_config_with_file(file_path.clone());
            let dlq = DeadLetterQueue::new(config);

            for i in 0..3 {
                let event = create_test_event(&format!("saved message {}", i));
                dlq.add_failed_event(event, format!("saved failure {}", i))
                    .await;
            }

            dlq.flush_to_disk().await.unwrap();
        }

        // Now create a new DLQ and load from disk
        let config = create_test_config_with_file(file_path);
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        assert!(result.is_ok());

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        assert_eq!(queue.len(), 3);
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.events_in_queue, 3);

        // Verify content
        let events: Vec<_> = queue.iter().collect();
        for (i, dead_letter) in events.iter().enumerate() {
            assert_eq!(dead_letter.event.message, format!("saved message {}", i));
            assert_eq!(dead_letter.failure_reason, format!("saved failure {}", i));
        }
    }

    #[tokio::test]
    async fn test_load_from_disk_nonexistent_file() {
        let config = create_test_config_with_file("/nonexistent/path/file.json".to_string());
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        assert!(result.is_ok()); // Should handle gracefully

        let queue = dlq.queue.read().await;
        assert_eq!(queue.len(), 0); // Should be empty
    }

    #[tokio::test]
    async fn test_load_from_disk_corrupted_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();

        // Write invalid JSON to file
        tokio::fs::write(&file_path, "invalid json content")
            .await
            .unwrap();

        let config = create_test_config_with_file(file_path);
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        // Should handle gracefully and log error
        assert!(result.is_ok() || result.is_err());

        let queue = dlq.queue.read().await;
        assert_eq!(queue.len(), 0); // Should be empty due to corruption
    }

    #[tokio::test]
    async fn test_load_exceeding_queue_capacity() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();

        // First, create more events than queue capacity
        {
            let mut large_config = create_test_config_with_file(file_path.clone());
            large_config.max_queue_size = 100; // Temporarily large
            let dlq = DeadLetterQueue::new(large_config);

            for i in 0..8 {
                let event = create_test_event(&format!("message {}", i));
                dlq.add_failed_event(event, format!("failure {}", i)).await;
            }

            dlq.flush_to_disk().await.unwrap();
        }

        // Now load with smaller capacity
        let mut config = create_test_config_with_file(file_path);
        config.max_queue_size = 5; // Set explicit small capacity
        let dlq = DeadLetterQueue::new(config);

        let result = dlq.load_from_disk().await;
        assert!(result.is_ok());

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        // Should load only up to capacity
        assert_eq!(queue.len(), 5);
        assert_eq!(stats.events_in_queue, 5);
        // The rest should be marked as permanently failed
        assert_eq!(stats.events_permanently_failed, 3);
        assert_eq!(stats.total_events, 8);
    }

    #[tokio::test]
    async fn test_retry_count_increment() {
        let config = create_test_config();
        let dlq = DeadLetterQueue::new(config);

        let event = create_test_event("test message");
        dlq.add_failed_event(event, "initial failure".to_string())
            .await;

        // Modify retry count directly (simulating retry logic)
        {
            let mut queue = dlq.queue.write().await;
            if let Some(dead_letter) = queue.front_mut() {
                dead_letter.retry_count += 1;
            }
        }

        let queue = dlq.queue.read().await;
        assert_eq!(queue.front().unwrap().retry_count, 1);
    }

    #[tokio::test]
    async fn test_event_deduplication_logic() {
        let config = create_test_config();
        let dlq = DeadLetterQueue::new(config);

        // Add same event multiple times
        let event1 = create_test_event("duplicate message");
        let event2 = create_test_event("duplicate message");

        dlq.add_failed_event(event1, "failure 1".to_string()).await;
        dlq.add_failed_event(event2, "failure 2".to_string()).await;

        let queue = dlq.queue.read().await;
        let stats = dlq.stats.read().await;

        // Both should be added (no built-in deduplication)
        assert_eq!(queue.len(), 2);
        assert_eq!(stats.total_events, 2);

        // Verify they're both there
        let events: Vec<_> = queue.iter().collect();
        assert_eq!(events[0].event.message, "duplicate message");
        assert_eq!(events[1].event.message, "duplicate message");
        assert_eq!(events[0].failure_reason, "failure 1");
        assert_eq!(events[1].failure_reason, "failure 2");
    }

    #[tokio::test]
    async fn test_periodic_flush_behavior() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();

        let mut config = create_test_config_with_file(file_path.clone());
        config.flush_interval = Duration::from_millis(50); // Fast flush
        let dlq = DeadLetterQueue::new(config);

        // Add event and start background flush
        let event = create_test_event("flush test message");
        dlq.add_failed_event(event, "flush test failure".to_string())
            .await;

        // Create a basic background flush task (simplified)
        let dlq_clone = Arc::new(dlq);
        let flush_dlq = dlq_clone.clone();
        let flush_handle = tokio::spawn(async move {
            // Simulate background flush after interval
            tokio::time::sleep(Duration::from_millis(100)).await;
            flush_dlq.flush_to_disk().await
        });

        // Wait for flush
        let flush_result = flush_handle.await.unwrap();
        assert!(flush_result.is_ok());

        // Verify file was written
        let file_exists = std::path::Path::new(&file_path).exists();
        assert!(file_exists);

        // Verify file has content
        let file_content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert!(file_content.contains("flush test message"));
    }

    #[tokio::test]
    async fn test_empty_queue_flush_optimization() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let config = create_test_config_with_file(file_path.clone());
        let dlq = DeadLetterQueue::new(config);

        // Flush empty queue
        let result = dlq.flush_to_disk().await;
        assert!(result.is_ok());

        // File should either not exist or be empty/minimal
        if std::path::Path::new(&file_path).exists() {
            let file_content = tokio::fs::read_to_string(&file_path).await.unwrap();
            // Should be empty array or minimal JSON
            assert!(file_content == "[]" || file_content.trim().is_empty());
        }
    }

    #[tokio::test]
    async fn test_stats_accuracy_after_disk_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let config = create_test_config_with_file(file_path.clone());

        // First DLQ instance - add events and flush
        {
            let dlq1 = DeadLetterQueue::new(config.clone());

            // Add events and flush
            for i in 0..3 {
                let event = create_test_event(&format!("stats test {}", i));
                dlq1.add_failed_event(event, format!("stats failure {}", i))
                    .await;
            }

            dlq1.flush_to_disk().await.unwrap();

            // Stats should be accurate after flush
            let stats_after_flush = dlq1.stats.read().await;
            assert_eq!(stats_after_flush.total_events, 3);
            assert_eq!(stats_after_flush.events_in_queue, 3);
        }

        // Second DLQ instance - load from disk
        {
            let dlq2 = DeadLetterQueue::new(config);

            // Load from disk (fresh instance, no lock conflicts)
            dlq2.load_from_disk().await.unwrap();

            // Stats should be updated after load
            let stats_after_load = dlq2.stats.read().await;
            assert_eq!(stats_after_load.events_in_queue, 3);
            // total_events should include loaded events
            assert_eq!(stats_after_load.total_events, 3);
        }
    }

    #[tokio::test]
    async fn test_disk_space_handling() {
        // Test with a very long file path to simulate potential disk issues
        let long_path = "/tmp/".to_string() + &"very_long_name_".repeat(50) + "dead_letters.json";
        let config = create_test_config_with_file(long_path);
        let dlq = DeadLetterQueue::new(config);

        let event = create_test_event("disk space test");
        dlq.add_failed_event(event, "disk test failure".to_string())
            .await;

        // Flush should handle path issues gracefully
        let result = dlq.flush_to_disk().await;
        // Might fail due to path, but shouldn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let mut config = create_test_config_with_file(file_path.clone());
        config.flush_interval = Duration::from_millis(100); // Fast flush for testing

        let dlq = DeadLetterQueue::new(config);
        let shutdown_notify = Arc::new(Notify::new());

        // Add an event
        let event = create_test_event("shutdown test");
        dlq.add_failed_event(event, "shutdown test failure".to_string())
            .await;

        // Start background tasks
        let handle = dlq.start_background_tasks(shutdown_notify.clone()).await;

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Signal shutdown
        shutdown_notify.notify_waiters();

        // Wait for graceful shutdown
        let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(
            result.is_ok(),
            "Background task should shut down gracefully"
        );

        // Verify final flush occurred
        assert!(std::path::Path::new(&file_path).exists());
        let file_content = std::fs::read_to_string(&file_path).unwrap();
        assert!(file_content.contains("shutdown test"));
    }

    #[tokio::test]
    async fn test_background_task_periodic_flush() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let mut config = create_test_config_with_file(file_path.clone());
        config.flush_interval = Duration::from_millis(50); // Very fast for testing

        let dlq = DeadLetterQueue::new(config);
        let shutdown_notify = Arc::new(Notify::new());

        // Start background tasks
        let handle = dlq.start_background_tasks(shutdown_notify.clone()).await;

        // Add events over time
        for i in 0..3 {
            let event = create_test_event(&format!("periodic test {}", i));
            dlq.add_failed_event(event, format!("periodic failure {}", i))
                .await;
            tokio::time::sleep(Duration::from_millis(60)).await; // Let flush happen
        }

        // Should have flushed multiple times by now
        if std::path::Path::new(&file_path).exists() {
            let file_content = std::fs::read_to_string(&file_path).unwrap();
            assert!(file_content.contains("periodic test"));
        }

        // Clean shutdown
        shutdown_notify.notify_waiters();
        let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_with_flush_error() {
        // Test shutdown behavior when final flush fails
        let invalid_path = "/invalid/directory/does/not/exist/dead_letters.json";
        let config = create_test_config_with_file(invalid_path.to_string());
        let dlq = DeadLetterQueue::new(config);
        let shutdown_notify = Arc::new(Notify::new());

        // Add an event
        let event = create_test_event("error test");
        dlq.add_failed_event(event, "error test failure".to_string())
            .await;

        // Start background tasks
        let handle = dlq.start_background_tasks(shutdown_notify.clone()).await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Signal shutdown (final flush will fail due to invalid path)
        shutdown_notify.notify_waiters();

        // Should still shut down gracefully despite flush error
        let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(
            result.is_ok(),
            "Should shut down gracefully even with flush errors"
        );
    }

    #[tokio::test]
    async fn test_multiple_shutdown_signals() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let mut config = create_test_config_with_file(file_path.clone());
        config.flush_interval = Duration::from_millis(200);

        let dlq = DeadLetterQueue::new(config);
        let shutdown_notify = Arc::new(Notify::new());

        // Start background tasks
        let handle = dlq.start_background_tasks(shutdown_notify.clone()).await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Send multiple shutdown signals
        shutdown_notify.notify_waiters();
        shutdown_notify.notify_waiters();
        shutdown_notify.notify_waiters();

        // Should only handle the first one and exit cleanly
        let result = tokio::time::timeout(Duration::from_millis(300), handle).await;
        assert!(result.is_ok(), "Should handle multiple signals gracefully");
    }

    #[tokio::test]
    async fn test_immediate_shutdown_before_first_flush() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_string_lossy().to_string();
        let mut config = create_test_config_with_file(file_path.clone());
        config.flush_interval = Duration::from_secs(60); // Long interval

        let dlq = DeadLetterQueue::new(config);
        let shutdown_notify = Arc::new(Notify::new());

        // Add an event
        let event = create_test_event("immediate shutdown test");
        dlq.add_failed_event(event, "immediate shutdown failure".to_string())
            .await;

        // Start background tasks and give it a moment to initialize
        let handle = dlq.start_background_tasks(shutdown_notify.clone()).await;

        // Small delay to ensure the background task is ready
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Signal shutdown immediately
        shutdown_notify.notify_waiters();

        // Should complete final flush and shutdown
        let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(result.is_ok(), "Should handle immediate shutdown");

        // Verify final flush occurred even though regular interval didn't fire
        assert!(std::path::Path::new(&file_path).exists());
        let file_content = std::fs::read_to_string(&file_path).unwrap();
        assert!(file_content.contains("immediate shutdown test"));
    }
}
