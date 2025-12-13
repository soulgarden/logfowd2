use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use tokio::sync::Notify;
use tokio::time::interval;

use crate::config::Settings;
use crate::domain::event::Event;
use crate::error::Result;
use crate::infrastructure::metrics::{are_metrics_enabled, metrics};
use crate::traits::EventProcessor;
use crate::transport::channels::{BoundedReceiver, BoundedSender};

pub struct Sender {
    conf: Settings,
    process_queue_receiver: BoundedReceiver<Event>,
    es_queue_sender: BoundedSender<Vec<Event>>,
    metrics_enabled: bool,
}

impl Sender {
    pub fn new(
        conf: Settings,
        process_queue: BoundedReceiver<Event>,
        es_queue_sender: BoundedSender<Vec<Event>>,
    ) -> Self {
        let metrics_enabled = are_metrics_enabled(&conf.metrics);

        Sender {
            conf,
            process_queue_receiver: process_queue,
            es_queue_sender,
            metrics_enabled,
        }
    }

    pub async fn run(&mut self, shutdown: Arc<Notify>) -> Result<()> {
        let mut ticker = interval(Duration::from_millis(
            self.conf.elasticsearch.flush_interval,
        ));
        ticker.tick().await; // Skip the immediate first tick

        let mut batch = Vec::new();

        loop {
            tokio::select! {
                // Timer-based flush
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await;
                    }
                }

                // Receive events with timeout to avoid hanging
                event_result = tokio::time::timeout(Duration::from_millis(100), self.process_queue_receiver.recv()) => {
                    match event_result {
                        Ok(Ok(event)) => {
                            batch.push(event);

                            // Check if we should flush due to size
                            if self.conf.elasticsearch.bulk_size > 0 && batch.len() >= self.conf.elasticsearch.bulk_size {
                                self.send_batch(&mut batch).await;
                            }
                        }
                        Ok(Err(_)) => {
                            info!("Process queue closed, initiating graceful shutdown");
                            if !batch.is_empty() {
                                self.send_batch(&mut batch).await;
                            }
                            return Ok(());
                        }
                        Err(_) => {
                            // Timeout - just continue with the loop to check other branches
                            continue;
                        }
                    }
                }

                // Shutdown
                _ = shutdown.notified() => {
                    info!("Sender received shutdown signal");

                    if !batch.is_empty() {
                        info!("Sending {} remaining events before shutdown", batch.len());
                        // Use send_batch directly - it has built-in retry with backoff
                        // (50 attempts × 100ms = 5 seconds max wait)
                        // This ensures events are not lost during graceful shutdown
                        self.send_batch(&mut batch).await;

                        if batch.is_empty() {
                            info!("Finished sending remaining events");
                        } else {
                            warn!("Could not deliver {} events during shutdown (ES unavailable)", batch.len());
                        }
                    }

                    return Ok(());
                }
            }
        }
    }

    async fn send_batch(&mut self, batch: &mut Vec<Event>) {
        if batch.is_empty() {
            return;
        }

        // For bulk_size = 0, send all events in one batch
        // For bulk_size > 0, send events in chunks
        if self.conf.elasticsearch.bulk_size == 0 {
            // Send all events at once
            let events = std::mem::take(batch);
            if let Some(failed_events) = self.send_events_to_es(events).await {
                // Put failed events back at the beginning for retry on next flush
                batch.splice(0..0, failed_events);
            }
        } else {
            // Send events in bulk_size chunks
            while !batch.is_empty() {
                let chunk_size = std::cmp::min(batch.len(), self.conf.elasticsearch.bulk_size);
                let events = batch.drain(..chunk_size).collect();
                if let Some(failed_events) = self.send_events_to_es(events).await {
                    // Put failed events back at the beginning for retry on next flush
                    batch.splice(0..0, failed_events);
                    // Stop processing more chunks - will retry on next flush cycle
                    break;
                }
            }
        }
    }

    /// Send events to ES workers using try_send with retry loop.
    /// Returns None on success, Some(events) on failure (events not lost).
    async fn send_events_to_es(&mut self, mut events: Vec<Event>) -> Option<Vec<Event>> {
        if events.is_empty() {
            return None;
        }

        let event_count = events.len() as u64;

        // Record batch size metric if enabled
        if self.metrics_enabled {
            metrics()
                .batch_size_histogram
                .with_label_values(&["sender"])
                .observe(event_count as f64);
        }

        // Retry loop with try_send - events are never lost
        let max_attempts = 50; // 50 * 100ms = 5 seconds total
        for attempt in 0..max_attempts {
            match self.es_queue_sender.try_send(events) {
                Ok(()) => {
                    // Success
                    if self.metrics_enabled {
                        metrics()
                            .events_processed_total
                            .with_label_values(&["sender", "success"])
                            .inc_by(event_count);
                    }
                    return None;
                }
                Err(crate::transport::channels::SendError::Full(returned_events)) => {
                    // Channel full - retry after delay
                    events = returned_events;
                    if attempt < max_attempts - 1 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                Err(crate::transport::channels::SendError::Closed(returned_events)) => {
                    // Channel closed - return events for later retry
                    warn!(
                        "ES channel closed, {} events will be retried",
                        returned_events.len()
                    );
                    if self.metrics_enabled {
                        metrics()
                            .errors_total
                            .with_label_values(&["sender", "channel_closed"])
                            .inc();
                    }
                    return Some(returned_events);
                }
            }
        }

        // Exhausted retries - return events for retry on next flush cycle
        warn!(
            "Failed to send {} events after {} attempts, will retry on next flush",
            events.len(),
            max_attempts
        );
        if self.metrics_enabled {
            metrics()
                .errors_total
                .with_label_values(&["sender", "retry_exhausted"])
                .inc();
        }
        Some(events)
    }
}

#[async_trait]
impl EventProcessor for Sender {
    async fn process(&mut self, event: Event) -> anyhow::Result<()> {
        // Process a single event by adding it to the batch
        // This is useful for testing and alternative implementations
        let mut batch = vec![event];
        self.send_batch(&mut batch).await;
        Ok(())
    }

    fn can_process(&self) -> bool {
        // Sender can process if the ES queue is not full
        // In the current implementation, we always return true as the sender
        // handles backpressure internally
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::settings::{ElasticsearchConfig, Settings};
    use crate::domain::event::{Event, Meta};
    use crate::transport::channels::BoundedChannel;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio::time::{Duration, sleep, timeout};

    fn create_test_conf(flush_interval: u64, bulk_size: usize) -> Settings {
        Settings {
            log_path: "/tmp/test".to_string(),
            state_file_path: None,
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: None,
            channels: None,
            metrics: None,
            logging: None,
            elasticsearch: ElasticsearchConfig {
                host: "http://localhost".to_string(),
                port: 9200,
                index_name: "test".to_string(),
                flush_interval,
                bulk_size,
                workers: 1,
            },
        }
    }

    fn create_test_event(message: &str) -> Event {
        Event::new(message.to_string(), Meta::default())
    }

    #[tokio::test]
    async fn test_sender_creation() {
        let conf = create_test_conf(1000, 10);
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let sender = Sender::new(
            conf.clone(),
            process_channel.receiver(),
            es_channel.sender(),
        );

        assert_eq!(sender.conf.elasticsearch.bulk_size, 10);
        assert_eq!(sender.conf.elasticsearch.flush_interval, 1000);
    }

    #[tokio::test]
    async fn test_sender_bulk_size_batching() {
        let conf = create_test_conf(10000, 3); // Long flush interval, small bulk size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send events to trigger bulk size batching
        for i in 0..6 {
            process_sender
                .send(create_test_event(&format!("message {}", i)))
                .await
                .unwrap();
        }

        // Should receive 2 batches of 3 events each
        let batch1 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch1.len(), 3);

        let batch2 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch2.len(), 3);

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_timer_based_flushing() {
        let conf = create_test_conf(50, 100); // Short flush interval, large bulk size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send fewer events than bulk size
        for i in 0..2 {
            process_sender
                .send(create_test_event(&format!("timer message {}", i)))
                .await
                .unwrap();
        }

        // Should receive batch due to timer flush
        let batch = timeout(Duration::from_millis(200), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 2);

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_shutdown_with_remaining_events() {
        let conf = create_test_conf(10000, 10); // Long flush interval, large bulk size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send some events (less than bulk size)
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("shutdown message {}", i)))
                .await
                .unwrap();
        }

        // Give time for events to be queued
        sleep(Duration::from_millis(10)).await;

        // Shutdown - should send remaining events
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();

        // Should receive batch with remaining events
        let batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn test_sender_empty_events_timer() {
        let conf = create_test_conf(50, 10); // Short flush interval
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Wait for multiple timer ticks without sending events
        sleep(Duration::from_millis(150)).await;

        // Should not receive any batches since no events
        let result = timeout(Duration::from_millis(50), es_receiver.recv()).await;
        assert!(result.is_err()); // Timeout - no batches sent

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_large_batch_processing() {
        let conf = create_test_conf(10000, 5); // Large bulk size for this test
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(50, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send exactly bulk_size * 2 events
        for i in 0..10 {
            process_sender
                .send(create_test_event(&format!("large batch {}", i)))
                .await
                .unwrap();
        }

        // Should receive exactly 2 batches of 5 events each
        let batch1 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch1.len(), 5);

        let batch2 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch2.len(), 5);

        // Should not receive any more batches immediately
        let result = timeout(Duration::from_millis(50), es_receiver.recv()).await;
        assert!(result.is_err()); // Timeout - no more batches

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_mixed_timer_and_bulk_batching() {
        let conf = create_test_conf(100, 3); // Moderate flush interval, small bulk size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send bulk_size events to trigger immediate batch
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("bulk {}", i)))
                .await
                .unwrap();
        }

        // Should receive bulk batch immediately
        let bulk_batch = timeout(Duration::from_millis(50), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bulk_batch.len(), 3);

        // Send fewer events and wait for timer
        for i in 0..2 {
            process_sender
                .send(create_test_event(&format!("timer {}", i)))
                .await
                .unwrap();
        }

        // Should receive timer batch
        let timer_batch = timeout(Duration::from_millis(200), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(timer_batch.len(), 2);

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_channel_error_handling() {
        let conf = create_test_conf(1000, 5);
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None); // Small capacity

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Fill ES channel and don't consume from it to test backpressure/errors
        for i in 0..5 {
            process_sender
                .send(create_test_event(&format!("backpressure {}", i)))
                .await
                .unwrap();
        }

        // Give time for processing
        sleep(Duration::from_millis(100)).await;

        // Consume one batch to unblock
        let _batch = es_receiver.recv().await.unwrap();

        // Give more time for processing
        sleep(Duration::from_millis(50)).await;

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(500), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_event_order_preservation() {
        let conf = create_test_conf(10000, 5);
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Start sender in background
        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send events in specific order
        let expected_messages = vec!["first", "second", "third", "fourth", "fifth"];
        for msg in &expected_messages {
            process_sender.send(create_test_event(msg)).await.unwrap();
        }

        // Should receive batch with events in FIFO order (VecDeque with pop_front())
        let batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 5);

        // Verify messages are in correct FIFO order
        let received_messages: Vec<String> = batch.iter().map(|e| e.message.clone()).collect();
        assert_eq!(
            received_messages, expected_messages,
            "Events should be in FIFO order"
        );

        // Also check individual order
        for (i, expected) in expected_messages.iter().enumerate() {
            assert_eq!(
                &received_messages[i], expected,
                "Event {} should be '{}', got '{}'",
                i, expected, received_messages[i]
            );
        }

        // Shutdown
        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_configuration_values() {
        // Test different configurations
        let confs = vec![
            create_test_conf(50, 1),    // Fast flush, small batch
            create_test_conf(1000, 10), // Slow flush, large batch
            create_test_conf(100, 5),   // Moderate settings
        ];

        for conf in confs {
            let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
            let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

            let sender = Sender::new(
                conf.clone(),
                process_channel.receiver(),
                es_channel.sender(),
            );

            assert_eq!(
                sender.conf.elasticsearch.flush_interval,
                conf.elasticsearch.flush_interval
            );
            assert_eq!(
                sender.conf.elasticsearch.bulk_size,
                conf.elasticsearch.bulk_size
            );
        }
    }

    #[tokio::test]
    async fn test_bulk_size_triggering() {
        let conf = create_test_conf(5000, 3); // Long flush interval, bulk size = 3
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        let start_time = std::time::Instant::now();

        // Send exactly bulk_size events rapidly
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("bulk trigger {}", i)))
                .await
                .unwrap();
        }

        // Should receive batch quickly due to bulk trigger (not timer)
        let batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(batch.len(), 3, "Should receive all 3 events in bulk");
        // Should be much faster than timer interval (bulk trigger)
        assert!(
            elapsed < Duration::from_millis(500),
            "Bulk trigger should be fast: {:?}",
            elapsed
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_timer_precision() {
        let conf = create_test_conf(100, 1000); // 100ms flush interval, very large bulk size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        let start_time = std::time::Instant::now();

        // Send one event to start timer
        process_sender
            .send(create_test_event("timer precision test"))
            .await
            .unwrap();

        // Should receive batch after flush_interval
        let batch = timeout(Duration::from_millis(200), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(batch.len(), 1);
        // Timer should fire within reasonable time (generous bounds for CI environments)
        assert!(
            elapsed >= Duration::from_millis(20),
            "Timer fired too early: {:?}",
            elapsed
        );
        assert!(
            elapsed <= Duration::from_millis(300),
            "Timer fired too late: {:?}",
            elapsed
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_bulk_vs_timer_race_condition() {
        let conf = create_test_conf(200, 3); // 200ms timer, bulk_size = 3
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 2 events to start timer (won't trigger bulk)
        for i in 0..2 {
            process_sender
                .send(create_test_event(&format!("race {}", i)))
                .await
                .unwrap();
        }

        // Wait most of the timer interval
        sleep(Duration::from_millis(150)).await;

        // Send 3rd event just before timer fires (should trigger bulk immediately)
        let bulk_time = std::time::Instant::now();
        process_sender
            .send(create_test_event("race trigger"))
            .await
            .unwrap();

        // Should receive batch immediately due to bulk trigger (not timer)
        let batch = timeout(Duration::from_millis(30), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let batch_elapsed = bulk_time.elapsed();

        assert_eq!(batch.len(), 3);
        // Should be much faster than timer interval (bulk trigger)
        assert!(
            batch_elapsed < Duration::from_millis(50),
            "Bulk trigger too slow: {:?}",
            batch_elapsed
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_consecutive_bulk_flushes() {
        let conf = create_test_conf(10000, 2); // Long timer, small bulk_size = 2
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 6 events rapidly (should create 3 consecutive bulk flushes)
        for i in 0..6 {
            process_sender
                .send(create_test_event(&format!("consecutive {}", i)))
                .await
                .unwrap();
        }

        // Should receive 3 batches of size 2 each
        let mut total_events = 0;
        for batch_num in 0..3 {
            let batch = timeout(Duration::from_millis(100), es_receiver.recv())
                .await
                .unwrap_or_else(|_| panic!("Failed to receive batch {}", batch_num))
                .unwrap();
            assert_eq!(batch.len(), 2, "Batch {} should have 2 events", batch_num);
            total_events += batch.len();
        }

        assert_eq!(total_events, 6);

        // Should not receive any more batches
        let extra_batch = timeout(Duration::from_millis(50), es_receiver.recv()).await;
        assert!(extra_batch.is_err(), "Should not receive extra batch");

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_zero_bulk_size_handling() {
        // Test edge case with bulk_size = 0 (should use timer-based flushing only)
        let conf = create_test_conf(100, 0); // 100ms timer, bulk_size = 0
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send multiple events
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("zero bulk {}", i)))
                .await
                .unwrap();
        }

        // Should flush by timer (bulk_size = 0 means no bulk triggering)
        // Allow extra time for skipped first tick (100ms timer + 100ms first tick skip + margin)
        let batch = timeout(Duration::from_millis(350), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!batch.is_empty(), "Should receive events via timer");
        assert!(batch.len() <= 3, "Should not exceed sent events");

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_backpressure_bulk_interaction() {
        let conf = create_test_conf(200, 3); // 200ms timer, bulk_size = 3
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, Some(0.5)); // Very small ES channel with backpressure

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send events to trigger bulk
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("backpressure bulk {}", i)))
                .await
                .unwrap();
        }

        // First batch should be sent
        let batch1 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch1.len(), 3);

        // Send more events (ES channel is full, should handle backpressure)
        for i in 3..6 {
            process_sender
                .send(create_test_event(&format!("backpressure bulk {}", i)))
                .await
                .unwrap();
        }

        // Give some time for backpressure handling
        sleep(Duration::from_millis(50)).await;

        // Consume from ES to relieve backpressure
        let batch2 = timeout(Duration::from_millis(200), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch2.len(), 3);

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_with_pending_events() {
        let conf = create_test_conf(10000, 10); // Very long timer, large bulk
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send some events that won't trigger bulk or timer
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("shutdown pending {}", i)))
                .await
                .unwrap();
        }

        // Give events time to be received
        sleep(Duration::from_millis(50)).await;

        // Shutdown before timer fires or bulk is reached
        shutdown.notify_one();
        let sender_result = timeout(Duration::from_millis(200), sender_handle)
            .await
            .unwrap()
            .unwrap();
        assert!(sender_result.is_ok());

        // Should receive pending events in final flush
        let final_batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_batch.len(), 3);
    }

    #[tokio::test]
    async fn test_high_frequency_mixed_flush_patterns() {
        let conf = create_test_conf(50, 4); // 50ms timer, bulk_size = 4
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(50, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(20, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        let mut total_batches = 0;
        let mut total_events = 0;

        // Send events in bursts with delays to mix bulk and timer triggers
        for burst in 0..3 {
            // Send 4 events quickly (bulk trigger)
            for i in 0..4 {
                process_sender
                    .send(create_test_event(&format!("burst {} event {}", burst, i)))
                    .await
                    .unwrap();
            }

            // Should get bulk flush
            let bulk_batch = timeout(Duration::from_millis(30), es_receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(bulk_batch.len(), 4);
            total_batches += 1;
            total_events += bulk_batch.len();

            // Send 2 events and wait for timer
            for i in 0..2 {
                process_sender
                    .send(create_test_event(&format!("timer {} event {}", burst, i)))
                    .await
                    .unwrap();
            }

            // Should get timer flush
            let timer_batch = timeout(Duration::from_millis(80), es_receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(timer_batch.len(), 2);
            total_batches += 1;
            total_events += timer_batch.len();

            // Small delay between bursts
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(total_batches, 6); // 3 bulk + 3 timer
        assert_eq!(total_events, 18); // 3 * (4 + 2)

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_metrics_counts_individual_events() {
        use crate::config::settings::MetricsConfig;
        use crate::infrastructure::metrics::{init_metrics, metrics};

        // Initialize metrics for this test
        let _ = init_metrics();

        let conf = Settings {
            log_path: "/tmp/test".to_string(),
            state_file_path: None,
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: None,
            channels: None,
            metrics: Some(MetricsConfig {
                enabled: true,
                port: 9090,
                path: "/metrics".to_string(),
            }),
            logging: None,
            elasticsearch: ElasticsearchConfig {
                host: "http://localhost".to_string(),
                port: 9200,
                index_name: "test".to_string(),
                flush_interval: 100,
                bulk_size: 5,
                workers: 1,
            },
        };

        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        // Get initial metric value
        let initial_count = metrics()
            .events_processed_total
            .with_label_values(&["sender", "success"])
            .get();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send exactly 5 events to trigger bulk flush
        for i in 0..5 {
            process_sender
                .send(create_test_event(&format!("metrics test {}", i)))
                .await
                .unwrap();
        }

        // Should receive batch of 5 events
        let batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 5);

        // Check that metrics were incremented by the correct count (5, not 1)
        let final_count = metrics()
            .events_processed_total
            .with_label_values(&["sender", "success"])
            .get();

        let events_counted = final_count - initial_count;
        assert_eq!(
            events_counted, 5,
            "Metrics should count individual events (5), not batches (1). Got: {}",
            events_counted
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_events_not_lost_on_full_es_channel() {
        // TDD: This test verifies that events are NOT lost when ES channel is full
        // Test scenario: ES channel is full, sender should retry until space available
        let conf = create_test_conf(10000, 5); // Long timer, bulk_size=5
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None); // Capacity=1 to simulate slow ES

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 10 events (2 batches of 5)
        for i in 0..10 {
            process_sender
                .send(create_test_event(&format!("important_msg_{}", i)))
                .await
                .unwrap();
        }

        // Give time for first batch to be sent (ES channel capacity=1, will block second batch)
        sleep(Duration::from_millis(100)).await;

        // Read first batch to unblock
        let batch1 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .expect("Should receive first batch")
            .unwrap();
        assert_eq!(batch1.len(), 5, "First batch should have 5 events");

        // Give time for second batch to be processed
        sleep(Duration::from_millis(100)).await;

        // Read second batch
        let batch2 = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .expect("Should receive second batch - events must NOT be lost!")
            .unwrap();
        assert_eq!(
            batch2.len(),
            5,
            "Second batch should have 5 events - none lost!"
        );

        // Verify total events
        let total_events = batch1.len() + batch2.len();
        assert_eq!(
            total_events, 10,
            "All 10 events must be delivered, none lost!"
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(500), sender_handle).await;
    }

    #[tokio::test]
    async fn test_try_send_returns_events_on_full_channel() {
        // TDD: Test that try_send returns events when channel is full (not loses them)
        use crate::transport::channels::SendError;

        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None);
        let mut es_sender = es_channel.sender();
        let _es_receiver = es_channel.receiver(); // Keep receiver alive

        // Fill the channel
        let batch1 = vec![create_test_event("first")];
        es_sender.send(batch1).await.unwrap();

        // Now channel is full - try_send should return the events, not lose them
        let batch2 = vec![create_test_event("second_1"), create_test_event("second_2")];

        // This should fail with Full error and return our events
        let result = es_sender.try_send(batch2);

        match result {
            Err(SendError::Full(returned_events)) => {
                assert_eq!(
                    returned_events.len(),
                    2,
                    "Events must be returned, not lost!"
                );
                assert_eq!(returned_events[0].message, "second_1");
                assert_eq!(returned_events[1].message, "second_2");
            }
            Ok(()) => panic!("Should have failed with Full error, not succeeded"),
            Err(SendError::Closed(_)) => panic!("Channel should not be closed"),
        }
    }

    #[tokio::test]
    async fn test_shutdown_delivers_events_with_slow_es_channel() {
        // TDD: Test that shutdown waits for events to be delivered even when ES channel is slow
        // This verifies fix for problem #2: shutdown timeout too short
        let conf = create_test_conf(10000, 5); // Long timer, bulk_size=5
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None); // Capacity=1 - slow ES

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 10 events (2 batches of 5)
        for i in 0..10 {
            process_sender
                .send(create_test_event(&format!("shutdown_test_{}", i)))
                .await
                .unwrap();
        }

        // Wait for first batch to be sent
        sleep(Duration::from_millis(50)).await;

        // Trigger shutdown - there should still be events pending
        shutdown.notify_one();

        // Now read all events - should get ALL 10 despite shutdown
        let mut total_received = 0;

        // Read batches until sender completes
        let sender_result = timeout(Duration::from_secs(6), async {
            loop {
                match timeout(Duration::from_millis(500), es_receiver.recv()).await {
                    Ok(Ok(batch)) => {
                        total_received += batch.len();
                        if total_received >= 10 {
                            break;
                        }
                    }
                    Ok(Err(_)) => break, // Channel closed
                    Err(_) => break,     // Timeout
                }
            }
        })
        .await;

        assert!(
            sender_result.is_ok(),
            "Sender should complete within timeout"
        );

        // Wait for sender to finish
        let _ = timeout(Duration::from_secs(1), sender_handle).await;

        assert_eq!(
            total_received, 10,
            "All 10 events must be delivered during shutdown, got {}",
            total_received
        );
    }

    #[tokio::test]
    async fn test_shutdown_waits_for_retry_to_complete() {
        // TDD: Verify that shutdown uses the full retry mechanism from send_events_to_es
        // rather than the 1-second external timeout
        //
        // Key insight: The 1-second timeout applies ONLY to events in `batch` at shutdown time
        // NOT to events that already triggered bulk flush during normal operation.
        //
        // Scenario:
        // 1. Pre-fill ES channel (capacity=1) with blocker
        // 2. Send 3 events (bulk_size=10, so they stay in batch)
        // 3. Trigger shutdown - events are in batch, send_batch called with 1-second timeout
        // 4. Wait 1.5 seconds (more than timeout), then drain
        // 5. With old code: events lost. With fix: events delivered.
        let conf = create_test_conf(10000, 10); // Long timer, bulk_size=10 (won't trigger)
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None); // capacity=1

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();
        let mut es_sender_direct = es_channel.sender();

        // Pre-fill ES channel with a dummy batch to block it
        es_sender_direct
            .send(vec![create_test_event("blocker")])
            .await
            .unwrap();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 3 events - less than bulk_size=10, so they stay in batch
        for i in 0..3 {
            process_sender
                .send(create_test_event(&format!("shutdown_retry_{}", i)))
                .await
                .unwrap();
        }

        // Give time for events to be received into batch
        sleep(Duration::from_millis(50)).await;

        // Trigger shutdown - events are in batch, will call send_batch with external timeout
        shutdown.notify_one();

        // Wait 1.5 seconds - more than old 1-second timeout
        // With old code, the timeout fires and events are lost
        sleep(Duration::from_millis(1500)).await;

        // Now drain ES channel - if retry is still running, events will come through
        let mut total_received = 0;

        // Drain all batches
        while let Ok(Ok(batch)) = timeout(Duration::from_millis(2000), es_receiver.recv()).await {
            // Skip the blocker batch
            if batch.len() == 1 && batch[0].message == "blocker" {
                continue;
            }
            total_received += batch.len();
            if total_received >= 3 {
                break;
            }
        }

        // Wait for sender to finish
        let _ = timeout(Duration::from_secs(2), sender_handle).await;

        // With proper retry (no 1-second timeout), we should get all 3 events
        // With old 1-second timeout, events would be lost
        assert_eq!(
            total_received, 3,
            "Should receive all 3 events - retry should wait beyond 1 second. Got {}",
            total_received
        );
    }

    #[tokio::test]
    async fn test_shutdown_completes_when_channel_closed() {
        // TDD: Test that sender completes gracefully when ES channel is closed
        let conf = create_test_conf(10000, 5);
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(20, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(1, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        // Send 5 events (1 batch)
        for i in 0..5 {
            process_sender
                .send(create_test_event(&format!("undelivered_{}", i)))
                .await
                .unwrap();
        }

        // Wait for events to be buffered
        sleep(Duration::from_millis(50)).await;

        // Drop receiver BEFORE shutdown to simulate ES being unavailable
        drop(es_receiver);

        // Trigger shutdown
        shutdown.notify_one();

        // Sender should complete (not hang forever) - within retry timeout
        let result = timeout(Duration::from_secs(6), sender_handle).await;
        assert!(
            result.is_ok(),
            "Sender should not hang when ES channel is closed"
        );
    }

    #[tokio::test]
    async fn test_timer_reset_behavior() {
        let conf = create_test_conf(150, 100); // 150ms timer, large bulk_size
        let process_channel: BoundedChannel<Event> = BoundedChannel::new(10, None);
        let es_channel: BoundedChannel<Vec<Event>> = BoundedChannel::new(10, None);

        let mut process_sender = process_channel.sender();
        let es_receiver = es_channel.receiver();

        let mut sender = Sender::new(conf, process_channel.receiver(), es_channel.sender());

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        let sender_handle = tokio::spawn(async move { sender.run(shutdown_clone).await });

        let start_time = std::time::Instant::now();

        // Send first event to start timer
        process_sender
            .send(create_test_event("timer reset 1"))
            .await
            .unwrap();

        // Wait 100ms (most of timer interval)
        sleep(Duration::from_millis(100)).await;

        // Send second event - this should NOT reset the timer
        process_sender
            .send(create_test_event("timer reset 2"))
            .await
            .unwrap();

        // Timer should still fire around original 150ms mark
        let batch = timeout(Duration::from_millis(100), es_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(batch.len(), 2);
        // Should fire around 150ms from first event (allow ±50ms tolerance)
        assert!(
            elapsed >= Duration::from_millis(100),
            "Timer fired too early: {:?}",
            elapsed
        );
        assert!(
            elapsed <= Duration::from_millis(250),
            "Timer fired too late: {:?}",
            elapsed
        );

        shutdown.notify_one();
        let _ = timeout(Duration::from_millis(100), sender_handle)
            .await
            .unwrap()
            .unwrap();
    }
}
