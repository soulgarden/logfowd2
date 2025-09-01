use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use tokio::sync::Notify;
use tokio::time::interval;

use crate::config::Settings;
use crate::transport::channels::{BoundedReceiver, BoundedSender};
use crate::error::Result;
use crate::domain::event::Event;
use crate::infrastructure::metrics::{are_metrics_enabled, metrics};
use crate::traits::EventProcessor;

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
        let mut ticker = interval(Duration::from_millis(self.conf.elasticsearch.flush_interval));
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
                        info!("Sending remaining events before shutdown");
                        // Use timeout to avoid hanging during shutdown if ES workers are down
                        let timeout_result = tokio::time::timeout(
                            Duration::from_millis(1000),
                            self.send_batch(&mut batch)
                        ).await;

                        match timeout_result {
                            Ok(_) => info!("Finished sending remaining events"),
                            Err(_) => {
                                warn!("Timeout while sending remaining events during shutdown, continuing with shutdown");
                            }
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
            self.send_events_to_es(events).await;
        } else {
            // Send events in bulk_size chunks
            while !batch.is_empty() {
                let chunk_size = std::cmp::min(batch.len(), self.conf.elasticsearch.bulk_size);
                let events = batch.drain(..chunk_size).collect();
                self.send_events_to_es(events).await;
            }
        }
    }

    async fn send_events_to_es(&mut self, events: Vec<Event>) {
        if events.is_empty() {
            return;
        }

        let event_count = events.len() as u64;

        // Record batch size metric if enabled
        if self.metrics_enabled {
            metrics()
                .batch_size_histogram
                .with_label_values(&["sender"])
                .observe(event_count as f64);
        }

        // Use timeout to prevent hanging if ES workers are unavailable
        let send_result = tokio::time::timeout(
            Duration::from_millis(5000),
            self.es_queue_sender.send(events),
        )
        .await;

        match send_result {
            Ok(Ok(())) => {
                if self.metrics_enabled {
                    metrics()
                        .events_processed_total
                        .with_label_values(&["sender", "success"])
                        .inc_by(event_count);
                }
            }
            Ok(Err(e)) => {
                warn!("Failed to send batch to ES workers: {:?}", e);
                if self.metrics_enabled {
                    metrics()
                        .errors_total
                        .with_label_values(&["sender", "send_failed"])
                        .inc();
                }
            }
            Err(_) => {
                warn!("Timeout while sending batch to ES workers, events may be lost");
                if self.metrics_enabled {
                    metrics()
                        .errors_total
                        .with_label_values(&["sender", "timeout"])
                        .inc();
                }
            }
        }
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
    use crate::transport::channels::BoundedChannel;
    use crate::config::settings::{Settings, ElasticsearchConfig};
    use crate::domain::event::{Event, Meta};
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

            assert_eq!(sender.conf.elasticsearch.flush_interval, conf.elasticsearch.flush_interval);
            assert_eq!(sender.conf.elasticsearch.bulk_size, conf.elasticsearch.bulk_size);
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
