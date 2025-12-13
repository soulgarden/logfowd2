use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{debug, warn};

use crate::domain::event::Event;
use crate::infrastructure::metrics::metrics;
use crate::transport::channels::{BoundedSender, SendError};

/// EventBridge provides a two-tier channel architecture to prevent notify callback blocking.
/// It uses an unbounded channel for the notify callback and bridges to a bounded channel
/// with proper backpressure handling and metrics.
pub struct EventBridge {
    /// Unbounded sender for notify callback - never blocks
    notify_sender: UnboundedSender<Event>,
    /// Unbounded receiver for bridge task
    notify_receiver: Option<UnboundedReceiver<Event>>,
    /// Configuration parameters
    config: EventBridgeConfig,
    /// Metrics enabled flag
    metrics_enabled: bool,
}

/// Configuration for EventBridge behavior
#[derive(Clone, Debug)]
pub struct EventBridgeConfig {
    /// Warning threshold for unbounded queue size
    pub notify_buffer_warning_threshold: usize,
    /// Interval for logging queue size warnings
    pub warning_log_interval: Duration,
}

impl Default for EventBridgeConfig {
    fn default() -> Self {
        Self {
            notify_buffer_warning_threshold: 1000,
            warning_log_interval: Duration::from_secs(30),
        }
    }
}

impl EventBridge {
    /// Create new EventBridge with configuration
    pub fn new(config: EventBridgeConfig, metrics_enabled: bool) -> Self {
        let (notify_sender, notify_receiver) = unbounded_channel();

        Self {
            notify_sender,
            notify_receiver: Some(notify_receiver),
            config,
            metrics_enabled,
        }
    }

    /// Get the notify sender for use in the notify callback
    pub fn notify_sender(&self) -> NotifyEventSender {
        NotifyEventSender {
            sender: self.notify_sender.clone(),
            config: self.config.clone(),
            metrics_enabled: self.metrics_enabled,
        }
    }

    /// Start the bridge task that forwards events from unbounded to bounded channel
    pub fn start_bridge_task(
        &mut self,
        bounded_sender: BoundedSender<Event>,
        shutdown_notify: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        // Take the receiver (can only be done once)
        let notify_receiver = self
            .notify_receiver
            .take()
            .expect("EventBridge can only start bridge task once");

        let config = self.config.clone();
        let metrics_enabled = self.metrics_enabled;

        // Spawn the bridge task
        tokio::spawn(async move {
            Self::bridge_task(
                notify_receiver,
                bounded_sender,
                config,
                metrics_enabled,
                shutdown_notify,
            )
            .await;
        })
    }

    /// Internal bridge task that forwards events with backpressure handling
    async fn bridge_task(
        mut receiver: UnboundedReceiver<Event>,
        mut bounded_sender: BoundedSender<Event>,
        config: EventBridgeConfig,
        metrics_enabled: bool,
        shutdown_notify: Arc<Notify>,
    ) {
        let mut last_warning = None;
        let mut events_forwarded = 0u64;
        let events_dropped_in_bridge = 0u64;

        debug!("EventBridge task started");

        loop {
            tokio::select! {
                // Receive event from notify callback
                event = receiver.recv() => {
                    match event {
                        Some(event) => {
                            // Check queue size and warn if needed
                            let queue_size = receiver.len();
                            if queue_size > config.notify_buffer_warning_threshold {
                                let now = Instant::now();
                                if let Some(last) = last_warning {
                                    if now.duration_since(last) >= config.warning_log_interval {
                                        warn!(
                                            "EventBridge queue size high: {} events (threshold: {})",
                                            queue_size, config.notify_buffer_warning_threshold
                                        );
                                        last_warning = Some(now);
                                    }
                                } else {
                                    warn!(
                                        "EventBridge queue size high: {} events (threshold: {})",
                                        queue_size, config.notify_buffer_warning_threshold
                                    );
                                    last_warning = Some(now);
                                }
                            }

                            // Update metrics
                            if metrics_enabled {
                                metrics()
                                    .queue_size
                                    .with_label_values(&["event_bridge", "notify_buffer"])
                                    .set(queue_size as i64);
                            }

                            // Forward to bounded channel
                            match bounded_sender.send(event).await {
                                Ok(()) => {
                                    events_forwarded += 1;
                                    if metrics_enabled && events_forwarded.is_multiple_of(1000) {
                                        metrics()
                                            .events_processed_total
                                            .with_label_values(&["event_bridge", "forwarded"])
                                            .inc_by(1000);
                                    }
                                }
                                Err(SendError::Closed(_)) => {
                                    debug!("Bounded channel closed, stopping EventBridge");
                                    break;
                                }
                                Err(SendError::Full(_)) => {
                                    // This shouldn't happen with async send(), but handle for completeness
                                    warn!("EventBridge: bounded channel unexpectedly full");
                                }
                            }
                        }
                        None => {
                            debug!("Notify channel closed, stopping EventBridge");
                            break;
                        }
                    }
                }

                // Shutdown signal
                _ = shutdown_notify.notified() => {
                    debug!("EventBridge received shutdown signal");

                    // Process remaining events with timeout
                    let timeout = Duration::from_secs(5);
                    let deadline = Instant::now() + timeout;

                    while let Ok(event) = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        receiver.recv()
                    ).await {
                        match event {
                            Some(event) => {
                                if bounded_sender.send(event).await.is_err() {
                                    break;
                                }
                                events_forwarded += 1;
                            }
                            None => break,
                        }
                    }

                    debug!("EventBridge shutdown complete. Forwarded {} events, dropped {} events",
                        events_forwarded, events_dropped_in_bridge);
                    break;
                }
            }
        }

        // Final metrics update
        if metrics_enabled {
            let remaining = events_forwarded % 1000;
            if remaining > 0 {
                metrics()
                    .events_processed_total
                    .with_label_values(&["event_bridge", "forwarded"])
                    .inc_by(remaining);
            }
            if events_dropped_in_bridge > 0 {
                metrics()
                    .events_processed_total
                    .with_label_values(&["event_bridge", "dropped"])
                    .inc_by(events_dropped_in_bridge);
            }
        }
    }
}

/// Sender wrapper for the notify callback that provides try_send with never-blocking behavior
pub struct NotifyEventSender {
    sender: UnboundedSender<Event>,
    #[allow(dead_code)]
    config: EventBridgeConfig,
    metrics_enabled: bool,
}

impl NotifyEventSender {
    /// Try to send event - never blocks, uses unbounded channel
    pub fn try_send(&mut self, event: Event) -> Result<(), NotifyEventSendError> {
        // Since we can't track actual queue size with tokio's UnboundedSender,
        // we'll rely on the bounded channel downstream to provide backpressure.
        // This simplifies the implementation while still preventing notify thread blocking.

        // Try to send
        match self.sender.send(event) {
            Ok(()) => {
                // Update metrics
                if self.metrics_enabled {
                    metrics()
                        .events_processed_total
                        .with_label_values(&["notify_callback", "sent"])
                        .inc();
                }
                Ok(())
            }
            Err(_) => {
                if self.metrics_enabled {
                    metrics()
                        .notify_events_dropped
                        .with_label_values(&["channel_closed"])
                        .inc();
                }
                Err(NotifyEventSendError::ChannelClosed)
            }
        }
    }

    /// Get current queue size for monitoring - not available for unbounded channels
    #[allow(dead_code)]
    pub fn queue_size(&self) -> usize {
        // UnboundedSender doesn't provide len(), so we can't track actual queue size
        // This is a limitation we accept for the benefit of never blocking the notify callback
        0
    }
}

impl Clone for NotifyEventSender {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            config: self.config.clone(),
            metrics_enabled: self.metrics_enabled,
        }
    }
}

#[derive(Debug)]
pub enum NotifyEventSendError {
    ChannelClosed,
}

impl std::fmt::Display for NotifyEventSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelClosed => write!(f, "Notify event channel is closed"),
        }
    }
}

impl std::error::Error for NotifyEventSendError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::{Event, Meta};
    use crate::transport::channels::create_bounded_channel;
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn test_event_bridge_creation() {
        let config = EventBridgeConfig::default();
        let bridge = EventBridge::new(config.clone(), false);

        let mut notify_sender = bridge.notify_sender();

        // Should be able to send events
        let event = Event::new("test".to_string(), Meta::default());
        assert!(notify_sender.try_send(event).is_ok());

        // Queue size is not trackable with UnboundedSender
        assert_eq!(notify_sender.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_notify_sender_basic_functionality() {
        let config = EventBridgeConfig::default();

        let bridge = EventBridge::new(config, false);
        let mut notify_sender = bridge.notify_sender();

        // Should be able to send events without blocking
        for i in 0..10 {
            let event = Event::new(format!("test {}", i), Meta::default());
            assert!(notify_sender.try_send(event).is_ok());
        }

        // UnboundedSender should never block or return QueueFull
        // The backpressure handling happens downstream in the EventBridge task
    }

    #[tokio::test]
    async fn test_event_bridge_forwarding() {
        let config = EventBridgeConfig::default();
        let mut bridge = EventBridge::new(config, false);

        // Create bounded channel for output
        let bounded_channel = create_bounded_channel(10, None, None, None, None);
        let bounded_sender = bounded_channel.sender();
        let bounded_receiver = bounded_channel.receiver();

        // Create shutdown notify
        let shutdown_notify = Arc::new(Notify::new());

        // Start bridge task
        let _bridge_handle = bridge.start_bridge_task(bounded_sender, shutdown_notify.clone());

        // Give bridge task time to start
        time::sleep(Duration::from_millis(10)).await;

        // Get notify sender and send event
        let mut notify_sender = bridge.notify_sender();
        let test_event = Event::new("test message".to_string(), Meta::default());

        notify_sender.try_send(test_event.clone()).unwrap();

        // Event should be forwarded to bounded channel
        let received = bounded_receiver.recv().await.unwrap();
        assert_eq!(received.message, test_event.message);

        // Cleanup
        shutdown_notify.notify_waiters();
    }

    #[tokio::test]
    async fn test_bridge_task_basic_operation() {
        let config = EventBridgeConfig::default();
        let mut bridge = EventBridge::new(config, false);

        let bounded_channel = create_bounded_channel(10, None, None, None, None);
        let bounded_sender = bounded_channel.sender();
        let bounded_receiver = bounded_channel.receiver();

        let shutdown_notify = Arc::new(Notify::new());

        // Start bridge task
        let _bridge_handle = bridge.start_bridge_task(bounded_sender, shutdown_notify.clone());

        // Send some events
        let mut notify_sender = bridge.notify_sender();
        for i in 0..3 {
            let event = Event::new(format!("message {}", i), Meta::default());
            notify_sender.try_send(event).unwrap();
        }

        // Give the bridge task some time to process events
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should be able to receive events that were processed
        let mut received_count = 0;
        for _ in 0..3 {
            if let Ok(result) =
                tokio::time::timeout(Duration::from_millis(100), bounded_receiver.recv()).await
            {
                if result.is_ok() {
                    received_count += 1;
                }
            }
        }

        // Should have received all events
        assert_eq!(received_count, 3, "Should have received all 3 events");

        // Test passes - we don't test shutdown because in the real system
        // the bridge runs for the lifetime of the watcher
    }

    #[tokio::test]
    async fn test_notify_sender_cloning() {
        let config = EventBridgeConfig::default();
        let bridge = EventBridge::new(config, false);

        let notify_sender1 = bridge.notify_sender();
        let mut notify_sender2 = notify_sender1.clone();

        // Both should work independently
        let event1 = Event::new("from sender 1".to_string(), Meta::default());
        let event2 = Event::new("from sender 2".to_string(), Meta::default());

        assert!(notify_sender1.sender.send(event1).is_ok());
        assert!(notify_sender2.try_send(event2).is_ok());
    }
}
