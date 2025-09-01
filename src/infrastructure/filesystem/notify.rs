use tracing::{debug, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::infrastructure::metrics::metrics;

/// NotifyBridge provides a two-tier channel architecture to prevent notify callback blocking.
/// It uses an unbounded channel for the notify callback and bridges to a bounded channel
/// with proper backpressure handling and metrics for filesystem events.
pub struct NotifyBridge {
    /// Unbounded sender for notify callback - never blocks
    notify_sender: UnboundedSender<notify::Result<notify::Event>>,
    /// Unbounded receiver for bridge task
    notify_receiver: Option<UnboundedReceiver<notify::Result<notify::Event>>>,
    /// Configuration parameters
    config: NotifyBridgeConfig,
    /// Metrics enabled flag
    metrics_enabled: bool,
}

/// Configuration for NotifyBridge behavior
#[derive(Clone, Debug)]
pub struct NotifyBridgeConfig {
    /// Warning threshold for unbounded queue size
    pub notify_buffer_warning_threshold: usize,
    /// Interval for logging queue size warnings
    pub warning_log_interval: Duration,
    /// Size of bounded channel to filesystem event processor
    pub bounded_channel_size: usize,
}

impl Default for NotifyBridgeConfig {
    fn default() -> Self {
        Self {
            notify_buffer_warning_threshold: 1000,
            warning_log_interval: Duration::from_secs(30),
            bounded_channel_size: 1024,
        }
    }
}

impl NotifyBridge {
    /// Create new NotifyBridge with configuration
    pub fn new(config: NotifyBridgeConfig, metrics_enabled: bool) -> Self {
        let (notify_sender, notify_receiver) = unbounded_channel();

        Self {
            notify_sender,
            notify_receiver: Some(notify_receiver),
            config,
            metrics_enabled,
        }
    }

    /// Get the notify sender for use in the notify callback - never blocks
    pub fn notify_sender(&self) -> NotifyFilesystemSender {
        NotifyFilesystemSender {
            sender: self.notify_sender.clone(),
            config: self.config.clone(),
            metrics_enabled: self.metrics_enabled,
        }
    }

    /// Start the bridge task that forwards events from unbounded to bounded channel
    /// Returns both the bridge task handle and the bounded receiver for the watcher
    pub fn start_bridge_task(
        &mut self,
        shutdown_notify: Arc<Notify>,
    ) -> (
        tokio::task::JoinHandle<()>,
        Receiver<notify::Result<notify::Event>>,
    ) {
        // Take the receiver (can only be done once)
        let notify_receiver = self
            .notify_receiver
            .take()
            .expect("NotifyBridge can only start bridge task once");

        let config = self.config.clone();
        let metrics_enabled = self.metrics_enabled;

        // Create bounded channel for the watcher
        let (bounded_sender, bounded_receiver) =
            tokio::sync::mpsc::channel(config.bounded_channel_size);

        // Spawn the bridge task
        let bridge_handle = tokio::spawn(async move {
            Self::bridge_task(
                notify_receiver,
                bounded_sender,
                config,
                metrics_enabled,
                shutdown_notify,
            )
            .await;
        });

        (bridge_handle, bounded_receiver)
    }

    /// Internal bridge task that forwards events with backpressure handling
    async fn bridge_task(
        mut receiver: UnboundedReceiver<notify::Result<notify::Event>>,
        bounded_sender: Sender<notify::Result<notify::Event>>,
        config: NotifyBridgeConfig,
        metrics_enabled: bool,
        shutdown_notify: Arc<Notify>,
    ) {
        let mut last_warning = None;
        let mut events_forwarded = 0u64;
        let mut events_dropped_in_bridge = 0u64;

        debug!(
            "NotifyBridge task started with bounded channel size: {}",
            config.bounded_channel_size
        );

        loop {
            tokio::select! {
                // Receive filesystem event from notify callback
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
                                            "NotifyBridge filesystem queue size high: {} events (threshold: {})",
                                            queue_size, config.notify_buffer_warning_threshold
                                        );
                                        last_warning = Some(now);
                                    }
                                } else {
                                    warn!(
                                        "NotifyBridge filesystem queue size high: {} events (threshold: {})",
                                        queue_size, config.notify_buffer_warning_threshold
                                    );
                                    last_warning = Some(now);
                                }
                            }

                            // Update metrics
                            if metrics_enabled {
                                metrics()
                                    .notify_filesystem_queue_size
                                    .with_label_values(&["notify_bridge", "unbounded_queue"])
                                    .set(queue_size as i64);
                            }

                            // Forward to bounded channel - use try_send to avoid blocking the bridge
                            match bounded_sender.try_send(event) {
                                Ok(()) => {
                                    events_forwarded += 1;
                                    if metrics_enabled && events_forwarded % 1000 == 0 {
                                        metrics()
                                            .notify_filesystem_events_forwarded
                                            .with_label_values(&["notify_bridge", "forwarded"])
                                            .inc_by(1000);
                                    }
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    // Bounded channel is full - drop the event and count it
                                    events_dropped_in_bridge += 1;
                                    if metrics_enabled {
                                        metrics()
                                            .notify_filesystem_events_dropped
                                            .with_label_values(&["bridge_bounded_channel_full"])
                                            .inc();
                                    }

                                    // Log occasionally to avoid spam
                                    if events_dropped_in_bridge % 100 == 1 {
                                        warn!("NotifyBridge dropping filesystem events due to full bounded channel. Dropped {} events so far.", events_dropped_in_bridge);
                                    }
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    debug!("Bounded channel closed, stopping NotifyBridge");
                                    break;
                                }
                            }
                        }
                        None => {
                            debug!("Notify unbounded channel closed, stopping NotifyBridge");
                            break;
                        }
                    }
                }

                // Shutdown signal
                _ = shutdown_notify.notified() => {
                    debug!("NotifyBridge received shutdown signal");

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

                    debug!("NotifyBridge shutdown complete. Forwarded {} events, dropped {} events",
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
                    .notify_filesystem_events_forwarded
                    .with_label_values(&["notify_bridge", "forwarded"])
                    .inc_by(remaining);
            }
            if events_dropped_in_bridge > 0 {
                metrics()
                    .notify_filesystem_events_dropped
                    .with_label_values(&["bridge_bounded_channel_full"])
                    .inc_by(events_dropped_in_bridge);
            }
        }
    }
}

/// Sender wrapper for the notify callback that provides non-blocking filesystem event sending
pub struct NotifyFilesystemSender {
    sender: UnboundedSender<notify::Result<notify::Event>>,
    #[allow(dead_code)]
    config: NotifyBridgeConfig,
    metrics_enabled: bool,
}

impl NotifyFilesystemSender {
    /// Send filesystem event - never blocks, uses unbounded channel
    pub fn send(
        &self,
        event: notify::Result<notify::Event>,
    ) -> Result<(), NotifyFilesystemSendError> {
        match self.sender.send(event) {
            Ok(()) => {
                // Update metrics
                if self.metrics_enabled {
                    metrics()
                        .notify_filesystem_events_forwarded
                        .with_label_values(&["notify_callback", "sent"])
                        .inc();
                }
                Ok(())
            }
            Err(_) => {
                if self.metrics_enabled {
                    metrics()
                        .notify_filesystem_events_dropped
                        .with_label_values(&["unbounded_channel_closed"])
                        .inc();
                }
                Err(NotifyFilesystemSendError::ChannelClosed)
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

impl Clone for NotifyFilesystemSender {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            config: self.config.clone(),
            metrics_enabled: self.metrics_enabled,
        }
    }
}

#[derive(Debug)]
pub enum NotifyFilesystemSendError {
    ChannelClosed,
}

impl std::fmt::Display for NotifyFilesystemSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelClosed => write!(f, "Notify filesystem event channel is closed"),
        }
    }
}

impl std::error::Error for NotifyFilesystemSendError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn test_notify_bridge_creation() {
        let config = NotifyBridgeConfig::default();
        let bridge = NotifyBridge::new(config.clone(), false);

        let notify_sender = bridge.notify_sender();

        // Should be able to send filesystem events
        let test_event = Ok(notify::Event {
            kind: notify::EventKind::Create(notify::event::CreateKind::File),
            paths: vec![std::path::PathBuf::from("/test/path")],
            attrs: Default::default(),
        });

        assert!(notify_sender.send(test_event).is_ok());

        // Queue size is not trackable with UnboundedSender
        assert_eq!(notify_sender.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_notify_sender_basic_functionality() {
        let config = NotifyBridgeConfig::default();
        let bridge = NotifyBridge::new(config, false);
        let notify_sender = bridge.notify_sender();

        // Should be able to send filesystem events without blocking
        for i in 0..10 {
            let test_event = Ok(notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                paths: vec![std::path::PathBuf::from(format!("/test/path/{}", i))],
                attrs: Default::default(),
            });
            assert!(notify_sender.send(test_event).is_ok());
        }

        // UnboundedSender should never block or return Full
        // The backpressure handling happens downstream in the NotifyBridge task
    }

    #[tokio::test]
    async fn test_notify_bridge_forwarding() {
        let config = NotifyBridgeConfig::default();
        let mut bridge = NotifyBridge::new(config, false);

        // Create shutdown notify
        let shutdown_notify = Arc::new(Notify::new());

        // Start bridge task
        let (bridge_handle, mut bounded_receiver) =
            bridge.start_bridge_task(shutdown_notify.clone());

        // Give bridge task time to start
        time::sleep(Duration::from_millis(10)).await;

        // Get notify sender and send filesystem event
        let notify_sender = bridge.notify_sender();
        let test_event = Ok(notify::Event {
            kind: notify::EventKind::Create(notify::event::CreateKind::File),
            paths: vec![std::path::PathBuf::from("/test/path")],
            attrs: Default::default(),
        });

        notify_sender.send(test_event).unwrap();

        // Event should be forwarded to bounded channel
        let received = bounded_receiver.recv().await.unwrap();
        if let Ok(event) = received {
            // Create a new test_event for comparison since the original was moved
            let expected_paths = vec![std::path::PathBuf::from("/test/path")];
            assert_eq!(event.paths, expected_paths);
        }

        // Cleanup
        shutdown_notify.notify_waiters();

        // Wait for bridge task to complete
        let _ = tokio::time::timeout(Duration::from_millis(100), bridge_handle).await;
    }

    #[tokio::test]
    async fn test_bridge_task_handles_errors() {
        let config = NotifyBridgeConfig::default();
        let mut bridge = NotifyBridge::new(config, false);

        let shutdown_notify = Arc::new(Notify::new());
        let (bridge_handle, mut bounded_receiver) =
            bridge.start_bridge_task(shutdown_notify.clone());

        // Send an error event
        let notify_sender = bridge.notify_sender();
        let error_event: notify::Result<notify::Event> = Err(notify::Error::generic("test error"));

        notify_sender.send(error_event).unwrap();

        // Should be able to receive the error
        let received = bounded_receiver.recv().await.unwrap();
        assert!(received.is_err());

        // Cleanup
        shutdown_notify.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_millis(100), bridge_handle).await;
    }

    #[tokio::test]
    async fn test_notify_sender_cloning() {
        let config = NotifyBridgeConfig::default();
        let bridge = NotifyBridge::new(config, false);

        let notify_sender1 = bridge.notify_sender();
        let notify_sender2 = notify_sender1.clone();

        // Both should work independently
        let event1 = Ok(notify::Event {
            kind: notify::EventKind::Create(notify::event::CreateKind::File),
            paths: vec![std::path::PathBuf::from("/sender1/path")],
            attrs: Default::default(),
        });
        let event2 = Ok(notify::Event {
            kind: notify::EventKind::Create(notify::event::CreateKind::File),
            paths: vec![std::path::PathBuf::from("/sender2/path")],
            attrs: Default::default(),
        });

        assert!(notify_sender1.send(event1).is_ok());
        assert!(notify_sender2.send(event2).is_ok());
    }

    #[tokio::test]
    async fn test_bridge_task_shutdown_behavior() {
        let config = NotifyBridgeConfig::default();
        let mut bridge = NotifyBridge::new(config, false);

        let shutdown_notify = Arc::new(Notify::new());
        let (bridge_handle, bounded_receiver) = bridge.start_bridge_task(shutdown_notify.clone());

        // Give bridge task time to start
        time::sleep(Duration::from_millis(10)).await;

        // Send events before shutdown
        let notify_sender = bridge.notify_sender();
        for i in 0..3 {
            let event = Ok(notify::Event {
                kind: notify::EventKind::Create(notify::event::CreateKind::File),
                paths: vec![std::path::PathBuf::from(format!("/test/path/{}", i))],
                attrs: Default::default(),
            });
            notify_sender.send(event).unwrap();
        }

        // Drop the bounded receiver to simulate the watcher shutting down
        drop(bounded_receiver);

        // Give the bridge task a moment to detect the closed channel
        time::sleep(Duration::from_millis(10)).await;

        // Trigger shutdown
        shutdown_notify.notify_waiters();

        // Bridge task should complete gracefully
        let result = tokio::time::timeout(Duration::from_secs(5), bridge_handle).await;
        assert!(result.is_ok(), "Bridge task should complete within timeout");
    }
}
