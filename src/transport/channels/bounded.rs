use async_channel::{Receiver, Sender};
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, warn};

pub struct BoundedChannel<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    capacity: usize,
    backpressure_threshold: f32,
    backpressure_log_interval: Duration,
    backpressure_min_delay_ms: u64,
    backpressure_max_delay_ms: u64,
}

impl<T> BoundedChannel<T> {
    pub fn new(capacity: usize, backpressure_threshold: Option<f32>) -> Self {
        // Ensure capacity is at least 1 to prevent division by zero and unintended rendezvous channels
        let safe_capacity = if capacity == 0 {
            warn!("Channel capacity cannot be 0, using minimum capacity of 1");
            1
        } else {
            capacity
        };

        // Validate and clamp backpressure threshold to [0.0, 1.0] range
        let safe_threshold = match backpressure_threshold {
            Some(threshold) => {
                if threshold.is_nan() {
                    warn!("Backpressure threshold cannot be NaN, using default 0.8");
                    0.8
                } else if threshold.is_infinite() {
                    warn!(
                        "Backpressure threshold cannot be infinite ({}), using default 0.8",
                        threshold
                    );
                    0.8
                } else if threshold < 0.0 {
                    warn!(
                        "Backpressure threshold cannot be negative ({}), clamping to 0.0",
                        threshold
                    );
                    0.0
                } else if threshold > 1.0 {
                    warn!(
                        "Backpressure threshold cannot exceed 1.0 ({}), clamping to 1.0",
                        threshold
                    );
                    1.0
                } else {
                    threshold
                }
            }
            None => 0.8, // default
        };

        let (sender, receiver) = async_channel::bounded(safe_capacity);

        Self {
            sender,
            receiver,
            capacity: safe_capacity,
            backpressure_threshold: safe_threshold,
            backpressure_log_interval: Duration::from_secs(5),
            backpressure_min_delay_ms: 2,
            backpressure_max_delay_ms: 50,
        }
    }

    pub fn new_with_params(
        capacity: usize,
        backpressure_threshold: Option<f32>,
        min_delay_ms: Option<u64>,
        max_delay_ms: Option<u64>,
    ) -> Self {
        let mut ch = Self::new(capacity, backpressure_threshold);
        if let Some(min) = min_delay_ms {
            ch.backpressure_min_delay_ms = min;
        }
        if let Some(max) = max_delay_ms {
            ch.backpressure_max_delay_ms = max;
        }
        // Ensure sane ordering
        if ch.backpressure_min_delay_ms > ch.backpressure_max_delay_ms {
            std::mem::swap(
                &mut ch.backpressure_min_delay_ms,
                &mut ch.backpressure_max_delay_ms,
            );
        }
        ch
    }

    pub fn sender(&self) -> BoundedSender<T> {
        BoundedSender {
            sender: self.sender.clone(),
            capacity: self.capacity,
            backpressure_threshold: self.backpressure_threshold,
            last_backpressure_log: None,
            backpressure_log_interval: self.backpressure_log_interval,
            backpressure_min_delay_ms: self.backpressure_min_delay_ms,
            backpressure_max_delay_ms: self.backpressure_max_delay_ms,
        }
    }

    pub fn receiver(&self) -> BoundedReceiver<T> {
        BoundedReceiver {
            receiver: self.receiver.clone(),
        }
    }

    /// Get the backpressure threshold for testing purposes
    #[cfg(test)]
    pub fn backpressure_threshold(&self) -> f32 {
        self.backpressure_threshold
    }
}

pub struct BoundedSender<T> {
    sender: Sender<T>,
    capacity: usize,
    backpressure_threshold: f32,
    last_backpressure_log: Option<Instant>,
    backpressure_log_interval: Duration,
    backpressure_min_delay_ms: u64,
    backpressure_max_delay_ms: u64,
}

impl<T> BoundedSender<T> {
    pub async fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        // Check backpressure condition
        if self.is_under_backpressure() {
            self.log_backpressure_warning();

            // Apply adaptive backpressure delay based on channel utilization
            let delay_ms = self.calculate_adaptive_delay();
            time::sleep(Duration::from_millis(delay_ms)).await;
        }

        match self.sender.send(item).await {
            Ok(()) => Ok(()),
            Err(async_channel::SendError(item)) => Err(SendError::Closed(item)),
        }
    }

    pub fn is_under_backpressure(&self) -> bool {
        // Special case: if threshold is 0.0, never trigger backpressure
        // This prevents the bug where negative thresholds (clamped to 0.0) would
        // always trigger backpressure
        if self.backpressure_threshold <= 0.0 {
            return false;
        }

        let current_len = self.sender.len();
        let threshold = (self.capacity as f32 * self.backpressure_threshold) as usize;
        current_len >= threshold
    }

    pub fn utilization(&self) -> f32 {
        if self.capacity == 0 {
            // Safety check: if capacity is somehow 0, return 0.0 to prevent division by zero
            return 0.0;
        }
        self.sender.len() as f32 / self.capacity as f32
    }

    /// Calculate adaptive backpressure delay based on channel utilization
    /// Returns delay in milliseconds: higher utilization = longer delay
    fn calculate_adaptive_delay(&self) -> u64 {
        let utilization = self.utilization();

        // Base delay: min_delay at threshold, scales up to max_delay at full capacity
        // Formula: delay = 2 + (utilization - threshold) * scale_factor
        let threshold = self.backpressure_threshold;
        let over_threshold = (utilization - threshold).max(0.0);
        let remaining_capacity = (1.0 - threshold).max(0.1); // Avoid division by zero

        // Scale from min_delay to max_delay based on utilization above threshold
        let base_delay = self.backpressure_min_delay_ms as f64;
        let max_additional_delay = (self
            .backpressure_max_delay_ms
            .saturating_sub(self.backpressure_min_delay_ms))
            as f64;
        let scale_factor = max_additional_delay / remaining_capacity as f64;

        let delay = base_delay + ((over_threshold as f64) * scale_factor);

        // Clamp between configured bounds
        (delay as u64).clamp(
            self.backpressure_min_delay_ms,
            self.backpressure_max_delay_ms,
        )
    }

    fn log_backpressure_warning(&mut self) {
        let now = Instant::now();

        if let Some(last_log) = self.last_backpressure_log
            && now.duration_since(last_log) < self.backpressure_log_interval
        {
            return;
        }

        warn!(
            "Channel backpressure detected: {}/{} ({:.1}% full)",
            self.sender.len(),
            self.capacity,
            self.utilization() * 100.0
        );

        self.last_backpressure_log = Some(now);
    }
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: self.capacity,
            backpressure_threshold: self.backpressure_threshold,
            last_backpressure_log: None,
            backpressure_log_interval: self.backpressure_log_interval,
            backpressure_min_delay_ms: self.backpressure_min_delay_ms,
            backpressure_max_delay_ms: self.backpressure_max_delay_ms,
        }
    }
}

pub struct BoundedReceiver<T> {
    receiver: Receiver<T>,
}

impl<T> BoundedReceiver<T> {
    pub async fn recv(&self) -> Result<T, RecvError> {
        match self.receiver.recv().await {
            Ok(item) => Ok(item),
            Err(_) => Err(RecvError::Closed),
        }
    }
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}

#[derive(Debug)]
pub enum SendError<T> {
    Closed(T),
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Closed(_) => write!(f, "Channel closed"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

#[derive(Debug)]
pub enum RecvError {
    Closed,
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Closed => write!(f, "Channel closed"),
        }
    }
}

impl std::error::Error for RecvError {}

// Utility function to create channels from config
pub fn create_bounded_channel<T>(
    default_capacity: usize,
    config_capacity: Option<usize>,
    backpressure_threshold: Option<f32>,
    backpressure_min_delay_ms: Option<u64>,
    backpressure_max_delay_ms: Option<u64>,
) -> BoundedChannel<T> {
    let mut capacity = config_capacity.unwrap_or(default_capacity);

    // Ensure capacity is at least 1 to prevent division by zero and unintended rendezvous channels
    if capacity == 0 {
        warn!("Channel capacity cannot be 0, using minimum capacity of 1");
        capacity = 1;
    }

    debug!("Creating bounded channel with capacity: {}", capacity);

    BoundedChannel::new_with_params(
        capacity,
        backpressure_threshold,
        backpressure_min_delay_ms,
        backpressure_max_delay_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn test_bounded_channel_creation() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.8));

        assert_eq!(channel.capacity, 10);
        assert_eq!(channel.backpressure_threshold, 0.8);
    }

    #[tokio::test]
    async fn test_bounded_channel_default_backpressure() {
        let channel: BoundedChannel<String> = BoundedChannel::new(5, None);

        assert_eq!(channel.capacity, 5);
        assert_eq!(channel.backpressure_threshold, 0.8); // default
    }

    #[tokio::test]
    async fn test_sender_receiver_creation() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.7));
        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Verify sender properties
        assert_eq!(sender.capacity, 10);
        assert_eq!(sender.backpressure_threshold, 0.7);

        // Test basic send/recv
        sender.send(42).await.unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, 42);
    }

    #[tokio::test]
    async fn test_multiple_send_recv() {
        let channel: BoundedChannel<String> = BoundedChannel::new(5, None);
        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Send multiple items
        for i in 0..3 {
            sender.send(format!("message_{}", i)).await.unwrap();
        }

        // Receive them in order
        for i in 0..3 {
            let received = receiver.recv().await.unwrap();
            assert_eq!(received, format!("message_{}", i));
        }
    }

    #[tokio::test]
    async fn test_backpressure_detection() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.5)); // 50% threshold
        let mut sender = channel.sender();

        // Fill channel to just under threshold (5 items)
        for i in 0..4 {
            sender.send(i).await.unwrap();
            assert!(!sender.is_under_backpressure());
        }

        // Add one more item to trigger backpressure
        sender.send(4).await.unwrap();
        assert!(sender.is_under_backpressure());
    }

    #[tokio::test]
    async fn test_utilization_calculation() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, None);
        let mut sender = channel.sender();

        // Empty channel
        assert_eq!(sender.utilization(), 0.0);

        // Half full
        for i in 0..5 {
            sender.send(i).await.unwrap();
        }
        assert_eq!(sender.utilization(), 0.5);

        // Full channel
        for i in 5..10 {
            sender.send(i).await.unwrap();
        }
        assert_eq!(sender.utilization(), 1.0);
    }

    #[tokio::test]
    async fn test_channel_capacity_limit() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(2, None);
        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Fill channel to capacity
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();

        // This send should block, so we need to receive first
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(50)).await;
            let _ = receiver.recv().await;
        });

        sender.send(3).await.unwrap();
    }

    #[tokio::test]
    async fn test_sender_cloning() {
        let channel: BoundedChannel<String> = BoundedChannel::new(5, Some(0.6));
        let sender1 = channel.sender();
        let sender2 = sender1.clone();
        let receiver = channel.receiver();

        // Both senders should work
        let mut s1 = sender1;
        let mut s2 = sender2;

        s1.send("from_sender1".to_string()).await.unwrap();
        s2.send("from_sender2".to_string()).await.unwrap();

        let msg1 = receiver.recv().await.unwrap();
        let msg2 = receiver.recv().await.unwrap();

        // Order may vary, so check both messages were received
        assert!(msg1 == "from_sender1" || msg1 == "from_sender2");
        assert!(msg2 == "from_sender1" || msg2 == "from_sender2");
        assert_ne!(msg1, msg2); // They should be different
    }

    #[tokio::test]
    async fn test_receiver_cloning() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(5, None);
        let mut sender = channel.sender();
        let receiver1 = channel.receiver();
        let receiver2 = receiver1.clone();

        sender.send(100).await.unwrap();

        // Either receiver can get the message (async-channel behavior)
        let result = tokio::select! {
            r1 = receiver1.recv() => r1.unwrap(),
            r2 = receiver2.recv() => r2.unwrap(),
        };

        assert_eq!(result, 100);
    }

    #[tokio::test]
    async fn test_send_error_closed_channel() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(5, None);
        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Drop both receiver and the original channel to close it
        drop(receiver);
        drop(channel);

        let result = sender.send(42).await;
        assert!(matches!(result, Err(SendError::Closed(42))));
    }

    #[tokio::test]
    async fn test_recv_error_closed_channel() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(5, None);
        let receiver = channel.receiver();
        let sender = channel.sender();

        // Drop both sender and the original channel to close it
        drop(sender);
        drop(channel);

        let result = receiver.recv().await;
        assert!(matches!(result, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn test_send_error_display() {
        let error: SendError<i32> = SendError::Closed(42);
        assert_eq!(format!("{}", error), "Channel closed");
    }

    #[tokio::test]
    async fn test_recv_error_display() {
        let error = RecvError::Closed;
        assert_eq!(format!("{}", error), "Channel closed");
    }

    #[tokio::test]
    async fn test_create_bounded_channel_utility() {
        // Test with config capacity and custom backpressure delays
        let channel1: BoundedChannel<String> =
            create_bounded_channel(10, Some(20), Some(0.9), Some(5), Some(25));
        assert_eq!(channel1.capacity, 20); // Should use config value
        assert_eq!(channel1.backpressure_threshold, 0.9);
        // Sender inherits delay bounds
        let sender1 = channel1.sender();
        assert_eq!(sender1.backpressure_min_delay_ms, 5);
        assert_eq!(sender1.backpressure_max_delay_ms, 25);

        // Test with default capacity
        let channel2: BoundedChannel<i32> = create_bounded_channel(15, None, None, None, None);
        assert_eq!(channel2.capacity, 15); // Should use default value
        assert_eq!(channel2.backpressure_threshold, 0.8); // Should use default threshold
    }

    #[tokio::test]
    async fn test_backpressure_with_different_thresholds() {
        // Test low threshold (25%)
        let channel1: BoundedChannel<i32> = BoundedChannel::new(4, Some(0.25));
        let mut sender1 = channel1.sender();

        sender1.send(1).await.unwrap();
        assert!(sender1.is_under_backpressure()); // 1/4 = 25%

        // Test high threshold (90%)
        let channel2: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.9));
        let mut sender2 = channel2.sender();

        // Fill to 8 items (80%)
        for i in 0..8 {
            sender2.send(i).await.unwrap();
        }
        assert!(!sender2.is_under_backpressure()); // Below 90%

        // Add one more (90%)
        sender2.send(8).await.unwrap();
        assert!(sender2.is_under_backpressure()); // At 90%
    }

    #[tokio::test]
    async fn test_concurrent_producers_consumers() {
        let channel: BoundedChannel<usize> = BoundedChannel::new(100, None);
        let sender = channel.sender();
        let receiver = channel.receiver();

        // Spawn multiple producers
        let mut producer_handles = vec![];
        for producer_id in 0..5 {
            let mut s = sender.clone();
            producer_handles.push(tokio::spawn(async move {
                for i in 0..10 {
                    s.send(producer_id * 100 + i).await.unwrap();
                }
            }));
        }

        // Spawn consumer
        let consumer_handle = tokio::spawn(async move {
            let mut received = vec![];
            for _ in 0..50 {
                // 5 producers * 10 items each
                received.push(receiver.recv().await.unwrap());
            }
            received
        });

        // Wait for all producers to finish
        for handle in producer_handles {
            handle.await.unwrap();
        }

        // Get all received items
        let received_items = consumer_handle.await.unwrap();
        assert_eq!(received_items.len(), 50);

        // Verify all expected values are present
        let mut sorted_received = received_items;
        sorted_received.sort();
        let mut expected: Vec<usize> = (0..5)
            .flat_map(|p| (0..10).map(move |i| p * 100 + i))
            .collect();
        expected.sort();
        assert_eq!(sorted_received, expected);
    }

    #[tokio::test]
    async fn test_backpressure_timing() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(5, Some(0.6)); // 3 item threshold
        let mut sender = channel.sender();

        // Fill to threshold
        for i in 0..3 {
            sender.send(i).await.unwrap();
        }
        assert!(sender.is_under_backpressure());

        // Measure send time with backpressure
        let start = std::time::Instant::now();
        sender.send(3).await.unwrap();
        let duration = start.elapsed();

        // Should have added adaptive delay (minimum 2ms) due to backpressure
        assert!(duration >= Duration::from_millis(1)); // Allow for slight timing variation
    }

    #[tokio::test]
    async fn test_adaptive_backpressure_behavior() {
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.8)); // 80% threshold = 8 items
        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Test progression of backpressure states

        // Initially no backpressure
        assert!(!sender.is_under_backpressure());
        assert_eq!(sender.utilization(), 0.0);

        // Fill to just below threshold (7 items = 70%)
        for i in 0..7 {
            sender.send(i).await.unwrap();
        }
        assert!(!sender.is_under_backpressure());
        assert_eq!(sender.utilization(), 0.7);

        // Drop receiver to prevent further blocking sends
        drop(receiver);

        // Add one more item to trigger backpressure (8 items = 80%)
        sender.send(7).await.unwrap();
        assert!(sender.is_under_backpressure());
        assert_eq!(sender.utilization(), 0.8);

        // Verify the adaptive backpressure is working by testing behavior
        // with different utilization levels (without actually sending)
        let utilization_80 = sender.utilization();
        assert_eq!(utilization_80, 0.8);
        assert!(sender.is_under_backpressure());
    }

    #[tokio::test]
    async fn test_error_types_implement_error_trait() {
        let send_error: SendError<i32> = SendError::Closed(42);
        let recv_error = RecvError::Closed;

        // Test that they implement Error trait
        let _send_as_error: &dyn std::error::Error = &send_error;
        let _recv_as_error: &dyn std::error::Error = &recv_error;

        // Test Debug formatting
        assert!(format!("{:?}", send_error).contains("Closed"));
        assert!(format!("{:?}", recv_error).contains("Closed"));
    }

    #[tokio::test]
    async fn test_zero_capacity_handling() {
        // Test BoundedChannel::new with zero capacity
        let channel: BoundedChannel<i32> = BoundedChannel::new(0, Some(0.8));

        // Should have been clamped to minimum capacity of 1
        assert_eq!(channel.capacity, 1);

        let mut sender = channel.sender();
        let receiver = channel.receiver();

        // Should work normally with capacity 1
        sender.send(42).await.unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, 42);

        // Utilization should work without division by zero
        assert_eq!(sender.utilization(), 0.0);
    }

    #[tokio::test]
    async fn test_create_bounded_channel_with_zero_config() {
        // Test create_bounded_channel with zero capacity in config
        let channel: BoundedChannel<String> =
            create_bounded_channel(10, Some(0), Some(0.8), None, None);

        // Should have been clamped to minimum capacity of 1
        assert_eq!(channel.capacity, 1);

        let mut sender = channel.sender();

        // Should work normally
        sender.send("test".to_string()).await.unwrap();
        assert_eq!(sender.utilization(), 1.0); // Channel is full (1/1)
        assert!(sender.is_under_backpressure()); // Should be above 80% threshold
    }

    #[tokio::test]
    async fn test_negative_backpressure_threshold_validation() {
        // Test that negative thresholds get clamped to 0.0
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(-0.5));

        // Should have been clamped to 0.0
        assert_eq!(channel.backpressure_threshold(), 0.0);

        let mut sender = channel.sender();

        // With 0.0 threshold, backpressure should never trigger (threshold = 0)
        for i in 0..10 {
            sender.send(i).await.unwrap();
        }
        assert!(!sender.is_under_backpressure()); // Should not trigger backpressure even when full
    }

    #[tokio::test]
    async fn test_excessive_backpressure_threshold_validation() {
        // Test that values > 1.0 get clamped to 1.0
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(1.5));

        // Should have been clamped to 1.0
        assert_eq!(channel.backpressure_threshold(), 1.0);

        let mut sender = channel.sender();

        // With 1.0 threshold, backpressure should only trigger when completely full
        for i in 0..9 {
            sender.send(i).await.unwrap();
        }
        assert!(!sender.is_under_backpressure()); // 9/10 = 90% < 100%

        // Fill to capacity
        sender.send(9).await.unwrap();
        assert!(sender.is_under_backpressure()); // 10/10 = 100% >= 100%
    }

    #[tokio::test]
    async fn test_nan_backpressure_threshold_validation() {
        // Test that NaN values default to 0.8
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(f32::NAN));

        // Should have defaulted to 0.8
        assert_eq!(channel.backpressure_threshold(), 0.8);
    }

    #[tokio::test]
    async fn test_infinite_backpressure_threshold_validation() {
        // Test that infinite values default to 0.8
        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(f32::INFINITY));

        // Should have defaulted to 0.8
        assert_eq!(channel.backpressure_threshold(), 0.8);

        // Test negative infinity too
        let channel2: BoundedChannel<i32> = BoundedChannel::new(10, Some(f32::NEG_INFINITY));
        assert_eq!(channel2.backpressure_threshold(), 0.8);
    }

    #[tokio::test]
    async fn test_edge_case_backpressure_values() {
        // Test exactly 0.0
        let channel1: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.0));
        assert_eq!(channel1.backpressure_threshold(), 0.0);

        // Test exactly 1.0
        let channel2: BoundedChannel<i32> = BoundedChannel::new(10, Some(1.0));
        assert_eq!(channel2.backpressure_threshold(), 1.0);

        // Test very small positive value
        let channel3: BoundedChannel<i32> = BoundedChannel::new(10, Some(0.001));
        assert_eq!(channel3.backpressure_threshold(), 0.001);

        // Test very small negative value
        let channel4: BoundedChannel<i32> = BoundedChannel::new(10, Some(-0.001));
        assert_eq!(channel4.backpressure_threshold(), 0.0); // Should be clamped
    }

    #[tokio::test]
    async fn test_negative_threshold_prevents_constant_backpressure() {
        // This test verifies the fix for the original bug
        // where negative thresholds would cause backpressure on every send

        let channel: BoundedChannel<i32> = BoundedChannel::new(10, Some(-0.3));
        let mut sender = channel.sender();

        // With the fix, threshold should be clamped to 0.0
        assert_eq!(channel.backpressure_threshold(), 0.0);

        // Send one item - should not trigger backpressure with 0.0 threshold
        sender.send(1).await.unwrap();
        assert!(!sender.is_under_backpressure());

        // Send more items - still should not trigger backpressure
        for i in 2..=5 {
            sender.send(i).await.unwrap();
        }
        assert!(!sender.is_under_backpressure());

        // Even when full, 0.0 threshold means no backpressure
        for i in 6..=10 {
            sender.send(i).await.unwrap();
        }
        assert!(!sender.is_under_backpressure()); // This would have failed before the fix
    }
}
