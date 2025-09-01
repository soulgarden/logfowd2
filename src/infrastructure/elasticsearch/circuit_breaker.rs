use async_trait::async_trait;
use tracing::{debug, info, warn};
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::traits::{HealthCheck, HealthStatus};

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Circuit is open, failing fast
    HalfOpen, // Testing if service is back
}

pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,    // Number of failures to trigger open
    pub timeout: Duration,           // How long to wait before going half-open
    pub success_threshold: usize,    // Successes needed in half-open to close
    pub monitoring_window: Duration, // Time window for failure counting
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout: Duration::from_secs(60),
            success_threshold: 3,
            monitoring_window: Duration::from_secs(60),
        }
    }
}

pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    name: String,
}

struct CircuitBreakerState {
    current_state: CircuitState,
    last_state_change: Instant,
    failures_in_window: Vec<Instant>,
}

impl CircuitBreaker {
    pub fn new(name: String, config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(CircuitBreakerState {
                current_state: CircuitState::Closed,
                last_state_change: Instant::now(),
                failures_in_window: Vec::new(),
            })),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure_time: Arc::new(RwLock::new(None)),
            name,
        }
    }

    pub async fn call<T, E, Fut>(
        &self,
        operation: impl FnOnce() -> Fut,
    ) -> Result<T, CircuitBreakerError<E>>
    where
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        // Check if circuit is open
        if self.is_open().await {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        // Execute the operation
        let result = operation().await;

        match result {
            Ok(success) => {
                self.record_success().await;
                Ok(success)
            }
            Err(error) => {
                self.record_failure().await;
                Err(CircuitBreakerError::Operation(error))
            }
        }
    }

    async fn is_open(&self) -> bool {
        let state = self.state.read().await;
        match state.current_state {
            CircuitState::Open => {
                // Check if we should transition to half-open
                if state.last_state_change.elapsed() >= self.config.timeout {
                    drop(state);
                    self.transition_to_half_open().await;
                    false
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => {
                // In half-open state, allow limited requests
                false
            }
            CircuitState::Closed => false,
        }
    }

    async fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);

        let state = self.state.read().await;
        if state.current_state == CircuitState::HalfOpen {
            let success_count = self.success_count.load(Ordering::Relaxed);
            if success_count >= self.config.success_threshold as u64 {
                drop(state);
                self.transition_to_closed().await;
            }
        }
    }

    async fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        *self.last_failure_time.write().await = Some(Instant::now());

        let mut state = self.state.write().await;
        let now = Instant::now();

        // Add current failure to window
        state.failures_in_window.push(now);

        // Remove old failures outside the monitoring window
        state
            .failures_in_window
            .retain(|&time| now.duration_since(time) <= self.config.monitoring_window);

        // Check if we should open the circuit
        match state.current_state {
            CircuitState::Closed => {
                if state.failures_in_window.len() >= self.config.failure_threshold {
                    state.current_state = CircuitState::Open;
                    state.last_state_change = now;
                    warn!(
                        "Circuit breaker '{}' opened after {} failures",
                        self.name,
                        state.failures_in_window.len()
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                state.current_state = CircuitState::Open;
                state.last_state_change = now;
                warn!(
                    "Circuit breaker '{}' reopened after failure in half-open state",
                    self.name
                );
            }
            CircuitState::Open => {
                // Already open, do nothing
            }
        }
    }

    async fn transition_to_half_open(&self) {
        let mut state = self.state.write().await;
        if matches!(state.current_state, CircuitState::Open) {
            state.current_state = CircuitState::HalfOpen;
            state.last_state_change = Instant::now();
            self.success_count.store(0, Ordering::Relaxed);
            info!("Circuit breaker '{}' transitioned to half-open", self.name);
        }
    }

    async fn transition_to_closed(&self) {
        let mut state = self.state.write().await;
        state.current_state = CircuitState::Closed;
        state.last_state_change = Instant::now();
        state.failures_in_window.clear();
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
        info!("Circuit breaker '{}' closed after recovery", self.name);
    }

    pub async fn get_state(&self) -> CircuitState {
        self.state.read().await.current_state.clone()
    }
}

#[async_trait]
impl HealthCheck for CircuitBreaker {
    async fn is_healthy(&self) -> bool {
        let state = self.get_state().await;
        matches!(state, CircuitState::Closed)
    }
    
    async fn health_status(&self) -> HealthStatus {
        let state = self.get_state().await;
        let failure_count = self.failure_count.load(Ordering::Relaxed);
        let success_count = self.success_count.load(Ordering::Relaxed);
        
        let (healthy, message) = match state {
            CircuitState::Closed => (true, format!("Circuit breaker '{}' is closed (healthy)", self.name)),
            CircuitState::Open => (false, format!("Circuit breaker '{}' is open (failing fast)", self.name)),
            CircuitState::HalfOpen => (false, format!("Circuit breaker '{}' is half-open (testing recovery)", self.name)),
        };
        
        let details = serde_json::json!({
            "state": format!("{:?}", state),
            "failure_count": failure_count,
            "success_count": success_count,
            "failure_threshold": self.config.failure_threshold,
            "success_threshold": self.config.success_threshold,
        });
        
        HealthStatus {
            healthy,
            message,
            details: Some(details),
        }
    }
}

#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    CircuitOpen,
    Operation(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::Operation(e) => write!(f, "Operation failed: {}", e),
        }
    }
}

impl<E: std::error::Error> std::error::Error for CircuitBreakerError<E> {}

// Factory function for common ES circuit breaker configuration
pub fn create_es_circuit_breaker(name: String) -> CircuitBreaker {
    let config = CircuitBreakerConfig {
        failure_threshold: 10,                       // 10 failures
        timeout: Duration::from_secs(30),            // Wait 30s before trying again
        success_threshold: 5,                        // Need 5 successes to recover
        monitoring_window: Duration::from_secs(120), // 2 minute window
    };

    debug!(
        "Creating ES circuit breaker '{}' with config: failure_threshold={}, timeout={}s",
        name,
        config.failure_threshold,
        config.timeout.as_secs()
    );

    CircuitBreaker::new(name, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    async fn create_test_circuit_breaker() -> CircuitBreaker {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout: Duration::from_millis(100),
            success_threshold: 2,
            monitoring_window: Duration::from_secs(5),
        };
        CircuitBreaker::new("test".to_string(), config)
    }

    async fn failing_operation() -> Result<String, &'static str> {
        Err("simulated failure")
    }

    async fn successful_operation() -> Result<String, &'static str> {
        Ok("success".to_string())
    }

    #[tokio::test]
    async fn test_initial_state() {
        let circuit_breaker = create_test_circuit_breaker().await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_successful_operation() {
        let circuit_breaker = create_test_circuit_breaker().await;

        let result = circuit_breaker.call(successful_operation).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_single_failure_keeps_circuit_closed() {
        let circuit_breaker = create_test_circuit_breaker().await;

        let result = circuit_breaker.call(failing_operation).await;

        assert!(result.is_err());
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_multiple_failures_open_circuit() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Trigger failures up to threshold
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_open_circuit_fails_fast() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Trigger failures to open circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Next call should fail fast without executing operation
        let result = circuit_breaker.call(successful_operation).await;

        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));
    }

    #[tokio::test]
    async fn test_transition_to_half_open_after_timeout() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open the circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Wait for timeout to elapse
        sleep(Duration::from_millis(150)).await;

        // Next call should transition to half-open and execute
        let result = circuit_breaker.call(successful_operation).await;

        assert!(result.is_ok());
        assert_eq!(circuit_breaker.get_state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_half_open_failure_reopens_circuit() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open the circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        // Wait and transition to half-open
        sleep(Duration::from_millis(150)).await;
        let _ = circuit_breaker.call(successful_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::HalfOpen);

        // Fail in half-open state
        let result = circuit_breaker.call(failing_operation).await;

        assert!(result.is_err());
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_half_open_recovery_closes_circuit() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open the circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        // Wait and transition to half-open
        sleep(Duration::from_millis(150)).await;
        let _ = circuit_breaker.call(successful_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::HalfOpen);

        // Succeed enough times to close circuit (success_threshold = 2)
        let _ = circuit_breaker.call(successful_operation).await;

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_monitoring_window_failure_expiration() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout: Duration::from_millis(100),
            success_threshold: 2,
            monitoring_window: Duration::from_millis(50), // Very short window
        };
        let circuit_breaker = CircuitBreaker::new("test".to_string(), config);

        // Add 2 failures
        for _ in 0..2 {
            let _ = circuit_breaker.call(failing_operation).await;
        }
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        // Wait for failures to expire from window
        sleep(Duration::from_millis(100)).await;

        // Add 2 more failures - should not open circuit as old failures expired
        for _ in 0..2 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_default_config_values() {
        let config = CircuitBreakerConfig::default();

        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.success_threshold, 3);
        assert_eq!(config.monitoring_window, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_es_circuit_breaker_factory() {
        let circuit_breaker = create_es_circuit_breaker("test-es".to_string());

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
        assert_eq!(circuit_breaker.name, "test-es");
        assert_eq!(circuit_breaker.config.failure_threshold, 10);
        assert_eq!(circuit_breaker.config.timeout, Duration::from_secs(30));
        assert_eq!(circuit_breaker.config.success_threshold, 5);
    }

    #[tokio::test]
    async fn test_error_display_formatting() {
        let circuit_open_error: CircuitBreakerError<&str> = CircuitBreakerError::CircuitOpen;
        let operation_error: CircuitBreakerError<&str> =
            CircuitBreakerError::Operation("test error");

        assert_eq!(format!("{}", circuit_open_error), "Circuit breaker is open");
        assert_eq!(
            format!("{}", operation_error),
            "Operation failed: test error"
        );
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let circuit_breaker = Arc::new(create_test_circuit_breaker().await);

        // Run concurrent successful operations
        let mut handles = vec![];
        for _ in 0..10 {
            let cb = circuit_breaker.clone();
            handles.push(tokio::spawn(
                async move { cb.call(successful_operation).await },
            ));
        }

        // All should succeed
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_state_transitions_under_load() {
        let circuit_breaker = Arc::new(create_test_circuit_breaker().await);

        // Run concurrent failing operations to trigger circuit opening
        let mut handles = vec![];
        for _ in 0..5 {
            let cb = circuit_breaker.clone();
            handles.push(tokio::spawn(
                async move { cb.call(failing_operation).await },
            ));
        }

        // Wait for all to complete
        for handle in handles {
            let _ = handle.await.unwrap();
        }

        // Circuit should be open
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Wait for timeout and test recovery
        sleep(Duration::from_millis(150)).await;

        // Should be able to make successful calls and eventually close
        for _ in 0..3 {
            let _ = circuit_breaker.call(successful_operation).await;
        }

        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_exact_failure_threshold_boundary() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Fail exactly threshold-1 times (should stay closed)
        for i in 0..(3 - 1) {
            let result = circuit_breaker.call(failing_operation).await;
            assert!(result.is_err(), "Call {} should fail", i);
            assert_eq!(
                circuit_breaker.get_state().await,
                CircuitState::Closed,
                "Circuit should remain closed after {} failures",
                i + 1
            );
        }

        // The threshold failure should open the circuit
        let result = circuit_breaker.call(failing_operation).await;
        assert!(
            result.is_err(),
            "Threshold failure should still execute and fail"
        );
        assert_eq!(
            circuit_breaker.get_state().await,
            CircuitState::Open,
            "Circuit should be open after threshold failures"
        );
    }

    #[tokio::test]
    async fn test_partial_recovery_in_half_open() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Wait and transition to half-open
        sleep(Duration::from_millis(150)).await;
        let _ = circuit_breaker.call(successful_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::HalfOpen);

        // Succeed once (need 2 total for success_threshold)
        let result1 = circuit_breaker.call(successful_operation).await;
        assert!(result1.is_ok());

        // Should now be closed (reached success_threshold of 2)
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_reset_after_successful_closure() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        // Recover through half-open
        sleep(Duration::from_millis(150)).await;
        for _ in 0..2 {
            let _ = circuit_breaker.call(successful_operation).await;
        }
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        // Should be able to handle new failures from clean slate
        for _ in 0..2 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        // Should still be closed (failure count reset after closure)
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        // One more failure to hit threshold again
        let _ = circuit_breaker.call(failing_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_mixed_success_failure_patterns() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Interleave successes and failures
        let _ = circuit_breaker.call(failing_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        let _ = circuit_breaker.call(successful_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        let _ = circuit_breaker.call(failing_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        let _ = circuit_breaker.call(successful_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Closed);

        // Two more failures should open it (total 3 failures)
        let _ = circuit_breaker.call(failing_operation).await;
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_immediate_transition_from_open_to_half_open() {
        let circuit_breaker = create_test_circuit_breaker().await;

        // Open the circuit
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }
        assert_eq!(circuit_breaker.get_state().await, CircuitState::Open);

        // Wait exact timeout duration
        sleep(Duration::from_millis(100)).await;

        // Verify first call after timeout transitions to half-open
        let result = circuit_breaker.call(successful_operation).await;
        assert!(result.is_ok());
        assert_eq!(circuit_breaker.get_state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_rapid_open_close_transitions() {
        // Test circuit's behavior under rapid state changes
        let config = CircuitBreakerConfig {
            failure_threshold: 1,               // Single failure opens circuit
            timeout: Duration::from_millis(10), // Very short timeout
            success_threshold: 1,               // Single success closes circuit
            monitoring_window: Duration::from_millis(1000),
        };
        let circuit_breaker = CircuitBreaker::new("rapid-test".to_string(), config);

        for cycle in 0..3 {
            // Fail to open
            let _ = circuit_breaker.call(failing_operation).await;
            assert_eq!(
                circuit_breaker.get_state().await,
                CircuitState::Open,
                "Cycle {}: Should be open after failure",
                cycle
            );

            // Wait and recover
            sleep(Duration::from_millis(20)).await;
            let _ = circuit_breaker.call(successful_operation).await;
            assert_eq!(
                circuit_breaker.get_state().await,
                CircuitState::Closed,
                "Cycle {}: Should be closed after recovery",
                cycle
            );
        }
    }

    #[tokio::test]
    async fn test_error_type_preservation() {
        let circuit_breaker = create_test_circuit_breaker().await;

        async fn custom_error_operation() -> Result<String, String> {
            Err("custom error message".to_string())
        }

        // Test that operation errors are preserved
        let result = circuit_breaker.call(custom_error_operation).await;
        match result {
            Err(CircuitBreakerError::Operation(msg)) => {
                assert_eq!(msg, "custom error message");
            }
            _ => panic!("Expected Operation error with custom message"),
        }

        // Open circuit with failures
        for _ in 0..3 {
            let _ = circuit_breaker.call(failing_operation).await;
        }

        // Test that circuit open errors are distinct
        let result = circuit_breaker.call(successful_operation).await;
        match result {
            Err(CircuitBreakerError::CircuitOpen) => {} // Expected
            _ => panic!("Expected CircuitOpen error when circuit is open"),
        }
    }
}
