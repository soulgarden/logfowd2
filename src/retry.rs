use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

#[derive(Clone)]
pub struct RetryManager {
    config: RetryConfig,
}

impl RetryManager {
    pub fn new(config: RetryConfig) -> Self {
        RetryManager { config }
    }

    #[allow(dead_code)] // Convenience constructor for default config
    pub fn with_default() -> Self {
        RetryManager::new(RetryConfig::default())
    }

    /// Execute a function with exponential backoff retry
    pub async fn execute_with_retry<F, Fut, T, E>(&self, operation: F) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut last_error = None;
        let mut delay = self.config.initial_delay;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                debug!("Retry attempt {} after delay of {:?}", attempt, delay);
                sleep(delay).await;

                // Calculate next delay with exponential backoff
                delay = Duration::from_millis(
                    ((delay.as_millis() as f64) * self.config.backoff_multiplier) as u64,
                )
                .min(self.config.max_delay);
            }

            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        debug!("Operation succeeded on attempt {}", attempt + 1);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    warn!("Operation failed on attempt {}: {}", attempt + 1, e);
                    last_error = Some(e);

                    if attempt == self.config.max_retries {
                        break;
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let retry_manager = RetryManager::with_default();

        let result = retry_manager
            .execute_with_retry(|| async { Ok::<i32, String>(42) })
            .await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let result = retry_manager
            .execute_with_retry(move || {
                let counter = counter_clone.clone();
                async move {
                    let count = counter.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err("Temporary failure".to_string())
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let result = retry_manager
            .execute_with_retry(move || {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, String>("Always fail".to_string())
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Always fail");
        assert_eq!(counter.load(Ordering::SeqCst), 3); // Initial attempt + 2 retries
    }

    #[tokio::test]
    async fn test_exponential_backoff_timing() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        });

        let call_times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let call_times_clone = call_times.clone();

        let start_time = Instant::now();

        let result = retry_manager
            .execute_with_retry(move || {
                let call_times = call_times_clone.clone();
                async move {
                    let elapsed = start_time.elapsed();
                    call_times.lock().unwrap().push(elapsed);
                    Err::<i32, String>("Always fail".to_string())
                }
            })
            .await;

        assert!(result.is_err());

        let times = call_times.lock().unwrap();
        assert_eq!(times.len(), 4); // Initial attempt + 3 retries

        // First call should be immediate (within 50ms tolerance)
        assert!(times[0] < Duration::from_millis(50));

        // Second call should be after ~100ms delay (allow ±50ms tolerance)
        assert!(times[1] >= Duration::from_millis(50));
        assert!(times[1] <= Duration::from_millis(200));

        // Third call should be after ~200ms additional delay (total ~300ms)
        assert!(times[2] >= Duration::from_millis(200));
        assert!(times[2] <= Duration::from_millis(450));

        // Fourth call should be after ~400ms additional delay (total ~700ms)
        assert!(times[3] >= Duration::from_millis(400));
        assert!(times[3] <= Duration::from_millis(900));
    }

    #[tokio::test]
    async fn test_max_delay_cap() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(300), // Cap at 300ms
            backoff_multiplier: 3.0,               // Aggressive multiplier
        });

        let call_times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let call_times_clone = call_times.clone();

        let start_time = Instant::now();

        let result = retry_manager
            .execute_with_retry(move || {
                let call_times = call_times_clone.clone();
                async move {
                    let elapsed = start_time.elapsed();
                    call_times.lock().unwrap().push(elapsed);
                    Err::<i32, String>("Always fail".to_string())
                }
            })
            .await;

        assert!(result.is_err());

        let times = call_times.lock().unwrap();

        // First call should be immediate
        assert!(times[0] < Duration::from_millis(50));

        // Subsequent calls should show capped exponential backoff
        for i in 1..times.len() {
            if i >= 2 {
                // After 2nd retry, delays should be capped at max_delay (300ms)
                // Allow some tolerance for timing variations in CI/test environments
                let delay_between_calls = times[i].saturating_sub(times[i - 1]);
                assert!(
                    delay_between_calls <= Duration::from_millis(400),
                    "Delay {} at attempt {} exceeded reasonable max_delay cap",
                    delay_between_calls.as_millis(),
                    i
                );
                assert!(
                    delay_between_calls >= Duration::from_millis(250),
                    "Delay {} at attempt {} should be near max_delay",
                    delay_between_calls.as_millis(),
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_backoff_multiplier_precision() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 1.5, // Non-integer multiplier
        });

        let call_times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let call_times_clone = call_times.clone();

        let start_time = Instant::now();

        let result = retry_manager
            .execute_with_retry(move || {
                let call_times = call_times_clone.clone();
                async move {
                    call_times.lock().unwrap().push(start_time.elapsed());
                    Err::<i32, String>("Always fail".to_string())
                }
            })
            .await;

        assert!(result.is_err());

        let times = call_times.lock().unwrap();

        // Verify multiplier precision:
        // 1st retry: 50ms delay
        // 2nd retry: 50 * 1.5 = 75ms additional delay
        // 3rd retry: 75 * 1.5 = 112.5ms additional delay

        if times.len() >= 2 {
            let delay_1 = times[1];
            assert!(delay_1 >= Duration::from_millis(40) && delay_1 <= Duration::from_millis(100));
        }

        if times.len() >= 3 {
            let delay_2_total = times[2];
            // Should be around 50ms + 75ms = 125ms total
            assert!(
                delay_2_total >= Duration::from_millis(100)
                    && delay_2_total <= Duration::from_millis(175)
            );
        }
    }

    #[tokio::test]
    async fn test_zero_retries_config() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 0,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let result = retry_manager
            .execute_with_retry(move || {
                let counter = counter_clone.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, String>("Always fail".to_string())
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only initial attempt, no retries
    }

    #[tokio::test]
    async fn test_minimal_delay_config() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(1), // Minimal delay
            max_delay: Duration::from_millis(5),     // Small max
            backoff_multiplier: 10.0,                // Large multiplier
        });

        let start_time = Instant::now();

        let result = retry_manager
            .execute_with_retry(|| async { Err::<i32, String>("Always fail".to_string()) })
            .await;

        let total_time = start_time.elapsed();

        assert!(result.is_err());
        // Should complete quickly even with retries due to small delays
        assert!(total_time < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_default_config_values() {
        let default_config = RetryConfig::default();

        assert_eq!(default_config.max_retries, 3);
        assert_eq!(default_config.initial_delay, Duration::from_millis(100));
        assert_eq!(default_config.max_delay, Duration::from_secs(10));
        assert_eq!(default_config.backoff_multiplier, 2.0);

        let retry_manager = RetryManager::with_default();
        assert_eq!(retry_manager.config.max_retries, 3);
    }

    #[tokio::test]
    async fn test_immediate_success_no_delay() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(1000), // Long delay
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        });

        let start_time = Instant::now();

        let result = retry_manager
            .execute_with_retry(|| async { Ok::<i32, String>(42) })
            .await;

        let elapsed = start_time.elapsed();

        assert_eq!(result.unwrap(), 42);
        // Should complete immediately without any delay
        assert!(elapsed < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_error_message_preservation() {
        let retry_manager = RetryManager::new(RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
        });

        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_clone = attempt.clone();

        let result = retry_manager
            .execute_with_retry(move || {
                let attempt = attempt_clone.clone();
                async move {
                    let current = attempt.fetch_add(1, Ordering::SeqCst) + 1;
                    Err::<i32, String>(format!("Attempt {} failed", current))
                }
            })
            .await;

        assert!(result.is_err());
        // Should preserve the error message from the last attempt
        assert!(result.unwrap_err().contains("Attempt 3 failed"));
    }
}
