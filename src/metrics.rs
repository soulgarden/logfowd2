use prometheus::{
    GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, Registry, TextEncoder, register_gauge_vec,
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
};
use std::sync::OnceLock;

// Global metrics registry
static METRICS: OnceLock<LogfwdMetrics> = OnceLock::new();

#[derive(Clone)]
pub struct LogfwdMetrics {
    // Performance metrics
    #[allow(dead_code)]
    pub events_processed_total: IntCounterVec,
    #[allow(dead_code)]
    pub events_per_second: GaugeVec,
    #[allow(dead_code)]
    pub batch_size_histogram: HistogramVec,
    #[allow(dead_code)]
    pub processing_duration_seconds: HistogramVec,

    // System health metrics
    #[allow(dead_code)]
    pub queue_size: IntGaugeVec,
    #[allow(dead_code)]
    pub backpressure_active: IntGaugeVec,
    #[allow(dead_code)]
    pub circuit_breaker_state: IntGaugeVec,
    #[allow(dead_code)]
    pub workers_active: IntGaugeVec,

    // Error and monitoring metrics
    #[allow(dead_code)]
    pub errors_total: IntCounterVec,
    #[allow(dead_code)]
    pub dead_letter_queue_size: IntGaugeVec,
    #[allow(dead_code)]
    pub files_tracked: IntGaugeVec,

    // Internal registry
    registry: Registry,
}

impl LogfwdMetrics {
    fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        // Performance metrics
        let events_processed_total = register_int_counter_vec!(
            "logfowd_events_processed_total",
            "Total number of log events processed",
            &["component", "status"] // component: watcher|sender|es_worker, status: success|error
        )?;
        registry.register(Box::new(events_processed_total.clone()))?;

        let events_per_second = register_gauge_vec!(
            "logfowd_events_per_second",
            "Current events processing rate per second",
            &["component"]
        )?;
        registry.register(Box::new(events_per_second.clone()))?;

        let batch_size_histogram = register_histogram_vec!(
            "logfowd_batch_size",
            "Distribution of batch sizes",
            &["component"], // sender|es_worker
            vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]
        )?;
        registry.register(Box::new(batch_size_histogram.clone()))?;

        let processing_duration_seconds = register_histogram_vec!(
            "logfowd_processing_duration_seconds",
            "Time spent processing events",
            &["component", "operation"], // operation: read_file|send_batch|es_index
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        )?;
        registry.register(Box::new(processing_duration_seconds.clone()))?;

        // System health metrics
        let queue_size = register_int_gauge_vec!(
            "logfowd_queue_size",
            "Current size of processing queues",
            &["component", "queue_type"] // component: watcher|sender|es_worker, queue_type: input|output|dlq
        )?;
        registry.register(Box::new(queue_size.clone()))?;

        let backpressure_active = register_int_gauge_vec!(
            "logfowd_backpressure_active",
            "Whether backpressure is currently active (0=no, 1=yes)",
            &["component"]
        )?;
        registry.register(Box::new(backpressure_active.clone()))?;

        let circuit_breaker_state = register_int_gauge_vec!(
            "logfowd_circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half_open)",
            &["component", "breaker_name"]
        )?;
        registry.register(Box::new(circuit_breaker_state.clone()))?;

        let workers_active = register_int_gauge_vec!(
            "logfowd_workers_active",
            "Number of active worker threads",
            &["component"]
        )?;
        registry.register(Box::new(workers_active.clone()))?;

        // Error and monitoring metrics
        let errors_total = register_int_counter_vec!(
            "logfowd_errors_total",
            "Total number of errors by type and component",
            &["component", "error_type"] // error_type: file_read|network|parse|timeout
        )?;
        registry.register(Box::new(errors_total.clone()))?;

        let dead_letter_queue_size = register_int_gauge_vec!(
            "logfowd_dead_letter_queue_size",
            "Current size of dead letter queue",
            &["queue_type"] // queue_type: events|retries
        )?;
        registry.register(Box::new(dead_letter_queue_size.clone()))?;

        let files_tracked = register_int_gauge_vec!(
            "logfowd_files_tracked",
            "Number of files currently being tracked",
            &["namespace"] // kubernetes namespace for tracking
        )?;
        registry.register(Box::new(files_tracked.clone()))?;

        Ok(LogfwdMetrics {
            events_processed_total,
            events_per_second,
            batch_size_histogram,
            processing_duration_seconds,
            queue_size,
            backpressure_active,
            circuit_breaker_state,
            workers_active,
            errors_total,
            dead_letter_queue_size,
            files_tracked,
            registry,
        })
    }

    pub fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_else(|e| format!("# Error encoding metrics: {}\n", e))
    }
}

// Initialize global metrics instance
pub fn init_metrics() -> Result<(), prometheus::Error> {
    let metrics = LogfwdMetrics::new()?;
    METRICS
        .set(metrics)
        .map_err(|_| prometheus::Error::Msg("Metrics already initialized".to_string()))?;
    Ok(())
}

// Get global metrics instance (lazy initialization)
pub fn metrics() -> &'static LogfwdMetrics {
    METRICS.get_or_init(|| LogfwdMetrics::new().expect("Failed to create metrics"))
}

// Check if metrics are enabled from config
pub fn are_metrics_enabled(config: &Option<crate::conf::MetricsConfig>) -> bool {
    config.as_ref().and_then(|c| c.enabled).unwrap_or(false) // Default to disabled
}

// Safe metrics operations - only execute if metrics are enabled
#[macro_export]
macro_rules! safe_inc_counter {
    ($enabled:expr, $metric:expr, $labels:expr) => {
        if $enabled {
            $metric.with_label_values($labels).inc()
        }
    };
}

#[macro_export]
macro_rules! safe_inc_counter_by {
    ($enabled:expr, $metric:expr, $labels:expr, $value:expr) => {
        if $enabled {
            $metric.with_label_values($labels).inc_by($value)
        }
    };
}

#[macro_export]
macro_rules! safe_set_gauge {
    ($enabled:expr, $metric:expr, $labels:expr, $value:expr) => {
        if $enabled {
            $metric.with_label_values($labels).set($value)
        }
    };
}

#[macro_export]
macro_rules! safe_observe_histogram {
    ($enabled:expr, $metric:expr, $labels:expr, $value:expr) => {
        if $enabled {
            $metric.with_label_values($labels).observe($value)
        }
    };
}

// Convenience macros for common metric operations
#[macro_export]
macro_rules! inc_counter {
    ($metric:expr, $labels:expr) => {
        $metric.with_label_values($labels).inc()
    };
}

#[macro_export]
macro_rules! set_gauge {
    ($metric:expr, $labels:expr, $value:expr) => {
        $metric.with_label_values($labels).set($value)
    };
}

#[macro_export]
macro_rules! observe_histogram {
    ($metric:expr, $labels:expr, $value:expr) => {
        $metric.with_label_values($labels).observe($value)
    };
}

// Timer helper for measuring durations
#[allow(dead_code)]
pub struct MetricsTimer {
    start: std::time::Instant,
    histogram: HistogramVec,
    labels: Vec<&'static str>,
}

#[allow(dead_code)]
impl MetricsTimer {
    pub fn new(histogram: HistogramVec, labels: Vec<&'static str>) -> Self {
        Self {
            start: std::time::Instant::now(),
            histogram,
            labels,
        }
    }
}

impl Drop for MetricsTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.histogram
            .with_label_values(&self.labels)
            .observe(duration);
    }
}

// Helper function to create a timer
#[allow(dead_code)]
pub fn start_timer(histogram: HistogramVec, labels: Vec<&'static str>) -> MetricsTimer {
    MetricsTimer::new(histogram, labels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT_ONCE: Once = Once::new();

    #[test]
    fn test_metrics_initialization() {
        // Initialize or use existing global metrics
        let _ = init_metrics(); // Ignore error if already initialized

        // Test that we can access the global instance
        if let Some(m) = METRICS.get() {
            m.events_processed_total
                .with_label_values(&["test_init", "success"])
                .inc();
        } else {
            // Fallback - directly initialize for this test
            INIT_ONCE.call_once(|| {
                let _ = init_metrics();
            });
            let m = metrics();
            m.events_processed_total
                .with_label_values(&["test_init", "success"])
                .inc();
        }
    }

    #[test]
    fn test_metrics_gathering() {
        // Use the global metrics instance (it will be initialized lazily)
        let m = metrics();

        // Increment a counter with unique labels for this test
        m.events_processed_total
            .with_label_values(&["test_gathering_func", "success"])
            .inc();

        // Set a gauge with unique labels
        m.queue_size
            .with_label_values(&["test_gathering_func", "input"])
            .set(42);

        // Observe histogram with unique labels
        m.batch_size_histogram
            .with_label_values(&["test_gathering_func"])
            .observe(100.0);

        let output = m.gather();

        // Check that the output contains our metrics
        assert!(output.contains("logfowd_events_processed_total"));
        assert!(output.contains("logfowd_queue_size"));
        assert!(output.contains("logfowd_batch_size"));
        assert!(output.contains("test_gathering_func"));
    }

    #[test]
    fn test_timer_functionality() {
        // Use the global metrics instance (it will be initialized lazily)
        let m = metrics();

        {
            let _timer = start_timer(
                m.processing_duration_seconds.clone(),
                vec!["test_timer_func", "operation"],
            );

            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(1));
        } // Timer drops here and records the duration

        let output = m.gather();
        assert!(output.contains("logfowd_processing_duration_seconds"));
        assert!(output.contains("test_timer_func"));
    }
}
