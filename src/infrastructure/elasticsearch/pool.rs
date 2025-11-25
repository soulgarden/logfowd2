use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BufMut;
use chrono::Utc;
use reqwest::Client;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::Settings;
use crate::domain::event::Event;
use crate::error::{EsError, Result};
use crate::infrastructure::elasticsearch::circuit_breaker::{
    CircuitBreaker, CircuitBreakerError, create_es_circuit_breaker,
};
use crate::infrastructure::elasticsearch::dead_letter_queue::{
    DeadLetterQueue, DeadLetterQueueConfig,
};
use crate::requests::{FieldsBody, Index};
use crate::retry::{RetryConfig, RetryManager};
use crate::transport::channels::BoundedReceiver;

pub struct EsWorkerPool {
    workers: Vec<EsWorker>,
    es_queue_receiver: BoundedReceiver<Vec<Event>>,
    dead_letter_queue: Arc<DeadLetterQueue>,
    conf: Settings,
}

struct EsWorker {
    id: usize,
    http_client: Arc<dyn HttpClient>,
    conf: Settings,
    retry_manager: RetryManager,
    circuit_breaker: CircuitBreaker,
    dead_letter_queue: Arc<DeadLetterQueue>,
    network_stats: NetworkStats,
}

#[derive(Debug, Clone)]
struct NetworkStats {
    pub avg_latency: Duration,
    pub success_count: u64,
    pub failure_count: u64,
    pub last_success: Option<std::time::Instant>,
    pub consecutive_failures: u32,
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self {
            avg_latency: Duration::from_millis(100), // Default 100ms
            success_count: 0,
            failure_count: 0,
            last_success: None,
            consecutive_failures: 0,
        }
    }
}

impl NetworkStats {
    fn record_success(&mut self, latency: Duration) {
        self.success_count += 1;
        self.consecutive_failures = 0;
        self.last_success = Some(std::time::Instant::now());

        // Exponential moving average: new_avg = 0.9 * old_avg + 0.1 * new_value
        let weight = 0.1;
        let new_latency_ms = latency.as_millis() as f64;
        let old_latency_ms = self.avg_latency.as_millis() as f64;
        let updated_latency_ms = (1.0 - weight) * old_latency_ms + weight * new_latency_ms;

        self.avg_latency = Duration::from_millis(updated_latency_ms as u64);

        debug!(
            "Network success: latency={}ms, avg={}ms",
            latency.as_millis(),
            self.avg_latency.as_millis()
        );
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        self.consecutive_failures += 1;

        warn!(
            "Network failure recorded: consecutive={}, total_failures={}",
            self.consecutive_failures, self.failure_count
        );
    }

    fn adaptive_timeout(&self) -> Duration {
        let base_timeout = Duration::from_secs(30);

        // Adjust timeout based on current network conditions
        let latency_factor = if self.avg_latency.as_millis() > 1000 {
            2.0 // High latency - double the timeout
        } else if self.avg_latency.as_millis() > 500 {
            1.5 // Medium latency - 1.5x timeout
        } else {
            1.0 // Normal latency - standard timeout
        };

        // Increase timeout if we have consecutive failures
        let failure_factor = if self.consecutive_failures > 5 {
            2.0
        } else if self.consecutive_failures > 2 {
            1.5
        } else {
            1.0
        };

        let adjusted_timeout = Duration::from_millis(
            (base_timeout.as_millis() as f64 * latency_factor * failure_factor) as u64,
        );

        // Cap at reasonable limits
        std::cmp::min(adjusted_timeout, Duration::from_secs(120))
    }

    fn is_network_degraded(&self) -> bool {
        // Consider network degraded if:
        // 1. High consecutive failures
        // 2. High average latency
        // 3. No recent successes

        if self.consecutive_failures > 3 {
            return true;
        }

        if self.avg_latency.as_millis() > 2000 {
            return true;
        }

        if let Some(last_success) = self.last_success {
            if last_success.elapsed() > Duration::from_secs(60) {
                return true;
            }
        } else if self.failure_count > 0 {
            return true;
        }

        false
    }
}

#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn post_bytes(&self, url: &str, body: Vec<u8>) -> std::result::Result<String, EsError>;
    async fn post_bytes_with_timeout(
        &self,
        url: &str,
        body: Vec<u8>,
        timeout: Duration,
    ) -> std::result::Result<(String, Duration), EsError>;
}

struct ReqwestHttpClient {
    client: Client,
}

impl ReqwestHttpClient {
    fn classify_reqwest_error(error: reqwest::Error) -> EsError {
        if error.is_timeout() {
            warn!("Request timeout: {}", error);
            return EsError::Timeout;
        }

        if error.is_connect() {
            warn!("Connection failed: {}", error);
            return EsError::ConnectionFailed(error.to_string());
        }

        if error.is_request() {
            if let Some(url) = error.url()
                && (error.to_string().contains("dns") || error.to_string().contains("resolve"))
            {
                warn!("DNS resolution failed for {}: {}", url, error);
                return EsError::DnsResolutionFailed(format!("{}: {}", url, error));
            }

            if error.to_string().contains("tls") || error.to_string().contains("ssl") {
                warn!("TLS handshake failed: {}", error);
                return EsError::TlsHandshakeFailed(error.to_string());
            }

            if error.to_string().contains("unreachable") || error.to_string().contains("route") {
                warn!("Network unreachable: {}", error);
                return EsError::NetworkUnreachable(error.to_string());
            }
        }

        // Generic fallback
        warn!("Generic request error: {}", error);
        EsError::RequestFailed(error.to_string())
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn post_bytes(&self, url: &str, body: Vec<u8>) -> std::result::Result<String, EsError> {
        let (response, _latency) = self
            .post_bytes_with_timeout(url, body, Duration::from_secs(30))
            .await?;
        Ok(response)
    }

    async fn post_bytes_with_timeout(
        &self,
        url: &str,
        body: Vec<u8>,
        timeout_duration: Duration,
    ) -> std::result::Result<(String, Duration), EsError> {
        let start_time = std::time::Instant::now();

        // Send request with adaptive timeout
        let request_future = self
            .client
            .post(url)
            .body(body)
            .header("Content-Type", "application/json")
            .send();

        let response = match timeout(timeout_duration, request_future).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                return Err(Self::classify_reqwest_error(e));
            }
            Err(_) => {
                warn!("Request timeout after {}ms", timeout_duration.as_millis());
                return Err(EsError::Timeout);
            }
        };

        let status = response.status();

        // Check for specific HTTP status codes
        match status.as_u16() {
            200..=299 => {
                // Success - process response body
                let response_body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read response body".to_string());

                let latency = start_time.elapsed();
                Ok((response_body, latency))
            }
            429 => {
                // Rate limited
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(Duration::from_secs);

                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Rate limit exceeded".to_string());

                warn!("Rate limited by Elasticsearch: {}", body);
                Err(EsError::RateLimited { retry_after })
            }
            500..=599 => {
                // Server errors
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Server error".to_string());

                if status == 503 {
                    warn!("Elasticsearch service unavailable: {}", body);
                    Err(EsError::ServiceUnavailable)
                } else {
                    error!("Elasticsearch server error {}: {}", status, body);
                    Err(EsError::HttpStatusError {
                        status: status.as_u16(),
                        body,
                    })
                }
            }
            400..=499 => {
                // Client errors
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Client error".to_string());

                error!("Elasticsearch client error {}: {}", status, body);
                Err(EsError::HttpStatusError {
                    status: status.as_u16(),
                    body,
                })
            }
            _ => {
                // Unexpected status codes
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unexpected status".to_string());

                warn!("Unexpected HTTP status {}: {}", status, body);
                Err(EsError::HttpStatusError {
                    status: status.as_u16(),
                    body,
                })
            }
        }
    }
}

impl EsWorkerPool {
    pub async fn new(
        conf: Settings,
        es_queue_receiver: BoundedReceiver<Vec<Event>>,
    ) -> std::result::Result<Self, EsError> {
        let worker_count = conf.elasticsearch.workers as usize;

        // Validate that at least one worker is configured
        if worker_count == 0 {
            return Err(EsError::RequestFailed(
                "Cannot create ES worker pool with 0 workers. This would cause deadlock as no receivers would be available for the work distribution channel.".to_string()
            ));
        }

        let mut workers = Vec::with_capacity(worker_count);

        // Create shared dead letter queue
        let dlq_config = DeadLetterQueueConfig::default();
        let dead_letter_queue = Arc::new(DeadLetterQueue::new(dlq_config));

        // Note: DLQ background tasks will be started in run() method with shutdown_notify

        // Try to load existing dead letters from disk
        if let Err(e) = dead_letter_queue.load_from_disk().await {
            warn!("Failed to load dead letters from disk: {}", e);
        }

        for i in 0..worker_count {
            let worker = EsWorker::new(i, conf.clone(), Arc::clone(&dead_letter_queue))?;
            workers.push(worker);
        }

        info!("Created ES worker pool with {} workers", worker_count);

        Ok(EsWorkerPool {
            workers,
            es_queue_receiver,
            dead_letter_queue,
            conf,
        })
    }

    /// Start a background task that periodically retries events from DLQ
    fn start_dlq_retry_task(
        dlq: Arc<DeadLetterQueue>,
        conf: Settings,
        shutdown_notify: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Create HTTP client for retry
            let client = match Client::builder()
                .pool_max_idle_per_host(5)
                .pool_idle_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create HTTP client for DLQ retry: {}", e);
                    return;
                }
            };

            let es_url = format!(
                "{}:{}/{}/_bulk",
                conf.elasticsearch.host, conf.elasticsearch.port, conf.elasticsearch.index_name
            );

            let mut retry_interval = Duration::from_secs(30);
            let max_interval = Duration::from_secs(300); // 5 min max
            let batch_size = 100;

            info!("DLQ retry task started (interval: {:?})", retry_interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(retry_interval) => {
                        // Take a batch from DLQ
                        let batch = dlq.take_batch(batch_size).await;
                        if batch.is_empty() {
                            // Reset interval when queue is empty
                            retry_interval = Duration::from_secs(30);
                            continue;
                        }

                        info!("DLQ retry: attempting to send {} events", batch.len());

                        // Convert DeadLetters back to Events
                        let events: Vec<Event> = batch.iter().map(|dl| dl.event.clone()).collect();
                        let event_count = events.len();

                        // Build bulk request body
                        let mut body = Vec::new();
                        for event in &events {
                            let index = Index::new();
                            let fields = FieldsBody::new(
                                event.message.clone(),
                                event.timestamp,
                                event.meta.pod_name.clone(),
                                event.meta.namespace.clone(),
                                event.meta.container_name.clone(),
                                event.meta.pod_id.clone(),
                            );

                            if let Ok(index_json) = serde_json::to_string(&index) {
                                body.put(index_json.as_bytes());
                                body.put_u8(b'\n');
                            }
                            if let Ok(fields_json) = serde_json::to_string(&fields) {
                                body.put(fields_json.as_bytes());
                                body.put_u8(b'\n');
                            }
                        }

                        // Send to ES
                        match timeout(
                            Duration::from_secs(30),
                            client
                                .post(&es_url)
                                .header("Content-Type", "application/x-ndjson")
                                .body(body)
                                .send()
                        ).await {
                            Ok(Ok(response)) if response.status().is_success() => {
                                info!("DLQ retry: successfully sent {} events", event_count);
                                dlq.mark_recovered(event_count).await;
                                retry_interval = Duration::from_secs(30); // Reset on success
                            }
                            Ok(Ok(response)) => {
                                warn!("DLQ retry: ES returned error status {}", response.status());
                                dlq.return_failed(batch).await;
                                retry_interval = (retry_interval * 2).min(max_interval);
                            }
                            Ok(Err(e)) => {
                                warn!("DLQ retry: request failed: {}", e);
                                dlq.return_failed(batch).await;
                                retry_interval = (retry_interval * 2).min(max_interval);
                            }
                            Err(_) => {
                                warn!("DLQ retry: request timed out");
                                dlq.return_failed(batch).await;
                                retry_interval = (retry_interval * 2).min(max_interval);
                            }
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        info!("DLQ retry task received shutdown signal");
                        break;
                    }
                }
            }

            info!("DLQ retry task shutdown complete");
        })
    }

    pub async fn run(&mut self, shutdown_notify: Arc<Notify>) -> Result<()> {
        let worker_count = self.workers.len(); // Capture worker count before draining

        // Start DLQ background tasks with shutdown coordination
        let _dlq_flush_handle = self
            .dead_letter_queue
            .start_background_tasks(shutdown_notify.clone())
            .await;

        // Start DLQ retry task
        let _dlq_retry_handle = Self::start_dlq_retry_task(
            Arc::clone(&self.dead_letter_queue),
            self.conf.clone(),
            shutdown_notify.clone(),
        );

        info!("Starting ES worker pool with {} workers", worker_count);

        // Create work distribution channel with bounded capacity to prevent memory spikes
        // Capacity = workers * 2 to allow some queueing without excessive buffering
        let channel_capacity = (worker_count * 2).max(4); // Minimum 4, 2x workers
        let (work_sender, work_receiver) = async_channel::bounded::<Vec<Event>>(channel_capacity);

        // Start all workers
        let mut worker_handles = Vec::new();

        for mut worker in self.workers.drain(..) {
            let work_receiver_clone = work_receiver.clone();
            let shutdown_notify_clone = shutdown_notify.clone();

            let handle = tokio::spawn(async move {
                worker.run(work_receiver_clone, shutdown_notify_clone).await
            });

            worker_handles.push(handle);
        }

        // Main event distribution loop
        let distribution_handle = {
            let work_sender = work_sender.clone();
            let es_queue_receiver = self.es_queue_receiver.clone();
            let shutdown_notify_clone = shutdown_notify.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        events_result = es_queue_receiver.recv() => {
                            match events_result {
                                Ok(events) => {
                                    // Try to send to workers with bounded channel
                                    match work_sender.try_send(events) {
                                        Ok(()) => {
                                            // Successfully distributed work
                                        }
                                        Err(async_channel::TrySendError::Full(events)) => {
                                            // Channel is full - workers are backpressured
                                            warn!("ES worker pool backpressured: {} workers busy, queueing batch of {} events",
                                                  worker_count, events.len());

                                            // Fall back to blocking send to maintain event ordering
                                            if let Err(e) = work_sender.send(events).await {
                                                error!("Failed to distribute work to workers after backpressure: {}", e);
                                                break;
                                            }
                                        }
                                        Err(async_channel::TrySendError::Closed(_)) => {
                                            error!("Work distribution channel closed");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("ES queue receiver error: {}", e);
                                    break;
                                }
                            }
                        }
                        _ = shutdown_notify_clone.notified() => {
                            info!("ES worker pool distributor received shutdown signal");
                            break;
                        }
                    }
                }
            })
        };

        // Wait for shutdown
        shutdown_notify.notified().await;
        info!("ES worker pool shutting down...");

        // Wait for distribution to finish
        if let Err(e) = distribution_handle.await {
            error!("Distribution handle error: {}", e);
        }

        // Wait for all workers to finish
        for handle in worker_handles {
            if let Err(e) = handle.await {
                error!("Worker handle error: {}", e);
            }
        }

        info!("ES worker pool shutdown complete");
        Ok(())
    }
}

impl EsWorker {
    fn new(
        id: usize,
        conf: Settings,
        dead_letter_queue: Arc<DeadLetterQueue>,
    ) -> std::result::Result<Self, EsError> {
        // Create HTTP client with connection pooling
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| EsError::RequestFailed(format!("Failed to create HTTP client: {}", e)))?;

        let retry_config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        };

        let retry_manager = RetryManager::new(retry_config);

        // Create circuit breaker for this worker
        let circuit_breaker = create_es_circuit_breaker(format!("es_worker_{}", id));

        Ok(EsWorker {
            id,
            http_client: Arc::new(ReqwestHttpClient { client }),
            conf,
            retry_manager,
            circuit_breaker,
            dead_letter_queue,
            network_stats: NetworkStats::default(),
        })
    }

    // DI-friendly constructor for tests or alternative clients
    #[allow(dead_code)]
    fn new_with_client(
        id: usize,
        conf: Settings,
        dead_letter_queue: Arc<DeadLetterQueue>,
        http_client: Arc<dyn HttpClient>,
    ) -> Self {
        let retry_config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        };

        let retry_manager = RetryManager::new(retry_config);
        let circuit_breaker = create_es_circuit_breaker(format!("es_worker_{}", id));

        EsWorker {
            id,
            http_client,
            conf,
            retry_manager,
            circuit_breaker,
            dead_letter_queue,
            network_stats: NetworkStats::default(),
        }
    }

    async fn run(
        &mut self,
        work_receiver: async_channel::Receiver<Vec<Event>>,
        shutdown_notify: Arc<Notify>,
    ) -> Result<()> {
        info!("ES Worker {} starting", self.id);

        loop {
            tokio::select! {
                events_result = work_receiver.recv() => {
                    match events_result {
                        Ok(events) => {
                            if let Err(e) = self.process_events(events).await {
                                error!("Worker {} failed to process events: {}", self.id, e);
                                // Continue processing other events instead of failing completely
                            }
                        }
                        Err(e) => {
                            warn!("Worker {} receiver error: {}", self.id, e);
                            break;
                        }
                    }
                }
                _ = shutdown_notify.notified() => {
                    info!("ES Worker {} received shutdown signal", self.id);
                    break;
                }
            }
        }

        info!("ES Worker {} shutdown complete", self.id);
        Ok(())
    }

    async fn process_events(&mut self, events: Vec<Event>) -> std::result::Result<(), EsError> {
        let event_count = events.len();
        debug!("Worker {} processing {} events", self.id, event_count);

        // Check circuit breaker state before processing
        let circuit_state = self.circuit_breaker.get_state().await;
        debug!(
            "Worker {} circuit breaker state: {:?}",
            self.id, circuit_state
        );

        // Clone events for potential dead letter queue usage
        let events_backup = events.clone();
        let body = self.make_body(events)?;
        let url = self.build_es_url();
        let http_client = self.http_client.clone();
        let worker_id = self.id;

        // Get adaptive timeout based on network conditions
        let adaptive_timeout = self.network_stats.adaptive_timeout();
        debug!(
            "Worker {} using adaptive timeout: {}ms (avg_latency={}ms, consecutive_failures={})",
            self.id,
            adaptive_timeout.as_millis(),
            self.network_stats.avg_latency.as_millis(),
            self.network_stats.consecutive_failures
        );

        // Check if network is degraded and log warning
        if self.network_stats.is_network_degraded() {
            warn!(
                "Worker {} operating with degraded network conditions: latency={}ms, failures={}",
                self.id,
                self.network_stats.avg_latency.as_millis(),
                self.network_stats.consecutive_failures
            );
        }

        // Execute request through circuit breaker and retry mechanism with network monitoring
        let start_time = std::time::Instant::now();
        let result = self
            .circuit_breaker
            .call(|| {
                let retry_manager = self.retry_manager.clone();
                let http_client = http_client.clone();
                let url = url.clone();
                let body = body.clone();
                let timeout = adaptive_timeout;

                async move {
                    retry_manager
                        .execute_with_retry(|| {
                            let http_client = http_client.clone();
                            let url = url.clone();
                            let body = body.clone();
                            let timeout_duration = timeout;

                            async move {
                                // Try with adaptive timeout first
                                match http_client
                                    .post_bytes_with_timeout(&url, body.clone(), timeout_duration)
                                    .await
                                {
                                    Ok((response, latency)) => Ok((response, Some(latency))),
                                    Err(_e) => {
                                        // Fallback to regular method for compatibility
                                        http_client.post_bytes(&url, body).await.map(|r| (r, None))
                                    }
                                }
                            }
                        })
                        .await
                }
            })
            .await;

        match result {
            Ok((response_body, latency_opt)) => {
                // Record network success and latency
                let actual_latency = latency_opt.unwrap_or_else(|| start_time.elapsed());
                self.network_stats.record_success(actual_latency);

                debug!(
                    "Worker {} successfully sent {} events in {}ms. Response: {}",
                    worker_id,
                    event_count,
                    actual_latency.as_millis(),
                    if response_body.len() > 100 {
                        &response_body[..100]
                    } else {
                        &response_body
                    }
                );
                Ok(())
            }
            Err(CircuitBreakerError::CircuitOpen) => {
                // Record network failure
                self.network_stats.record_failure();

                let failure_reason = "Circuit breaker is open".to_string();
                warn!(
                    "Worker {} skipped {} events due to open circuit breaker (network_degraded={})",
                    worker_id,
                    event_count,
                    self.network_stats.is_network_degraded()
                );

                // Add failed events to dead letter queue
                for event in events_backup {
                    self.dead_letter_queue
                        .add_failed_event(event, failure_reason.clone())
                        .await;
                }

                Err(EsError::RequestFailed(failure_reason))
            }
            Err(CircuitBreakerError::Operation(e)) => {
                // Record network failure
                self.network_stats.record_failure();

                let failure_reason = format!("ES operation failed: {}", e);
                error!(
                    "Worker {} failed to send {} events: {} (avg_latency={}ms, consecutive_failures={})",
                    worker_id,
                    event_count,
                    failure_reason,
                    self.network_stats.avg_latency.as_millis(),
                    self.network_stats.consecutive_failures
                );

                // Add failed events to dead letter queue
                for event in events_backup {
                    self.dead_letter_queue
                        .add_failed_event(event, failure_reason.clone())
                        .await;
                }

                Err(e)
            }
        }
    }

    fn make_body(&self, events: Vec<Event>) -> std::result::Result<Vec<u8>, EsError> {
        let mut body: Vec<u8> = Vec::new();

        for event in events {
            // Add index action
            let index = Index::new();
            serde_json::to_writer(&mut body, &index)
                .map_err(|e| EsError::SerializationFailed(format!("Index serialization: {}", e)))?;
            body.put_slice(b"\n");

            // Add document
            let fields_body = FieldsBody::new(
                event.message,
                event.timestamp,
                event.meta.pod_name,
                event.meta.namespace,
                event.meta.container_name,
                event.meta.pod_id,
            );

            serde_json::to_writer(&mut body, &fields_body).map_err(|e| {
                EsError::SerializationFailed(format!("Document serialization: {}", e))
            })?;
            body.put_slice(b"\n");
        }

        debug!("Worker {} created body with {} bytes", self.id, body.len());
        Ok(body)
    }

    fn build_es_url(&self) -> String {
        build_es_url_from_conf(&self.conf)
    }
}

// Helper that does not require constructing a worker/client; useful for tests
pub(crate) fn build_es_url_from_conf(conf: &Settings) -> String {
    format!(
        "{}:{}/{}-{}/_bulk",
        conf.elasticsearch.host,
        conf.elasticsearch.port,
        conf.elasticsearch.index_name,
        Utc::now().format("%Y.%m.%d")
    )
}

// Lightweight helper for testing worker sizing logic without constructing clients
#[cfg(test)]
pub(crate) fn planned_worker_count(conf: &Settings) -> usize {
    conf.elasticsearch.workers as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    // Tests would use Event and Meta when more comprehensive tests are added
    use crate::domain::event::{Event, Meta};

    #[test]
    fn test_planned_worker_count() {
        let conf = create_test_config();
        assert_eq!(
            planned_worker_count(&conf),
            conf.elasticsearch.workers as usize
        );
    }

    #[test]
    fn test_url_building() {
        let conf = create_test_config();
        let url = build_es_url_from_conf(&conf);
        assert!(url.contains("http://127.0.0.1:9200"));
        assert!(url.contains("logfowd"));
        assert!(url.contains("/_bulk"));
    }

    struct NoopClient;
    #[async_trait]
    impl HttpClient for NoopClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            Ok("ok".to_string())
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Ok(("ok".to_string(), Duration::from_millis(50)))
        }
    }

    #[tokio::test]
    async fn test_worker_process_events_with_di() {
        use crate::infrastructure::elasticsearch::dead_letter_queue::{
            DeadLetterQueue, DeadLetterQueueConfig,
        };
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(NoopClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("line".to_string(), meta)];

        let res = worker.process_events(events).await;
        assert!(res.is_ok());
    }

    struct FailingClient;
    #[async_trait]
    impl HttpClient for FailingClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            Err(EsError::RequestFailed("boom".to_string()))
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Err(EsError::RequestFailed("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn test_worker_sends_failed_events_to_dlq() {
        use crate::infrastructure::elasticsearch::dead_letter_queue::{
            DeadLetter, DeadLetterQueue, DeadLetterQueueConfig,
        };
        use tempfile::NamedTempFile;

        let conf = create_test_config();

        // Prepare DLQ with persistence to a temp file
        let tmp = NamedTempFile::new().unwrap();
        let dlq_path = tmp.path().to_string_lossy().to_string();
        let dlq = DeadLetterQueue::new(DeadLetterQueueConfig {
            max_queue_size: 100,
            persistence_file: Some(dlq_path.clone()),
            flush_interval: std::time::Duration::from_secs(60),
            max_retry_count: 5,
        });

        let http = std::sync::Arc::new(FailingClient);
        let mut worker = EsWorker::new_with_client(0, conf, std::sync::Arc::new(dlq), http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![
            Event::new("e1".to_string(), meta.clone()),
            Event::new("e2".to_string(), meta),
        ];

        let res = worker.process_events(events).await;
        assert!(res.is_err(), "Expected ES failure to bubble up");

        // Flush DLQ and verify persisted items
        let dlq_clone = worker.dead_letter_queue.clone();
        dlq_clone.flush_to_disk().await.unwrap();

        let contents = std::fs::read_to_string(&dlq_path).unwrap();
        let stored: Vec<DeadLetter> = serde_json::from_str(&contents).unwrap();
        assert!(stored.len() >= 2);
    }

    fn create_test_config() -> Settings {
        use crate::config::settings::{ChannelsConfig, ElasticsearchConfig};

        Settings {
            log_path: "/test".to_string(),
            state_file_path: Some("/tmp/test.json".to_string()),
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: Some(50),
            channels: Some(ChannelsConfig {
                watcher_buffer_size: Some(1000),
                es_buffer_size: Some(1000),
                backpressure_threshold: Some(0.8),
                backpressure_min_delay_ms: None,
                backpressure_max_delay_ms: None,
                notify_buffer_warning_threshold: None,
                notify_buffer_max_size: None,
                notify_drop_on_overflow: None,
                notify_filesystem_buffer_warning_threshold: None,
                notify_filesystem_buffer_size: None,
            }),
            metrics: None,
            logging: None,
            elasticsearch: ElasticsearchConfig {
                host: "http://127.0.0.1".to_string(),
                port: 9200,
                index_name: "logfowd".to_string(),
                flush_interval: 1000,
                bulk_size: 100,
                workers: 2,
            },
        }
    }

    // Test different network failure scenarios
    struct TimeoutClient;
    #[async_trait]
    impl HttpClient for TimeoutClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            Err(EsError::Timeout)
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Err(EsError::Timeout)
        }
    }

    struct DnsFailureClient;
    #[async_trait]
    impl HttpClient for DnsFailureClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            Err(EsError::DnsResolutionFailed(
                "elasticsearch.example.com: dns lookup failed".to_string(),
            ))
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Err(EsError::DnsResolutionFailed(
                "elasticsearch.example.com: dns lookup failed".to_string(),
            ))
        }
    }

    struct RateLimitedClient;
    #[async_trait]
    impl HttpClient for RateLimitedClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            Err(EsError::RateLimited {
                retry_after: Some(Duration::from_secs(10)),
            })
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Err(EsError::RateLimited {
                retry_after: Some(Duration::from_secs(10)),
            })
        }
    }

    struct SlowClient;
    #[async_trait]
    impl HttpClient for SlowClient {
        async fn post_bytes(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> std::result::Result<String, EsError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok("slow response".to_string())
        }

        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(("slow response".to_string(), Duration::from_millis(100)))
        }
    }

    #[tokio::test]
    async fn test_network_timeout_handling() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(TimeoutClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("timeout test".to_string(), meta)];

        let res = worker.process_events(events).await;
        assert!(res.is_err(), "Expected timeout error");

        // Check that failure was recorded
        assert!(worker.network_stats.consecutive_failures > 0);
        assert!(worker.network_stats.failure_count > 0);
    }

    #[tokio::test]
    async fn test_dns_failure_handling() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(DnsFailureClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("dns test".to_string(), meta)];

        let res = worker.process_events(events).await;
        assert!(res.is_err(), "Expected DNS error");

        // Verify failure was recorded
        assert!(worker.network_stats.consecutive_failures > 0);
    }

    #[tokio::test]
    async fn test_rate_limiting_handling() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(RateLimitedClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("rate limit test".to_string(), meta)];

        let res = worker.process_events(events).await;
        assert!(res.is_err(), "Expected rate limit error");

        // Verify failure was recorded and network is considered degraded
        assert!(worker.network_stats.consecutive_failures > 0);
    }

    #[tokio::test]
    async fn test_adaptive_timeout_success() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(SlowClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("slow test".to_string(), meta)];

        // Process events multiple times to build up latency statistics
        for _ in 0..3 {
            let events_clone = events.clone();
            let res = worker.process_events(events_clone).await;
            assert!(res.is_ok(), "Expected slow client to succeed");
        }

        // Verify that success was recorded and latency stats updated
        assert!(worker.network_stats.success_count >= 3);
        assert!(worker.network_stats.consecutive_failures == 0);
        assert!(worker.network_stats.avg_latency.as_millis() > 0);
        assert!(worker.network_stats.last_success.is_some());
    }

    #[tokio::test]
    async fn test_network_degradation_detection() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));
        let http = Arc::new(FailingClient);
        let mut worker = EsWorker::new_with_client(0, conf, dlq, http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("degradation test".to_string(), meta)];

        // Process multiple failing requests to trigger degradation
        for _ in 0..5 {
            let events_clone = events.clone();
            let _res = worker.process_events(events_clone).await;
        }

        // Verify network is considered degraded after multiple failures
        assert!(
            worker.network_stats.is_network_degraded(),
            "Network should be degraded after multiple failures"
        );
        assert!(worker.network_stats.consecutive_failures > 3);

        // Check that adaptive timeout increases with failures
        let timeout = worker.network_stats.adaptive_timeout();
        assert!(
            timeout > Duration::from_secs(30),
            "Timeout should increase with consecutive failures"
        );
    }

    #[tokio::test]
    async fn test_network_stats_initialization() {
        let stats = NetworkStats::default();

        assert_eq!(stats.success_count, 0);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.consecutive_failures, 0);
        assert!(stats.last_success.is_none());
        assert_eq!(stats.avg_latency, Duration::from_millis(100)); // Default latency
        assert!(!stats.is_network_degraded()); // Should not be degraded initially

        let timeout = stats.adaptive_timeout();
        assert_eq!(timeout, Duration::from_secs(30)); // Default timeout
    }

    #[tokio::test]
    async fn test_network_recovery_after_success() {
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig::default()));

        // Start with failing client
        let http = Arc::new(FailingClient);
        let mut worker = EsWorker::new_with_client(0, conf.clone(), dlq.clone(), http);

        let meta = Meta {
            namespace: "ns".to_string(),
            pod_name: "pod".to_string(),
            container_name: "cont".to_string(),
            pod_id: "id".to_string(),
        };
        let events = vec![Event::new("recovery test".to_string(), meta.clone())];

        // Generate some failures
        for _ in 0..3 {
            let events_clone = events.clone();
            let _res = worker.process_events(events_clone).await;
        }

        assert!(worker.network_stats.consecutive_failures > 0);

        // Switch to successful client
        worker.http_client = Arc::new(NoopClient);

        // Process successful request
        let events_success = vec![Event::new("success test".to_string(), meta)];
        let res = worker.process_events(events_success).await;
        assert!(
            res.is_ok(),
            "Expected success after switching to NoopClient"
        );

        // Verify recovery
        assert_eq!(
            worker.network_stats.consecutive_failures, 0,
            "Consecutive failures should reset on success"
        );
        assert!(worker.network_stats.success_count > 0);
        assert!(worker.network_stats.last_success.is_some());
    }

    #[tokio::test]
    async fn test_es_worker_pool_zero_workers_validation() {
        use crate::transport::channels::create_bounded_channel;

        // Create a test config with 0 workers
        let mut conf = create_test_config();
        conf.elasticsearch.workers = 0;

        // Create a dummy channel for the test
        let es_queue_channel = create_bounded_channel(10, None, None, None, None);
        let es_queue_receiver = es_queue_channel.receiver();

        // Attempt to create the worker pool - should fail
        let result = EsWorkerPool::new(conf, es_queue_receiver).await;

        assert!(
            result.is_err(),
            "Expected EsWorkerPool::new to fail with 0 workers"
        );

        let error = result.err().unwrap();
        match error {
            EsError::RequestFailed(msg) => {
                assert!(msg.contains("Cannot create ES worker pool with 0 workers"));
                assert!(msg.contains("would cause deadlock"));
            }
            _ => panic!("Expected RequestFailed error, got: {:?}", error),
        }
    }

    #[tokio::test]
    async fn test_es_worker_pool_valid_workers() {
        use crate::transport::channels::create_bounded_channel;

        // Create a test config with 1 worker
        let conf = create_test_config();
        assert!(
            conf.elasticsearch.workers > 0,
            "Test config should have at least 1 worker"
        );

        // Create a dummy channel for the test
        let es_queue_channel = create_bounded_channel(10, None, None, None, None);
        let es_queue_receiver = es_queue_channel.receiver();

        // Attempt to create the worker pool - should succeed
        let result = EsWorkerPool::new(conf, es_queue_receiver).await;

        assert!(
            result.is_ok(),
            "Expected EsWorkerPool::new to succeed with valid worker count"
        );
    }

    #[test]
    fn test_planned_worker_count_zero() {
        let mut conf = create_test_config();
        conf.elasticsearch.workers = 0;

        // The planned_worker_count function should return 0
        assert_eq!(planned_worker_count(&conf), 0);

        // But this configuration should fail validation when used
        let validation_result = conf.validate();
        assert!(
            validation_result.is_err(),
            "Config with 0 workers should fail validation"
        );
    }
}
