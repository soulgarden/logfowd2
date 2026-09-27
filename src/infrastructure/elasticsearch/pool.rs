use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BufMut;
use chrono::Utc;
use reqwest::Client;
use tokio::sync::{Notify, RwLock, watch};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::Settings;
use crate::domain::event::Event;
use crate::domain::state::AppState;
use crate::error::{EsError, Result};
use crate::infrastructure::elasticsearch::bulk::failed_items;
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
    app_state: Option<Arc<RwLock<AppState>>>,
}

struct EsWorker {
    id: usize,
    http_client: Arc<dyn HttpClient>,
    conf: Settings,
    retry_manager: RetryManager,
    circuit_breaker: CircuitBreaker,
    dead_letter_queue: Arc<DeadLetterQueue>,
    network_stats: NetworkStats,
    app_state: Option<Arc<RwLock<AppState>>>,
}

async fn acknowledge_events(app_state: Option<&Arc<RwLock<AppState>>>, events: &[Event]) {
    let Some(app_state) = app_state else {
        return;
    };
    let mut state = app_state.write().await;
    for event in events {
        if let Some(source) = &event.source {
            state.mark_source_delivered(source);
        }
    }
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
            .header("Content-Type", "application/x-ndjson")
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
                let response_body = response.text().await.map_err(|e| {
                    EsError::RequestFailed(format!("Failed to read bulk response: {e}"))
                })?;

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
            app_state: None,
        })
    }

    pub fn with_app_state(mut self, app_state: Arc<RwLock<AppState>>) -> Self {
        for worker in &mut self.workers {
            worker.app_state = Some(app_state.clone());
        }
        self.app_state = Some(app_state);
        self
    }

    /// Start a background task that periodically retries events from DLQ
    fn start_dlq_retry_task(
        dlq: Arc<DeadLetterQueue>,
        conf: Settings,
        mut shutdown: watch::Receiver<bool>,
        app_state: Option<Arc<RwLock<AppState>>>,
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

            let mut retry_interval = Duration::from_secs(30);
            let max_interval = Duration::from_secs(300); // 5 min max
            let batch_size = 100;

            info!("DLQ retry task started (interval: {:?})", retry_interval);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(retry_interval) => {
                        if let Err(e) = dlq.flush_to_disk().await {
                            warn!("DLQ retry: could not persist queue before retry: {e}");
                        }
                        let retry_guard = dlq.lock_retry().await;
                        // Take a batch from DLQ
                        let batch = dlq.take_batch(batch_size).await;
                        if batch.is_empty() {
                            drop(retry_guard);
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
                            let index = Index::for_event(event);
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
                        let es_url = build_es_url_from_conf(&conf);
                        match timeout(
                            Duration::from_secs(30),
                            client
                                .post(&es_url)
                                .header("Content-Type", "application/x-ndjson")
                                .body(body)
                                .send()
                        ).await {
                            Ok(Ok(response)) if response.status().is_success() => {
                                let outcome = response.text().await
                                    .map_err(|e| EsError::RequestFailed(format!("Failed to read bulk response: {e}")))
                                    .and_then(|body| failed_items(&body, event_count));
                                match outcome {
                                    Ok(failures) => {
                                        let mut failure_reasons: HashMap<usize, String> = failures.into_iter().collect();
                                        let mut to_retry = Vec::new();
                                        let mut succeeded = Vec::new();
                                        for (index, letter) in batch.into_iter().enumerate() {
                                            if failure_reasons.remove(&index).is_some() {
                                                to_retry.push(letter);
                                            } else {
                                                succeeded.push(letter.event);
                                            }
                                        }
                                        acknowledge_events(app_state.as_ref(), &succeeded).await;
                                        dlq.mark_recovered(succeeded.len()).await;
                                        if to_retry.is_empty() {
                                            info!("DLQ retry: recovered {} events", succeeded.len());
                                            retry_interval = Duration::from_secs(30);
                                        } else {
                                            warn!("DLQ retry: {} of {} items failed", to_retry.len(), event_count);
                                            dlq.return_failed(to_retry).await;
                                            retry_interval = (retry_interval * 2).min(max_interval);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("DLQ retry: {e}");
                                        dlq.return_failed(batch).await;
                                        retry_interval = (retry_interval * 2).min(max_interval);
                                    }
                                }
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

                        drop(retry_guard);
                        if let Err(e) = dlq.flush_to_disk().await {
                            warn!("DLQ retry: could not persist queue after retry: {e}");
                        }
                    }
                    _ = shutdown.changed() => {
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
        let (background_stop, background_shutdown) = watch::channel(false);

        // Start DLQ background tasks with shutdown coordination
        let dlq_flush_handle = self
            .dead_letter_queue
            .start_background_tasks(background_shutdown.clone())
            .await;

        // Start DLQ retry task
        let dlq_retry_handle = Self::start_dlq_retry_task(
            Arc::clone(&self.dead_letter_queue),
            self.conf.clone(),
            background_shutdown,
            self.app_state.clone(),
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
            let handle = tokio::spawn(async move { worker.run(work_receiver_clone).await });

            worker_handles.push(handle);
        }

        // Main event distribution loop
        let distribution_handle = {
            let work_sender = work_sender.clone();
            let es_queue_receiver = self.es_queue_receiver.clone();
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
                    }
                }
            })
        };

        let mut distribution_handle = distribution_handle;
        let distribution_result = tokio::select! {
            result = &mut distribution_handle => result,
            _ = shutdown_notify.notified() => {
                self.es_queue_receiver.close();
                distribution_handle.await
            }
        };
        if let Err(e) = distribution_result {
            error!("Distribution handle error: {}", e);
        }
        drop(work_sender);

        for handle in worker_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("Worker error: {}", e),
                Err(e) => error!("Worker handle error: {}", e),
            }
        }

        let _ = background_stop.send(true);
        if let Err(e) = dlq_retry_handle.await {
            error!("DLQ retry handle error: {}", e);
        }
        if let Err(e) = dlq_flush_handle.await {
            error!("DLQ flush handle error: {}", e);
        }
        if let Err(e) = self.dead_letter_queue.flush_to_disk().await {
            error!("Final DLQ flush failed: {}", e);
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
            app_state: None,
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
            app_state: None,
        }
    }

    async fn run(&mut self, work_receiver: async_channel::Receiver<Vec<Event>>) -> Result<()> {
        info!("ES Worker {} starting", self.id);

        while let Ok(events) = work_receiver.recv().await {
            if let Err(e) = self.process_events(events).await {
                error!("Worker {} failed to process events: {}", self.id, e);
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
                                http_client
                                    .post_bytes_with_timeout(&url, body, timeout_duration)
                                    .await
                                    .map(|(response, latency)| (response, Some(latency)))
                            }
                        })
                        .await
                }
            })
            .await;

        match result {
            Ok((response_body, latency_opt)) => {
                let failures = match failed_items(&response_body, event_count) {
                    Ok(failures) => failures,
                    Err(e) => {
                        self.network_stats.record_failure();
                        for event in events_backup {
                            self.dead_letter_queue
                                .add_failed_event(event, e.to_string())
                                .await;
                        }
                        return Err(e);
                    }
                };

                let failure_count = failures.len();
                let mut failure_reasons: HashMap<usize, String> = failures.into_iter().collect();
                let mut succeeded = Vec::with_capacity(event_count - failure_count);
                for (index, event) in events_backup.into_iter().enumerate() {
                    if let Some(reason) = failure_reasons.remove(&index) {
                        self.dead_letter_queue.add_failed_event(event, reason).await;
                    } else {
                        succeeded.push(event);
                    }
                }
                acknowledge_events(self.app_state.as_ref(), &succeeded).await;

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
                if failure_count == 0 {
                    Ok(())
                } else {
                    Err(EsError::RequestFailed(format!(
                        "{failure_count} of {event_count} bulk items failed"
                    )))
                }
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
            let index = Index::for_event(&event);
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
    use crate::domain::event::{Event, Meta, SourcePosition};

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
    const SUCCESSFUL_BULK_RESPONSE: &str = r#"{"errors":false,"items":[{"index":{"status":201}}]}"#;
    #[async_trait]
    impl HttpClient for NoopClient {
        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Ok((
                SUCCESSFUL_BULK_RESPONSE.to_string(),
                Duration::from_millis(50),
            ))
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

    struct CountingClient(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl HttpClient for CountingClient {
        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            let count = body.iter().filter(|byte| **byte == b'\n').count() / 2;
            self.0.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
            let items: Vec<_> = (0..count)
                .map(|_| serde_json::json!({"index": {"status": 201}}))
                .collect();
            Ok((
                serde_json::json!({"errors": false, "items": items}).to_string(),
                Duration::from_millis(1),
            ))
        }
    }

    #[tokio::test]
    async fn test_pool_drains_queued_batches_after_input_closes() {
        use crate::transport::channels::create_bounded_channel;
        let channel = create_bounded_channel(4, None, None, None, None);
        let mut sender = channel.sender();
        let receiver = channel.receiver();
        drop(channel);
        let conf = create_test_config();
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig {
            persistence_file: None,
            ..DeadLetterQueueConfig::default()
        }));
        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = EsWorker::new_with_client(
            0,
            conf.clone(),
            dlq.clone(),
            Arc::new(CountingClient(delivered.clone())),
        );
        let mut pool = EsWorkerPool {
            workers: vec![worker],
            es_queue_receiver: receiver,
            dead_letter_queue: dlq,
            conf,
            app_state: None,
        };

        for n in 0..3 {
            sender
                .send(vec![bulk_test_event(&format!("batch {n}"))])
                .await
                .unwrap();
        }
        drop(sender);
        let result =
            tokio::time::timeout(Duration::from_secs(2), pool.run(Arc::new(Notify::new())))
                .await
                .expect("pool must exit after draining a closed input");
        assert!(result.is_ok());
        assert_eq!(delivered.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_sender_and_pool_drain_every_event_on_shutdown() {
        use crate::sender::Sender;
        use crate::transport::channels::create_bounded_channel;

        let watcher_channel = create_bounded_channel(16, None, None, None, None);
        let mut input = watcher_channel.sender();
        let sender_input = watcher_channel.receiver();
        drop(watcher_channel);
        let es_channel = create_bounded_channel(2, None, None, None, None);
        let sender_output = es_channel.sender();
        let pool_input = es_channel.receiver();
        drop(es_channel);

        let mut conf = create_test_config();
        conf.elasticsearch.bulk_size = 3;
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig {
            persistence_file: None,
            ..DeadLetterQueueConfig::default()
        }));
        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = EsWorker::new_with_client(
            0,
            conf.clone(),
            dlq.clone(),
            Arc::new(CountingClient(delivered.clone())),
        );
        let mut pool = EsWorkerPool {
            workers: vec![worker],
            es_queue_receiver: pool_input,
            dead_letter_queue: dlq,
            conf: conf.clone(),
            app_state: None,
        };
        let mut sender = Sender::new(conf, sender_input, sender_output);
        let sender_task = tokio::spawn(async move { sender.run(Arc::new(Notify::new())).await });
        let pool_task = tokio::spawn(async move { pool.run(Arc::new(Notify::new())).await });

        for n in 0..7 {
            input
                .send(bulk_test_event(&format!("event {n}")))
                .await
                .unwrap();
        }
        drop(input);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), sender_task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), pool_task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert_eq!(delivered.load(std::sync::atomic::Ordering::SeqCst), 7);
    }

    struct BulkResponseClient(&'static str);

    #[async_trait]
    impl HttpClient for BulkResponseClient {
        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            Ok((self.0.to_string(), Duration::from_millis(1)))
        }
    }

    fn bulk_test_event(message: &str) -> Event {
        Event::new(message.to_string(), Meta::default())
    }

    #[tokio::test]
    async fn test_bulk_200_with_failed_item_goes_to_dlq() {
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig {
            persistence_file: None,
            ..DeadLetterQueueConfig::default()
        }));
        let response = r#"{"errors":true,"items":[{"index":{"status":429,"error":{"type":"es_rejected_execution_exception"}}}]}"#;
        let mut worker = EsWorker::new_with_client(
            0,
            create_test_config(),
            dlq.clone(),
            Arc::new(BulkResponseClient(response)),
        );

        assert!(
            worker
                .process_events(vec![bulk_test_event("rejected")])
                .await
                .is_err()
        );
        let failed = dlq.take_batch(10).await;
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].event.message, "rejected");
    }

    #[tokio::test]
    async fn test_bulk_partial_success_queues_only_failed_item() {
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig {
            persistence_file: None,
            ..DeadLetterQueueConfig::default()
        }));
        let response = r#"{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":429,"error":{"type":"es_rejected_execution_exception"}}}]}"#;
        let mut worker = EsWorker::new_with_client(
            0,
            create_test_config(),
            dlq.clone(),
            Arc::new(BulkResponseClient(response)),
        );

        let events = vec![bulk_test_event("accepted"), bulk_test_event("rejected")];
        assert!(worker.process_events(events).await.is_err());
        let failed = dlq.take_batch(10).await;
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].event.message, "rejected");
    }

    #[tokio::test]
    async fn test_bulk_partial_success_commits_only_acknowledged_prefix() {
        let path = "/var/log/pods/test.log";
        let mut state = AppState::new();
        state.add_file(path.to_string(), 42, 20, 0);
        state.update_file_position(path.to_string(), 20);
        state.register_pending(path, 42, 10);
        state.register_pending(path, 42, 20);
        let generation = state.file_generation(path).unwrap();
        let app_state = Arc::new(RwLock::new(state));
        let dlq = Arc::new(DeadLetterQueue::new(DeadLetterQueueConfig {
            persistence_file: None,
            ..DeadLetterQueueConfig::default()
        }));
        let response = r#"{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":429,"error":{"type":"es_rejected_execution_exception"}}}]}"#;
        let mut worker = EsWorker::new_with_client(
            0,
            create_test_config(),
            dlq.clone(),
            Arc::new(BulkResponseClient(response)),
        );
        worker.app_state = Some(app_state.clone());

        let events = [10, 20]
            .into_iter()
            .map(|end| {
                Event::from_file(
                    format!("line {end}"),
                    Meta::default(),
                    SourcePosition {
                        path: path.to_string(),
                        inode: 42,
                        end,
                        generation,
                    },
                )
            })
            .collect();
        assert!(worker.process_events(events).await.is_err());
        assert_eq!(
            app_state
                .read()
                .await
                .clone_for_save()
                .get_file_position(path),
            Some(10)
        );
        assert_eq!(dlq.take_batch(10).await.len(), 1);
    }

    struct FailingClient;
    #[async_trait]
    impl HttpClient for FailingClient {
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
        async fn post_bytes_with_timeout(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _timeout: Duration,
        ) -> std::result::Result<(String, Duration), EsError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok((
                SUCCESSFUL_BULK_RESPONSE.to_string(),
                Duration::from_millis(100),
            ))
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
