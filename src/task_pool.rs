use log::{debug, error, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::transport::channels::BoundedSender;
use crate::domain::event::Event;
use crate::domain::file::FileTracker;
use crate::domain::state::AppState;

/// Smart TaskPool with dynamic scaling and memory optimization
/// 
/// This component is designed for future integration with the Watcher to enable
/// parallel file reading, which would significantly improve performance when 
/// processing many log files concurrently. The configuration field 
/// `max_concurrent_file_readers` is already validated in config and can control
/// the pool size (2-10 workers with dynamic scaling).
/// 
/// Benefits when integrated:
/// - Parallel processing of multiple log files
/// - Dynamic worker scaling based on load
/// - Memory optimization through bounded concurrency
/// - Automatic idle worker timeout (30s) to free resources
/// 
/// TODO: Integrate into Watcher for parallel file processing
// Kept for future parallel file processing optimization
#[allow(dead_code)]
pub struct SmartTaskPool {
    min_workers: usize,
    max_workers: usize,
    active_workers: Arc<AtomicUsize>,
    idle_timeout: Duration,
    semaphore: Arc<Semaphore>,
    join_set: JoinSet<Vec<Event>>,
    last_activity: Arc<RwLock<Instant>>,
}

#[allow(dead_code)]
impl SmartTaskPool {
    /// Create new SmartTaskPool with dynamic scaling
    ///
    /// * `min_workers` - Minimum workers to keep alive (baseline memory)
    /// * `max_workers` - Maximum concurrent workers allowed
    /// * `idle_timeout` - How long to wait before scaling down idle workers
    pub fn new(min_workers: usize, max_workers: usize, idle_timeout: Duration) -> Self {
        Self {
            min_workers,
            max_workers,
            active_workers: Arc::new(AtomicUsize::new(0)),
            idle_timeout,
            semaphore: Arc::new(Semaphore::new(max_workers)),
            join_set: JoinSet::new(),
            last_activity: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create with production-optimized defaults
    pub fn production_optimized() -> Self {
        Self::new(
            2,                       // Min 2 workers (low baseline memory)
            10,                      // Max 10 workers (down from 50)
            Duration::from_secs(30), // Scale down after 30s idle
        )
    }

    /// Spawn a file reading task with dynamic worker management
    pub async fn spawn_file_read_task(
        &mut self,
        mut file_tracker: FileTracker,
        app_state: Arc<RwLock<AppState>>,
        path: String,
    ) {
        let semaphore = Arc::clone(&self.semaphore);
        let active_workers = Arc::clone(&self.active_workers);
        let last_activity = Arc::clone(&self.last_activity);

        self.join_set.spawn(async move {
            // Acquire permit and track active worker
            let _permit = semaphore.acquire().await.expect("Semaphore closed");
            let worker_count = active_workers.fetch_add(1, Ordering::Relaxed) + 1;

            debug!(
                "Starting dynamic file read task for {} (worker {})",
                path, worker_count
            );

            // Update activity timestamp
            {
                let mut activity = last_activity.write().await;
                *activity = Instant::now();
            }

            let result = {
                let mut state = app_state.write().await;
                match file_tracker.read_new_lines(&mut state).await {
                    Ok(events) => {
                        debug!(
                            "Read {} events from {} (worker {} completing)",
                            events.len(),
                            path,
                            worker_count
                        );
                        events
                    }
                    Err(e) => {
                        error!(
                            "Failed to read from {} (worker {}): {}",
                            path, worker_count, e
                        );
                        Vec::new()
                    }
                }
            };

            // Decrement active worker count
            active_workers.fetch_sub(1, Ordering::Relaxed);
            result
        });
    }

    /// Spawn a file change check task with dynamic worker management
    pub async fn spawn_file_change_task(
        &mut self,
        mut file_tracker: FileTracker,
        app_state: Arc<RwLock<AppState>>,
        path: String,
    ) {
        let semaphore = Arc::clone(&self.semaphore);
        let active_workers = Arc::clone(&self.active_workers);
        let last_activity = Arc::clone(&self.last_activity);

        self.join_set.spawn(async move {
            let _permit = semaphore.acquire().await.expect("Semaphore closed");
            let worker_count = active_workers.fetch_add(1, Ordering::Relaxed) + 1;

            debug!("Starting dynamic file change check for {} (worker {})", path, worker_count);

            // Update activity timestamp
            {
                let mut activity = last_activity.write().await;
                *activity = Instant::now();
            }

            let result = {
                let mut state = app_state.write().await;
                match file_tracker.check_file_changes(&mut state).await {
                    Ok((changed, rotation_events)) => {
                        if changed {
                            debug!("File {} changed, reading new content (worker {})", path, worker_count);
                            // Start with rotation events, then read additional content
                            let mut all_events = rotation_events;
                            match file_tracker.read_new_lines(&mut state).await {
                                Ok(mut events) => {
                                    all_events.append(&mut events);
                                    all_events
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to read after change detection from {} (worker {}): {}",
                                        path, worker_count, e
                                    );
                                    all_events // Return at least the rotation events
                                }
                            }
                        } else {
                            rotation_events // Return any events even if no change detected
                        }
                    }
                    Err(e) => {
                        error!("Failed to check file changes for {} (worker {}): {}", path, worker_count, e);
                        Vec::new()
                    }
                }
            };

            // Decrement active worker count
            active_workers.fetch_sub(1, Ordering::Relaxed);
            result
        });
    }

    /// Collect results from completed tasks
    pub async fn collect_events(&mut self, sender: &mut BoundedSender<Event>) -> usize {
        let mut total_events = 0;

        // Collect all completed tasks without blocking
        while let Some(result) = self.join_set.try_join_next() {
            match result {
                Ok(events) => {
                    for event in events {
                        if let Err(e) = sender.send(event).await {
                            warn!("Failed to send event from task pool: {:?}", e);
                            break;
                        }
                        total_events += 1;
                    }
                }
                Err(e) => {
                    error!("Task pool task failed: {}", e);
                }
            }
        }

        total_events
    }

    /// Wait for all tasks to complete and collect results
    pub async fn collect_all_events(&mut self, sender: &mut BoundedSender<Event>) -> usize {
        let mut total_events = 0;

        while let Some(result) = self.join_set.join_next().await {
            match result {
                Ok(events) => {
                    for event in events {
                        if let Err(e) = sender.send(event).await {
                            warn!("Failed to send event from task pool: {:?}", e);
                            break;
                        }
                        total_events += 1;
                    }
                }
                Err(e) => {
                    error!("Task pool task failed: {}", e);
                }
            }
        }

        total_events
    }

    /// Get current number of active workers
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    /// Get utilization percentage based on max workers
    pub fn utilization(&self) -> f32 {
        self.active_workers() as f32 / self.max_workers as f32
    }

    /// Get worker pool statistics for monitoring
    pub fn stats(&self) -> TaskPoolStats {
        TaskPoolStats {
            active_workers: self.active_workers(),
            available_permits: self.semaphore.available_permits(),
            min_workers: self.min_workers,
            max_workers: self.max_workers,
            utilization: self.utilization(),
        }
    }

    /// Check if pool is under-utilized and can scale down
    pub async fn should_scale_down(&self) -> bool {
        let active = self.active_workers();
        if active <= self.min_workers {
            return false;
        }

        let last_activity = *self.last_activity.read().await;
        let idle_duration = Instant::now().duration_since(last_activity);

        idle_duration > self.idle_timeout && active > self.min_workers
    }

    /// Force cleanup of completed tasks (for testing)
    pub fn cleanup_completed(&mut self) -> usize {
        let mut cleaned = 0;
        while self.join_set.try_join_next().is_some() {
            cleaned += 1;
        }
        cleaned
    }
}

/// Statistics for monitoring the SmartTaskPool
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TaskPoolStats {
    pub active_workers: usize,
    pub available_permits: usize,
    pub min_workers: usize,
    pub max_workers: usize,
    pub utilization: f32,
}

impl Drop for SmartTaskPool {
    fn drop(&mut self) {
        self.join_set.abort_all();
    }
}

// Backward compatibility alias
#[allow(dead_code)]
pub type TaskPool = SmartTaskPool;
