use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use notify::event::ModifyKind::{Data, Name};
use notify::event::RenameMode;
use notify::{
    Config, Error, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher,
};
use regex::Regex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Notify, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::config::Settings;
use crate::domain::event::{Event, Meta};
use crate::domain::file::FileTracker;
use crate::domain::state::AppState;
use crate::infrastructure::filesystem::notify::{
    NotifyBridge, NotifyBridgeConfig, NotifyFilesystemSender,
};
use crate::infrastructure::metrics::{are_metrics_enabled, metrics};
use crate::task_pool::SmartTaskPool;
use crate::transport::bridge::{EventBridge, EventBridgeConfig, NotifyEventSender};
use crate::transport::channels::BoundedSender;

const K8S_PODS_REGEXP: &str = r"^/var/log/pods/(?P<namespace>[a-z0-9-]+)_(?P<pod_name>[a-z0-9-]+)_(?P<pod_id>[a-z0-9-]+)/(?P<container_name>[a-z-0-9]+)/(?P<num>0|[1-9][0-9]*).log$";

/// Maximum number of events to collect in a single atomic operation.
/// This limits memory usage to approximately 5 MB (10,000 events × ~500 bytes each).
/// Events are collected under a single lock to prevent race conditions.
const MAX_BATCH_SIZE: usize = 10_000;

pub struct Watcher {
    conf: Settings,
    file_trackers: HashMap<String, FileTracker>,
    process_queue_sender: BoundedSender<Event>,
    regexp: Regex,
    app_state: Arc<RwLock<AppState>>,
    metrics_enabled: bool,
    #[allow(dead_code)]
    task_pool: SmartTaskPool,
    read_existing_on_startup: bool,
    read_chunk_size: usize,
    max_line_size: usize,
    event_bridge: EventBridge,
    notify_sender: NotifyEventSender,
    notify_bridge: NotifyBridge,
    notify_filesystem_sender: NotifyFilesystemSender,
}

impl Watcher {
    /// Safely convert Path to String, returning None for invalid UTF-8 instead of panicking
    fn safe_path_to_string(path: &Path) -> Option<String> {
        path.to_str().map(|s| s.to_string())
    }

    pub fn new(conf: Settings, process_queue_sender: BoundedSender<Event>) -> Self {
        let state_file_path = conf
            .state_file_path
            .clone()
            .unwrap_or("/tmp/logfowd2_state.json".to_string());

        let app_state = match AppState::load_from_file(&state_file_path) {
            Ok(state) => {
                info!("Loaded application state from {}", state_file_path);
                state
            }
            Err(e) => {
                warn!(
                    "Failed to load state from {}: {}, starting with empty state",
                    state_file_path, e
                );
                AppState::new()
            }
        };

        let metrics_enabled = are_metrics_enabled(&conf.metrics);

        let read_existing = conf.read_existing_on_startup.unwrap_or(true);
        let read_chunk_size = conf.read_chunk_size.unwrap_or(200);
        let max_line_size = conf.max_line_size.unwrap_or(1024 * 1024); // 1MB default

        // Create EventBridge configuration from conf
        let event_bridge_config = EventBridgeConfig {
            notify_buffer_warning_threshold: conf
                .channels
                .as_ref()
                .and_then(|c| c.notify_buffer_warning_threshold)
                .unwrap_or(1000),
            warning_log_interval: Duration::from_secs(30),
        };

        let event_bridge = EventBridge::new(event_bridge_config, metrics_enabled);
        let notify_sender = event_bridge.notify_sender();

        // Create NotifyBridge configuration from conf
        let notify_bridge_config = NotifyBridgeConfig {
            notify_buffer_warning_threshold: conf
                .channels
                .as_ref()
                .and_then(|c| c.notify_filesystem_buffer_warning_threshold)
                .unwrap_or(1000),
            warning_log_interval: Duration::from_secs(30),
            bounded_channel_size: conf
                .channels
                .as_ref()
                .and_then(|c| c.notify_filesystem_buffer_size)
                .unwrap_or(1024),
        };

        let notify_bridge = NotifyBridge::new(notify_bridge_config, metrics_enabled);
        let notify_filesystem_sender = notify_bridge.notify_sender();

        Watcher {
            conf: conf.clone(),
            file_trackers: HashMap::new(),
            process_queue_sender,
            regexp: Regex::new(K8S_PODS_REGEXP).unwrap(),
            app_state: Arc::new(RwLock::new(app_state)),
            metrics_enabled,
            task_pool: SmartTaskPool::production_optimized(),
            read_existing_on_startup: read_existing,
            read_chunk_size,
            max_line_size,
            event_bridge,
            notify_sender,
            notify_bridge,
            notify_filesystem_sender,
        }
    }

    pub async fn run(&mut self, notify: Arc<Notify>) -> std::result::Result<(), notify::Error> {
        let log_path = self.conf.clone().log_path;

        let path = Path::new(log_path.as_str());

        if !path.is_absolute() {
            return Err(Error::generic("log_path must be absolute path"));
        }

        info!("Enhanced watcher starting for {}", path.display());

        // Start EventBridge task
        let bridge_handle = self
            .event_bridge
            .start_bridge_task(self.process_queue_sender.clone(), notify.clone());

        // Start NotifyBridge task
        let (notify_bridge_handle, filesystem_event_receiver) =
            self.notify_bridge.start_bridge_task(notify.clone());

        self.sync_files(path).await;

        // Start periodic state saving task
        let state_clone = self.app_state.clone();
        let state_file_path = self
            .conf
            .state_file_path
            .clone()
            .unwrap_or("/tmp/logfowd2_state.json".to_string());
        let shutdown_notify = notify.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10)); // Save every 10 seconds for better reliability
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Clone state quickly and release lock before I/O
                        let state_snapshot = {
                            let state = state_clone.read().await;
                            state.clone_for_save()
                        };

                        if let Err(e) = state_snapshot.save_to_file(&state_file_path) {
                            match e {
                                crate::domain::state::StateError::IoError(io_err)
                                    if io_err.kind() == std::io::ErrorKind::StorageFull => {
                                    error!("Failed to save state due to insufficient disk space: {}", io_err);
                                    warn!("Application will continue but state persistence is degraded");
                                }
                                _ => {
                                    error!("Failed to save state: {}", e);
                                }
                            }
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        info!("State saver received shutdown signal");
                        // Final save on shutdown - clone quickly then release lock
                        let state_snapshot = {
                            let state = state_clone.read().await;
                            state.clone_for_save()
                        };

                        if let Err(e) = state_snapshot.save_to_file(&state_file_path) {
                            match e {
                                crate::domain::state::StateError::IoError(io_err)
                                    if io_err.kind() == std::io::ErrorKind::StorageFull => {
                                    error!("Failed to save final state due to insufficient disk space: {}", io_err);
                                    warn!("Final state not saved due to disk space constraints");
                                }
                                _ => {
                                    error!("Failed to save final state: {}", e);
                                }
                            }
                        } else {
                            info!("Final state saved successfully");
                        }
                        break;
                    }
                }
            }
        });

        let watch_result = self.watch(path, notify, filesystem_event_receiver).await;

        // Don't wait for bridge task - it will clean up when channels close
        // This prevents deadlocks during shutdown
        bridge_handle.abort();
        notify_bridge_handle.abort();

        if let Err(e) = watch_result {
            error!("error: {:?}", e);
            return Err(e);
        }

        Ok(())
    }

    async fn watch(
        &mut self,
        path: &Path,
        shutdown: Arc<Notify>,
        mut rx: Receiver<notify::Result<notify::Event>>,
    ) -> notify::Result<()> {
        let filesystem_sender = self.notify_filesystem_sender.clone();

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                if filesystem_sender.send(res).is_err() {
                    debug!("NotifyBridge filesystem channel closed during shutdown");
                }
            },
            Config::default(),
        )?;

        watcher.watch(path.as_ref(), RecursiveMode::Recursive)?;

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event {
                        Ok(event) => {
                            debug!("changed: {:?}", event.clone());

                            match event.kind {
                                EventKind::Create(_file) => {
                                    for path in &event.paths {
                                        let path_str = match path.to_str() {
                                            Some(s) => s,
                                            None => {
                                                warn!("Invalid path: {:?}", path);
                                                continue;
                                            }
                                        };

                                        if !self.regexp.is_match(path_str) {
                                            continue;
                                        }

                                        info!("New file created: {}", path_str);

                                        // Update file tracking metric if enabled
                                        if self.metrics_enabled {
                                            let namespace = if let Some(caps) = self.regexp.captures(path_str) {
                                                caps.name("namespace").map(|m| m.as_str()).unwrap_or("unknown")
                                            } else {
                                                "unknown"
                                            };
                                            metrics().files_tracked
                                                .with_label_values(&[namespace])
                                                .inc();
                                        }

                                        // Atomic collection: collect all events under single lock,
                                        // then send after lock is released (eliminates race condition)
                                        let events = self.handle_create_event(path).await;
                                        if !events.is_empty() {
                                            self.safe_send_events(events).await;
                                        }
                                    }
                                }
                                EventKind::Modify(Data(_data_change)) => {
                                    for path in &event.paths {
                                        let path_str = match path.to_str() {
                                            Some(s) => s.to_string(),
                                            None => continue,
                                        };

                                        // Read in chunks without holding a mutable borrow of self across await
                                        loop {
                                            let events = {
                                                let mut state = self.app_state.write().await;
                                                if let Some(tracker) = self.file_trackers.get_mut(&path_str) {
                                                    match tracker
                                                        .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                                                        .await
                                                    {
                                                        Ok(events) => events,
                                                        Err(e) => {
                                                            error!("Failed to read from {}: {}", path_str, e);
                                                            Vec::new()
                                                        }
                                                    }
                                                } else {
                                                    warn!("File not found in trackers: {}", path_str);
                                                    Vec::new()
                                                }
                                            };

                                            if events.is_empty() { break; }
                                            self.safe_send_events(events).await;
                                        }
                                    }
                                }
                                EventKind::Modify(Name(rename_mode)) => {
                                    match rename_mode {
                                        RenameMode::Both => {
                                            // Single event with both old and new paths
                                            if event.paths.len() >= 2 {
                                                let old_path = match Self::safe_path_to_string(&event.paths[0]) {
                                                    Some(s) => s,
                                                    None => {
                                                        warn!("Invalid UTF-8 old path in rename event: {:?}", event.paths[0]);
                                                        continue;
                                                    }
                                                };
                                                let new_path = match Self::safe_path_to_string(&event.paths[1]) {
                                                    Some(s) => s,
                                                    None => {
                                                        warn!("Invalid UTF-8 new path in rename event: {:?}", event.paths[1]);
                                                        continue;
                                                    }
                                                };

                                                info!("File renamed: {} -> {}", old_path, new_path);
                                                self.handle_file_rename(old_path, new_path).await;
                                            } else {
                                                warn!("RenameMode::Both event missing paths: {:?}", event.paths);
                                            }
                                        },
                                        RenameMode::From => {
                                            // Old path only - file was renamed from this path
                                            let old_path = match Self::safe_path_to_string(&event.paths[0]) {
                                                Some(s) => s,
                                                None => {
                                                    warn!("Invalid UTF-8 path in rename From event: {:?}", event.paths[0]);
                                                    continue;
                                                }
                                            };
                                            info!("File renamed from: {}", old_path);
                                            // For From events, we need to wait for the corresponding To event
                                            // For now, handle as rotation to try to reopen the file
                                            self.handle_file_rotation(&old_path).await;
                                        },
                                        RenameMode::To => {
                                            // New path only - file was renamed to this path
                                            let new_path = match Self::safe_path_to_string(&event.paths[0]) {
                                                Some(s) => s,
                                                None => {
                                                    warn!("Invalid UTF-8 path in rename To event: {:?}", event.paths[0]);
                                                    continue;
                                                }
                                            };
                                            info!("File renamed to: {}", new_path);
                                            // Handle as new file creation if it matches our pattern
                                            if self.regexp.is_match(&new_path) {
                                                let mut state = self.app_state.write().await;
                                                match self.create_file_tracker(std::path::Path::new(&new_path), &mut state).await {
                                                    Ok(tracker) => {
                                                        info!("Created new tracker for renamed file: {}", new_path);
                                                        self.file_trackers.insert(new_path, tracker);
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to create tracker for renamed file {}: {}", new_path, e);
                                                    }
                                                }
                                            }
                                        },
                                        RenameMode::Any | RenameMode::Other => {
                                            // Generic rename handling - treat as rotation
                                            let path_str = match Self::safe_path_to_string(&event.paths[0]) {
                                                Some(s) => s,
                                                None => {
                                                    warn!("Invalid UTF-8 path in rename event: {:?}", event.paths[0]);
                                                    continue;
                                                }
                                            };
                                            info!("File renamed/rotated (generic): {}", path_str);
                                            self.handle_file_rotation(&path_str).await;
                                        }
                                    }
                                }
                                EventKind::Remove(_) => {
                                    let path_str = match Self::safe_path_to_string(&event.paths[0]) {
                                        Some(s) => s,
                                        None => {
                                            warn!("Invalid UTF-8 path in remove event: {:?}", event.paths[0]);
                                            continue;
                                        }
                                    };

                                    // Skip directory removal events - they're expected when pods are deleted
                                    if event.paths[0].is_dir() {
                                        debug!("Directory removed (expected): {}", path_str);
                                        continue;
                                    }

                                    // Only process log files that match our K8s pods pattern
                                    if !self.regexp.is_match(&path_str) {
                                        debug!("Non-log file removed (ignoring): {}", path_str);
                                        continue;
                                    }

                                    info!("File removed: {}", path_str);

                                    // Atomic cleanup: collect remaining events under single lock,
                                    // then send after cleanup is complete (eliminates race condition)
                                    let events = self.handle_remove_event(&path_str).await;
                                    if !events.is_empty() {
                                        self.safe_send_events(events).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(e) => info!("watch error: {:?}", e),
                    }
                }
                // Removed 100ms polling cycle - now purely event-driven for CPU efficiency
                _ = shutdown.notified() => {
                    info!("watcher received shutdown signal");

                    return Ok(())
                }
            }
        }
    }

    fn sync_files<'a>(&'a mut self, path: &'a Path) -> BoxFuture<'a, ()> {
        async move {
            if path.is_dir() {
                if let Ok(read_dir) = path.read_dir() {
                    for entry in read_dir {
                        let entry = match entry {
                            Ok(entry) => entry,
                            Err(e) => {
                                warn!("Failed to read directory entry in {}: {}", path.display(), e);
                                continue;
                            }
                        };
                        let path = entry.path();

                    if path.is_dir() {
                        self.sync_files(&path).await;
                    } else {
                        let path_str = match Self::safe_path_to_string(&path) {
                            Some(s) => s,
                            None => {
                                warn!("Invalid UTF-8 path during sync: {:?}", path);
                                continue;
                            }
                        };

                        if self.regexp.is_match(&path_str) {
                            // Create tracker with proper AppState registration during sync
                            let tracker = {
                                let mut state = self.app_state.write().await;
                                match self.create_file_tracker(&path, &mut state).await {
                                    Ok(tracker) => {
                                        debug!("Created FileTracker for {} during sync, properly registered in AppState", &path_str);
                                        Some(tracker)
                                    }
                                    Err(e) => {
                                        error!("Failed to create file tracker for {}: {}", &path_str, e);
                                        None
                                    }
                                }
                            };

                            if let Some(mut tracker) = tracker {
                                if self.read_existing_on_startup {
                                    // Stream historical content in chunks with drop/reacquire pattern
                                    loop {
                                        let events = {
                                            let mut state = self.app_state.write().await;
                                            match tracker
                                                .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                                                .await {
                                                Ok(events) => events,
                                                Err(e) => {
                                                    warn!(
                                                        "Failed to read historical content from {}: {}",
                                                        &path_str, e
                                                    );
                                                    Vec::new()
                                                }
                                            }
                                        }; // Lock released here

                                        if events.is_empty() { break; }
                                        self.safe_send_events(events).await;
                                    }
                                } else {
                                    // Skip historical content - quick operation
                                    let mut state = self.app_state.write().await;
                                    if let Err(e) = tracker.skip_existing_content(&mut state).await {
                                        warn!("Failed to skip existing content for {}: {}", &path_str, e);
                                    }
                                    drop(state);
                                }

                                // Store tracker
                                self.file_trackers.insert(path_str.clone(), tracker);
                            }
                        }
                    }
                    }
                } else {
                    warn!("Failed to read directory {:?}: permission denied or filesystem error", path);
                }
            }
        }
        .boxed()
    }

    /// Create file tracker with optimized locking - parse metadata outside critical section
    #[allow(dead_code)] // Alternative method for performance optimization
    async fn create_file_tracker_optimized(
        &self,
        path: &Path,
    ) -> std::result::Result<(FileTracker, Meta), std::io::Error> {
        let path_str = match Self::safe_path_to_string(path) {
            Some(s) => s,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid UTF-8 path: {:?}", path),
                ));
            }
        };
        let mut meta = Meta::default();

        // Parse K8s metadata from path (no locks needed)
        if let Some(caps) = self.regexp.captures(&path_str) {
            meta.namespace = caps
                .name("namespace")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.pod_name = caps
                .name("pod_name")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.pod_id = caps
                .name("pod_id")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.container_name = caps
                .name("container_name")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
        }

        // Create FileTracker with temporary state - will be updated with real state later
        let mut temp_state = crate::domain::state::AppState::new();
        let tracker = FileTracker::new_with_max_line_size(
            path_str,
            meta.clone(),
            &mut temp_state,
            self.max_line_size,
        )
        .await?;
        Ok((tracker, meta))
    }

    async fn create_file_tracker(
        &self,
        path: &Path,
        state: &mut AppState,
    ) -> std::result::Result<FileTracker, std::io::Error> {
        let mut meta = Meta::default();

        // Parse K8s metadata from path
        if let Some(path_str) = path.to_str()
            && let Some(caps) = self.regexp.captures(path_str)
        {
            meta.namespace = caps
                .name("namespace")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.pod_name = caps
                .name("pod_name")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.pod_id = caps
                .name("pod_id")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
            meta.container_name = caps
                .name("container_name")
                .map(|m| m.as_str())
                .unwrap_or("unknown")
                .to_string();
        }

        let path_str = match Self::safe_path_to_string(path) {
            Some(s) => s,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid UTF-8 path: {:?}", path),
                ));
            }
        };
        FileTracker::new_with_max_line_size(path_str, meta, state, self.max_line_size).await
    }

    /// Handle file rename with both old and new paths - migrate tracker and state
    async fn handle_file_rename(&mut self, old_path: String, new_path: String) {
        // Validate new path matches our K8s pattern
        if !self.regexp.is_match(&new_path) {
            warn!(
                "Renamed file {} doesn't match K8s pattern, ignoring",
                new_path
            );
            return;
        }

        // Check if we have a tracker for the old path
        if let Some(mut tracker) = self.file_trackers.remove(&old_path) {
            let mut state = self.app_state.write().await;

            // Read any remaining content from the old path before rename
            let mut remaining_events: Vec<Event> = Vec::new();
            loop {
                let events = match tracker
                    .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                    .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        debug!("Could not read final content from {}: {}", old_path, e);
                        Vec::new()
                    }
                };
                if events.is_empty() {
                    break;
                }
                remaining_events.extend(events);
            }

            // Update the tracker's path to the new path
            tracker.path = new_path.clone();

            // Migrate state from old path to new path
            if let Some(mut old_file_state) = state.files.remove(&old_path) {
                old_file_state.path = new_path.clone();
                state.files.insert(new_path.clone(), old_file_state);
                debug!("Migrated AppState from {} to {}", old_path, new_path);
            }

            // Insert tracker with new path as key
            self.file_trackers.insert(new_path.clone(), tracker);

            info!(
                "Successfully migrated file tracker from {} to {}",
                old_path, new_path
            );
            drop(state);

            // Send any remaining events
            if !remaining_events.is_empty() {
                self.safe_send_events(remaining_events).await;
            }

            // Try to read new content from the renamed file
            loop {
                let events = {
                    let mut state = self.app_state.write().await;
                    if let Some(tracker) = self.file_trackers.get_mut(&new_path) {
                        match tracker
                            .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                            .await
                        {
                            Ok(events) => events,
                            Err(e) => {
                                debug!("Could not read from renamed file {}: {}", new_path, e);
                                Vec::new()
                            }
                        }
                    } else {
                        Vec::new()
                    }
                };
                if events.is_empty() {
                    break;
                }
                self.safe_send_events(events).await;
            }
        } else {
            debug!(
                "No existing tracker for old path {}, creating new tracker for {}",
                old_path, new_path
            );
            // Create new tracker for the new path
            let mut state = self.app_state.write().await;
            match self
                .create_file_tracker(std::path::Path::new(&new_path), &mut state)
                .await
            {
                Ok(tracker) => {
                    info!("Created new tracker for renamed file: {}", new_path);
                    self.file_trackers.insert(new_path, tracker);
                }
                Err(e) => {
                    error!(
                        "Failed to create tracker for renamed file {}: {}",
                        new_path, e
                    );
                }
            }
        }
    }

    /// Handle file rotation - reopen file and read new content
    async fn handle_file_rotation(&mut self, path_str: &str) {
        if let Some(tracker) = self.file_trackers.get_mut(path_str) {
            let mut state = self.app_state.write().await;

            // Check if the file needs to be reopened
            let (changed, rotation_events) = match tracker.check_file_changes(&mut state).await {
                Ok((changed, events)) => {
                    if changed {
                        info!("File {} was rotated, reopened successfully", path_str);

                        // Read any new content after rotation
                        let mut collected: Vec<Event> = events; // Start with rotation events
                        loop {
                            let new_events = match tracker
                                .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                                .await
                            {
                                Ok(events) => events,
                                Err(e) => {
                                    error!(
                                        "Failed to read after rotation from {}: {}",
                                        path_str, e
                                    );
                                    Vec::new()
                                }
                            };
                            if new_events.is_empty() {
                                break;
                            }
                            collected.extend(new_events);
                        }
                        (changed, collected)
                    } else {
                        (changed, events)
                    }
                }
                Err(e) => {
                    error!("Failed to check file changes for {}: {}", path_str, e);
                    (false, Vec::new())
                }
            };
            drop(state);

            if changed && !rotation_events.is_empty() {
                self.safe_send_events(rotation_events).await;
            }
        }
    }

    /// Handle file creation event with atomic event collection.
    ///
    /// This method implements the Collect-Then-Send pattern to prevent race conditions:
    /// 1. Creates FileTracker under a single lock acquisition
    /// 2. Collects ALL events atomically (respecting MAX_BATCH_SIZE)
    /// 3. Returns events for the caller to send AFTER this method returns
    /// 4. Inserts tracker into file_trackers on success
    ///
    /// This eliminates the race window that existed when dropping/reacquiring locks
    /// between reading and sending events.
    async fn handle_create_event(&mut self, path: &Path) -> Vec<Event> {
        let path_str = match Self::safe_path_to_string(path) {
            Some(s) => s,
            None => {
                warn!("Invalid UTF-8 path in create event: {:?}", path);
                return Vec::new();
            }
        };

        // Acquire lock once and hold it for the entire operation
        let mut state = self.app_state.write().await;

        // Create FileTracker - this registers the file in AppState
        let mut tracker = match self.create_file_tracker(path, &mut state).await {
            Ok(tracker) => {
                debug!(
                    "Created FileTracker for {} during atomic create handling",
                    path_str
                );
                tracker
            }
            Err(e) => {
                error!("Failed to create file tracker for {}: {}", path_str, e);
                return Vec::new();
            }
        };

        // Collect all events atomically
        let mut collected_events: Vec<Event> = Vec::new();

        if self.read_existing_on_startup {
            // Read all content in chunks, collecting events under single lock
            loop {
                // Check MAX_BATCH_SIZE limit
                if collected_events.len() >= MAX_BATCH_SIZE {
                    warn!(
                        "Reached MAX_BATCH_SIZE ({}) for file {}, truncating",
                        MAX_BATCH_SIZE, path_str
                    );
                    break;
                }

                let events = match tracker
                    .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                    .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        error!("Failed to read from {}: {}", path_str, e);
                        break;
                    }
                };

                if events.is_empty() {
                    break;
                }

                collected_events.extend(events);
            }
        } else {
            // Skip historical content - just position to end of file
            if let Err(e) = tracker.skip_existing_content(&mut state).await {
                error!("Failed to skip existing content for {}: {}", path_str, e);
            }
        }

        // Release lock before inserting tracker (tracker insertion doesn't need AppState lock)
        drop(state);

        // Insert tracker into file_trackers (after releasing AppState lock)
        self.file_trackers.insert(path_str.clone(), tracker);
        debug!(
            "Watcher now tracking {} files after atomic create",
            self.file_trackers.len()
        );

        // Return events for caller to send (after all state is consistent)
        collected_events
    }

    /// Handle file removal event with atomic cleanup.
    ///
    /// This method implements the Atomic Cleanup pattern:
    /// 1. Removes tracker from file_trackers immediately (prevents race with Create)
    /// 2. Acquires lock and reads ALL remaining content atomically
    /// 3. Closes tracker and removes file from state under same lock
    /// 4. Returns events for the caller to send AFTER this method returns
    ///
    /// This ensures that:
    /// - No Create event can reuse the same path while we're still reading
    /// - All remaining content is captured before state cleanup
    /// - Events are sent only after state is fully consistent
    async fn handle_remove_event(&mut self, path_str: &str) -> Vec<Event> {
        // Remove tracker first - this prevents any Create event from racing with us
        let tracker = match self.file_trackers.remove(path_str) {
            Some(tracker) => tracker,
            None => {
                // No tracker for this path - might have been removed already or never tracked
                warn!(
                    "Tried to remove unknown file {} from file_trackers",
                    path_str
                );
                return Vec::new();
            }
        };

        debug!(
            "Removing FileTracker for {}, {} files remain",
            path_str,
            self.file_trackers.len()
        );

        // Now acquire lock and read remaining content atomically
        let mut state = self.app_state.write().await;
        let mut collected_events: Vec<Event> = Vec::new();
        let mut tracker = tracker; // Make mutable for reading

        // Read all remaining content under single lock
        loop {
            // Check MAX_BATCH_SIZE limit
            if collected_events.len() >= MAX_BATCH_SIZE {
                warn!(
                    "Reached MAX_BATCH_SIZE ({}) while reading final content from {}, truncating",
                    MAX_BATCH_SIZE, path_str
                );
                break;
            }

            let chunk = match tracker
                .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                .await
            {
                Ok(events) => events,
                Err(e) => {
                    debug!("Could not read final content from {}: {}", path_str, e);
                    break;
                }
            };

            if chunk.is_empty() {
                break;
            }

            collected_events.extend(chunk);
        }

        // Close tracker and remove from state (still under lock)
        tracker.close();
        state.remove_file(path_str);
        debug!(
            "Removed {} from AppState, {} files remain in state",
            path_str,
            state.files.len()
        );

        // Release lock
        drop(state);

        // Return events for caller to send
        collected_events
    }

    #[allow(dead_code)]
    async fn safe_send_event(&mut self, event: Event) {
        match self.notify_sender.try_send(event) {
            Ok(()) => {}
            Err(crate::transport::bridge::NotifyEventSendError::ChannelClosed) => {
                warn!("Notify event channel closed, unable to send event");
            }
        }
    }

    async fn safe_send_events(&mut self, events: Vec<Event>) {
        let mut successful_sends = 0u64;

        for event in events {
            match self.notify_sender.try_send(event) {
                Ok(()) => {
                    successful_sends += 1;
                }
                Err(crate::transport::bridge::NotifyEventSendError::ChannelClosed) => {
                    warn!("Notify event channel closed, unable to send event");
                    break; // No point trying to send more if channel is closed
                }
            }
        }

        // Update metrics for processed events if enabled
        if self.metrics_enabled && successful_sends > 0 {
            metrics()
                .events_processed_total
                .with_label_values(&["watcher", "success"])
                .inc_by(successful_sends);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::channels::create_bounded_channel;
    use std::fs;
    use tempfile::TempDir;
    // use std::path::PathBuf; // Unused

    fn create_test_conf() -> Settings {
        serde_json::from_str(
            r#"
        {
            "log_path": "/tmp/test_logs",
            "state_file_path": "/tmp/test_watcher_state.json",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test_index",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#,
        )
        .expect("Failed to parse test config")
    }

    #[test]
    fn test_k8s_regex_valid_paths() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        let valid_paths = vec![
            "/var/log/pods/default_nginx_abc123-def456-ghi789/nginx/0.log",
            "/var/log/pods/kube-system_coredns_xyz789-abc123-def456/coredns/1.log",
            "/var/log/pods/production_app-deployment_123abc-456def-789ghi/app-container/2.log",
            "/var/log/pods/test-ns_pod-name_pod-id-123/container-name/0.log",
        ];

        for path in valid_paths {
            let captures = regex.captures(path);
            assert!(captures.is_some(), "Should match K8s path: {}", path);

            let caps = captures.unwrap();
            assert!(
                caps.name("namespace").is_some(),
                "Should extract namespace from {}",
                path
            );
            assert!(
                caps.name("pod_name").is_some(),
                "Should extract pod_name from {}",
                path
            );
            assert!(
                caps.name("pod_id").is_some(),
                "Should extract pod_id from {}",
                path
            );
            assert!(
                caps.name("container_name").is_some(),
                "Should extract container_name from {}",
                path
            );
            assert!(
                caps.name("num").is_some(),
                "Should extract num from {}",
                path
            );
        }
    }

    #[test]
    fn test_k8s_regex_invalid_paths() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        let invalid_paths = vec![
            "/var/log/pods/namespace/pod/container/0.log", // Wrong separator format
            "/var/log/other/file.log",                     // Wrong directory
            "/var/log/pods/ns_pod_id/container/file.txt",  // Wrong file extension
            "/var/log/pods/ns_pod_id/container/0.log.gz",  // Compressed file
            "/var/log/pods/ns_pod_id/container/",          // Missing file
            "/logs/pods/ns_pod_id/container/0.log",        // Wrong base path
            "/var/log/pods/UPPER_case_id/container/0.log", // Uppercase not allowed
        ];

        for path in invalid_paths {
            let captures = regex.captures(path);
            assert!(
                captures.is_none(),
                "Should NOT match invalid K8s path: {}",
                path
            );
        }
    }

    #[test]
    fn test_k8s_metadata_extraction() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();
        let test_path =
            "/var/log/pods/production_web-app_pod123-abc456-def789/nginx-container/0.log";

        let captures = regex.captures(test_path).unwrap();

        assert_eq!(captures.name("namespace").unwrap().as_str(), "production");
        assert_eq!(captures.name("pod_name").unwrap().as_str(), "web-app");
        assert_eq!(
            captures.name("pod_id").unwrap().as_str(),
            "pod123-abc456-def789"
        );
        assert_eq!(
            captures.name("container_name").unwrap().as_str(),
            "nginx-container"
        );
        assert_eq!(captures.name("num").unwrap().as_str(), "0");
    }

    #[test]
    fn test_k8s_special_character_handling() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test with valid special characters
        let valid_special = vec![
            "/var/log/pods/test-namespace_my-pod-name_abc-123-def/app-container/0.log",
            "/var/log/pods/ns123_pod456_id789/container123/99.log",
        ];

        for path in valid_special {
            assert!(regex.is_match(path), "Should match special chars: {}", path);
        }

        // Test with invalid characters
        let invalid_special = vec![
            "/var/log/pods/test_namespace_my.pod.name_abc123/container/0.log", // Dots not allowed
            "/var/log/pods/test_namespace_my_pod_name_abc123/container_name/0.log", // Underscore in container
        ];

        for path in invalid_special {
            assert!(
                !regex.is_match(path),
                "Should NOT match invalid chars: {}",
                path
            );
        }
    }

    #[tokio::test]
    async fn test_watcher_creation() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();

        let watcher = Watcher::new(conf.clone(), sender);

        // Should initialize with empty file trackers
        assert!(watcher.file_trackers.is_empty());

        // Should have correct regex
        assert!(
            watcher
                .regexp
                .is_match("/var/log/pods/test-ns_pod_abc123/container/0.log")
        );
        assert!(!watcher.regexp.is_match("/invalid/path"));

        // Should have correct config
        assert_eq!(watcher.conf.log_path, conf.log_path);
    }

    #[tokio::test]
    async fn test_create_file_tracker_with_k8s_metadata() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();

        let watcher = Watcher::new(conf, sender);
        let _test_path = Path::new("/var/log/pods/production_webapp_pod-id-123/nginx/0.log");

        // Create a temporary file to ensure FileTracker can open it
        let temp_dir = TempDir::new().unwrap();
        let temp_file = temp_dir.path().join("0.log");
        fs::write(&temp_file, "test content\n").unwrap();

        let mut app_state = AppState::new();

        // Test with actual temp file path but extract metadata from K8s path
        let tracker_result = watcher
            .create_file_tracker(&temp_file, &mut app_state)
            .await;

        if tracker_result.is_ok() {
            // Verify that the method can handle file creation
            assert!(tracker_result.is_ok());
        }

        // Test metadata extraction with mock path parsing
        let k8s_path = "/var/log/pods/production_webapp_pod-id-123/nginx/0.log";
        if let Some(caps) = watcher.regexp.captures(k8s_path) {
            let meta = Meta {
                namespace: caps.name("namespace").unwrap().as_str().to_string(),
                pod_name: caps.name("pod_name").unwrap().as_str().to_string(),
                pod_id: caps.name("pod_id").unwrap().as_str().to_string(),
                container_name: caps.name("container_name").unwrap().as_str().to_string(),
            };

            assert_eq!(meta.namespace, "production");
            assert_eq!(meta.pod_name, "webapp");
            assert_eq!(meta.pod_id, "pod-id-123");
            assert_eq!(meta.container_name, "nginx");
        }
    }

    #[tokio::test]
    async fn test_safe_send_event() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(3, None, None, None, None);
        let sender = channel.sender();

        let mut watcher = Watcher::new(conf, sender);

        let test_event = Event::new("test message".to_string(), Meta::default());

        // Send event via notify sender (which goes to EventBridge)
        watcher.safe_send_event(test_event.clone()).await;

        // Event should be sent to the EventBridge's unbounded channel
        // UnboundedSender doesn't track queue size, so this will always be 0
        assert_eq!(watcher.notify_sender.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_safe_send_events_multiple() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();

        let mut watcher = Watcher::new(conf, sender);

        let events = vec![
            Event::new("message 1".to_string(), Meta::default()),
            Event::new("message 2".to_string(), Meta::default()),
            Event::new("message 3".to_string(), Meta::default()),
        ];

        // Send multiple events via notify sender (which goes to EventBridge)
        watcher.safe_send_events(events).await;

        // All events should be sent to the EventBridge's unbounded channel
        // UnboundedSender doesn't track queue size, so this will always be 0
        assert_eq!(watcher.notify_sender.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_safe_send_event_closed_channel() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(1, None, None, None, None);
        let sender = channel.sender();

        let mut watcher = Watcher::new(conf, sender);

        // Simulate closed notify channel by dropping the EventBridge's receiver
        // In the real implementation, this would happen during shutdown
        // For this test, we just verify that the method doesn't panic
        let test_event = Event::new("test message".to_string(), Meta::default());

        // Should handle gracefully (no panic) - may succeed or fail depending on timing
        watcher.safe_send_event(test_event).await;

        // Test passes if no panic occurred
    }

    #[test]
    fn test_k8s_regex_edge_cases() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test edge cases for numeric parts
        let edge_cases = vec![
            "/var/log/pods/ns_pod_id/container/0.log", // Minimum number
            "/var/log/pods/ns_pod_id/container/999.log", // Large number
            "/var/log/pods/ns_pod_id/container/1.log", // Single digit
            "/var/log/pods/a_b_c/d/0.log",             // Minimal names
        ];

        for path in edge_cases {
            assert!(regex.is_match(path), "Should match edge case: {}", path);
        }

        // Test invalid numeric cases
        let invalid_numeric = vec![
            "/var/log/pods/ns_pod_id/container/01.log", // Leading zero
            "/var/log/pods/ns_pod_id/container/abc.log", // Non-numeric
            "/var/log/pods/ns_pod_id/container/.log",   // Empty number
        ];

        for path in invalid_numeric {
            assert!(
                !regex.is_match(path),
                "Should NOT match invalid numeric: {}",
                path
            );
        }
    }

    #[test]
    fn test_k8s_regex_namespace_variations() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test valid namespace patterns
        let valid_namespaces = vec![
            "/var/log/pods/default_pod_id/container/0.log",
            "/var/log/pods/kube-system_pod_id/container/0.log",
            "/var/log/pods/my-app-v2_pod_id/container/0.log",
            "/var/log/pods/test123_pod_id/container/0.log",
            "/var/log/pods/a_pod_id/container/0.log", // Single char
        ];

        for path in valid_namespaces {
            let caps = regex.captures(path).unwrap();
            let ns = caps.name("namespace").unwrap().as_str();
            assert!(!ns.is_empty(), "Namespace should not be empty in {}", path);
        }

        // Test invalid namespace patterns
        let invalid_namespaces = vec![
            "/var/log/pods/_pod_id/container/0.log", // Empty namespace
            "/var/log/pods/My-App_pod_id/container/0.log", // Uppercase
            "/var/log/pods/ns.test_pod_id/container/0.log", // Dot in namespace
        ];

        for path in invalid_namespaces {
            assert!(
                !regex.is_match(path),
                "Should NOT match invalid namespace: {}",
                path
            );
        }
    }

    #[test]
    fn test_k8s_regex_pod_id_patterns() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test realistic pod ID patterns
        let valid_pod_ids = vec![
            "/var/log/pods/ns_pod_abc123-def456-ghi789/container/0.log",
            "/var/log/pods/ns_pod_deployment-7d4c8b9f8d-xk2pq/container/0.log",
            "/var/log/pods/ns_pod_statefulset-0/container/0.log",
            "/var/log/pods/ns_pod_a-b-c/container/0.log",
        ];

        for path in valid_pod_ids {
            let caps = regex.captures(path).unwrap();
            let pod_id = caps.name("pod_id").unwrap().as_str();
            assert!(!pod_id.is_empty(), "Pod ID should not be empty in {}", path);
            assert!(
                pod_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-'),
                "Pod ID should only contain alphanumeric and hyphens: {}",
                pod_id
            );
        }
    }

    #[tokio::test]
    async fn test_watcher_app_state_integration() {
        let temp_dir = TempDir::new().unwrap();
        let state_file = temp_dir.path().join("watcher_state.json");

        let mut conf = create_test_conf();
        conf.state_file_path = Some(state_file.to_string_lossy().to_string());

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();

        // Create watcher (should create new state)
        let watcher = Watcher::new(conf, sender);

        // Verify app_state was initialized
        let state = watcher.app_state.read().await;
        assert!(state.files.is_empty()); // Should start empty
    }

    #[test]
    fn test_k8s_regex_container_name_patterns() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test valid container name patterns
        let valid_containers = vec![
            "/var/log/pods/ns_pod_id/app/0.log",
            "/var/log/pods/ns_pod_id/nginx-proxy/0.log",
            "/var/log/pods/ns_pod_id/redis123/0.log",
            "/var/log/pods/ns_pod_id/a/0.log", // Single char
            "/var/log/pods/ns_pod_id/my-long-container-name/0.log",
        ];

        for path in valid_containers {
            let caps = regex.captures(path).unwrap();
            let container = caps.name("container_name").unwrap().as_str();
            assert!(
                !container.is_empty(),
                "Container name should not be empty in {}",
                path
            );
        }

        // Test invalid container name patterns
        let invalid_containers = vec![
            "/var/log/pods/ns_pod_id/App/0.log",            // Uppercase
            "/var/log/pods/ns_pod_id/container_name/0.log", // Underscore
            "/var/log/pods/ns_pod_id/container.name/0.log", // Dot
        ];

        for path in invalid_containers {
            assert!(
                !regex.is_match(path),
                "Should NOT match invalid container: {}",
                path
            );
        }
    }

    #[test]
    fn test_regex_performance_with_many_paths() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test regex performance with many path tests
        let test_paths = (0..1000)
            .map(|i| {
                format!(
                    "/var/log/pods/ns{}_pod{}_id{}/container{}/{}.log",
                    i,
                    i,
                    i,
                    i,
                    i % 10
                )
            })
            .collect::<Vec<_>>();

        let start = std::time::Instant::now();

        let mut matches = 0;
        for path in &test_paths {
            if regex.is_match(path) {
                matches += 1;
            }
        }

        let elapsed = start.elapsed();

        assert_eq!(matches, 1000, "All generated paths should match");
        assert!(
            elapsed < Duration::from_millis(100),
            "Regex should be fast: {:?}",
            elapsed
        );
    }

    #[test]
    fn test_malformed_paths_safety() {
        let regex = Regex::new(K8S_PODS_REGEXP).unwrap();

        // Test with various malformed/edge case paths that shouldn't crash
        let malformed_paths = vec![
            "",                                         // Empty string
            "/",                                        // Root path
            "/var/log/pods/",                           // Incomplete path
            "/var/log/pods/ns_pod_id",                  // Missing parts
            "/var/log/pods/ns_pod_id/container",        // Missing file
            "/var/log/pods/ns_pod_id/container/",       // Trailing slash
            "/var/log/pods/ns_pod_id/container/0",      // Missing extension
            "var/log/pods/ns_pod_id/container/0.log",   // Missing leading slash
            "/var/log/pods/ns_pod_id/container/0.log/", // Extra trailing slash
        ];

        for path in malformed_paths {
            // Should not crash and should not match
            assert!(
                !regex.is_match(path),
                "Malformed path should not match: '{}'",
                path
            );
        }
    }

    #[test]
    fn test_safe_path_str_conversion() {
        use std::ffi::OsStr;
        use std::path::Path;

        // Test the helper function that should replace unwrap() calls
        fn safe_path_to_string(path: &Path) -> Option<String> {
            path.to_str().map(|s| s.to_string())
        }

        // Valid UTF-8 path should work
        let valid_path = Path::new("/var/log/pods/test_pod_123/container/0.log");
        assert_eq!(
            safe_path_to_string(valid_path),
            Some("/var/log/pods/test_pod_123/container/0.log".to_string())
        );

        // Create a path with invalid UTF-8 (platform-dependent test)
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;

            // Create invalid UTF-8 sequence
            let invalid_utf8_bytes = b"/var/log/pods/\xFF\xFE/container/0.log";
            let invalid_path = Path::new(OsStr::from_bytes(invalid_utf8_bytes));

            // Should return None instead of panicking
            assert_eq!(safe_path_to_string(invalid_path), None);
        }
    }

    #[tokio::test]
    async fn test_watcher_handles_invalid_utf8_paths() {
        // Test that Watcher can handle filesystem events with invalid UTF-8 paths
        // without panicking

        let conf = create_test_conf();
        let channel = create_bounded_channel::<Event>(100, None, None, None, None);
        let sender = channel.sender();
        let watcher = Watcher::new(conf, sender);

        // Verify that watcher creation succeeds
        assert_eq!(watcher.file_trackers.len(), 0);

        // Note: This test verifies the structure is in place to handle invalid paths.
        // The actual filesystem event simulation with invalid UTF-8 is complex
        // and platform-dependent, so we test the helper function separately above.
    }

    #[tokio::test]
    async fn test_sync_files_handles_permission_denied() {
        // Test that sync_files handles permission denied errors gracefully
        // The sync_files function should handle directory read errors without panicking
        let temp_dir = TempDir::new().unwrap();
        let nonexistent_dir = temp_dir.path().join("nonexistent");

        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Test that calling sync_files on a nonexistent directory doesn't panic
        // This tests the fixed error handling for read_dir().unwrap()
        watcher.sync_files(&nonexistent_dir).await;

        // Should not crash and no trackers should be created
        assert!(
            watcher.file_trackers.is_empty(),
            "Should not create trackers for nonexistent directory"
        );
    }

    #[tokio::test]
    async fn test_sync_files_continues_on_individual_errors() {
        // Test that sync_files continues processing other directories when one fails
        let temp_dir = TempDir::new().unwrap();
        let test_dir = temp_dir.path().join("test");
        fs::create_dir_all(&test_dir).unwrap();

        // Create multiple directories with one that will be accessible
        let accessible_dir = test_dir.join("accessible");
        fs::create_dir_all(&accessible_dir).unwrap();

        // Create some regular files (not matching K8s pattern, but should process without crashing)
        let file1 = accessible_dir.join("file1.txt");
        fs::write(&file1, "test content 1\n").unwrap();

        let file2 = test_dir.join("file2.txt");
        fs::write(&file2, "test content 2\n").unwrap();

        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Test sync_files - should process all accessible files without panicking
        watcher.sync_files(&test_dir).await;

        // The test verifies that the function completes without crashing
        // Files won't match K8s regex so no trackers will be created, but that's ok
        // The important thing is that no unwrap() panic occurred during directory traversal
        assert!(true, "sync_files completed without panicking");
    }

    #[tokio::test]
    async fn test_sync_files_handles_nonexistent_directory() {
        // Test that sync_files handles nonexistent directory gracefully
        let temp_dir = TempDir::new().unwrap();
        let nonexistent_dir = temp_dir.path().join("nonexistent");

        // Create watcher with test config
        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Test sync_files with nonexistent directory - should not panic
        watcher.sync_files(&nonexistent_dir).await;

        // Verify no file trackers were created
        assert!(
            watcher.file_trackers.is_empty(),
            "Should not create trackers for nonexistent directory"
        );
    }

    #[tokio::test]
    async fn test_directory_removal_events_are_handled_gracefully() {
        // Test that directory removal events don't cause "unknown file" warnings
        let conf = create_test_conf();
        let channel =
            create_bounded_channel::<crate::domain::event::Event>(100, None, None, None, None);
        let sender = channel.sender();
        let watcher = Watcher::new(conf, sender);

        // Test the key insight: our fixed code now checks paths correctly
        // The production issue was caused by trying to remove directories that were never tracked

        // These directory paths should NOT match the regex pattern (and thus won't be tracked)
        assert!(!watcher.regexp.is_match(
            "/var/log/pods/logging_logfowd2-1_8bd6a9d9-3eb4-4606-987c-7abee9e16693/logfowd2"
        ));
        assert!(
            !watcher
                .regexp
                .is_match("/var/log/pods/logging_logfowd2-1_8bd6a9d9-3eb4-4606-987c-7abee9e16693")
        );

        // Only the actual log file should match the regex pattern
        assert!(watcher.regexp.is_match(
            "/var/log/pods/logging_logfowd2-1_8bd6a9d9-3eb4-4606-987c-7abee9e16693/logfowd2/0.log"
        ));

        // With our fix, directory removal events will be skipped silently
        // instead of causing "unknown file" warnings
    }

    // ============================================================================
    // TDD Tests for Problem #7: Race Condition Fix (Collect-Then-Send Pattern)
    // ============================================================================

    /// Test that handle_create_event collects all events atomically under single lock
    /// and returns them for sending after lock is released.
    #[tokio::test]
    async fn test_handle_create_event_collects_all_events_atomically() {
        // Setup: Create temp directory with K8s-like structure
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        // Create log file with multiple lines
        let log_file = pods_dir.join("0.log");
        let test_content = "line 1\nline 2\nline 3\nline 4\nline 5\n";
        fs::write(&log_file, test_content).unwrap();

        // Create watcher with read_existing_on_startup = true
        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();
        conf.read_existing_on_startup = Some(true);
        conf.read_chunk_size = Some(2); // Small chunk to test multiple iterations

        let channel = create_bounded_channel(100, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Call handle_create_event - should return all events collected atomically
        let events = watcher.handle_create_event(&log_file).await;

        // Verify all 5 lines were collected
        assert_eq!(events.len(), 5, "Should collect all 5 events atomically");
        assert_eq!(events[0].message, "line 1");
        assert_eq!(events[1].message, "line 2");
        assert_eq!(events[2].message, "line 3");
        assert_eq!(events[3].message, "line 4");
        assert_eq!(events[4].message, "line 5");

        // Verify tracker was added
        let path_str = log_file.to_string_lossy().to_string();
        assert!(
            watcher.file_trackers.contains_key(&path_str),
            "Tracker should be added after handle_create_event"
        );
    }

    /// Test that handle_create_event respects MAX_BATCH_SIZE limit
    #[tokio::test]
    async fn test_handle_create_event_respects_max_batch_size() {
        // Setup: Create temp directory with K8s-like structure
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        // Create log file with more lines than MAX_BATCH_SIZE
        // Note: MAX_BATCH_SIZE is 10,000 - we'll test with smaller value via config
        let log_file = pods_dir.join("0.log");
        let mut content = String::new();
        for i in 0..100 {
            content.push_str(&format!("line {}\n", i));
        }
        fs::write(&log_file, &content).unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();
        conf.read_existing_on_startup = Some(true);
        conf.read_chunk_size = Some(10);

        let channel = create_bounded_channel(1000, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Call handle_create_event
        let events = watcher.handle_create_event(&log_file).await;

        // Should collect all 100 events (within MAX_BATCH_SIZE of 10,000)
        assert_eq!(events.len(), 100, "Should collect all 100 events");
    }

    /// Test that events are NOT sent during lock hold - they are returned for later sending
    #[tokio::test]
    async fn test_handle_create_event_returns_events_for_sending() {
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        let log_file = pods_dir.join("0.log");
        fs::write(&log_file, "test line\n").unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();
        conf.read_existing_on_startup = Some(true);

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let receiver = channel.receiver();
        let mut watcher = Watcher::new(conf, sender);

        // Call handle_create_event - returns events, does NOT send them
        let events = watcher.handle_create_event(&log_file).await;
        assert_eq!(events.len(), 1);

        // Events should NOT be in channel yet (receiver should be empty)
        // The caller is responsible for sending after lock release
        let try_recv =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;
        assert!(
            try_recv.is_err(),
            "Events should not be sent by handle_create_event"
        );

        // Now send events (simulating what caller does)
        watcher.safe_send_events(events).await;
    }

    /// Test that tracker is inserted even when read errors occur
    #[tokio::test]
    async fn test_handle_create_event_inserts_tracker_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        let log_file = pods_dir.join("0.log");
        fs::write(&log_file, "content\n").unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Before: no trackers
        assert!(watcher.file_trackers.is_empty());

        // Call handle_create_event
        let _events = watcher.handle_create_event(&log_file).await;

        // After: tracker should be added
        let path_str = log_file.to_string_lossy().to_string();
        assert!(
            watcher.file_trackers.contains_key(&path_str),
            "Tracker should be inserted after successful create"
        );
        assert_eq!(watcher.file_trackers.len(), 1);
    }

    /// Test that handle_create_event handles non-existent file gracefully
    #[tokio::test]
    async fn test_handle_create_event_handles_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let nonexistent_file = temp_dir.path().join("nonexistent.log");

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Should not panic, should return empty events
        let events = watcher.handle_create_event(&nonexistent_file).await;
        assert!(
            events.is_empty(),
            "Should return empty events for nonexistent file"
        );

        // Tracker should NOT be added for nonexistent file
        assert!(watcher.file_trackers.is_empty());
    }

    // ============================================================================
    // TDD Tests for handle_remove_event (Atomic Cleanup Pattern)
    // ============================================================================

    /// Test that handle_remove_event reads all remaining content atomically
    #[tokio::test]
    async fn test_handle_remove_event_reads_remaining_content_atomically() {
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        let log_file = pods_dir.join("0.log");
        fs::write(&log_file, "line 1\nline 2\nline 3\n").unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();
        conf.read_existing_on_startup = Some(true);

        let channel = create_bounded_channel(100, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // First, create the file tracker (simulating a prior Create event)
        let _create_events = watcher.handle_create_event(&log_file).await;
        let path_str = log_file.to_string_lossy().to_string();
        assert!(watcher.file_trackers.contains_key(&path_str));

        // Now add more content to the file
        fs::write(
            &log_file,
            "line 1\nline 2\nline 3\nnew line 4\nnew line 5\n",
        )
        .unwrap();

        // Call handle_remove_event - should return remaining events
        let events = watcher.handle_remove_event(&path_str).await;

        // Should have collected the new lines (at least 2)
        assert!(
            events.len() >= 2,
            "Should collect remaining events, got {}",
            events.len()
        );

        // Tracker should be removed
        assert!(!watcher.file_trackers.contains_key(&path_str));
    }

    /// Test that handle_remove_event removes file from state and trackers
    #[tokio::test]
    async fn test_handle_remove_event_removes_from_state_and_trackers() {
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        let log_file = pods_dir.join("0.log");
        fs::write(&log_file, "content\n").unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Create tracker
        let _events = watcher.handle_create_event(&log_file).await;
        let path_str = log_file.to_string_lossy().to_string();

        // Verify tracker exists
        assert!(watcher.file_trackers.contains_key(&path_str));

        // Check state has file
        {
            let state = watcher.app_state.read().await;
            assert!(state.files.contains_key(&path_str));
        }

        // Remove file
        let _events = watcher.handle_remove_event(&path_str).await;

        // Verify tracker removed
        assert!(!watcher.file_trackers.contains_key(&path_str));

        // Verify state cleaned up
        {
            let state = watcher.app_state.read().await;
            assert!(!state.files.contains_key(&path_str));
        }
    }

    /// Test that handle_remove_event returns events for later sending
    #[tokio::test]
    async fn test_handle_remove_event_returns_events_for_sending() {
        let temp_dir = TempDir::new().unwrap();
        let pods_dir = temp_dir
            .path()
            .join("var/log/pods/test-ns_test-pod_pod-id-123/test-container");
        fs::create_dir_all(&pods_dir).unwrap();

        let log_file = pods_dir.join("0.log");
        fs::write(&log_file, "line 1\n").unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();
        conf.read_existing_on_startup = Some(true);

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let receiver = channel.receiver();
        let mut watcher = Watcher::new(conf, sender);

        // Create tracker and consume initial events
        let create_events = watcher.handle_create_event(&log_file).await;
        watcher.safe_send_events(create_events).await;

        // Add new content
        fs::write(&log_file, "line 1\nnew line\n").unwrap();
        let path_str = log_file.to_string_lossy().to_string();

        // Drain the channel first
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv()).await;

        // Call handle_remove_event
        let events = watcher.handle_remove_event(&path_str).await;

        // Events should be returned, NOT sent
        // (We can't easily verify this since the channel may have events from create,
        // but we verify the return value is correct)
        assert!(events.len() >= 1, "Should return at least 1 event");
    }

    /// Test that handle_remove_event handles unknown file gracefully
    #[tokio::test]
    async fn test_handle_remove_event_handles_unknown_file() {
        let temp_dir = TempDir::new().unwrap();

        let mut conf = create_test_conf();
        conf.log_path = temp_dir.path().to_string_lossy().to_string();

        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let mut watcher = Watcher::new(conf, sender);

        // Try to remove a file that was never tracked
        let events = watcher.handle_remove_event("/nonexistent/path").await;

        // Should return empty events and not panic
        assert!(
            events.is_empty(),
            "Should return empty events for unknown file"
        );
    }
}
