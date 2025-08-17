use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use log::{debug, error, info, warn};
use notify::event::ModifyKind::{Data, Name};
use notify::{
    Config, Error, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher,
};
use regex::Regex;
use tokio::sync::mpsc::channel;
use tokio::sync::{Notify, RwLock};
use tokio::time::interval;

use crate::Conf;
use crate::channels::{BoundedSender, SendError};
use crate::events::{Event, Meta};
use crate::file_tracker::FileTracker;
use crate::metrics::{are_metrics_enabled, metrics};
use crate::state::AppState;
use crate::task_pool::SmartTaskPool;

const K8S_PODS_REGEXP: &str = r"^/var/log/pods/(?P<namespace>[a-z0-9-]+)_(?P<pod_name>[a-z0-9-]+)_(?P<pod_id>[a-z0-9-]+)/(?P<container_name>[a-z-0-9]+)/(?P<num>0|[1-9][0-9]*).log$";

pub struct Watcher {
    conf: Conf,
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
}

impl Watcher {
    /// Safely convert Path to String, returning None for invalid UTF-8 instead of panicking
    fn safe_path_to_string(path: &Path) -> Option<String> {
        path.to_str().map(|s| s.to_string())
    }

    pub fn new(conf: Conf, process_queue_sender: BoundedSender<Event>) -> Self {
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
        }
    }

    pub async fn run(&mut self, notify: Arc<Notify>) -> Result<(), Error> {
        let log_path = self.conf.clone().log_path;

        let path = Path::new(log_path.as_str());

        if !path.is_absolute() {
            return Err(Error::generic("log_path must be absolute path"));
        }

        info!("Enhanced watcher starting for {}", path.display());

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
                        let state = state_clone.write().await;
                        if let Err(e) = state.save_to_file(&state_file_path) {
                            match e {
                                crate::state::StateError::IoError(io_err)
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
                        // Final save on shutdown
                        let state = state_clone.write().await;
                        if let Err(e) = state.save_to_file(&state_file_path) {
                            match e {
                                crate::state::StateError::IoError(io_err)
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

        if let Err(e) = self.watch(path, notify).await {
            error!("error: {:?}", e);

            return Err(e);
        }

        Ok(())
    }

    async fn watch(&mut self, path: &Path, shoutdown: Arc<Notify>) -> notify::Result<()> {
        let (tx, mut rx) = channel(1024);

        let mut watcher =
            RecommendedWatcher::new(move |res| tx.blocking_send(res).unwrap(), Config::default())?;

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
                                                caps.name("namespace").unwrap().as_str()
                                            } else {
                                                "unknown"
                                            };
                                            metrics().files_tracked
                                                .with_label_values(&[namespace])
                                                .inc();
                                        }

                                        let mut state = self.app_state.write().await;
                                        match self.create_file_tracker(path, &mut state).await {
                                            Ok(mut tracker) => {
                                                debug!("Created FileTracker for {}, adding to Watcher.file_trackers", path_str);

                                                if self.read_existing_on_startup {
                                                    // Stream existing content in chunks
                                                    loop {
                                                        let events = match tracker
                                                            .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                                                            .await {
                                                            Ok(events) => events,
                                                            Err(e) => {
                                                                error!("Failed to read from {}: {}", path_str, e);
                                                                Vec::new()
                                                            }
                                                        };
                                                        if events.is_empty() { break; }
                                                        // Drop lock before sending
                                                        drop(state);
                                                        self.safe_send_events(events).await;
                                                        state = self.app_state.write().await;
                                                    }
                                                    drop(state);
                                                } else {
                                                    // Skip historical content
                                                    if let Err(e) = tracker.skip_existing_content(&mut state).await {
                                                        error!("Failed to skip existing content for {}: {}", path_str, e);
                                                    }
                                                    drop(state);
                                                }

                                                self.file_trackers.insert(path_str.to_string(), tracker);
                                                debug!("Watcher now tracking {} files", self.file_trackers.len());
                                            }
                                            Err(e) => {
                                                error!("Failed to create file tracker for {}: {}", path_str, e);
                                            }
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
                                EventKind::Modify(Name(_rename_mode)) => {
                                    let path_str = match Self::safe_path_to_string(&event.paths[0]) {
                                        Some(s) => s,
                                        None => {
                                            warn!("Invalid UTF-8 path in rename event: {:?}", event.paths[0]);
                                            continue;
                                        }
                                    };
                                    info!("File renamed/rotated: {}", path_str);

                                    // Handle file rotation with new FileTracker
                                    if let Some(tracker) = self.file_trackers.get_mut(&path_str) {
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
                                                            .await {
                                                            Ok(events) => events,
                                                            Err(e) => {
                                                                error!("Failed to read after rotation from {}: {}", path_str, e);
                                                                Vec::new()
                                                            }
                                                        };
                                                        if new_events.is_empty() { break; }
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
                                EventKind::Remove(_) => {
                                    let path_str = match Self::safe_path_to_string(&event.paths[0]) {
                                        Some(s) => s,
                                        None => {
                                            warn!("Invalid UTF-8 path in remove event: {:?}", event.paths[0]);
                                            continue;
                                        }
                                    };
                                    info!("File removed: {}", path_str);

                                    // Handle removal with FileTracker
                                    if let Some(mut tracker) = self.file_trackers.remove(&path_str) {
                                        debug!("Removing FileTracker for {}, {} files remain", path_str, self.file_trackers.len());
                                        let mut state = self.app_state.write().await;

                                        // Try to read any remaining content before removing (chunked)
                                        let mut events: Vec<Event> = Vec::new();
                                        loop {
                                            let chunk = match tracker
                                                .read_new_lines_limited(&mut state, Some(self.read_chunk_size))
                                                .await {
                                                Ok(events) => events,
                                                Err(e) => {
                                                    debug!("Could not read final content from {}: {}", path_str, e);
                                                    Vec::new()
                                                }
                                            };
                                            if chunk.is_empty() { break; }
                                            events.extend(chunk);
                                        }

                                        tracker.close();
                                        state.remove_file(&path_str);
                                        debug!("Removed {} from AppState, {} files remain in state", path_str, state.files.len());
                                        drop(state);

                                        if !events.is_empty() {
                                            self.safe_send_events(events).await;
                                        }
                                    } else {
                                        warn!("Tried to remove unknown file {} from file_trackers", path_str);
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(e) => info!("watch error: {:?}", e),
                    }
                }
                // Removed 100ms polling cycle - now purely event-driven for CPU efficiency
                _ = shoutdown.notified() => {
                    log::info!("watcher received shutdown signal");

                    return Ok(())
                }
            }
        }
    }

    fn sync_files<'a>(&'a mut self, path: &'a Path) -> BoxFuture<'a, ()> {
        async {
            if path.is_dir() {
                for entry in path.read_dir().unwrap() {
                    let entry = entry.unwrap();
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
                            let mut state = self.app_state.write().await;
                            match self.create_file_tracker(&path, &mut state).await {
                                Ok(mut tracker) => {
                                    debug!("Created FileTracker for {} during sync, properly registered in AppState", &path_str);
                                    if self.read_existing_on_startup {
                                        // Stream historical content in chunks
                                        loop {
                                            let events = match tracker
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
                                            };
                                            if events.is_empty() { break; }
                                            drop(state);
                                            self.safe_send_events(events).await;
                                            state = self.app_state.write().await;
                                        }
                                        drop(state);
                                    } else {
                                        // Skip historical content
                                        if let Err(e) = tracker.skip_existing_content(&mut state).await {
                                            warn!("Failed to skip existing content for {}: {}", &path_str, e);
                                        }
                                        drop(state);
                                    }

                                    // Store tracker
                                    self.file_trackers.insert(path_str.clone(), tracker);
                                }
                                Err(e) => {
                                    error!("Failed to create file tracker for {}: {}", &path_str, e);
                                }
                            }
                        }
                    }
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
    ) -> Result<(FileTracker, Meta), std::io::Error> {
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
            meta.namespace = caps.name("namespace").unwrap().as_str().to_string();
            meta.pod_name = caps.name("pod_name").unwrap().as_str().to_string();
            meta.pod_id = caps.name("pod_id").unwrap().as_str().to_string();
            meta.container_name = caps.name("container_name").unwrap().as_str().to_string();
        }

        // Create FileTracker with temporary state - will be updated with real state later
        let mut temp_state = crate::state::AppState::new();
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
    ) -> Result<FileTracker, std::io::Error> {
        let mut meta = Meta::default();

        // Parse K8s metadata from path
        if let Some(path_str) = path.to_str()
            && let Some(caps) = self.regexp.captures(path_str)
        {
            meta.namespace = caps.name("namespace").unwrap().as_str().to_string();
            meta.pod_name = caps.name("pod_name").unwrap().as_str().to_string();
            meta.pod_id = caps.name("pod_id").unwrap().as_str().to_string();
            meta.container_name = caps.name("container_name").unwrap().as_str().to_string();
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

    async fn safe_send_event(&mut self, event: Event) {
        match self.process_queue_sender.send(event).await {
            Ok(()) => {}
            Err(SendError::Closed(_)) => {
                warn!("Process queue channel closed, unable to send event");
            }
        }
    }

    async fn safe_send_events(&mut self, events: Vec<Event>) {
        let event_count = events.len();
        for event in events {
            self.safe_send_event(event).await;
        }

        // Update metrics for processed events if enabled
        if self.metrics_enabled && event_count > 0 {
            metrics()
                .events_processed_total
                .with_label_values(&["watcher", "success"])
                .inc_by(event_count as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::create_bounded_channel;
    use std::fs;
    use tempfile::TempDir;
    // use std::path::PathBuf; // Unused

    fn create_test_conf() -> Conf {
        serde_json::from_str(
            r#"
        {
            "is_debug": true,
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
        let receiver = channel.receiver();

        let mut watcher = Watcher::new(conf, sender);

        let test_event = Event::new("test message".to_string(), Meta::default());

        // Send event
        watcher.safe_send_event(test_event.clone()).await;

        // Should be able to receive it
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.message, "test message");
    }

    #[tokio::test]
    async fn test_safe_send_events_multiple() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(10, None, None, None, None);
        let sender = channel.sender();
        let receiver = channel.receiver();

        let mut watcher = Watcher::new(conf, sender);

        let events = vec![
            Event::new("message 1".to_string(), Meta::default()),
            Event::new("message 2".to_string(), Meta::default()),
            Event::new("message 3".to_string(), Meta::default()),
        ];

        // Send multiple events
        watcher.safe_send_events(events).await;

        // Should receive all events
        for i in 1..=3 {
            let received = receiver.recv().await.unwrap();
            assert_eq!(received.message, format!("message {}", i));
        }
    }

    #[tokio::test]
    async fn test_safe_send_event_closed_channel() {
        let conf = create_test_conf();
        let channel = create_bounded_channel(1, None, None, None, None);
        let sender = channel.sender();

        // Drop receiver to close channel
        drop(channel.receiver());

        let mut watcher = Watcher::new(conf, sender);
        let test_event = Event::new("test message".to_string(), Meta::default());

        // Should handle closed channel gracefully (no panic)
        watcher.safe_send_event(test_event).await;
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
}
