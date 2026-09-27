use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};

use crate::domain::event::SourcePosition;

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub path: String,
    pub position: u64,
    pub inode: u64,
    pub size: u64,
    pub last_modified: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppState {
    pub files: HashMap<String, FileState>,
    pub version: u32,
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    next_generation: u64,
    #[serde(skip)]
    committed: HashMap<u64, u64>,
    #[serde(skip)]
    pending: HashMap<u64, BTreeMap<u64, bool>>,
}

#[derive(Debug)]
pub enum StateError {
    IoError(std::io::Error),
    SerializationError(serde_json::Error),
    ChecksumMismatch,
    #[allow(dead_code)] // Error variant for future state validation
    CorruptedState,
}

impl AppState {
    pub fn new() -> Self {
        AppState {
            files: HashMap::new(),
            version: 1,
            checksum: None,
            next_generation: 0,
            committed: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    pub fn load_from_file(state_file_path: &str) -> Result<Self, StateError> {
        if !Path::new(state_file_path).exists() {
            info!(
                "State file {} does not exist, creating new state",
                state_file_path
            );
            return Ok(AppState::new());
        }

        // Try to load the main file first
        match Self::load_and_verify(state_file_path) {
            Ok(state) => {
                info!(
                    "Loaded state from {}, {} files tracked, version {}",
                    state_file_path,
                    state.files.len(),
                    state.version
                );
                return Ok(state);
            }
            Err(StateError::ChecksumMismatch) => {
                // Checksum mismatch should not fall back to backup - it's a data integrity issue
                return Err(StateError::ChecksumMismatch);
            }
            Err(e) => {
                warn!("Failed to load main state file: {:?}", e);
            }
        }

        // Try backup file
        let backup_path = format!("{}.backup", state_file_path);
        if Path::new(&backup_path).exists() {
            match Self::load_and_verify(&backup_path) {
                Ok(state) => {
                    warn!(
                        "Loaded state from backup {}, {} files tracked",
                        backup_path,
                        state.files.len()
                    );
                    return Ok(state);
                }
                Err(e) => {
                    error!("Failed to load backup state file: {:?}", e);
                }
            }
        }

        warn!("Both main and backup state files are corrupted, creating new state");
        Ok(AppState::new())
    }

    fn load_and_verify(file_path: &str) -> Result<AppState, StateError> {
        let content = fs::read_to_string(file_path).map_err(StateError::IoError)?;
        let mut state: AppState =
            serde_json::from_str(&content).map_err(StateError::SerializationError)?;

        // Verify checksum if present
        if let Some(stored_checksum) = &state.checksum {
            let content_for_checksum = Self::create_content_without_checksum(&state)?;
            let calculated_checksum = Self::calculate_checksum(&content_for_checksum);

            if stored_checksum != &calculated_checksum {
                return Err(StateError::ChecksumMismatch);
            }
        }

        // Update checksum to current format using deterministic calculation
        let content_for_checksum = Self::create_content_without_checksum(&state)?;
        state.checksum = Some(Self::calculate_checksum(&content_for_checksum));
        // Older checkpoints have no generation field. Assign stable generations
        // after checksum verification so their original checksum remains valid.
        state.next_generation = state
            .files
            .values()
            .map(|file| file.generation)
            .max()
            .unwrap_or(0)
            .max(state.next_generation);
        let mut legacy_paths: Vec<_> = state
            .files
            .iter()
            .filter(|(_, file)| file.generation == 0)
            .map(|(path, _)| path.clone())
            .collect();
        legacy_paths.sort();
        for path in legacy_paths {
            let generation = state.allocate_generation();
            state.files.get_mut(&path).unwrap().generation = generation;
        }
        state.committed = state
            .files
            .values()
            .map(|file| (file.generation, file.position))
            .collect();
        Ok(state)
    }

    pub fn save_to_file(&self, state_file_path: &str) -> Result<(), StateError> {
        self.save_to_file_atomic(state_file_path)
    }

    pub fn save_to_file_atomic(&self, state_file_path: &str) -> Result<(), StateError> {
        // Create a copy with updated checksum
        let mut state_copy = self.clone_for_save();

        // Create content without checksum for checksum calculation
        let content_for_checksum = Self::create_content_without_checksum(&state_copy)?;
        let checksum = Self::calculate_checksum(&content_for_checksum);
        state_copy.checksum = Some(checksum);

        let state_json =
            serde_json::to_string_pretty(&state_copy).map_err(StateError::SerializationError)?;

        // Atomic write: write to temporary file first
        let temp_path = format!("{}.tmp", state_file_path);
        let backup_path = format!("{}.backup", state_file_path);

        // Write to temporary file with disk space error handling
        match fs::write(&temp_path, &state_json) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::StorageFull => {
                error!("Disk full while writing state file to {}", temp_path);
                return Err(StateError::IoError(e));
            }
            Err(e) => return Err(StateError::IoError(e)),
        }

        // Create backup of current file if it exists
        if Path::new(state_file_path).exists() {
            fs::copy(state_file_path, &backup_path).map_err(StateError::IoError)?;
        }

        // Atomically move temp file to final location
        fs::rename(&temp_path, state_file_path).map_err(StateError::IoError)?;

        debug!(
            "Atomically saved state to {}, {} files tracked, checksum: {}",
            state_file_path,
            state_copy.files.len(),
            state_copy.checksum.as_ref().unwrap_or(&"none".to_string())
        );
        Ok(())
    }

    pub fn get_file_position(&self, file_path: &str) -> Option<u64> {
        self.files.get(file_path).map(|state| state.position)
    }

    pub fn update_file_position(&mut self, file_path: String, position: u64) {
        if let Some(file_state) = self.files.get_mut(&file_path) {
            file_state.position = position;
            debug!("Updated position for {}: {}", file_path, position);
        } else {
            warn!(
                "Trying to update position for unknown file: {}. Known files: [{}]",
                file_path,
                self.files
                    .keys()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    /// Update file position with fallback creation if file is unknown
    #[allow(dead_code)] // Available for future use
    pub fn update_file_position_with_fallback(
        &mut self,
        file_path: String,
        position: u64,
        inode: u64,
        size: u64,
    ) {
        if let Some(file_state) = self.files.get_mut(&file_path) {
            file_state.position = position;
            debug!("Updated position for {}: {}", file_path, position);
        } else {
            warn!("Unknown file {}, creating fallback FileState", file_path);
            // Create emergency FileState to prevent position loss
            let file_state = FileState {
                path: file_path.clone(),
                position,
                inode,
                size,
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                generation: self.allocate_generation(),
            };
            self.files.insert(file_path.clone(), file_state);
            debug!(
                "Created fallback FileState for {}: position {}",
                file_path, position
            );
        }
    }

    pub fn add_file(&mut self, file_path: String, inode: u64, size: u64, last_modified: u64) {
        let generation = self.allocate_generation();
        let file_state = FileState {
            path: file_path.clone(),
            position: 0,
            inode,
            size,
            last_modified,
            generation,
        };

        if let Some(previous) = self.files.insert(file_path.clone(), file_state) {
            self.prune_generation(previous.generation);
        }
        self.committed.insert(generation, 0);
        debug!("Added file to state: {}", file_path);
    }

    fn allocate_generation(&mut self) -> u64 {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("file generation counter exhausted");
        self.next_generation
    }

    pub fn file_generation(&self, file_path: &str) -> Option<u64> {
        self.files.get(file_path).map(|file| file.generation)
    }

    pub fn skip_to_position(&mut self, file_path: &str, inode: u64, position: u64) {
        if let Some(file) = self.files.get_mut(file_path)
            && file.inode == inode
        {
            file.position = position;
            self.committed.insert(file.generation, position);
            self.pending.remove(&file.generation);
        }
    }

    fn prune_generation(&mut self, generation: u64) {
        if !self
            .files
            .values()
            .any(|file| file.generation == generation)
            && !self.pending.contains_key(&generation)
        {
            self.committed.remove(&generation);
        }
    }

    pub fn register_pending(&mut self, file_path: &str, inode: u64, end_position: u64) {
        if let Some(file) = self.files.get(file_path)
            && file.inode == inode
            && end_position > *self.committed.get(&file.generation).unwrap_or(&0)
        {
            self.pending
                .entry(file.generation)
                .or_default()
                .entry(end_position)
                .or_insert(false);
        }
    }

    #[cfg(test)]
    pub fn mark_delivered(&mut self, file_path: &str, inode: u64, end_position: u64) {
        if let Some(file) = self.files.get(file_path)
            && file.inode == inode
        {
            self.mark_generation_delivered(file.generation, end_position);
        }
    }

    pub fn mark_source_delivered(&mut self, source: &SourcePosition) {
        self.mark_generation_delivered(source.generation, source.end);
    }

    fn mark_generation_delivered(&mut self, generation: u64, end_position: u64) {
        let Some(pending) = self.pending.get_mut(&generation) else {
            return;
        };
        let Some(acked) = pending.get_mut(&end_position) else {
            return;
        };
        *acked = true;

        while pending.first_key_value().is_some_and(|(_, acked)| *acked) {
            if let Some((position, _)) = pending.pop_first() {
                self.committed.insert(generation, position);
            }
        }
        if pending.is_empty() {
            self.pending.remove(&generation);
            self.prune_generation(generation);
        }
    }

    pub fn rename_file(&mut self, old_path: &str, new_path: &str, inode: u64) {
        let Some(mut file) = self.files.remove(old_path) else {
            return;
        };
        if file.inode != inode {
            self.files.insert(old_path.to_string(), file);
            return;
        }
        file.path = new_path.to_string();
        if let Some(replaced) = self.files.insert(new_path.to_string(), file) {
            self.prune_generation(replaced.generation);
        }
    }

    pub fn remove_file(&mut self, file_path: &str) {
        if let Some(file) = self.files.remove(file_path) {
            self.prune_generation(file.generation);
            debug!("Removed file from state: {}", file_path);
        }
    }

    pub fn is_file_truncated(&self, file_path: &str, current_size: u64) -> bool {
        if let Some(file_state) = self.files.get(file_path) {
            return current_size < file_state.position;
        }
        false
    }

    pub fn handle_file_truncation(&mut self, file_path: &str) {
        if !self.files.contains_key(file_path) {
            return;
        }
        let generation = self.allocate_generation();
        if let Some(file_state) = self.files.get_mut(file_path) {
            warn!(
                "File truncated detected: {}, resetting position from {} to 0",
                file_path, file_state.position
            );
            file_state.position = 0;
            let old_generation = std::mem::replace(&mut file_state.generation, generation);
            self.committed.insert(generation, 0);
            self.prune_generation(old_generation);
        }
    }

    pub fn should_reopen_file(&self, file_path: &str, current_inode: u64) -> bool {
        if let Some(file_state) = self.files.get(file_path) {
            return file_state.inode != current_inode;
        }
        true // Unknown file, should open
    }

    fn create_content_without_checksum(state: &AppState) -> Result<String, StateError> {
        // Create a deterministic representation by using sorted keys
        let sorted_files: BTreeMap<String, &FileState> =
            state.files.iter().map(|(k, v)| (k.clone(), v)).collect();

        // Create a temporary structure for deterministic serialization
        #[derive(Serialize)]
        struct DeterministicAppState<'a> {
            files: BTreeMap<String, &'a FileState>,
            version: u32,
            checksum: Option<String>,
            #[serde(skip_serializing_if = "is_zero")]
            next_generation: u64,
        }

        let deterministic_state = DeterministicAppState {
            files: sorted_files,
            version: state.version,
            checksum: None, // Always None for checksum calculation
            next_generation: state.next_generation,
        };

        serde_json::to_string_pretty(&deterministic_state).map_err(StateError::SerializationError)
    }

    fn calculate_checksum(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let digest = hasher.finalize();
        let mut checksum = String::with_capacity(digest.len() * 2);
        use std::fmt::Write;
        for byte in digest.iter() {
            write!(&mut checksum, "{byte:02x}").expect("writing to a String cannot fail");
        }
        checksum
    }

    /// Create an efficient clone for save operations
    pub fn clone_for_save(&self) -> Self {
        let mut snapshot = Self {
            files: self.files.clone(),
            version: self.version,
            checksum: self.checksum.clone(),
            next_generation: self.next_generation,
            committed: self.committed.clone(),
            pending: HashMap::new(),
        };
        for file in snapshot.files.values_mut() {
            file.position = *self.committed.get(&file.generation).unwrap_or(&0);
        }
        snapshot
    }

    /// Get a specific file state without holding a long-term lock
    #[allow(dead_code)] // Available for future use
    pub fn get_file_state_clone(&self, file_path: &str) -> Option<FileState> {
        self.files.get(file_path).cloned()
    }
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateError::IoError(e) => write!(f, "IO error: {}", e),
            StateError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            StateError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            StateError::CorruptedState => write!(f, "State file is corrupted"),
        }
    }
}

impl std::error::Error for StateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_state_save_load() {
        let mut state = AppState::new();
        state.add_file("/test/file.log".to_string(), 12345, 1000, 1234567890);
        state.update_file_position("/test/file.log".to_string(), 500);
        state.register_pending("/test/file.log", 12345, 500);
        state.mark_delivered("/test/file.log", 12345, 500);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Save state
        state.save_to_file(path).unwrap();

        // Load state
        let loaded_state = AppState::load_from_file(path).unwrap();

        assert_eq!(loaded_state.get_file_position("/test/file.log"), Some(500));
    }

    #[test]
    fn test_checkpoint_waits_for_contiguous_acknowledgements() {
        let mut state = AppState::new();
        state.add_file("/test/file.log".to_string(), 12345, 200, 1234567890);
        state.update_file_position("/test/file.log".to_string(), 200);
        state.register_pending("/test/file.log", 12345, 100);
        state.register_pending("/test/file.log", 12345, 200);
        state.mark_delivered("/test/file.log", 12345, 200);
        assert_eq!(
            state.clone_for_save().get_file_position("/test/file.log"),
            Some(0)
        );

        state.mark_delivered("/test/file.log", 99999, 100);
        assert_eq!(
            state.clone_for_save().get_file_position("/test/file.log"),
            Some(0)
        );

        state.mark_delivered("/test/file.log", 12345, 100);
        assert_eq!(
            state.clone_for_save().get_file_position("/test/file.log"),
            Some(200)
        );
    }

    #[test]
    fn test_rename_keeps_in_flight_acknowledgements() {
        let mut state = AppState::new();
        state.add_file("/old.log".to_string(), 42, 20, 1);
        let generation = state.file_generation("/old.log").unwrap();
        state.update_file_position("/old.log".to_string(), 20);
        state.register_pending("/old.log", 42, 10);
        state.register_pending("/old.log", 42, 20);

        state.rename_file("/old.log", "/new.log", 42);
        assert!(!state.files.contains_key("/old.log"));
        state.mark_source_delivered(&crate::domain::event::SourcePosition {
            path: "/old.log".to_string(),
            inode: 42,
            end: 20,
            generation,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/new.log"),
            Some(0)
        );
        state.mark_source_delivered(&crate::domain::event::SourcePosition {
            path: "/old.log".to_string(),
            inode: 42,
            end: 10,
            generation,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/new.log"),
            Some(20)
        );
    }

    #[test]
    fn test_removed_and_rotated_generations_keep_pending_acknowledgements() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 42, 10, 1);
        let old_generation = state.file_generation("/file.log").unwrap();
        state.register_pending("/file.log", 42, 10);
        state.remove_file("/file.log");
        assert!(!state.files.contains_key("/file.log"));

        state.add_file("/file.log".to_string(), 43, 10, 2);
        let new_generation = state.file_generation("/file.log").unwrap();
        assert_ne!(old_generation, new_generation);
        state.mark_source_delivered(&crate::domain::event::SourcePosition {
            path: "/file.log".to_string(),
            inode: 42,
            end: 10,
            generation: old_generation,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/file.log"),
            Some(0)
        );
        assert!(!state.pending.contains_key(&old_generation));
    }

    #[test]
    fn test_truncation_starts_new_generation_even_with_same_inode() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 42, 10, 1);
        let old_generation = state.file_generation("/file.log").unwrap();
        state.register_pending("/file.log", 42, 10);
        state.handle_file_truncation("/file.log");
        let new_generation = state.file_generation("/file.log").unwrap();
        assert_ne!(old_generation, new_generation);
        state.register_pending("/file.log", 42, 10);
        state.mark_source_delivered(&crate::domain::event::SourcePosition {
            path: "/file.log".to_string(),
            inode: 42,
            end: 10,
            generation: old_generation,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/file.log"),
            Some(0)
        );
        state.mark_source_delivered(&crate::domain::event::SourcePosition {
            path: "/file.log".to_string(),
            inode: 42,
            end: 10,
            generation: new_generation,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/file.log"),
            Some(10)
        );
    }

    #[test]
    fn test_legacy_checkpoint_loads_and_upgrades_generation() {
        let mut legacy = AppState::new();
        legacy.files.insert(
            "/legacy.log".to_string(),
            FileState {
                path: "/legacy.log".to_string(),
                position: 5,
                inode: 42,
                size: 10,
                last_modified: 1,
                generation: 0,
            },
        );
        legacy.committed.insert(0, 5);
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        legacy.save_to_file(path).unwrap();
        let serialized: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(
            serialized["files"]["/legacy.log"]
                .get("generation")
                .is_none()
        );
        assert!(serialized.get("next_generation").is_none());

        let upgraded = AppState::load_from_file(path).unwrap();
        let generation = upgraded.file_generation("/legacy.log").unwrap();
        assert!(generation > 0);
        assert_eq!(
            upgraded.clone_for_save().get_file_position("/legacy.log"),
            Some(5)
        );
        upgraded.save_to_file(path).unwrap();
        let reloaded = AppState::load_from_file(path).unwrap();
        assert_eq!(reloaded.file_generation("/legacy.log"), Some(generation));
    }

    #[test]
    fn test_prepared_snapshot_keeps_checkpoint_when_saved() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 42, 10, 1);
        state.register_pending("/file.log", 42, 10);
        state.update_file_position("/file.log".to_string(), 10);
        state.mark_delivered("/file.log", 42, 10);
        let snapshot = state.clone_for_save();
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        snapshot.save_to_file(path).unwrap();
        assert_eq!(
            AppState::load_from_file(path)
                .unwrap()
                .get_file_position("/file.log"),
            Some(10)
        );
    }

    #[test]
    fn test_legacy_source_cannot_ack_ambiguous_new_generation() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 42, 10, 1);
        state.register_pending("/file.log", 42, 10);
        state.mark_source_delivered(&SourcePosition {
            path: "/file.log".to_string(),
            inode: 42,
            end: 10,
            generation: 0,
        });
        assert_eq!(
            state.clone_for_save().get_file_position("/file.log"),
            Some(0)
        );
    }

    #[test]
    fn test_generation_counter_survives_removal_and_restart() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 42, 10, 1);
        let old_generation = state.file_generation("/file.log").unwrap();
        state.remove_file("/file.log");
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        state.save_to_file(path).unwrap();

        let mut restored = AppState::load_from_file(path).unwrap();
        restored.add_file("/file.log".to_string(), 42, 10, 1);
        assert!(restored.file_generation("/file.log").unwrap() > old_generation);
    }

    #[test]
    fn test_truncation_detection() {
        let mut state = AppState::new();
        state.add_file("/test/file.log".to_string(), 12345, 1000, 1234567890);
        state.update_file_position("/test/file.log".to_string(), 500);

        // File size smaller than position indicates truncation
        assert!(state.is_file_truncated("/test/file.log", 200));
        assert!(!state.is_file_truncated("/test/file.log", 600));
    }

    #[test]
    fn test_update_position_for_unknown_file() {
        let mut state = AppState::new();
        // Add one known file for the "Known files" log
        state.add_file("/known/file.log".to_string(), 11111, 500, 1234567890);

        // This should trigger the WARN log we added
        state.update_file_position("/unknown/file.log".to_string(), 100);

        // Position should not be updated for unknown file
        assert_eq!(state.get_file_position("/unknown/file.log"), None);
        // Known file should remain unchanged
        assert_eq!(state.get_file_position("/known/file.log"), Some(0));
    }

    #[test]
    fn test_update_file_position_with_fallback() {
        let mut state = AppState::new();

        // Test fallback creation for unknown file
        state.update_file_position_with_fallback("/new/file.log".to_string(), 250, 33333, 1000);

        // File should be automatically created
        assert_eq!(state.get_file_position("/new/file.log"), Some(250));
        assert!(state.files.contains_key("/new/file.log"));

        let file_state = state.files.get("/new/file.log").unwrap();
        assert_eq!(file_state.inode, 33333);
        assert_eq!(file_state.size, 1000);
        assert_eq!(file_state.position, 250);
    }

    #[test]
    fn test_update_file_position_existing_vs_unknown() {
        let mut state = AppState::new();
        state.add_file("/existing.log".to_string(), 12345, 1000, 1234567890);

        // Update existing file - should work normally
        state.update_file_position("/existing.log".to_string(), 100);
        assert_eq!(state.get_file_position("/existing.log"), Some(100));

        // Update unknown file - should trigger warning but not crash
        state.update_file_position("/missing.log".to_string(), 200);
        assert_eq!(state.get_file_position("/missing.log"), None);

        // Original file should be unaffected
        assert_eq!(state.get_file_position("/existing.log"), Some(100));
    }

    #[test]
    fn test_load_from_backup_when_main_corrupted() {
        let mut state = AppState::new();
        state.add_file("/file.log".to_string(), 1, 10, 100);
        state.update_file_position("/file.log".to_string(), 5);
        state.register_pending("/file.log", 1, 5);
        state.mark_delivered("/file.log", 1, 5);

        let main = NamedTempFile::new().unwrap();
        let main_path = main.path().to_str().unwrap().to_string();
        let backup_path = format!("{}.backup", main_path);

        // Write valid backup
        state.save_to_file(&backup_path).unwrap();

        // Corrupt main
        std::fs::write(&main_path, b"{not_json}").unwrap();

        // Load should fallback to backup
        let loaded = AppState::load_from_file(&main_path).unwrap();
        assert_eq!(loaded.get_file_position("/file.log"), Some(5));
    }

    #[test]
    fn test_new_state_when_both_main_and_backup_corrupted() {
        let main = NamedTempFile::new().unwrap();
        let main_path = main.path().to_str().unwrap().to_string();
        let backup_path = format!("{}.backup", main_path);

        // Corrupt both files
        std::fs::write(&main_path, b"{broken}").unwrap();
        std::fs::write(&backup_path, b"{also_broken}").unwrap();

        let loaded = AppState::load_from_file(&main_path).unwrap();
        // Expect new empty state
        assert!(loaded.files.is_empty());
    }

    #[test]
    fn test_checksum_consistency_across_multiple_serializations() {
        // Create a state with multiple files to increase chances of HashMap order variation
        let mut state = AppState::new();
        state.add_file(
            "/var/log/pods/ns1_pod1_uid1/container1/0.log".to_string(),
            1001,
            500,
            1000,
        );
        state.add_file(
            "/var/log/pods/ns2_pod2_uid2/container2/0.log".to_string(),
            1002,
            600,
            2000,
        );
        state.add_file(
            "/var/log/pods/ns3_pod3_uid3/container3/0.log".to_string(),
            1003,
            700,
            3000,
        );
        state.add_file(
            "/var/log/pods/ns4_pod4_uid4/container4/0.log".to_string(),
            1004,
            800,
            4000,
        );
        state.add_file(
            "/var/log/pods/ns5_pod5_uid5/container5/0.log".to_string(),
            1005,
            900,
            5000,
        );

        // Update positions to make data more complex
        state.update_file_position(
            "/var/log/pods/ns1_pod1_uid1/container1/0.log".to_string(),
            100,
        );
        state.update_file_position(
            "/var/log/pods/ns2_pod2_uid2/container2/0.log".to_string(),
            200,
        );
        state.update_file_position(
            "/var/log/pods/ns3_pod3_uid3/container3/0.log".to_string(),
            300,
        );

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Serialize and calculate checksum multiple times
        let mut checksums = Vec::new();
        for _ in 0..10 {
            // Save and reload to force serialization/deserialization
            state.save_to_file(path).unwrap();
            let reloaded_state = AppState::load_from_file(path).unwrap();

            // Extract the checksum
            if let Some(checksum) = reloaded_state.checksum {
                checksums.push(checksum);
            }
        }

        // All checksums should be identical for the same data
        assert!(!checksums.is_empty(), "No checksums were generated");
        let first_checksum = &checksums[0];
        for (i, checksum) in checksums.iter().enumerate() {
            assert_eq!(
                checksum, first_checksum,
                "Checksum mismatch at iteration {}: expected '{}', got '{}'",
                i, first_checksum, checksum
            );
        }
    }

    #[test]
    fn test_checksum_calculation_deterministic() {
        // Test that the same state produces the same checksum when serialized multiple times
        let mut state = AppState::new();
        state.add_file("/test/file1.log".to_string(), 1, 100, 1000);
        state.add_file("/test/file2.log".to_string(), 2, 200, 2000);
        state.add_file("/test/file3.log".to_string(), 3, 300, 3000);

        // Calculate checksum multiple times
        let content1 = AppState::create_content_without_checksum(&state).unwrap();
        let content2 = AppState::create_content_without_checksum(&state).unwrap();
        let content3 = AppState::create_content_without_checksum(&state).unwrap();

        assert_eq!(content1, content2, "Content should be deterministic");
        assert_eq!(content2, content3, "Content should be deterministic");

        let checksum1 = AppState::calculate_checksum(&content1);
        let checksum2 = AppState::calculate_checksum(&content2);
        let checksum3 = AppState::calculate_checksum(&content3);

        assert_eq!(checksum1, checksum2, "Checksums should be identical");
        assert_eq!(checksum2, checksum3, "Checksums should be identical");
    }

    #[test]
    fn test_checksum_matches_sha256_hex() {
        assert_eq!(
            AppState::calculate_checksum("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_checksum_verification_on_load() {
        let mut state = AppState::new();
        state.add_file("/test/file.log".to_string(), 12345, 1000, 1234567890);
        state.update_file_position("/test/file.log".to_string(), 500);
        state.register_pending("/test/file.log", 12345, 500);
        state.mark_delivered("/test/file.log", 12345, 500);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Save state with checksum
        state.save_to_file(path).unwrap();

        // Load and verify checksum passes
        let loaded_state = AppState::load_from_file(path).unwrap();
        assert_eq!(loaded_state.get_file_position("/test/file.log"), Some(500));
        assert!(
            loaded_state.checksum.is_some(),
            "Checksum should be present"
        );

        // Read the original content for debugging
        let original_content = std::fs::read_to_string(path).unwrap();
        let original_parsed: serde_json::Value = serde_json::from_str(&original_content).unwrap();
        let original_checksum = original_parsed["checksum"].as_str().unwrap();

        // Manually corrupt the file by changing data but keeping the original checksum
        let mut modified: serde_json::Value = serde_json::from_str(&original_content).unwrap();
        if let Some(files) = modified["files"].as_object_mut()
            && let Some(file_state) = files.values_mut().next()
            && let Some(position) = file_state.get_mut("position")
        {
            *position = serde_json::Value::Number(serde_json::Number::from(999));
        }
        // Keep the original checksum - this should cause a mismatch
        modified["checksum"] = serde_json::Value::String(original_checksum.to_string());

        std::fs::write(path, serde_json::to_string_pretty(&modified).unwrap()).unwrap();

        // Load should fail due to checksum mismatch
        match AppState::load_from_file(path) {
            Err(StateError::ChecksumMismatch) => {
                // This is expected - checksum should detect the modification
            }
            Ok(_) => panic!("Expected checksum mismatch error"),
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }
}
