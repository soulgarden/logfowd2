use std::collections::HashMap;
use std::fs;
use std::path::Path;

use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub path: String,
    pub position: u64,
    pub inode: u64,
    pub size: u64,
    pub last_modified: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppState {
    pub files: HashMap<String, FileState>,
    pub version: u32,
    pub checksum: Option<String>,
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

        // Update checksum to current format
        state.checksum = Some(Self::calculate_checksum(&content));
        Ok(state)
    }

    pub fn save_to_file(&self, state_file_path: &str) -> Result<(), StateError> {
        self.save_to_file_atomic(state_file_path)
    }

    pub fn save_to_file_atomic(&self, state_file_path: &str) -> Result<(), StateError> {
        // Create a copy with updated checksum
        let mut state_copy = self.clone();

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
            };
            self.files.insert(file_path.clone(), file_state);
            debug!(
                "Created fallback FileState for {}: position {}",
                file_path, position
            );
        }
    }

    pub fn add_file(&mut self, file_path: String, inode: u64, size: u64, last_modified: u64) {
        let file_state = FileState {
            path: file_path.clone(),
            position: 0,
            inode,
            size,
            last_modified,
        };

        self.files.insert(file_path.clone(), file_state);
        debug!("Added file to state: {}", file_path);
    }

    pub fn remove_file(&mut self, file_path: &str) {
        if self.files.remove(file_path).is_some() {
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
        if let Some(file_state) = self.files.get_mut(file_path) {
            warn!(
                "File truncated detected: {}, resetting position from {} to 0",
                file_path, file_state.position
            );
            file_state.position = 0;
        }
    }

    pub fn should_reopen_file(&self, file_path: &str, current_inode: u64) -> bool {
        if let Some(file_state) = self.files.get(file_path) {
            return file_state.inode != current_inode;
        }
        true // Unknown file, should open
    }

    fn create_content_without_checksum(state: &AppState) -> Result<String, StateError> {
        let mut state_copy = state.clone();
        state_copy.checksum = None;
        serde_json::to_string_pretty(&state_copy).map_err(StateError::SerializationError)
    }

    fn calculate_checksum(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        format!("{:x}", hasher.finalize())
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

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Save state
        state.save_to_file(path).unwrap();

        // Load state
        let loaded_state = AppState::load_from_file(path).unwrap();

        assert_eq!(loaded_state.get_file_position("/test/file.log"), Some(500));
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

        let main = NamedTempFile::new().unwrap();
        let main_path = main.path().to_str().unwrap().to_string();
        let backup_path = format!("{}.backup", &main_path);

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
        let backup_path = format!("{}.backup", &main_path);

        // Corrupt both files
        std::fs::write(&main_path, b"{broken}").unwrap();
        std::fs::write(&backup_path, b"{also_broken}").unwrap();

        let loaded = AppState::load_from_file(&main_path).unwrap();
        // Expect new empty state
        assert!(loaded.files.is_empty());
    }
}
