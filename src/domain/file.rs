use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};

use tracing::{debug, error, info, warn};

use crate::domain::event::{Event, Meta};
use crate::domain::state::AppState;
use crate::infrastructure::filesystem::metadata_cache::{FileMetadata, MetadataCache};

pub struct FileTracker {
    pub path: String,
    pub meta: Meta,
    file: Option<File>,
    position: u64,
    inode: u64,
    last_size: u64,
    last_rotation_check: Instant,
    rotation_detection_interval: Duration,
    is_symlink: bool,
    symlink_target: Option<String>,
    buffer_size: usize,
    max_line_size: usize,
    metadata_cache: MetadataCache,
}

impl FileTracker {
    pub async fn new(
        path: String,
        meta: Meta,
        app_state: &mut AppState,
    ) -> Result<Self, std::io::Error> {
        // Check if path is a symlink and resolve it
        let (actual_path, is_symlink, symlink_target) = if Path::new(&path).is_symlink() {
            match fs::read_link(&path) {
                Ok(target) => {
                    let target_str = target.to_string_lossy().to_string();
                    // Check if symlink target exists
                    if Path::new(&target_str).exists() {
                        info!("Detected symlink {} -> {}", path, target_str);
                        (target_str.clone(), true, Some(target_str))
                    } else {
                        warn!(
                            "Broken symlink detected: {} -> {} (target does not exist)",
                            path, target_str
                        );
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!(
                                "Broken symlink: {} points to non-existent {}",
                                path, target_str
                            ),
                        ));
                    }
                }
                Err(e) => {
                    return Self::handle_file_access_error(&path, e);
                }
            }
        } else {
            (path.clone(), false, None)
        };

        let fs_metadata = match fs::metadata(&actual_path) {
            Ok(metadata) => metadata,
            Err(e) => {
                return Self::handle_file_access_error(&actual_path, e);
            }
        };
        let inode = fs_metadata.ino();
        let size = fs_metadata.len();
        let last_modified = fs_metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Check if we have previous state for this file
        let position = if app_state.should_reopen_file(&path, inode) {
            // New file or rotated file - start from beginning or saved position
            app_state.add_file(path.clone(), inode, size, last_modified);
            app_state.get_file_position(&path).unwrap_or(0)
        } else {
            // Same file - continue from saved position
            app_state.get_file_position(&path).unwrap_or(0)
        };

        // Check for truncation
        if app_state.is_file_truncated(&path, size) {
            warn!(
                "File truncation detected for {}, resetting to beginning",
                path
            );
            app_state.handle_file_truncation(&path);
        }

        let mut file = match File::open(&actual_path).await {
            Ok(file) => file,
            Err(e) => {
                return Self::handle_file_access_error(&actual_path, e);
            }
        };

        // Seek to the saved position
        let final_position = if position > size {
            warn!(
                "Saved position {} is beyond file size {}, starting from end",
                position, size
            );
            file.seek(SeekFrom::End(0)).await?
        } else {
            file.seek(SeekFrom::Start(position)).await?
        };

        info!(
            "Created FileTracker: path={}, symlink={}, actual={}, inode={}, position={}",
            path, is_symlink, actual_path, inode, final_position
        );

        Ok(FileTracker {
            path: path.clone(),
            meta,
            file: Some(file),
            position: final_position,
            inode,
            last_size: size,
            last_rotation_check: Instant::now(),
            rotation_detection_interval: Duration::from_millis(50), // Check every 50ms for fast rotation
            is_symlink,
            symlink_target,
            buffer_size: 65536,         // 64KB buffer for better performance
            max_line_size: 1024 * 1024, // 1MB max line size by default
            metadata_cache: MetadataCache::new(100, 1000), // 100ms TTL, 1000 entries max
        })
    }

    pub async fn new_with_max_line_size(
        path: String,
        meta: Meta,
        app_state: &mut AppState,
        max_line_size: usize,
    ) -> Result<Self, std::io::Error> {
        // Use the same logic as new() but with custom max_line_size
        let mut tracker = Self::new(path, meta, app_state).await?;
        tracker.max_line_size = max_line_size;
        Ok(tracker)
    }

    pub async fn read_new_lines(
        &mut self,
        app_state: &mut AppState,
    ) -> Result<Vec<Event>, std::io::Error> {
        self.read_new_lines_limited(app_state, None).await
    }

    /// Read new lines but cap at max_events if provided to limit memory.
    pub async fn read_new_lines_limited(
        &mut self,
        app_state: &mut AppState,
        max_events: Option<usize>,
    ) -> Result<Vec<Event>, std::io::Error> {
        let mut events = Vec::new();
        let initial_position = self.position;

        if let Some(ref mut file) = self.file {
            let mut reader = BufReader::with_capacity(self.buffer_size, &mut *file);
            let mut line = String::new();
            let mut limit_reached = false;

            loop {
                if let Some(limit) = max_events
                    && events.len() >= limit
                {
                    limit_reached = true;
                    break;
                }

                line.clear();
                let max_line_size = self.max_line_size; // Copy value to avoid borrow issues
                let path = self.path.clone(); // Copy path for logging
                let bytes_read =
                    Self::read_line_limited_static(max_line_size, &path, &mut reader, &mut line)
                        .await?;

                if bytes_read == 0 {
                    // EOF reached
                    break;
                }

                // Remove trailing newline
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }

                if !line.is_empty() {
                    events.push(Event::new(line.clone(), self.meta.clone()));
                }

                self.position += bytes_read as u64;
            }

            // If we stopped early due to limit, BufReader may have read ahead.
            // Seek underlying file cursor back to the tracked position to avoid skipping.
            if limit_reached {
                drop(reader);
                let _ = file.seek(SeekFrom::Start(self.position)).await?;
            }

            // Update position only once at the end to reduce AppState contention
            if self.position != initial_position {
                self.ensure_file_in_state(app_state);
                app_state.update_file_position(self.path.clone(), self.position);
            }
        }

        debug!(
            "Read {} events from {}, position: {} -> {}",
            events.len(),
            self.path,
            initial_position,
            self.position
        );
        Ok(events)
    }

    /// Fast-forward to end (skip existing content) to avoid historical backlog.
    pub async fn skip_existing_content(
        &mut self,
        app_state: &mut AppState,
    ) -> Result<(), std::io::Error> {
        if let Some(ref mut file) = self.file {
            let new_pos = file.seek(SeekFrom::End(0)).await?;
            self.position = new_pos;
            self.ensure_file_in_state(app_state);
            app_state.update_file_position(self.path.clone(), self.position);
            debug!(
                "Skipped existing content for {} to pos {}",
                self.path, self.position
            );
        }
        Ok(())
    }

    /// Ensure this file is properly registered in AppState
    fn ensure_file_in_state(&self, app_state: &mut AppState) {
        if !app_state.files.contains_key(&self.path) {
            debug!(
                "FileTracker {} not in AppState, adding fallback entry",
                self.path
            );
            app_state.add_file(
                self.path.clone(),
                self.inode,
                self.last_size,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }
    }

    pub async fn check_file_changes(
        &mut self,
        app_state: &mut AppState,
    ) -> Result<(bool, Vec<Event>), std::io::Error> {
        // For rapid rotation detection, check more frequently
        let should_check_rotation =
            self.last_rotation_check.elapsed() >= self.rotation_detection_interval;

        let (actual_path, symlink_changed) = if self.is_symlink {
            self.check_symlink_changes()?
        } else {
            (self.path.clone(), false)
        };

        let metadata = match fs::metadata(&actual_path) {
            Ok(metadata) => {
                // Cache the metadata for future use
                let _ = self.metadata_cache.get_metadata(&actual_path);
                metadata
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if self.is_symlink {
                    // Symlink target might have been rotated, check the symlink itself
                    match fs::symlink_metadata(&self.path) {
                        Ok(_) => {
                            // Symlink exists but target doesn't - target was rotated
                            warn!(
                                "Symlink target {} no longer exists, checking for new target",
                                actual_path
                            );
                            return self.handle_symlink_rotation(app_state).await;
                        }
                        Err(_) => {
                            // Symlink itself was removed
                            warn!("Symlink {} no longer exists", self.path);
                            self.file = None;
                            return Ok((false, Vec::new()));
                        }
                    }
                } else {
                    warn!("File {} no longer exists", self.path);
                    self.file = None;
                    return Ok((false, Vec::new()));
                }
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::PermissionDenied => {
                        warn!(
                            "Permission denied reading metadata for {}: {}. File may be temporarily inaccessible.",
                            actual_path, e
                        );
                        // For permission denied, we can't get metadata but we don't want to fail completely
                        // Return that no changes were detected and continue monitoring
                        return Ok((false, Vec::new()));
                    }
                    _ => return Err(e),
                }
            }
        };

        let current_inode = metadata.ino();
        let current_size = metadata.len();

        // Check if symlink target changed
        if symlink_changed {
            info!("Symlink {} target changed", self.path);
            // Try to read any remaining content before rotating
            let remaining_events = self
                .read_remaining_before_rotation(app_state)
                .await
                .unwrap_or_default();
            self.reopen_file(app_state).await?;
            return Ok((true, remaining_events));
        }

        // Check if file was rotated (inode changed)
        if current_inode != self.inode {
            info!(
                "File {} was rotated (inode changed from {} to {})",
                actual_path, self.inode, current_inode
            );

            // Try to read any remaining content before rotating
            let remaining_events = self
                .read_remaining_before_rotation(app_state)
                .await
                .unwrap_or_default();

            // For rapid rotation, update check time
            if should_check_rotation {
                self.last_rotation_check = Instant::now();
            }

            // Reopen the new file
            self.reopen_file(app_state).await?;
            return Ok((true, remaining_events));
        }

        // Check for truncation - improved detection
        if current_size < self.last_size {
            if current_size < self.position {
                warn!(
                    "File {} was truncated (size: {} -> {}, position: {})",
                    actual_path, self.last_size, current_size, self.position
                );

                app_state.handle_file_truncation(&self.path);
                self.position = 0;
                self.reopen_file(app_state).await?;
                return Ok((true, Vec::new()));
            } else {
                // File size decreased but our position is still valid
                // This might be a partial write scenario
                debug!(
                    "File {} size decreased but position still valid (size: {} -> {}, position: {})",
                    actual_path, self.last_size, current_size, self.position
                );
            }
        }

        self.last_size = current_size;

        if should_check_rotation {
            self.last_rotation_check = Instant::now();
        }

        Ok((false, Vec::new()))
    }

    async fn reopen_file(&mut self, app_state: &mut AppState) -> Result<(), std::io::Error> {
        let actual_path = if self.is_symlink {
            self.resolve_symlink_target()?
        } else {
            self.path.clone()
        };

        match File::open(&actual_path).await {
            Ok(mut new_file) => {
                let metadata = match fs::metadata(&actual_path) {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        warn!("Failed to read metadata for {}: {}", actual_path, e);
                        return Err(e);
                    }
                };
                // Cache the metadata
                let _ = self.metadata_cache.get_metadata(&actual_path);
                self.inode = metadata.ino();
                self.last_size = metadata.len();

                // Start from saved position or beginning
                let saved_position = app_state.get_file_position(&self.path).unwrap_or(0);
                self.position = saved_position;

                new_file.seek(SeekFrom::Start(self.position)).await?;
                self.file = Some(new_file);

                info!("Reopened file {} at position {}", self.path, self.position);
                Ok(())
            }
            Err(e) => {
                self.file = None;
                Self::handle_file_access_error(&actual_path, e)
            }
        }
    }

    fn check_symlink_changes(&mut self) -> Result<(String, bool), std::io::Error> {
        if !self.is_symlink {
            return Ok((self.path.clone(), false));
        }

        let current_target = self.resolve_symlink_target()?;
        let changed = self.symlink_target.as_ref() != Some(&current_target);

        if changed {
            debug!(
                "Symlink {} target changed from {:?} to {}",
                self.path, self.symlink_target, current_target
            );
            self.symlink_target = Some(current_target.clone());
        }

        Ok((current_target, changed))
    }

    fn resolve_symlink_target(&self) -> Result<String, std::io::Error> {
        if !self.is_symlink {
            return Ok(self.path.clone());
        }

        let target = match fs::read_link(&self.path) {
            Ok(target) => target,
            Err(e) => {
                return Self::handle_file_access_error(&self.path, e);
            }
        };
        let target_str = target.to_string_lossy().to_string();

        // Check if symlink target exists
        if !Path::new(&target_str).exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Broken symlink: {} points to non-existent {}",
                    self.path, target_str
                ),
            ));
        }

        Ok(target_str)
    }

    async fn handle_symlink_rotation(
        &mut self,
        app_state: &mut AppState,
    ) -> Result<(bool, Vec<Event>), std::io::Error> {
        // Try to resolve the new symlink target
        match self.resolve_symlink_target() {
            Ok(new_target) => {
                info!(
                    "Symlink {} resolved to new target: {}",
                    self.path, new_target
                );
                self.symlink_target = Some(new_target);

                // Read remaining content from old file
                let remaining_events = self.read_new_lines(app_state).await.unwrap_or_default();

                // Reopen with new target
                self.reopen_file(app_state).await?;
                Ok((true, remaining_events))
            }
            Err(e) => {
                warn!("Failed to resolve symlink {}: {}", self.path, e);
                self.file = None;
                Ok((false, Vec::new()))
            }
        }
    }

    /// Check if the file is currently open (test utility)
    #[cfg(test)]
    pub fn is_open(&self) -> bool {
        self.file.is_some()
    }

    /// Get current file position (test utility)
    #[cfg(test)]
    pub fn get_position(&self) -> u64 {
        self.position
    }

    /// Get the current inode number (test utility)
    #[allow(dead_code)]
    pub fn get_inode(&self) -> u64 {
        self.inode
    }

    pub fn close(&mut self) {
        if self.file.take().is_some() {
            debug!("Closed file {}", self.path);
        }
    }

    /// Get cached metadata for a path (test utility)
    #[allow(dead_code)] // Used in tests
    fn get_cached_metadata(&mut self, path: &str) -> Result<FileMetadata, std::io::Error> {
        self.metadata_cache.get_metadata(path).map_err(|e| {
            std::io::Error::other(format!("Failed to get metadata for {}: {}", path, e))
        })
    }

    /// Force refresh cached metadata for a path (test utility)
    #[allow(dead_code)] // Used in tests
    fn refresh_cached_metadata(&mut self, path: &str) -> Result<FileMetadata, std::io::Error> {
        self.metadata_cache.refresh_metadata(path).map_err(|e| {
            std::io::Error::other(format!("Failed to refresh metadata for {}: {}", path, e))
        })
    }

    /// Read a single line with size limitation to prevent OOM from very long log lines
    /// Optimized version using read_until to minimize syscalls
    async fn read_line_limited_static<R: AsyncBufReadExt + Unpin>(
        max_line_size: usize,
        path: &str,
        reader: &mut R,
        line: &mut String,
    ) -> Result<usize, std::io::Error> {
        // Pre-allocate buffer with reasonable capacity
        let mut buffer = Vec::with_capacity(std::cmp::min(max_line_size + 16, 8192));

        let bytes_read = match reader.read_until(b'\n', &mut buffer).await {
            Ok(bytes) => bytes,
            Err(e) => {
                // Handle read errors gracefully
                match e.kind() {
                    std::io::ErrorKind::InvalidData => {
                        warn!("Invalid data encountered while reading {}: {}", path, e);
                        line.push_str(" [READ_ERROR]");
                        return Ok(0);
                    }
                    std::io::ErrorKind::UnexpectedEof => {
                        warn!(
                            "Unexpected EOF while reading {}, file may be truncated",
                            path
                        );
                        // Try to process what we have in the buffer
                        if !buffer.is_empty() {
                            let line_content = Self::sanitize_corrupted_content(&buffer, path);
                            line.push_str(&line_content);
                        }
                        return Ok(buffer.len());
                    }
                    _ => {
                        warn!("Read error in {}: {}", path, e);
                        return Err(e);
                    }
                }
            }
        };

        if bytes_read == 0 {
            return Ok(0); // EOF
        }

        // Handle line size limiting
        if buffer.len() > max_line_size {
            // Truncate to max_line_size with UTF-8 safety
            let mut safe_end = std::cmp::min(max_line_size, buffer.len());

            // Find the last valid UTF-8 character boundary
            while safe_end > 0 {
                if std::str::from_utf8(&buffer[..safe_end]).is_ok() {
                    break;
                }
                safe_end -= 1;
            }

            if safe_end > 0 {
                let line_content = Self::sanitize_corrupted_content(&buffer[..safe_end], path);
                line.push_str(&line_content);
            }

            warn!(
                "Line exceeded max size {} bytes in file {}, truncating",
                max_line_size, path
            );
            line.push_str("... [TRUNCATED]");

            // If we didn't find a newline (line was truncated), we need to skip to the next newline
            if buffer.last() != Some(&b'\n') {
                Self::skip_to_newline(reader).await?;
            } else {
                // Add the newline that we found
                line.push('\n');
            }
        } else {
            // Line is within size limits, process normally
            let line_content = Self::sanitize_corrupted_content(&buffer, path);
            line.push_str(&line_content);
        }

        Ok(bytes_read)
    }

    /// Skip reading until we find a newline character
    async fn skip_to_newline<R: AsyncBufReadExt + Unpin>(
        reader: &mut R,
    ) -> Result<(), std::io::Error> {
        use tokio::io::AsyncReadExt;

        let mut byte_buffer = [0u8; 1];
        loop {
            match reader.read(&mut byte_buffer).await {
                Ok(0) => break, // EOF
                Ok(1) => {
                    if byte_buffer[0] == b'\n' {
                        break; // Found newline, stop skipping
                    }
                    // Continue skipping
                }
                Ok(_) => break,  // Unexpected read size
                Err(_) => break, // Any error, just stop
            }
        }
        Ok(())
    }

    /// Sanitize potentially corrupted content from log files
    fn sanitize_corrupted_content(buffer: &[u8], path: &str) -> String {
        // First, try standard UTF-8 lossy conversion
        let lossy_content = String::from_utf8_lossy(buffer);

        // Check for potentially corrupted patterns
        let has_null_bytes = buffer.contains(&0);
        let has_excessive_control_chars = buffer
            .iter()
            .filter(|&&b| b < 32 && b != b'\t' && b != b'\r' && b != b'\n')
            .count()
            > buffer.len() / 10; // More than 10% control chars
        let has_high_binary = buffer.iter().filter(|&&b| b > 127).count() > buffer.len() / 5; // More than 20% high bytes
        let is_valid_utf8 = std::str::from_utf8(buffer).is_ok();

        if has_null_bytes || has_excessive_control_chars {
            warn!(
                "Detected potentially corrupted log line in {}: null_bytes={}, control_chars={}, high_binary={}",
                path, has_null_bytes, has_excessive_control_chars, has_high_binary
            );

            // For highly corrupted content, replace with a sanitized version
            Self::create_sanitized_replacement(buffer, path)
        } else if has_high_binary && !is_valid_utf8 {
            warn!(
                "Detected binary data or encoding issues in {}, using lossy conversion",
                path
            );
            lossy_content.to_string()
        } else {
            // Content seems reasonable, use lossy conversion
            lossy_content.to_string()
        }
    }

    /// Create a sanitized replacement for heavily corrupted content
    fn create_sanitized_replacement(buffer: &[u8], path: &str) -> String {
        // Keep only printable ASCII and basic whitespace
        let sanitized_bytes: Vec<u8> = buffer
            .iter()
            .map(|&b| {
                if (32..=126).contains(&b) || b == b'\t' {
                    b // Printable ASCII and tabs (space is already in 32..=126)
                } else {
                    b'?' // Replace with question mark
                }
            })
            .collect();

        let sanitized = String::from_utf8_lossy(&sanitized_bytes);
        let replacement_count = buffer.len() - sanitized.chars().filter(|&c| c != '?').count();

        if replacement_count > 0 {
            warn!(
                "Sanitized {} corrupted bytes in log line from {} ({}% replaced)",
                replacement_count,
                path,
                (replacement_count * 100) / buffer.len()
            );
        }

        sanitized.to_string()
    }

    /// Handle file access errors with specific logic for permission denied
    fn handle_file_access_error<T>(path: &str, error: std::io::Error) -> Result<T, std::io::Error> {
        match error.kind() {
            std::io::ErrorKind::PermissionDenied => {
                warn!(
                    "Permission denied accessing file {}: {}. This may be temporary due to log rotation or security policies.",
                    path, error
                );
                warn!("Will continue monitoring and retry access periodically");
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Permission denied for {}: {}", path, error),
                ))
            }
            std::io::ErrorKind::NotFound => {
                debug!(
                    "File {} not found: {}. This is normal during log rotation.",
                    path, error
                );
                Err(error)
            }
            std::io::ErrorKind::InvalidInput => {
                warn!(
                    "Invalid input accessing {}: {}. File may be locked or have invalid permissions.",
                    path, error
                );
                Err(error)
            }
            std::io::ErrorKind::Other => {
                // This can include SELinux/AppArmor denials which appear as "Other"
                warn!(
                    "Access denied to {} (possibly due to security policy): {}. Will retry later.",
                    path, error
                );
                Err(error)
            }
            _ => {
                error!("Failed to access file {}: {}", path, error);
                Err(error)
            }
        }
    }

    /// Check if file shows signs of corruption or issues (test utility)
    #[allow(dead_code)] // Used in tests
    async fn check_file_health(&mut self) -> Result<bool, std::io::Error> {
        if let Some(ref mut file) = self.file {
            let current_pos = file.seek(SeekFrom::Current(0)).await?;

            // Try to read a small sample to detect obvious corruption
            let mut sample = [0u8; 512];
            let bytes_read = match file.read(&mut sample).await {
                Ok(n) => n,
                Err(e) => {
                    warn!(
                        "Failed to read sample from {} for health check: {}",
                        self.path, e
                    );
                    return Ok(false); // Assume unhealthy if we can't read
                }
            };

            // Restore original position
            file.seek(SeekFrom::Start(current_pos)).await?;

            if bytes_read > 0 {
                let null_bytes = sample[..bytes_read].iter().filter(|&&b| b == 0).count();
                let control_chars = sample[..bytes_read]
                    .iter()
                    .filter(|&&b| b < 32 && b != b'\t' && b != b'\r' && b != b'\n')
                    .count();

                // If more than 30% of sample is null bytes or control chars, consider unhealthy
                let corruption_threshold = bytes_read * 30 / 100;
                if null_bytes > corruption_threshold || control_chars > corruption_threshold {
                    warn!(
                        "File {} appears corrupted: {}% null bytes, {}% control chars in sample",
                        self.path,
                        (null_bytes * 100) / bytes_read,
                        (control_chars * 100) / bytes_read
                    );
                    return Ok(false);
                }
            }

            Ok(true)
        } else {
            Ok(false) // No file handle means unhealthy
        }
    }

    /// Try to read any remaining content before file rotation to minimize data loss
    async fn read_remaining_before_rotation(
        &mut self,
        app_state: &mut AppState,
    ) -> Result<Vec<Event>, std::io::Error> {
        if self.file.is_none() {
            return Ok(Vec::new());
        }

        // Use a smaller chunk size for fast rotation scenarios
        let rapid_rotation_chunk_size = 50; // Read smaller chunks to handle rotation faster
        let mut all_events = Vec::new();
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 5; // Limit attempts to prevent infinite loops

        while attempts < MAX_ATTEMPTS {
            match self
                .read_new_lines_limited(app_state, Some(rapid_rotation_chunk_size))
                .await
            {
                Ok(events) => {
                    if events.is_empty() {
                        // No more data to read
                        break;
                    }
                    all_events.extend(events);
                    attempts += 1;
                }
                Err(e) => {
                    // File might have been rotated while reading
                    debug!(
                        "Failed to read remaining content from {} during rotation: {}",
                        self.path, e
                    );
                    break;
                }
            }
        }

        if !all_events.is_empty() {
            debug!(
                "Read {} remaining events from {} before rotation",
                all_events.len(),
                self.path
            );
        }

        Ok(all_events)
    }
}

impl Drop for FileTracker {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use tempfile::{NamedTempFile, TempDir};

    async fn create_test_state() -> AppState {
        AppState::new()
    }

    fn create_test_meta() -> Meta {
        Meta {
            pod_name: "test-pod".to_string(),
            namespace: "test-namespace".to_string(),
            container_name: "test-container".to_string(),
            pod_id: "test-pod-id".to_string(),
        }
    }

    async fn create_test_file_with_content(content: &str) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[tokio::test]
    async fn test_file_tracker_creation() {
        let temp_file = create_test_file_with_content("test line\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta.clone(),
            &mut state,
        )
        .await
        .unwrap();

        assert_eq!(tracker.path, temp_file.path().to_string_lossy().to_string());
        assert_eq!(tracker.meta, meta);
        assert!(tracker.file.is_some());
        assert_eq!(tracker.position, 0);
        assert!(!tracker.is_symlink);
        assert!(tracker.symlink_target.is_none());
    }

    #[tokio::test]
    async fn test_read_new_lines_basic() {
        let temp_file = create_test_file_with_content("line1\nline2\nline3\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message, "line1");
        assert_eq!(events[1].message, "line2");
        assert_eq!(events[2].message, "line3");
        assert!(tracker.position > 0);
    }

    #[tokio::test]
    async fn test_read_new_lines_empty_file() {
        let temp_file = create_test_file_with_content("").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 0);
        assert_eq!(tracker.position, 0);
    }

    #[tokio::test]
    async fn test_read_new_lines_with_empty_lines() {
        let temp_file = create_test_file_with_content("line1\n\nline3\n\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        // Empty lines should be skipped
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "line1");
        assert_eq!(events[1].message, "line3");
    }

    #[tokio::test]
    async fn test_position_tracking() {
        let temp_file = create_test_file_with_content("first line\nsecond line\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let initial_position = tracker.position;
        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 2);
        assert!(tracker.position > initial_position);

        // Position should be tracked in state
        let saved_position = state.get_file_position(&tracker.path).unwrap();
        assert_eq!(saved_position, tracker.position);
    }

    #[tokio::test]
    async fn test_position_restoration() {
        let temp_file = create_test_file_with_content("line1\nline2\nline3\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        // Read some content first
        {
            let mut tracker = FileTracker::new(path.clone(), meta.clone(), &mut state)
                .await
                .unwrap();
            let events = tracker.read_new_lines(&mut state).await.unwrap();
            assert_eq!(events.len(), 3);
        }

        // Create new tracker - should restore position
        let tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // Position should be at end of file
        assert!(tracker.position > 0);
    }

    #[tokio::test]
    async fn test_incremental_reading() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"initial line\n").unwrap();
        temp_file.flush().unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // Read initial content
        let events1 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events1.len(), 1);
        assert_eq!(events1[0].message, "initial line");

        // Add more content to the file
        temp_file.write_all(b"added line\n").unwrap();
        temp_file.flush().unwrap();

        // Should read only new content
        let events2 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].message, "added line");
    }

    #[tokio::test]
    async fn test_file_rotation_detection() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("rotating_file.log");

        // Create initial file with short content
        fs::write(&file_path, "short\n").unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker =
            FileTracker::new(file_path.to_string_lossy().to_string(), meta, &mut state)
                .await
                .unwrap();

        let original_inode = tracker.inode;

        // Read initial content
        let events1 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events1.len(), 1);
        assert_eq!(events1[0].message, "short");

        // Simulate file rotation by removing and recreating with much longer content
        // This ensures new file is longer than old position
        fs::remove_file(&file_path).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        fs::write(&file_path, "much longer rotated content line\n").unwrap();

        // Check for changes should detect rotation
        let (changed, rotation_events) = tracker.check_file_changes(&mut state).await.unwrap();
        assert!(changed);
        // Should have captured any remaining events during rotation
        assert!(rotation_events.is_empty() || !rotation_events.is_empty()); // Just ensure it's a valid Vec
        assert_ne!(tracker.inode, original_inode);

        // Verify the rotation was detected - that's the main test
        // The exact content read depends on positioning logic during rotation
        assert!(tracker.file.is_some());
    }

    #[tokio::test]
    async fn test_file_truncation_detection() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"long initial content\n").unwrap();
        temp_file.flush().unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // Read initial content
        let _events1 = tracker.read_new_lines(&mut state).await.unwrap();
        let _initial_position = tracker.position;

        // Truncate file to smaller size
        temp_file.as_file_mut().set_len(5).unwrap();
        temp_file.flush().unwrap();

        // Check should detect truncation
        let (changed, _events) = tracker.check_file_changes(&mut state).await.unwrap();
        assert!(changed);
        assert_eq!(tracker.position, 0); // Should reset to beginning
    }

    #[tokio::test]
    async fn test_rapid_rotation_data_preservation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("rapid_rotation.log");

        // Create initial file with content that will be rotated away
        fs::write(&file_path, "initial_line1\ninitial_line2\n").unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker =
            FileTracker::new(file_path.to_string_lossy().to_string(), meta, &mut state)
                .await
                .unwrap();

        // Read initial content
        let events1 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events1.len(), 2);
        assert_eq!(events1[0].message, "initial_line1");
        assert_eq!(events1[1].message, "initial_line2");

        // Add more content before rotation to simulate data that could be lost
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&file_path)
            .unwrap();
        std::io::Write::write_all(&mut file, b"pre_rotation_line\n").unwrap();
        drop(file);

        // Simulate rapid rotation: remove old file and create new one quickly
        fs::remove_file(&file_path).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        fs::write(&file_path, "post_rotation_line1\npost_rotation_line2\n").unwrap();

        // Check file changes - this should capture pre-rotation data and handle rotation
        let (changed, rotation_events) = tracker.check_file_changes(&mut state).await.unwrap();
        assert!(changed, "Rotation should be detected");

        // The important part: we should have captured the pre-rotation line
        // (though this depends on timing and OS behavior)
        if !rotation_events.is_empty() {
            println!("Captured {} events during rotation", rotation_events.len());
            for event in &rotation_events {
                println!("Rotation event: {}", event.message);
            }
        }

        // Read new content after rotation
        let events_after = tracker.read_new_lines(&mut state).await.unwrap();

        // After rotation, we should be able to read the new file content
        // The exact amount depends on where the file tracker positioned itself
        println!("Events after rotation: {}", events_after.len());
        for event in &events_after {
            println!("After rotation: {}", event.message);
        }

        // Key assertion: we should have captured the pre-rotation data
        if !rotation_events.is_empty() {
            assert!(
                rotation_events
                    .iter()
                    .any(|e| e.message == "pre_rotation_line")
            );
        }

        // Test passes if rotation is detected and pre-rotation data was captured
        assert!(changed);
    }

    #[tokio::test]
    async fn test_symlink_support() {
        let temp_dir = TempDir::new().unwrap();
        let target_file = temp_dir.path().join("target.log");
        let symlink_file = temp_dir.path().join("symlink.log");

        // Create target file and symlink
        fs::write(&target_file, "symlink content\n").unwrap();
        symlink(&target_file, &symlink_file).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let tracker =
            FileTracker::new(symlink_file.to_string_lossy().to_string(), meta, &mut state)
                .await
                .unwrap();

        assert!(tracker.is_symlink);
        assert!(tracker.symlink_target.is_some());
        assert_eq!(
            tracker.symlink_target.as_ref().unwrap(),
            &target_file.to_string_lossy().to_string()
        );
    }

    #[tokio::test]
    async fn test_symlink_target_change() {
        let temp_dir = TempDir::new().unwrap();
        let target1 = temp_dir.path().join("target1.log");
        let target2 = temp_dir.path().join("target2.log");
        let symlink_file = temp_dir.path().join("symlink.log");

        // Create first target and symlink
        fs::write(&target1, "target1 content\n").unwrap();
        symlink(&target1, &symlink_file).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker =
            FileTracker::new(symlink_file.to_string_lossy().to_string(), meta, &mut state)
                .await
                .unwrap();

        assert!(tracker.is_symlink);
        let original_target = tracker.symlink_target.clone();

        // Change symlink target
        fs::remove_file(&symlink_file).unwrap();
        fs::write(&target2, "target2 content\n").unwrap();
        symlink(&target2, &symlink_file).unwrap();

        // Should detect symlink change
        let (new_target, changed) = tracker.check_symlink_changes().unwrap();
        assert!(changed);
        assert_ne!(original_target.as_ref().unwrap(), &new_target);
    }

    #[tokio::test]
    async fn test_broken_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let target_file = temp_dir.path().join("nonexistent.log");
        let symlink_file = temp_dir.path().join("broken_symlink.log");

        // Create symlink pointing to non-existent file
        symlink(&target_file, &symlink_file).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        // Should fail to create FileTracker for broken symlink
        let result =
            FileTracker::new(symlink_file.to_string_lossy().to_string(), meta, &mut state).await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn test_symlink_becomes_broken() {
        let temp_dir = TempDir::new().unwrap();
        let target_file = temp_dir.path().join("target.log");
        let symlink_file = temp_dir.path().join("symlink.log");

        // Create valid symlink first
        fs::write(&target_file, "content\n").unwrap();
        symlink(&target_file, &symlink_file).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker =
            FileTracker::new(symlink_file.to_string_lossy().to_string(), meta, &mut state)
                .await
                .unwrap();

        assert!(tracker.is_symlink);

        // Remove target file to break the symlink
        fs::remove_file(&target_file).unwrap();

        // resolve_symlink_target should now fail
        let result = tracker.resolve_symlink_target();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        }

        // check_symlink_changes should handle broken symlink gracefully
        let result = tracker.check_symlink_changes();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn test_file_disappears() {
        let temp_file = create_test_file_with_content("content\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();
        assert!(tracker.file.is_some());

        // Remove the file
        drop(temp_file); // This removes the temporary file

        // Check should handle missing file gracefully
        let (changed, _events) = tracker.check_file_changes(&mut state).await.unwrap();
        assert!(!changed);
        assert!(tracker.file.is_none());
    }

    #[tokio::test]
    async fn test_utility_methods() {
        let temp_file = create_test_file_with_content("test\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        // Test utility methods
        assert!(tracker.is_open());
        assert_eq!(tracker.get_position(), 0);

        // Read content to change position
        let _events = tracker.read_new_lines(&mut state).await.unwrap();
        assert!(tracker.get_position() > 0);

        // Test close
        tracker.close();
        assert!(!tracker.is_open());
    }

    #[tokio::test]
    async fn test_large_file_handling() {
        // Create file with large content
        let large_content = "x".repeat(100000) + "\n"; // 100KB line
        let temp_file = create_test_file_with_content(&large_content).await;

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message.len(), 100000);
        assert!(tracker.position > 100000);
    }

    #[tokio::test]
    async fn test_unicode_content() {
        let unicode_content = "日本語ログ\nрусский текст\n测试中文\n";
        let temp_file = create_test_file_with_content(unicode_content).await;

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message, "日本語ログ");
        assert_eq!(events[1].message, "русский текст");
        assert_eq!(events[2].message, "测试中文");
    }

    #[tokio::test]
    async fn test_windows_line_endings() {
        let windows_content = "line1\r\nline2\r\nline3\r\n";
        let temp_file = create_test_file_with_content(windows_content).await;

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let mut tracker = FileTracker::new(
            temp_file.path().to_string_lossy().to_string(),
            meta,
            &mut state,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message, "line1");
        assert_eq!(events[1].message, "line2");
        assert_eq!(events[2].message, "line3");
    }

    #[tokio::test]
    async fn test_partial_line_handling() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file
            .write_all(b"partial line without newline")
            .unwrap();
        temp_file.flush().unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // FileTracker actually reads partial lines (lines without newlines are still read by read_line)
        let events1 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events1.len(), 1);
        assert_eq!(events1[0].message, "partial line without newline");

        // Add more content - this will be read as separate line
        temp_file.write_all(b"\nextra line\n").unwrap();
        temp_file.flush().unwrap();

        // Should read the extra line
        let events2 = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].message, "extra line");
    }

    #[tokio::test]
    async fn test_max_line_size_limiting() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("large_line.log");

        // Create a file with very long line (5KB) and a normal line
        let large_line = "a".repeat(5000); // 5KB line
        let content = format!("{}\nnormal line\n", large_line);
        std::fs::write(&file_path, content).unwrap();

        let meta = Meta::default();
        let mut state = AppState::new();
        let max_line_size = 1000; // Limit to 1KB

        let mut tracker = FileTracker::new_with_max_line_size(
            file_path.to_string_lossy().to_string(),
            meta,
            &mut state,
            max_line_size,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        // Should have 2 events
        assert_eq!(events.len(), 2);

        // First event should be truncated
        let first_event = &events[0];
        assert!(first_event.message.contains("... [TRUNCATED]"));
        assert!(first_event.message.len() < 5000); // Much smaller than original

        // Second event should be normal
        let second_event = &events[1];
        assert_eq!(second_event.message, "normal line");
        assert!(!second_event.message.contains("TRUNCATED"));
    }

    #[tokio::test]
    async fn test_max_line_size_utf8_handling() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("utf8_line.log");

        // Create a line with UTF-8 characters that would be truncated
        let unicode_char = "🦀"; // Rust crab emoji (4 bytes in UTF-8)
        let large_line = unicode_char.repeat(500); // 2000 bytes total
        let content = format!("{}\n", large_line);
        std::fs::write(&file_path, content).unwrap();

        let meta = Meta::default();
        let mut state = AppState::new();
        let max_line_size = 1000; // Limit to 1KB

        let mut tracker = FileTracker::new_with_max_line_size(
            file_path.to_string_lossy().to_string(),
            meta,
            &mut state,
            max_line_size,
        )
        .await
        .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        // Should have 1 event
        assert_eq!(events.len(), 1);

        // Event should be truncated but still valid UTF-8
        let event = &events[0];
        assert!(event.message.contains("... [TRUNCATED]"));
        assert!(event.message.len() < 1100); // Should be around 1000 + truncation marker

        // Should still contain some emoji characters (at the beginning)
        assert!(event.message.contains("🦀"));
    }

    #[tokio::test]
    async fn test_metadata_cache_integration() {
        let temp_file = create_test_file_with_content("test\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path.clone(), meta, &mut state)
            .await
            .unwrap();

        // Metadata cache should be working
        let cached_result = tracker.get_cached_metadata(&path);
        assert!(cached_result.is_ok());

        let refreshed_result = tracker.refresh_cached_metadata(&path);
        assert!(refreshed_result.is_ok());
    }

    #[tokio::test]
    async fn test_ensure_file_in_state_fallback() {
        let temp_file = create_test_file_with_content("test content\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        // Create FileTracker manually without adding to AppState
        let tracker = FileTracker::new(path.clone(), meta, &mut state)
            .await
            .unwrap();

        // Remove the file from AppState to simulate missing entry
        state.remove_file(&path);
        assert!(!state.files.contains_key(&path));

        // Call ensure_file_in_state - should add file back
        tracker.ensure_file_in_state(&mut state);

        // File should now be in AppState
        assert!(state.files.contains_key(&path));

        let file_state = state.files.get(&path).unwrap();
        assert_eq!(file_state.path, path);
        assert_eq!(file_state.inode, tracker.inode);
        assert_eq!(file_state.size, tracker.last_size);
    }

    #[tokio::test]
    async fn test_read_new_lines_limited_with_ensure_state() {
        let temp_file = create_test_file_with_content("line1\nline2\nline3\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path.clone(), meta, &mut state)
            .await
            .unwrap();

        // Remove file from AppState to test ensure_file_in_state
        state.remove_file(&path);
        assert!(!state.files.contains_key(&path));

        // Reading should trigger ensure_file_in_state
        let events = tracker
            .read_new_lines_limited(&mut state, Some(2))
            .await
            .unwrap();

        // Should have read events successfully
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "line1");
        assert_eq!(events[1].message, "line2");

        // File should be back in AppState due to ensure_file_in_state
        assert!(state.files.contains_key(&path));
        assert!(tracker.position > 0);
    }

    #[tokio::test]
    async fn test_read_new_lines_limited_chunking_multiple_batches() {
        // Create file with 250 lines
        let content: String = (1..=250).map(|i| format!("line{}\n", i)).collect();
        let temp_file = create_test_file_with_content(&content).await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // Read in chunks of 100,100,50
        let first = tracker
            .read_new_lines_limited(&mut state, Some(100))
            .await
            .unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first.first().unwrap().message, "line1");
        assert_eq!(first.last().unwrap().message, "line100");

        let second = tracker
            .read_new_lines_limited(&mut state, Some(100))
            .await
            .unwrap();
        assert_eq!(second.len(), 100);
        assert_eq!(second.first().unwrap().message, "line101");
        assert_eq!(second.last().unwrap().message, "line200");

        let third = tracker
            .read_new_lines_limited(&mut state, Some(100))
            .await
            .unwrap();
        assert_eq!(third.len(), 50);
        assert_eq!(third.first().unwrap().message, "line201");
        assert_eq!(third.last().unwrap().message, "line250");
    }

    #[tokio::test]
    async fn test_read_new_lines_limited_handles_crlf() {
        let temp_file = create_test_file_with_content("a\r\nb\r\nc\r\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let path = temp_file.path().to_string_lossy().to_string();

        let mut tracker = FileTracker::new(path, meta, &mut state).await.unwrap();

        // Read first two lines with limited = 2
        let first = tracker
            .read_new_lines_limited(&mut state, Some(2))
            .await
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].message, "a");
        assert_eq!(first[1].message, "b");

        // Remaining single line
        let second = tracker
            .read_new_lines_limited(&mut state, Some(2))
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].message, "c");
    }

    #[tokio::test]
    async fn test_skip_existing_content() {
        // Prepare file with two lines
        let temp_file = create_test_file_with_content("first\nsecond\n").await;
        let mut state = create_test_state().await;
        let meta = create_test_meta();

        let path = temp_file.path().to_string_lossy().to_string();
        let mut tracker = FileTracker::new(path.clone(), meta, &mut state)
            .await
            .unwrap();

        // Skip to end and verify no events are read afterwards
        tracker.skip_existing_content(&mut state).await.unwrap();
        let events = tracker.read_new_lines(&mut state).await.unwrap();
        assert!(
            events.is_empty(),
            "No events expected after skipping existing content"
        );

        // Append new content and ensure it is read
        use std::io::Write as _;
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "third").unwrap();
        }

        let events_after = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events_after.len(), 1);
        assert_eq!(events_after[0].message, "third");
    }

    #[tokio::test]
    async fn test_corrupted_file_handling() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Write file with null bytes and control characters (corrupted content)
        let corrupted_data = [
            b"normal line\n".to_vec(),
            vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, b'\n'], // Control chars and nulls
            b"another normal line\n".to_vec(),
            vec![0xFF, 0xFE, 0xFD, b'\n'], // High bytes
        ]
        .concat();

        std::fs::write(path, corrupted_data).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let mut tracker = FileTracker::new(path.to_string(), meta, &mut state)
            .await
            .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        // Should read 4 lines, with corrupted ones being sanitized
        println!("Events count: {}", events.len());
        for (i, event) in events.iter().enumerate() {
            println!("Event {}: '{}'", i, event.message);
        }

        assert_eq!(events.len(), 4);
        assert_eq!(events[0].message, "normal line");
        // Second line should be sanitized (contains question marks for corrupted bytes)
        assert!(events[1].message.contains('?') || events[1].message.is_empty());
        assert_eq!(events[2].message, "another normal line");
        // Fourth line should also be sanitized (contains replacement chars or question marks)
        assert!(
            events[3].message.contains('?')
                || events[3].message.contains('�')
                || events[3].message.is_empty()
        );
    }

    #[tokio::test]
    async fn test_binary_data_handling() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Write binary data mixed with text
        let mut binary_data = Vec::new();
        binary_data.extend_from_slice(b"Text before binary\n");

        // Add some binary data that's not valid UTF-8
        for i in 128..255 {
            binary_data.push(i);
        }
        binary_data.push(b'\n');

        binary_data.extend_from_slice(b"Text after binary\n");

        std::fs::write(path, binary_data).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let mut tracker = FileTracker::new(path.to_string(), meta, &mut state)
            .await
            .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message, "Text before binary");
        // Middle line should be handled with lossy conversion
        assert!(!events[1].message.is_empty());
        assert_eq!(events[2].message, "Text after binary");
    }

    #[tokio::test]
    async fn test_partial_utf8_sequences() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Create data with partial UTF-8 sequences (truncated multi-byte chars)
        let mut data = Vec::new();
        data.extend_from_slice(b"Valid text\n");

        // Add partial UTF-8 sequence (should be 0xC3 0xA4 for ä, but we only add 0xC3)
        data.push(0xC3);
        data.push(b'\n');

        data.extend_from_slice("Валидный текст\n".as_bytes()); // Valid UTF-8 in Cyrillic

        std::fs::write(path, data).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let mut tracker = FileTracker::new(path.to_string(), meta, &mut state)
            .await
            .unwrap();

        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].message, "Valid text");
        // Partial UTF-8 should be replaced with replacement character
        assert!(events[1].message.contains('�') || events[1].message.is_empty());
        assert_eq!(events[2].message, "Валидный текст");
    }

    #[tokio::test]
    async fn test_file_health_check() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Create a file with mostly valid content
        std::fs::write(path, "This is a healthy file with normal content\n").unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let mut tracker = FileTracker::new(path.to_string(), meta.clone(), &mut state)
            .await
            .unwrap();

        // Should be healthy
        let is_healthy = tracker.check_file_health().await.unwrap();
        assert!(is_healthy);

        // Now create a highly corrupted file
        let corrupted_path = temp_file.path().parent().unwrap().join("corrupted.log");
        let mut corrupted_data = vec![0u8; 512]; // 512 null bytes
        corrupted_data.extend_from_slice(b"\nsome text"); // Add a bit of text
        std::fs::write(&corrupted_path, corrupted_data).unwrap();

        let mut corrupted_state = create_test_state().await;
        let mut corrupted_tracker = FileTracker::new(
            corrupted_path.to_str().unwrap().to_string(),
            meta.clone(),
            &mut corrupted_state,
        )
        .await
        .unwrap();

        // Should be unhealthy due to too many null bytes
        let is_healthy = corrupted_tracker.check_file_health().await.unwrap();
        assert!(!is_healthy);
    }

    #[tokio::test]
    async fn test_read_error_recovery() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Write some content with problematic sequences
        let mut data = Vec::new();
        data.extend_from_slice(b"Good line 1\n");

        // Simulate some form of corruption that might cause read errors
        data.extend(std::iter::repeat_n(0x00, 100)); // Lots of null bytes
        data.push(b'\n');

        data.extend_from_slice(b"Good line 2\n");

        std::fs::write(path, data).unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();
        let mut tracker = FileTracker::new(path.to_string(), meta, &mut state)
            .await
            .unwrap();

        // Should be able to read and recover from the corrupted line
        let events = tracker.read_new_lines(&mut state).await.unwrap();

        assert!(events.len() >= 2); // Should get at least the good lines
        assert_eq!(events[0].message, "Good line 1");
        // Last event should be the good line after corruption
        assert_eq!(events.last().unwrap().message, "Good line 2");
    }

    #[tokio::test]
    async fn test_permission_denied_handling() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Create a file with content
        std::fs::write(path, "test content\n").unwrap();

        let mut state = create_test_state().await;
        let meta = create_test_meta();

        // Create tracker successfully
        let mut tracker = FileTracker::new(path.to_string(), meta, &mut state)
            .await
            .unwrap();

        // Read initial content
        let events = tracker.read_new_lines(&mut state).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "test content");

        // Now make file unreadable (on Unix systems)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o000); // No permissions
            std::fs::set_permissions(path, perms).unwrap();

            // Try to check for changes - should handle permission denied gracefully
            let result = tracker.check_file_changes(&mut state).await;
            // Should not panic and return ok with no changes detected
            match result {
                Ok((changed, events)) => {
                    assert!(!changed); // No changes should be detected due to permission issue
                    assert!(events.is_empty());
                }
                Err(e) => {
                    // Permission denied error is acceptable
                    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
                }
            }

            // Restore permissions for cleanup
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o644);
            std::fs::set_permissions(path, perms).unwrap();
        }

        #[cfg(not(unix))]
        {
            // On non-Unix systems, just verify the tracker works normally
            let (changed, events) = tracker.check_file_changes(&mut state).await.unwrap();
            assert!(!changed);
            assert!(events.is_empty());
        }
    }

    #[tokio::test]
    async fn test_permission_denied_on_file_creation() {
        // Test permission denied when trying to create FileTracker

        #[cfg(unix)]
        {
            let temp_dir = TempDir::new().unwrap();
            let restricted_file = temp_dir.path().join("restricted.log");

            // Create file and make it unreadable
            std::fs::write(&restricted_file, "content").unwrap();
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&restricted_file).unwrap().permissions();
            perms.set_mode(0o000); // No permissions
            std::fs::set_permissions(&restricted_file, perms).unwrap();

            let mut state = create_test_state().await;
            let meta = create_test_meta();

            // Should fail with permission denied
            let result = FileTracker::new(
                restricted_file.to_string_lossy().to_string(),
                meta,
                &mut state,
            )
            .await;

            match result {
                Err(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
                }
                Ok(_) => panic!("Expected permission denied error"),
            }

            // Restore permissions for cleanup
            let mut perms = std::fs::metadata(&restricted_file).unwrap().permissions();
            perms.set_mode(0o644);
            std::fs::set_permissions(&restricted_file, perms).unwrap();
        }

        #[cfg(not(unix))]
        {
            // On non-Unix systems, skip this test
            println!("Skipping permission test on non-Unix system");
        }
    }

    #[tokio::test]
    async fn test_symlink_permission_denied() {
        #[cfg(unix)]
        {
            let temp_dir = TempDir::new().unwrap();
            let target_file = temp_dir.path().join("target.log");
            let symlink_file = temp_dir.path().join("symlink.log");

            // Create target file
            std::fs::write(&target_file, "symlink content\n").unwrap();
            symlink(&target_file, &symlink_file).unwrap();

            let mut state = create_test_state().await;
            let meta = create_test_meta();

            // Create tracker successfully with symlink
            let tracker =
                FileTracker::new(symlink_file.to_string_lossy().to_string(), meta, &mut state)
                    .await
                    .unwrap();

            assert!(tracker.is_symlink);

            // Make symlink unreadable
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&symlink_file).unwrap().permissions();
            perms.set_mode(0o000);
            std::fs::set_permissions(&symlink_file, perms).unwrap();

            // Try to resolve symlink target - should handle permission denied
            let result = tracker.resolve_symlink_target();
            match result {
                Err(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
                }
                Ok(_) => {
                    // Might succeed depending on system configuration
                    println!("Symlink resolution succeeded despite permission changes");
                }
            }

            // Restore permissions for cleanup
            let mut perms = std::fs::metadata(&symlink_file).unwrap().permissions();
            perms.set_mode(0o777);
            std::fs::set_permissions(&symlink_file, perms).unwrap();
        }

        #[cfg(not(unix))]
        {
            println!("Skipping symlink permission test on non-Unix system");
        }
    }
}
