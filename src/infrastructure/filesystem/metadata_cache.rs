use log::debug;
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileMetadata {
    pub inode: u64,
    pub size: u64,
    pub modified: u64,
    pub cached_at: Instant,
}

impl FileMetadata {
    pub fn from_std_metadata(metadata: &fs::Metadata) -> Result<Self, Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt;

        let modified = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        Ok(Self {
            inode: metadata.ino(),
            size: metadata.len(),
            modified,
            cached_at: Instant::now(),
        })
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

pub struct MetadataCache {
    cache: HashMap<String, FileMetadata>,
    ttl: Duration,
    max_size: usize,
    hits: u64,
    misses: u64,
}

#[allow(dead_code)]
impl MetadataCache {
    pub fn new(ttl_ms: u64, max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            ttl: Duration::from_millis(ttl_ms),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    /// Get metadata from cache or filesystem
    pub fn get_metadata(&mut self, path: &str) -> Result<FileMetadata, Box<dyn std::error::Error>> {
        // Check cache first
        if let Some(cached) = self.cache.get(path) {
            if !cached.is_expired(self.ttl) {
                self.hits += 1;
                debug!("Metadata cache hit for: {}", path);
                return Ok(cached.clone());
            }
            // Remove expired entry
            self.cache.remove(path);
        }

        // Cache miss - load from filesystem
        self.misses += 1;
        debug!("Metadata cache miss for: {}", path);

        let metadata = fs::metadata(path)?;
        let file_metadata = FileMetadata::from_std_metadata(&metadata)?;

        // Evict old entries if cache is full
        if self.cache.len() >= self.max_size {
            self.evict_expired_or_oldest();
        }

        // Add to cache
        self.cache.insert(path.to_string(), file_metadata.clone());

        Ok(file_metadata)
    }

    /// Force refresh metadata for a path
    pub fn refresh_metadata(
        &mut self,
        path: &str,
    ) -> Result<FileMetadata, Box<dyn std::error::Error>> {
        self.cache.remove(path);
        self.get_metadata(path)
    }

    /// Remove path from cache
    pub fn remove(&mut self, path: &str) {
        self.cache.remove(path);
    }

    /// Evict expired entries or oldest if cache is full
    fn evict_expired_or_oldest(&mut self) {
        let _now = Instant::now();

        // First try to remove expired entries
        let expired_keys: Vec<String> = self
            .cache
            .iter()
            .filter(|(_, metadata)| metadata.cached_at.elapsed() > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            self.cache.remove(&key);
        }

        // If still at capacity, remove oldest entries
        if self.cache.len() >= self.max_size {
            let oldest_keys: Vec<String> = {
                let mut entries: Vec<_> = self.cache.iter().collect();
                entries.sort_by(|a, b| a.1.cached_at.cmp(&b.1.cached_at));

                let remove_count = (self.cache.len() - self.max_size + 1).min(self.cache.len() / 4);
                entries
                    .into_iter()
                    .take(remove_count)
                    .map(|(k, _)| k.clone())
                    .collect()
            };

            for key in oldest_keys {
                self.cache.remove(&key);
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            capacity: self.max_size,
            hits: self.hits,
            misses: self.misses,
            hit_rate: if self.hits + self.misses > 0 {
                self.hits as f64 / (self.hits + self.misses) as f64
            } else {
                0.0
            },
        }
    }

    /// Clear all expired entries
    pub fn cleanup_expired(&mut self) {
        let expired_keys: Vec<String> = self
            .cache
            .iter()
            .filter(|(_, metadata)| metadata.is_expired(self.ttl))
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            self.cache.remove(&key);
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetadataCache: {}/{} entries, {} hits, {} misses, {:.1}% hit rate",
            self.size,
            self.capacity,
            self.hits,
            self.misses,
            self.hit_rate * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_metadata_cache_basic() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Create test file
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"test data").unwrap();
        file.sync_all().unwrap();

        let mut cache = MetadataCache::new(1000, 10);
        let path_str = file_path.to_str().unwrap();

        // First call should be cache miss
        let metadata1 = cache.get_metadata(path_str).unwrap();
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 0);

        // Second call should be cache hit
        let metadata2 = cache.get_metadata(path_str).unwrap();
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 1);

        assert_eq!(metadata1.inode, metadata2.inode);
        assert_eq!(metadata1.size, metadata2.size);
    }

    #[test]
    fn test_cache_expiration() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        File::create(&file_path).unwrap();

        let mut cache = MetadataCache::new(1, 10); // Very short TTL
        let path_str = file_path.to_str().unwrap();

        // First call
        let _metadata1 = cache.get_metadata(path_str).unwrap();
        assert_eq!(cache.stats().misses, 1);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        // Should be cache miss due to expiration
        let _metadata2 = cache.get_metadata(path_str).unwrap();
        assert_eq!(cache.stats().misses, 2);
    }
}
