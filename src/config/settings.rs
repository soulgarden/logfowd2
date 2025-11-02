use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Settings {
    pub log_path: String,

    #[serde(rename = "es")]
    pub elasticsearch: ElasticsearchConfig,

    pub state_file_path: Option<String>,
    pub read_existing_on_startup: Option<bool>,
    pub read_chunk_size: Option<usize>,
    pub max_line_size: Option<usize>,
    pub max_concurrent_file_readers: Option<usize>,

    pub channels: Option<ChannelsConfig>,
    pub metrics: Option<MetricsConfig>,
    pub logging: Option<LoggingConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ElasticsearchConfig {
    pub host: String,
    pub port: u16,
    pub index_name: String,
    pub flush_interval: u64,
    pub bulk_size: usize,
    pub workers: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ChannelsConfig {
    pub watcher_buffer_size: Option<usize>,
    pub es_buffer_size: Option<usize>,
    pub backpressure_threshold: Option<f32>,
    pub backpressure_min_delay_ms: Option<u64>,
    pub backpressure_max_delay_ms: Option<u64>,

    // Notify callback configuration
    pub notify_buffer_warning_threshold: Option<usize>,
    pub notify_buffer_max_size: Option<usize>,
    pub notify_drop_on_overflow: Option<bool>,

    // NotifyBridge filesystem event configuration
    pub notify_filesystem_buffer_warning_threshold: Option<usize>,
    pub notify_filesystem_buffer_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    #[serde(default = "default_metrics_port")]
    pub port: u16,

    #[serde(default = "default_metrics_path")]
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct LoggingConfig {
    #[serde(default = "default_log_format")]
    pub log_format: String,

    #[serde(default = "default_log_level")]
    pub log_level: String,
}

// Default functions for serde
fn default_metrics_enabled() -> bool {
    false
}
fn default_metrics_port() -> u16 {
    9090
}
fn default_metrics_path() -> String {
    "/metrics".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "simple".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
            path: default_metrics_path(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_format: default_log_format(),
            log_level: default_log_level(),
        }
    }
}

impl Settings {
    pub fn load() -> Result<Self> {
        let path = std::env::var("CFG_PATH").unwrap_or_else(|_| "./config.json".to_string());
        Self::load_from_file(&path)
    }

    pub fn load_from_file(path: &str) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let settings: Self = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path))?;

        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_elasticsearch()?;
        self.validate_paths()?;
        self.validate_channels()?;
        self.validate_performance()?;
        self.validate_metrics()?;
        self.validate_logging()?;
        Ok(())
    }

    fn validate_elasticsearch(&self) -> Result<()> {
        let es = &self.elasticsearch;

        if es.workers == 0 {
            bail!("ES workers must be at least 1, got 0. Zero workers would cause deadlock.");
        }

        if es.flush_interval == 0 {
            bail!(
                "ES flush_interval must be greater than 0. Zero causes tokio::time::interval panic."
            );
        }

        if !es.host.starts_with("http://") && !es.host.starts_with("https://") {
            bail!(
                "ES host must start with http:// or https://, got: '{}'",
                es.host
            );
        }

        if es.host.contains(' ') {
            bail!("ES host URL cannot contain spaces, got: '{}'", es.host);
        }

        if es.port == 0 {
            bail!("ES port must be in range 1-65535, got: {}", es.port);
        }

        if es.index_name.is_empty() {
            bail!("ES index_name cannot be empty");
        }

        if es.index_name.starts_with('-')
            || es.index_name.starts_with('_')
            || es.index_name.starts_with('+')
        {
            bail!(
                "ES index_name cannot start with -, _, or +, got: '{}'",
                es.index_name
            );
        }

        if es
            .index_name
            .chars()
            .any(|c| c.is_uppercase() || c.is_whitespace())
        {
            bail!(
                "ES index_name cannot contain uppercase letters or whitespace, got: '{}'",
                es.index_name
            );
        }

        if es.bulk_size == 0 {
            bail!("ES bulk_size must be greater than 0");
        }

        Ok(())
    }

    fn validate_paths(&self) -> Result<()> {
        let log_path = Path::new(&self.log_path);

        if !log_path.exists() {
            bail!("log_path does not exist: '{}'", self.log_path);
        }

        if !log_path.is_dir() {
            bail!(
                "log_path must be a directory, got file: '{}'",
                self.log_path
            );
        }

        fs::read_dir(log_path)
            .with_context(|| format!("log_path is not readable: '{}'", self.log_path))?;

        if let Some(state_path) = &self.state_file_path {
            let state_path = Path::new(state_path);
            let parent_dir = state_path.parent().unwrap_or(Path::new("/tmp"));

            if !parent_dir.exists() {
                bail!(
                    "state_file_path parent directory does not exist: '{}'",
                    parent_dir.display()
                );
            }

            // Test write permissions
            let test_file = parent_dir.join(".logfowd2_write_test");
            fs::write(&test_file, b"test").with_context(|| {
                format!(
                    "state_file_path parent directory is not writable: '{}'",
                    parent_dir.display()
                )
            })?;
            let _ = fs::remove_file(&test_file);
        }

        Ok(())
    }

    fn validate_channels(&self) -> Result<()> {
        if let Some(channels) = &self.channels {
            if let Some(size) = channels.watcher_buffer_size
                && size == 0
            {
                bail!("watcher_buffer_size must be greater than 0");
            }

            if let Some(size) = channels.es_buffer_size
                && size == 0
            {
                bail!("es_buffer_size must be greater than 0");
            }

            if let Some(threshold) = channels.backpressure_threshold {
                if threshold.is_nan() {
                    bail!("backpressure_threshold cannot be NaN");
                }
                if threshold.is_infinite() {
                    bail!("backpressure_threshold cannot be infinite");
                }
                if !(0.0..=1.0).contains(&threshold) {
                    bail!(
                        "backpressure_threshold must be in range 0.0-1.0, got: {}",
                        threshold
                    );
                }
            }

            if let (Some(min), Some(max)) = (
                channels.backpressure_min_delay_ms,
                channels.backpressure_max_delay_ms,
            ) && min > max
            {
                bail!(
                    "backpressure_min_delay_ms ({}) must be <= backpressure_max_delay_ms ({})",
                    min,
                    max
                );
            }

            if let Some(size) = channels.notify_buffer_max_size
                && size == 0
            {
                bail!("notify_buffer_max_size must be greater than 0");
            }

            if let Some(size) = channels.notify_filesystem_buffer_size
                && size == 0
            {
                bail!("notify_filesystem_buffer_size must be greater than 0");
            }
        }

        Ok(())
    }

    fn validate_performance(&self) -> Result<()> {
        if let Some(size) = self.read_chunk_size
            && size == 0
        {
            bail!("read_chunk_size must be greater than 0");
        }

        if let Some(size) = self.max_line_size
            && size == 0
        {
            bail!("max_line_size must be greater than 0");
        }

        if let Some(readers) = self.max_concurrent_file_readers
            && readers == 0
        {
            bail!("max_concurrent_file_readers must be greater than 0");
        }

        Ok(())
    }

    fn validate_metrics(&self) -> Result<()> {
        if let Some(metrics) = &self.metrics {
            if metrics.port == 0 {
                bail!(
                    "metrics.port must be in range 1-65535, got: {}",
                    metrics.port
                );
            }

            if metrics.path.is_empty() {
                bail!("metrics.path cannot be empty");
            }

            if !metrics.path.starts_with('/') {
                bail!("metrics.path must start with '/', got: '{}'", metrics.path);
            }
        }

        Ok(())
    }

    fn validate_logging(&self) -> Result<()> {
        if let Some(logging) = &self.logging {
            const VALID_LEVELS: &[&str] = &["trace", "debug", "info", "warn", "error"];
            if !VALID_LEVELS.contains(&logging.log_level.as_str()) {
                bail!(
                    "logging.log_level must be one of {:?}, got: '{}'",
                    VALID_LEVELS,
                    logging.log_level
                );
            }

            const VALID_FORMATS: &[&str] = &["simple", "json"];
            if !VALID_FORMATS.contains(&logging.log_format.as_str()) {
                bail!(
                    "logging.log_format must be one of {:?}, got: '{}'",
                    VALID_FORMATS,
                    logging.log_format
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_deserialization() {
        let config_json = r#"
        {
            "log_path": "/var/log/pods",
            "state_file_path": "/tmp/state.json",
            "max_concurrent_file_readers": 100,
            "channels": {
                "watcher_buffer_size": 5000,
                "es_buffer_size": 2000,
                "backpressure_threshold": 0.8,
                "notify_buffer_warning_threshold": 1000,
                "notify_buffer_max_size": 10000,
                "notify_drop_on_overflow": true
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test_index",
                "flush_interval": 1000,
                "bulk_size": 500,
                "workers": 5
            }
        }
        "#;

        let settings: Settings = serde_json::from_str(config_json).unwrap();

        assert_eq!(settings.log_path, "/var/log/pods");
        assert_eq!(
            settings.state_file_path,
            Some("/tmp/state.json".to_string())
        );
        assert_eq!(settings.max_concurrent_file_readers, Some(100));

        // Test channels config
        let channels = settings.channels.unwrap();
        assert_eq!(channels.watcher_buffer_size, Some(5000));
        assert_eq!(channels.es_buffer_size, Some(2000));
        assert_eq!(channels.backpressure_threshold, Some(0.8));
        assert_eq!(channels.notify_buffer_warning_threshold, Some(1000));
        assert_eq!(channels.notify_buffer_max_size, Some(10000));
        assert_eq!(channels.notify_drop_on_overflow, Some(true));

        // Test ES config
        assert_eq!(settings.elasticsearch.host, "http://localhost");
        assert_eq!(settings.elasticsearch.port, 9200);
        assert_eq!(settings.elasticsearch.index_name, "test_index");
        assert_eq!(settings.elasticsearch.flush_interval, 1000);
        assert_eq!(settings.elasticsearch.bulk_size, 500);
        assert_eq!(settings.elasticsearch.workers, 5);
    }

    #[test]
    fn test_minimal_json_deserialization() {
        let config_json = r#"
        {
            "log_path": "/test/logs",
            "es": {
                "host": "http://es-server",
                "port": 9200,
                "index_name": "minimal",
                "flush_interval": 2000,
                "bulk_size": 1000,
                "workers": 3
            }
        }
        "#;

        let settings: Settings = serde_json::from_str(config_json).unwrap();

        assert_eq!(settings.log_path, "/test/logs");
        assert_eq!(settings.state_file_path, None);
        assert_eq!(settings.max_concurrent_file_readers, None);
        assert_eq!(settings.channels, None);

        assert_eq!(settings.elasticsearch.host, "http://es-server");
        assert_eq!(settings.elasticsearch.index_name, "minimal");
    }

    #[test]
    fn test_invalid_json_parsing() {
        let invalid_json = "{ invalid json content }";

        let result: Result<Settings, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_fields() {
        let incomplete_config = r#"
        {
            "log_path": "/test"
        }
        "#;

        let result: Result<Settings, _> = serde_json::from_str(incomplete_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_channels_config_optional_fields() {
        let config_json = r#"
        {
            "log_path": "/var/log",
            "channels": {
                "watcher_buffer_size": 1000
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 1000,
                "workers": 1
            }
        }
        "#;

        let settings: Settings = serde_json::from_str(config_json).unwrap();

        let channels = settings.channels.unwrap();
        assert_eq!(channels.watcher_buffer_size, Some(1000));
        assert_eq!(channels.es_buffer_size, None);
        assert_eq!(channels.backpressure_threshold, None);
        assert_eq!(channels.notify_buffer_warning_threshold, None);
        assert_eq!(channels.notify_buffer_max_size, None);
        assert_eq!(channels.notify_drop_on_overflow, None);
    }

    #[test]
    fn test_settings_cloning() {
        let settings = Settings {
            log_path: "/test/path".to_string(),
            state_file_path: Some("/test/state.json".to_string()),
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: Some(10),
            channels: None,
            metrics: None,
            logging: None,
            elasticsearch: ElasticsearchConfig {
                host: "http://test".to_string(),
                port: 9200,
                index_name: "test".to_string(),
                flush_interval: 1000,
                bulk_size: 500,
                workers: 5,
            },
        };

        let cloned = settings.clone();

        assert_eq!(settings.log_path, cloned.log_path);
        assert_eq!(settings.state_file_path, cloned.state_file_path);
        assert_eq!(settings.elasticsearch.host, cloned.elasticsearch.host);
        assert_eq!(settings.elasticsearch.port, cloned.elasticsearch.port);
    }
}
