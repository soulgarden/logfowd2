use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::{env, fmt, fs, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ConfError {
    pub message: String,
}

impl fmt::Display for ConfError {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "ConfError: {}", self.message)
    }
}

impl std::error::Error for ConfError {}

#[derive(Deserialize, Clone, Debug)]
pub struct Conf {
    pub is_debug: bool,
    pub es: ES,
    pub log_path: String,
    pub state_file_path: Option<String>,
    // If true (default), read existing file content on startup/creation
    pub read_existing_on_startup: Option<bool>,
    // Optional limit for per-read chunked lines to cap memory
    pub read_chunk_size: Option<usize>,
    // Maximum line size in bytes to prevent OOM from very long log lines
    pub max_line_size: Option<usize>,
    pub channels: Option<ChannelConfig>,
    #[allow(dead_code)]
    pub max_concurrent_file_readers: Option<usize>,
    pub metrics: Option<MetricsConfig>,
    #[allow(dead_code)]
    pub logging: Option<LoggingConfig>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ChannelConfig {
    pub watcher_buffer_size: Option<usize>,
    pub es_buffer_size: Option<usize>,
    pub backpressure_threshold: Option<f32>,
    pub backpressure_min_delay_ms: Option<u64>,
    pub backpressure_max_delay_ms: Option<u64>,
    // Notify callback configuration to prevent blocking (EventBridge)
    pub notify_buffer_warning_threshold: Option<usize>,
    pub notify_buffer_max_size: Option<usize>,
    pub notify_drop_on_overflow: Option<bool>,
    // NotifyBridge filesystem event configuration
    pub notify_filesystem_buffer_warning_threshold: Option<usize>,
    pub notify_filesystem_buffer_size: Option<usize>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct MetricsConfig {
    pub enabled: Option<bool>,
    pub port: Option<u16>,
    pub path: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct LoggingConfig {
    pub level: Option<String>,
    pub format: Option<String>, // "structured" or "simple"
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: Some(false), // Metrics disabled by default
            port: Some(9090),
            path: Some("/metrics".to_string()),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: Some("info".to_string()),
            format: Some("simple".to_string()),
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct ES {
    pub host: String,
    pub port: u16,
    pub index_name: String,
    pub flush_interval: u64,
    pub bulk_size: usize,
    pub workers: u16,
}

impl Conf {
    pub fn new() -> Result<Conf, ConfError> {
        let path = match env::var("CFG_PATH") {
            Ok(path) => path,
            Err(_) => "./config.json".to_string(),
        };

        let file = File::open(path).map_err(|e| ConfError {
            message: format!("can't open config.json file, {e}"),
        })?;

        let mut buf_reader = BufReader::new(file);

        let mut contents = String::new();

        buf_reader
            .read_to_string(&mut contents)
            .map_err(|e| ConfError {
                message: format!("can't read config.json file, {e}"),
            })?;

        let conf: Conf = serde_json::from_str(contents.as_str()).map_err(|e| ConfError {
            message: format!("can't parse config.json file, {e}"),
        })?;

        conf.validate()?;

        Ok(conf)
    }

    /// Validate the configuration for logical consistency
    pub fn validate(&self) -> Result<(), ConfError> {
        // Validate Elasticsearch configuration
        self.validate_es_config()?;

        // Validate paths
        self.validate_paths()?;

        // Validate channel configuration
        self.validate_channels()?;

        // Validate performance parameters
        self.validate_performance_params()?;

        // Validate metrics configuration
        self.validate_metrics_config()?;

        // Validate logging configuration
        self.validate_logging_config()?;

        Ok(())
    }

    /// Validate Elasticsearch configuration
    fn validate_es_config(&self) -> Result<(), ConfError> {
        // Ensure at least one ES worker is configured
        if self.es.workers == 0 {
            return Err(ConfError {
                message: "ES workers count must be at least 1, got 0. Zero workers would cause deadlock as no receivers would be available for the work distribution channel.".to_string(),
            });
        }

        // Ensure flush_interval is greater than 0 to prevent tokio::time::interval panic
        if self.es.flush_interval == 0 {
            return Err(ConfError {
                message: "ES flush_interval must be greater than 0, got 0. Zero flush_interval causes tokio::time::interval to panic when creating the timer.".to_string(),
            });
        }

        // Validate ES host URL format
        if !self.es.host.starts_with("http://") && !self.es.host.starts_with("https://") {
            return Err(ConfError {
                message: format!(
                    "ES host must start with http:// or https://, got: '{}'",
                    self.es.host
                ),
            });
        }

        // Basic URL validation - check for valid characters
        if self.es.host.contains(' ') {
            return Err(ConfError {
                message: format!("ES host URL cannot contain spaces, got: '{}'", self.es.host),
            });
        }

        // Validate ES port range
        if self.es.port == 0 {
            return Err(ConfError {
                message: format!("ES port must be in range 1-65535, got: {}", self.es.port),
            });
        }

        // Validate ES index name
        if self.es.index_name.is_empty() {
            return Err(ConfError {
                message: "ES index_name cannot be empty".to_string(),
            });
        }

        // Check for invalid characters in index name (Elasticsearch naming rules)
        if self.es.index_name.starts_with('-')
            || self.es.index_name.starts_with('_')
            || self.es.index_name.starts_with('+')
        {
            return Err(ConfError {
                message: format!(
                    "ES index_name cannot start with -, _, or +, got: '{}'",
                    self.es.index_name
                ),
            });
        }

        if self
            .es
            .index_name
            .chars()
            .any(|c| c.is_uppercase() || c.is_whitespace())
        {
            return Err(ConfError {
                message: format!(
                    "ES index_name cannot contain uppercase letters or whitespace, got: '{}'",
                    self.es.index_name
                ),
            });
        }

        // Validate bulk size
        if self.es.bulk_size == 0 {
            return Err(ConfError {
                message: "ES bulk_size must be greater than 0, got 0".to_string(),
            });
        }

        Ok(())
    }

    /// Validate path configuration
    fn validate_paths(&self) -> Result<(), ConfError> {
        // Validate log_path exists and is readable
        let log_path = Path::new(&self.log_path);
        if !log_path.exists() {
            return Err(ConfError {
                message: format!("log_path does not exist: '{}'", self.log_path),
            });
        }

        if !log_path.is_dir() {
            return Err(ConfError {
                message: format!(
                    "log_path must be a directory, got file: '{}'",
                    self.log_path
                ),
            });
        }

        // Check if directory is readable
        if fs::read_dir(log_path).is_err() {
            return Err(ConfError {
                message: format!("log_path is not readable: '{}'", self.log_path),
            });
        }

        // Validate state_file_path parent directory if specified
        if let Some(state_path) = &self.state_file_path {
            let state_path = Path::new(state_path);
            let parent_dir = state_path.parent().unwrap_or(Path::new("/tmp"));

            if !parent_dir.exists() {
                return Err(ConfError {
                    message: format!(
                        "state_file_path parent directory does not exist: '{}'",
                        parent_dir.display()
                    ),
                });
            }

            // Test if parent directory is writable by trying to create a temporary file
            let test_file = parent_dir.join(".logfowd2_write_test");
            match fs::write(&test_file, b"test") {
                Ok(_) => {
                    let _ = fs::remove_file(&test_file); // Clean up test file
                }
                Err(_) => {
                    return Err(ConfError {
                        message: format!(
                            "state_file_path parent directory is not writable: '{}'",
                            parent_dir.display()
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate channel configuration
    fn validate_channels(&self) -> Result<(), ConfError> {
        if let Some(channels) = &self.channels {
            // Validate buffer sizes are greater than 0
            if let Some(watcher_buffer_size) = channels.watcher_buffer_size
                && watcher_buffer_size == 0 {
                    return Err(ConfError {
                        message: "watcher_buffer_size must be greater than 0, got 0".to_string(),
                    });
                }

            if let Some(es_buffer_size) = channels.es_buffer_size
                && es_buffer_size == 0 {
                    return Err(ConfError {
                        message: "es_buffer_size must be greater than 0, got 0".to_string(),
                    });
                }

            // Validate backpressure threshold is in valid range
            if let Some(threshold) = channels.backpressure_threshold {
                if threshold.is_nan() {
                    return Err(ConfError {
                        message: "backpressure_threshold cannot be NaN".to_string(),
                    });
                }
                if threshold.is_infinite() {
                    return Err(ConfError {
                        message: format!(
                            "backpressure_threshold cannot be infinite, got: {}",
                            threshold
                        ),
                    });
                }
                if !(0.0..=1.0).contains(&threshold) {
                    return Err(ConfError {
                        message: format!(
                            "backpressure_threshold must be in range 0.0-1.0, got: {}",
                            threshold
                        ),
                    });
                }
            }

            // Validate backpressure delay ordering
            if let (Some(min_delay), Some(max_delay)) = (
                channels.backpressure_min_delay_ms,
                channels.backpressure_max_delay_ms,
            )
                && min_delay > max_delay {
                    return Err(ConfError {
                        message: format!(
                            "backpressure_min_delay_ms ({}) must be <= backpressure_max_delay_ms ({})",
                            min_delay, max_delay
                        ),
                    });
                }

            // Validate notify buffer sizes
            if let Some(notify_buffer_size) = channels.notify_buffer_max_size
                && notify_buffer_size == 0 {
                    return Err(ConfError {
                        message: "notify_buffer_max_size must be greater than 0, got 0".to_string(),
                    });
                }

            if let Some(notify_fs_buffer_size) = channels.notify_filesystem_buffer_size
                && notify_fs_buffer_size == 0 {
                    return Err(ConfError {
                        message: "notify_filesystem_buffer_size must be greater than 0, got 0"
                            .to_string(),
                    });
                }
        }

        Ok(())
    }

    /// Validate performance parameters
    fn validate_performance_params(&self) -> Result<(), ConfError> {
        // Validate read_chunk_size
        if let Some(chunk_size) = self.read_chunk_size
            && chunk_size == 0 {
                return Err(ConfError {
                    message: "read_chunk_size must be greater than 0, got 0".to_string(),
                });
            }

        // Validate max_line_size
        if let Some(max_line_size) = self.max_line_size
            && max_line_size == 0 {
                return Err(ConfError {
                    message: "max_line_size must be greater than 0, got 0".to_string(),
                });
            }

        // Validate max_concurrent_file_readers
        if let Some(max_readers) = self.max_concurrent_file_readers
            && max_readers == 0 {
                return Err(ConfError {
                    message: "max_concurrent_file_readers must be greater than 0, got 0"
                        .to_string(),
                });
            }

        Ok(())
    }

    /// Validate metrics configuration
    fn validate_metrics_config(&self) -> Result<(), ConfError> {
        if let Some(metrics) = &self.metrics {
            // Validate metrics port range
            if let Some(port) = metrics.port
                && port == 0 {
                    return Err(ConfError {
                        message: format!("metrics.port must be in range 1-65535, got: {}", port),
                    });
                }

            // Validate metrics path format
            if let Some(path) = &metrics.path {
                if path.is_empty() {
                    return Err(ConfError {
                        message: "metrics.path cannot be empty".to_string(),
                    });
                }
                if !path.starts_with('/') {
                    return Err(ConfError {
                        message: format!("metrics.path must start with '/', got: '{}'", path),
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate logging configuration
    fn validate_logging_config(&self) -> Result<(), ConfError> {
        if let Some(logging) = &self.logging {
            // Validate log level
            if let Some(level) = &logging.level {
                let valid_levels = ["trace", "debug", "info", "warn", "error"];
                if !valid_levels.contains(&level.as_str()) {
                    return Err(ConfError {
                        message: format!(
                            "logging.level must be one of [trace, debug, info, warn, error], got: '{}'",
                            level
                        ),
                    });
                }
            }

            // Validate log format
            if let Some(format) = &logging.format {
                let valid_formats = ["simple", "structured"];
                if !valid_formats.contains(&format.as_str()) {
                    return Err(ConfError {
                        message: format!(
                            "logging.format must be one of [simple, structured], got: '{}'",
                            format
                        ),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conf_error_display() {
        let error = ConfError {
            message: "test error message".to_string(),
        };

        assert_eq!(format!("{}", error), "ConfError: test error message");
    }

    #[test]
    fn test_json_deserialization() {
        let config_json = r#"
        {
            "is_debug": true,
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

        let conf: Conf = serde_json::from_str(config_json).unwrap();

        assert!(conf.is_debug);
        assert_eq!(conf.log_path, "/var/log/pods");
        assert_eq!(conf.state_file_path, Some("/tmp/state.json".to_string()));
        assert_eq!(conf.max_concurrent_file_readers, Some(100));

        // Test channels config
        let channels = conf.channels.unwrap();
        assert_eq!(channels.watcher_buffer_size, Some(5000));
        assert_eq!(channels.es_buffer_size, Some(2000));
        assert_eq!(channels.backpressure_threshold, Some(0.8));
        assert_eq!(channels.notify_buffer_warning_threshold, Some(1000));
        assert_eq!(channels.notify_buffer_max_size, Some(10000));
        assert_eq!(channels.notify_drop_on_overflow, Some(true));

        // Test ES config
        assert_eq!(conf.es.host, "http://localhost");
        assert_eq!(conf.es.port, 9200);
        assert_eq!(conf.es.index_name, "test_index");
        assert_eq!(conf.es.flush_interval, 1000);
        assert_eq!(conf.es.bulk_size, 500);
        assert_eq!(conf.es.workers, 5);
    }

    #[test]
    fn test_minimal_json_deserialization() {
        let config_json = r#"
        {
            "is_debug": false,
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

        let conf: Conf = serde_json::from_str(config_json).unwrap();

        assert!(!conf.is_debug);
        assert_eq!(conf.log_path, "/test/logs");
        assert_eq!(conf.state_file_path, None);
        assert_eq!(conf.max_concurrent_file_readers, None);
        assert_eq!(conf.channels, None);

        assert_eq!(conf.es.host, "http://es-server");
        assert_eq!(conf.es.index_name, "minimal");
    }

    #[test]
    fn test_invalid_json_parsing() {
        let invalid_json = "{ invalid json content }";

        let result: Result<Conf, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_fields() {
        let incomplete_config = r#"
        {
            "is_debug": true
        }
        "#;

        let result: Result<Conf, _> = serde_json::from_str(incomplete_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_channels_config_optional_fields() {
        let config_json = r#"
        {
            "is_debug": true,
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

        let conf: Conf = serde_json::from_str(config_json).unwrap();

        let channels = conf.channels.unwrap();
        assert_eq!(channels.watcher_buffer_size, Some(1000));
        assert_eq!(channels.es_buffer_size, None);
        assert_eq!(channels.backpressure_threshold, None);
        assert_eq!(channels.notify_buffer_warning_threshold, None);
        assert_eq!(channels.notify_buffer_max_size, None);
        assert_eq!(channels.notify_drop_on_overflow, None);
    }

    #[test]
    fn test_channel_config_cloning() {
        let channels = ChannelConfig {
            watcher_buffer_size: Some(5000),
            es_buffer_size: Some(2000),
            backpressure_threshold: Some(0.8),
            backpressure_min_delay_ms: None,
            backpressure_max_delay_ms: None,
            notify_buffer_warning_threshold: Some(1000),
            notify_buffer_max_size: Some(10000),
            notify_drop_on_overflow: Some(true),
            notify_filesystem_buffer_warning_threshold: Some(1000),
            notify_filesystem_buffer_size: Some(2048),
        };

        let cloned = channels.clone();

        assert_eq!(channels.watcher_buffer_size, cloned.watcher_buffer_size);
        assert_eq!(channels.es_buffer_size, cloned.es_buffer_size);
        assert_eq!(
            channels.backpressure_threshold,
            cloned.backpressure_threshold
        );
    }

    #[test]
    fn test_es_config_cloning() {
        let es = ES {
            host: "http://test".to_string(),
            port: 9200,
            index_name: "test".to_string(),
            flush_interval: 1000,
            bulk_size: 500,
            workers: 5,
        };

        let cloned = es.clone();

        assert_eq!(es.host, cloned.host);
        assert_eq!(es.port, cloned.port);
        assert_eq!(es.index_name, cloned.index_name);
        assert_eq!(es.flush_interval, cloned.flush_interval);
        assert_eq!(es.bulk_size, cloned.bulk_size);
        assert_eq!(es.workers, cloned.workers);
    }

    #[test]
    fn test_conf_cloning() {
        let conf = Conf {
            is_debug: true,
            log_path: "/test/path".to_string(),
            state_file_path: Some("/test/state.json".to_string()),
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: Some(50),
            channels: Some(ChannelConfig {
                watcher_buffer_size: Some(1000),
                es_buffer_size: Some(500),
                backpressure_threshold: Some(0.9),
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
            es: ES {
                host: "http://test".to_string(),
                port: 9200,
                index_name: "test".to_string(),
                flush_interval: 2000,
                bulk_size: 1000,
                workers: 3,
            },
        };

        let cloned = conf.clone();

        assert_eq!(conf.is_debug, cloned.is_debug);
        assert_eq!(conf.log_path, cloned.log_path);
        assert_eq!(conf.state_file_path, cloned.state_file_path);
        assert_eq!(
            conf.max_concurrent_file_readers,
            cloned.max_concurrent_file_readers
        );

        let channels = conf.channels.unwrap();
        let cloned_channels = cloned.channels.unwrap();
        assert_eq!(channels, cloned_channels);

        assert_eq!(conf.es.host, cloned.es.host);
        assert_eq!(conf.es.index_name, cloned.es.index_name);
    }

    #[test]
    fn test_json_with_extra_fields() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/var/log/pods",
            "extra_field": "should_be_ignored",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 1000,
                "workers": 1,
                "extra_es_field": "also_ignored"
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();

        assert!(!conf.is_debug);
        assert_eq!(conf.log_path, "/var/log/pods");
        assert_eq!(conf.es.host, "http://localhost");
    }

    #[test]
    fn test_negative_values_in_config() {
        let config_json = r#"
        {
            "is_debug": true,
            "log_path": "/var/log",
            "max_concurrent_file_readers": 0,
            "channels": {
                "watcher_buffer_size": 0,
                "es_buffer_size": 0,
                "backpressure_threshold": -0.5
            },
            "es": {
                "host": "http://localhost",
                "port": 0,
                "index_name": "",
                "flush_interval": 0,
                "bulk_size": 0,
                "workers": 0
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();

        assert_eq!(conf.max_concurrent_file_readers, Some(0));
        // Config can load negative values, but they will be validated/clamped
        // when used in BoundedChannel::new()
        assert_eq!(
            conf.channels.as_ref().unwrap().backpressure_threshold,
            Some(-0.5)
        );
        assert_eq!(conf.es.port, 0);
        assert_eq!(conf.es.workers, 0);
        assert_eq!(conf.es.flush_interval, 0);
        assert_eq!(conf.es.index_name, "");

        // Note: This config would fail validation due to workers=0 and flush_interval=0
        // but this test is only verifying JSON parsing, not validation
        let validation_result = conf.validate();
        assert!(
            validation_result.is_err(),
            "Expected validation to fail due to workers=0 and flush_interval=0"
        );
    }

    #[test]
    fn test_boolean_variations() {
        let config_json = r#"
        {
            "is_debug": 1,
            "log_path": "/var/log",
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

        let result: Result<Conf, _> = serde_json::from_str(config_json);
        assert!(result.is_err()); // Should fail because is_debug expects boolean
    }

    #[test]
    fn test_conf_debug_formatting() {
        let conf = Conf {
            is_debug: true,
            log_path: "/test".to_string(),
            state_file_path: None,
            read_existing_on_startup: None,
            read_chunk_size: None,
            max_line_size: None,
            max_concurrent_file_readers: None,
            channels: None,
            metrics: None,
            logging: None,
            es: ES {
                host: "http://localhost".to_string(),
                port: 9200,
                index_name: "test".to_string(),
                flush_interval: 1000,
                bulk_size: 500,
                workers: 1,
            },
        };

        let debug_str = format!("{:?}", conf);
        assert!(debug_str.contains("is_debug: true"));
        assert!(debug_str.contains("log_path: \"/test\""));
        assert!(debug_str.contains("state_file_path: None"));
        assert!(debug_str.contains("host: \"http://localhost\""));
    }

    #[test]
    fn test_integration_config_with_negative_threshold() {
        // This test verifies that even with a negative threshold in config,
        // the actual channel behavior is corrected by validation
        use crate::channels::create_bounded_channel;

        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/test",
            "channels": {
                "watcher_buffer_size": 10,
                "es_buffer_size": 5,
                "backpressure_threshold": -0.3
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let channels_config = conf.channels.unwrap();

        // Config loads the negative value
        assert_eq!(channels_config.backpressure_threshold, Some(-0.3));

        // But when creating actual channel, the value gets validated/clamped
        let channel: crate::channels::BoundedChannel<String> = create_bounded_channel(
            10,
            channels_config.es_buffer_size,
            channels_config.backpressure_threshold,
            None,
            None,
        );

        // The channel should have a valid threshold (clamped to 0.0)
        assert_eq!(channel.backpressure_threshold(), 0.0);
    }

    #[test]
    fn test_zero_workers_validation() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/test",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 0
            }
        }
        "#;

        // Config should parse successfully
        let conf: Conf = serde_json::from_str(config_json).unwrap();
        assert_eq!(conf.es.workers, 0);

        // But validation should fail
        let validation_result = conf.validate();
        assert!(
            validation_result.is_err(),
            "Expected validation to fail for zero workers"
        );

        let error = validation_result.unwrap_err();
        assert!(
            error
                .message
                .contains("ES workers count must be at least 1")
        );
        assert!(error.message.contains("Zero workers would cause deadlock"));
    }

    #[test]
    fn test_valid_workers_validation() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        assert_eq!(conf.es.workers, 1);

        // Validation should pass
        let validation_result = conf.validate();
        assert!(
            validation_result.is_ok(),
            "Expected validation to pass for one or more workers"
        );
    }

    #[test]
    fn test_multiple_workers_validation() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 5
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        assert_eq!(conf.es.workers, 5);

        // Validation should pass
        let validation_result = conf.validate();
        assert!(
            validation_result.is_ok(),
            "Expected validation to pass for multiple workers"
        );
    }

    #[test]
    fn test_zero_flush_interval_validation() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/test",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 0,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        // Config should parse successfully
        let conf: Conf = serde_json::from_str(config_json).unwrap();
        assert_eq!(conf.es.flush_interval, 0);

        // But validation should fail
        let validation_result = conf.validate();
        assert!(
            validation_result.is_err(),
            "Expected validation to fail for zero flush_interval"
        );

        let error = validation_result.unwrap_err();
        assert!(
            error
                .message
                .contains("flush_interval must be greater than 0")
        );
        assert!(error.message.contains("tokio::time::interval"));
    }

    #[test]
    fn test_valid_flush_interval_validation() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        assert_eq!(conf.es.flush_interval, 1);

        // Validation should pass
        let validation_result = conf.validate();
        assert!(
            validation_result.is_ok(),
            "Expected validation to pass for positive flush_interval"
        );
    }

    // ES Host Validation Tests
    #[test]
    fn test_invalid_es_host_no_protocol() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "elasticsearch.local",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("ES host must start with http://")
        );
    }

    #[test]
    fn test_valid_es_host_https() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "https://elasticsearch.example.com",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_ok());
    }

    #[test]
    fn test_invalid_es_host_with_spaces() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://elasticsearch local",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("cannot contain spaces")
        );
    }

    // ES Port Validation Tests
    #[test]
    fn test_invalid_es_port_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 0,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("port must be in range 1-65535")
        );
    }

    // Note: test_invalid_es_port_too_high is not needed because u16 cannot exceed 65535

    // ES Index Name Validation Tests
    #[test]
    fn test_invalid_es_index_empty() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("index_name cannot be empty")
        );
    }

    #[test]
    fn test_invalid_es_index_starts_with_dash() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "-test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("cannot start with -, _, or +")
        );
    }

    #[test]
    fn test_invalid_es_index_uppercase() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "TestIndex",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("cannot contain uppercase letters")
        );
    }

    // ES Bulk Size Validation Tests
    #[test]
    fn test_invalid_es_bulk_size_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 0,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("bulk_size must be greater than 0")
        );
    }

    // Path Validation Tests
    #[test]
    fn test_invalid_log_path_not_exist() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/nonexistent/path",
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("log_path does not exist")
        );
    }

    // Channel Validation Tests
    #[test]
    fn test_invalid_watcher_buffer_size_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "channels": {
                "watcher_buffer_size": 0
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("watcher_buffer_size must be greater than 0")
        );
    }

    #[test]
    fn test_invalid_backpressure_threshold_range() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "channels": {
                "backpressure_threshold": 1.5
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("backpressure_threshold must be in range 0.0-1.0")
        );
    }

    #[test]
    fn test_invalid_backpressure_delay_ordering() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "channels": {
                "backpressure_min_delay_ms": 100,
                "backpressure_max_delay_ms": 50
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        let error = validation_result.unwrap_err();
        assert!(error.message.contains("backpressure_min_delay_ms"));
        assert!(error.message.contains("backpressure_max_delay_ms"));
    }

    // Performance Parameters Tests
    #[test]
    fn test_invalid_read_chunk_size_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "read_chunk_size": 0,
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("read_chunk_size must be greater than 0")
        );
    }

    #[test]
    fn test_invalid_max_line_size_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "max_line_size": 0,
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("max_line_size must be greater than 0")
        );
    }

    #[test]
    fn test_invalid_max_concurrent_file_readers_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "max_concurrent_file_readers": 0,
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("max_concurrent_file_readers must be greater than 0")
        );
    }

    // Metrics Configuration Tests
    #[test]
    fn test_invalid_metrics_port_zero() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "metrics": {
                "enabled": true,
                "port": 0
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("metrics.port must be in range 1-65535")
        );
    }

    #[test]
    fn test_invalid_metrics_path_no_slash() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "metrics": {
                "enabled": true,
                "path": "metrics"
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        assert!(
            validation_result
                .unwrap_err()
                .message
                .contains("metrics.path must start with '/'")
        );
    }

    // Logging Configuration Tests
    #[test]
    fn test_invalid_logging_level() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "logging": {
                "level": "verbose"
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        let error = validation_result.unwrap_err();
        assert!(error.message.contains("logging.level must be one of"));
        assert!(error.message.contains("verbose"));
    }

    #[test]
    fn test_invalid_logging_format() {
        let config_json = r#"
        {
            "is_debug": false,
            "log_path": "/tmp",
            "logging": {
                "format": "json"
            },
            "es": {
                "host": "http://localhost",
                "port": 9200,
                "index_name": "test",
                "flush_interval": 1000,
                "bulk_size": 100,
                "workers": 1
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(validation_result.is_err());
        let error = validation_result.unwrap_err();
        assert!(error.message.contains("logging.format must be one of"));
        assert!(error.message.contains("json"));
    }

    // Valid Configuration Test
    #[test]
    fn test_comprehensive_valid_config() {
        let config_json = r#"
        {
            "is_debug": true,
            "log_path": "/tmp",
            "state_file_path": "/tmp/test_state.json",
            "read_existing_on_startup": true,
            "read_chunk_size": 200,
            "max_line_size": 1048576,
            "max_concurrent_file_readers": 10,
            "channels": {
                "watcher_buffer_size": 1000,
                "es_buffer_size": 100,
                "backpressure_threshold": 0.8,
                "backpressure_min_delay_ms": 10,
                "backpressure_max_delay_ms": 100,
                "notify_buffer_max_size": 2000,
                "notify_filesystem_buffer_size": 1024
            },
            "metrics": {
                "enabled": true,
                "port": 9090,
                "path": "/metrics"
            },
            "logging": {
                "level": "info",
                "format": "structured"
            },
            "es": {
                "host": "https://elasticsearch.example.com",
                "port": 9200,
                "index_name": "logfowd-test",
                "flush_interval": 5000,
                "bulk_size": 500,
                "workers": 3
            }
        }
        "#;

        let conf: Conf = serde_json::from_str(config_json).unwrap();
        let validation_result = conf.validate();
        assert!(
            validation_result.is_ok(),
            "Valid config should pass validation: {:?}",
            validation_result.err()
        );
    }
}
