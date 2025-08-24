use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::{env, fmt};

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
            "log_path": "/test",
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
            "log_path": "/test",
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
            "log_path": "/test",
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
}
