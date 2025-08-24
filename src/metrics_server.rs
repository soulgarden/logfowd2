use axum::{
    Router,
    response::{IntoResponse, Response},
    routing::get,
};
use log::{error, info};
use std::sync::Arc;
use tokio::sync::Notify;
use tower::ServiceBuilder;

use crate::conf::MetricsConfig;
use crate::metrics::metrics;

pub struct MetricsServer {
    config: MetricsConfig,
}

impl MetricsServer {
    pub fn new(config: MetricsConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self, shutdown: Arc<Notify>) -> Result<(), Box<dyn std::error::Error>> {
        let enabled = self.config.enabled.unwrap_or(false);
        if !enabled {
            info!("Metrics server disabled in configuration");
            return Ok(());
        }

        let port = self.config.port.unwrap_or(9090);
        let path = self.config.path.as_deref().unwrap_or("/metrics");

        let app = Router::new()
            .route(path, get(metrics_handler))
            .route("/health", get(health_handler))
            .layer(ServiceBuilder::new());

        let addr = format!("0.0.0.0:{}", port);

        info!("Starting metrics server on {}{}", addr, path);

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind metrics server to {}: {}", addr, e);
                return Err(Box::new(e));
            }
        };

        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.notified().await;
            info!("Metrics server received shutdown signal");
        });

        if let Err(e) = server.await {
            error!("Metrics server error: {}", e);
            return Err(Box::new(e));
        }

        info!("Metrics server shutdown complete");
        Ok(())
    }
}

// Handler for metrics endpoint
async fn metrics_handler() -> Response {
    let metrics_output = metrics().gather();

    (
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics_output,
    )
        .into_response()
}

// Health check endpoint
async fn health_handler() -> Response {
    "OK".into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::MetricsConfig;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_metrics_server_disabled() {
        let config = MetricsConfig {
            enabled: Some(false),
            port: Some(9091),
            path: Some("/test-metrics".to_string()),
        };

        let server = MetricsServer::new(config);
        let shutdown = Arc::new(Notify::new());

        // Server should return immediately when disabled
        let result = timeout(Duration::from_millis(100), server.run(shutdown)).await;

        assert!(
            result.is_ok(),
            "Server should complete quickly when disabled"
        );
        assert!(
            result.unwrap().is_ok(),
            "Disabled server should not return error"
        );
    }

    #[tokio::test]
    async fn test_metrics_server_default_disabled() {
        // Test with enabled: None to verify default behavior
        let config = MetricsConfig {
            enabled: None, // This should default to false (disabled)
            port: Some(9092),
            path: Some("/test-default-metrics".to_string()),
        };

        let server = MetricsServer::new(config);
        let shutdown = Arc::new(Notify::new());

        // Server should return immediately when enabled is None (defaults to disabled)
        let result = timeout(Duration::from_millis(100), server.run(shutdown)).await;

        assert!(
            result.is_ok(),
            "Server should complete quickly when enabled is None (defaults to disabled)"
        );
        assert!(
            result.unwrap().is_ok(),
            "Default disabled server should not return error"
        );
    }

    #[test]
    fn test_consistency_with_are_metrics_enabled() {
        use crate::metrics::are_metrics_enabled;

        // Test that both functions have the same default behavior
        let config_none: Option<MetricsConfig> = None;
        let config_enabled_none = Some(MetricsConfig {
            enabled: None,
            port: Some(9090),
            path: Some("/metrics".to_string()),
        });

        // Both should return false when config is None or enabled is None
        assert_eq!(
            are_metrics_enabled(&config_none),
            false,
            "are_metrics_enabled should default to false when config is None"
        );
        assert_eq!(
            are_metrics_enabled(&config_enabled_none),
            false,
            "are_metrics_enabled should default to false when enabled is None"
        );

        // Verify that unwrap_or(false) in metrics_server matches this behavior
        assert_eq!(
            config_enabled_none
                .as_ref()
                .unwrap()
                .enabled
                .unwrap_or(false),
            false,
            "metrics_server should also default to false when enabled is None"
        );
    }

    #[tokio::test]
    async fn test_metrics_handler() {
        // Try to initialize metrics, ignore if already initialized
        let _ = crate::metrics::init_metrics();

        let _response = metrics_handler().await;

        // Check that we get a proper response
        // Note: We can't easily extract the body in this test setup,
        // but we can verify the handler doesn't panic
        // Test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_health_handler() {
        let _response = health_handler().await;

        // Health handler should return a simple response
        // Test passes if no panic occurs
    }
}
