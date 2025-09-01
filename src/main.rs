#![deny(warnings)]
#![forbid(unsafe_code)]

extern crate core;

use std::sync::Arc;

use tracing::{error, info, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::config::Settings;
use crate::error::{AppError, Result};
use crate::infrastructure::elasticsearch::EsWorkerPool;
use crate::infrastructure::metrics::MetricsServer;
use crate::sender::Sender;
use crate::signals::listen_signals;
use crate::transport::channels::create_bounded_channel;
use crate::watcher::Watcher;

mod config;
mod domain;
mod error;
mod infrastructure;
#[cfg(test)]
mod integration_tests;
mod requests;
mod retry;
mod sender;
mod signals;
mod task_pool;
mod traits;
mod transport;
mod watcher;

fn init_tracing(config: &Settings) {
    let logging_config = config.logging.clone().unwrap_or_default();
    let filter = EnvFilter::new(&logging_config.log_level);

    if logging_config.log_format == "json" {
        // JSON formatted output for monitoring systems
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_timer(fmt::time::UtcTime::rfc_3339())
                    .with_target(true),
            )
            .with(filter)
            .init();
    } else {
        // Human-readable output for development
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(filter)
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let conf = Settings::load().map_err(|err| {
        eprintln!("failed to load configuration, {}", err);
        AppError::Config(err.to_string())
    })?;

    // Initialize tracing based on configuration
    init_tracing(&conf);

    // Initialize metrics system only if enabled
    let metrics_enabled = crate::infrastructure::metrics::are_metrics_enabled(&conf.metrics);
    if metrics_enabled {
        if let Err(e) = crate::infrastructure::metrics::init_metrics() {
            warn!("Failed to initialize metrics: {}", e);
        } else {
            info!("Metrics system initialized and enabled");
        }
    } else {
        info!("Metrics system disabled in configuration");
    }

    let shutdown_notify: Arc<tokio::sync::Notify> = listen_signals()?;

    let watcher_shutdown_notify = shutdown_notify.clone();
    let sender_shutdown_notify = shutdown_notify.clone();
    let es_shutdown_notify = shutdown_notify.clone();
    let metrics_shutdown_notify = shutdown_notify.clone();

    // Create bounded channels with backpressure
    let channel_config = conf.channels.as_ref();
    let backpressure_threshold = channel_config.and_then(|c| c.backpressure_threshold);
    let backpressure_min_delay_ms = channel_config.and_then(|c| c.backpressure_min_delay_ms);
    let backpressure_max_delay_ms = channel_config.and_then(|c| c.backpressure_max_delay_ms);

    // Channel from sender to ES workers
    let es_queue_channel = create_bounded_channel(
        1000, // default capacity
        channel_config.and_then(|c| c.es_buffer_size),
        backpressure_threshold,
        backpressure_min_delay_ms,
        backpressure_max_delay_ms,
    );
    let es_queue_sender = es_queue_channel.sender();
    let es_queue_receiver = es_queue_channel.receiver();

    // Channel from watcher to sender
    let watcher_channel = create_bounded_channel(
        5000, // default capacity for watcher events
        channel_config.and_then(|c| c.watcher_buffer_size),
        backpressure_threshold,
        backpressure_min_delay_ms,
        backpressure_max_delay_ms,
    );
    let es_process_queue_sender = watcher_channel.sender();
    let es_process_queue_receiver = watcher_channel.receiver();

    let sender = Sender::new(conf.clone(), es_process_queue_receiver, es_queue_sender);

    let mut watcher = Watcher::new(conf.clone(), es_process_queue_sender);

    // Create metrics server
    let metrics_config = conf.metrics.clone().unwrap_or_default();
    let metrics_server = MetricsServer::new(metrics_config);

    let result = if metrics_enabled {
        // Start all components including metrics server
        tokio::try_join!(
            async move {
                watcher
                    .run(watcher_shutdown_notify)
                    .await
                    .map_err(AppError::from)
            },
            async move {
                let mut sender = sender;
                sender.run(sender_shutdown_notify).await
            },
            async move {
                let mut worker_pool = EsWorkerPool::new(conf.clone(), es_queue_receiver).await?;
                worker_pool.run(es_shutdown_notify).await
            },
            async move {
                metrics_server
                    .run(metrics_shutdown_notify)
                    .await
                    .map_err(|e| AppError::ComponentStartup {
                        component: format!("Metrics server: {}", e),
                    })
            },
        )
    } else {
        // Start components without metrics server
        info!("Metrics server disabled - starting core components only");
        tokio::try_join!(
            async move {
                watcher
                    .run(watcher_shutdown_notify)
                    .await
                    .map_err(AppError::from)
            },
            async move {
                let mut sender = sender;
                sender.run(sender_shutdown_notify).await
            },
            async move {
                let mut worker_pool = EsWorkerPool::new(conf.clone(), es_queue_receiver).await?;
                worker_pool.run(es_shutdown_notify).await
            },
            // Dummy future to maintain same tuple structure
            async move {
                tokio::select! {
                    _ = metrics_shutdown_notify.notified() => {
                        info!("Dummy metrics task received shutdown signal");
                    }
                }
                Ok::<(), AppError>(())
            },
        )
    };

    match result {
        Ok(_) => {
            info!("shutdown completed");
            Ok(())
        }
        Err(e) => {
            error!("component failure: {}", e);
            Err(e)
        }
    }
}
