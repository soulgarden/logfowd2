#![deny(warnings)]
#![forbid(unsafe_code)]

extern crate core;

use std::sync::Arc;

use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

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

fn record_task_result(
    first_error: &mut Option<AppError>,
    component: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) {
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(error) => AppError::from(error),
    };
    error!("{} failed: {}", component, error);
    if first_error.is_none() {
        *first_error = Some(error);
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

    let shutdown_signal = listen_signals()?;
    let watcher_shutdown = Arc::new(tokio::sync::Notify::new());
    let sender_shutdown = Arc::new(tokio::sync::Notify::new());
    let pool_shutdown = Arc::new(tokio::sync::Notify::new());
    let metrics_shutdown = Arc::new(tokio::sync::Notify::new());

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
    let app_state = watcher.state_handle();
    let state_file_path = conf
        .state_file_path
        .clone()
        .unwrap_or("/tmp/logfowd2_state.json".to_string());
    let mut worker_pool = EsWorkerPool::new(conf.clone(), es_queue_receiver)
        .await?
        .with_app_state(app_state.clone());

    drop(watcher_channel);
    drop(es_queue_channel);

    let metrics_config = conf.metrics.clone().unwrap_or_default();
    let metrics_server = MetricsServer::new(metrics_config);
    let watcher_stop = watcher_shutdown.clone();
    let mut watcher_task =
        tokio::spawn(async move { watcher.run(watcher_stop).await.map_err(AppError::from) });
    let sender_stop = sender_shutdown.clone();
    let mut sender_task = tokio::spawn(async move {
        let mut sender = sender;
        sender.run(sender_stop).await
    });
    let pool_stop = pool_shutdown.clone();
    let mut pool_task = tokio::spawn(async move { worker_pool.run(pool_stop).await });
    let mut metrics_task = if metrics_enabled {
        let metrics_stop = metrics_shutdown.clone();
        Some(tokio::spawn(async move {
            metrics_server
                .run(metrics_stop)
                .await
                .map_err(|error| AppError::ComponentStartup {
                    component: format!("Metrics server: {}", error),
                })
        }))
    } else {
        None
    };

    let mut watcher_result = None;
    let mut sender_result = None;
    let mut pool_result = None;
    let mut metrics_result = None;
    tokio::select! {
        _ = shutdown_signal.notified() => info!("shutdown requested"),
        result = &mut watcher_task => watcher_result = Some(result),
        result = &mut sender_task => sender_result = Some(result),
        result = &mut pool_task => pool_result = Some(result),
        result = async {
            match metrics_task.as_mut() {
                Some(task) => task.await,
                None => std::future::pending().await,
            }
        } => metrics_result = Some(result),
    }

    watcher_shutdown.notify_one();
    let mut first_error = None;
    record_task_result(
        &mut first_error,
        "watcher",
        match watcher_result {
            Some(result) => result,
            None => watcher_task.await,
        },
    );
    record_task_result(
        &mut first_error,
        "sender",
        match sender_result {
            Some(result) => result,
            None => sender_task.await,
        },
    );
    record_task_result(
        &mut first_error,
        "Elasticsearch pool",
        match pool_result {
            Some(result) => result,
            None => pool_task.await,
        },
    );

    metrics_shutdown.notify_one();
    if let Some(task) = metrics_task {
        record_task_result(
            &mut first_error,
            "metrics server",
            match metrics_result {
                Some(result) => result,
                None => task.await,
            },
        );
    }

    let snapshot = app_state.read().await.clone_for_save();
    if let Err(error) = snapshot.save_to_file(&state_file_path) {
        record_task_result(
            &mut first_error,
            "final state save",
            Ok(Err(AppError::ComponentStartup {
                component: format!("state save: {}", error),
            })),
        );
    }

    if let Some(error) = first_error {
        Err(error)
    } else {
        info!("shutdown completed");
        Ok(())
    }
}
