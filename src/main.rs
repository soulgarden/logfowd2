#![deny(warnings)]
#![forbid(unsafe_code)]

extern crate core;

use std::sync::Arc;

use json_env_logger2::builder;
use json_env_logger2::env_logger::Target;
use log::{LevelFilter, warn};
use tokio::sync::RwLock;

use crate::channels::create_bounded_channel;
use crate::conf::Conf;
use crate::es_worker_pool::EsWorkerPool;
use crate::metrics_server::MetricsServer;
use crate::sender::Sender;
use crate::signals::listen_signals;
use crate::watcher::Watcher;

mod channels;
mod circuit_breaker;
mod conf;
mod dead_letter_queue;
mod es_worker_pool;
mod events;
mod file_tracker;
#[cfg(test)]
mod integration_tests;
mod metadata_cache;
mod metrics;
mod metrics_server;
mod requests;
mod retry;
mod sender;
mod signals;
mod state;
mod task_pool;
mod watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    json_env_logger2::panic_hook();

    let mut builder = builder();

    builder.target(Target::Stdout);
    builder.filter_level(LevelFilter::Debug);
    builder.try_init().unwrap();

    let conf = match Conf::new() {
        Ok(conf) => conf,
        Err(err) => {
            warn!("failed to load configuration, {}", err);

            std::process::exit(1);
        }
    };

    // Initialize metrics system only if enabled
    let metrics_enabled = crate::metrics::are_metrics_enabled(&conf.metrics);
    if metrics_enabled {
        if let Err(e) = crate::metrics::init_metrics() {
            warn!("Failed to initialize metrics: {}", e);
        } else {
            log::info!("Metrics system initialized and enabled");
        }
    } else {
        log::info!("Metrics system disabled in configuration");
    }

    if !conf.is_debug {
        log::set_max_level(LevelFilter::Info);
    }

    let shutdown_notify = listen_signals();

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

    let sender = Arc::new(RwLock::new(Sender::new(
        conf.clone(),
        es_process_queue_receiver,
        es_queue_sender,
    )));

    let mut watcher = Watcher::new(conf.clone(), es_process_queue_sender);

    // Create metrics server
    let metrics_config = conf.metrics.clone().unwrap_or_default();
    let metrics_server = MetricsServer::new(metrics_config);

    let result = if metrics_enabled {
        // Start all components including metrics server
        tokio::try_join!(
            tokio::task::spawn(async move {
                watcher.run(watcher_shutdown_notify).await?;
                Ok::<(), notify::Error>(())
            }),
            tokio::task::spawn(async move {
                sender.write().await.run(sender_shutdown_notify).await?;
                Ok::<(), String>(())
            }),
            tokio::task::spawn(async move {
                let mut worker_pool = EsWorkerPool::new(conf.clone(), es_queue_receiver)
                    .await
                    .map_err(|e| format!("Failed to create ES worker pool: {}", e))?;
                worker_pool.run(es_shutdown_notify).await?;
                Ok::<(), String>(())
            }),
            tokio::task::spawn(async move {
                metrics_server
                    .run(metrics_shutdown_notify)
                    .await
                    .map_err(|e| format!("Metrics server error: {}", e))?;
                Ok::<(), String>(())
            }),
        )
    } else {
        // Start components without metrics server
        log::info!("Metrics server disabled - starting core components only");
        tokio::try_join!(
            tokio::task::spawn(async move {
                watcher.run(watcher_shutdown_notify).await?;
                Ok::<(), notify::Error>(())
            }),
            tokio::task::spawn(async move {
                sender.write().await.run(sender_shutdown_notify).await?;
                Ok::<(), String>(())
            }),
            tokio::task::spawn(async move {
                let mut worker_pool = EsWorkerPool::new(conf.clone(), es_queue_receiver)
                    .await
                    .map_err(|e| format!("Failed to create ES worker pool: {}", e))?;
                worker_pool.run(es_shutdown_notify).await?;
                Ok::<(), String>(())
            }),
            // Dummy task to maintain same tuple structure
            tokio::task::spawn(async move {
                tokio::select! {
                    _ = metrics_shutdown_notify.notified() => {
                        log::info!("Dummy metrics task received shutdown signal");
                    }
                }
                Ok::<(), String>(())
            }),
        )
    };

    match result {
        Ok(_) => log::info!("shutdown completed"),
        Err(e) => log::error!("thread join error {}", e),
    }

    Ok(())
}
