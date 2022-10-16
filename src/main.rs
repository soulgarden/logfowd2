// #![deny(warnings)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use json_env_logger2::builder;
use json_env_logger2::env_logger::Target;
use log::{debug, warn, LevelFilter};
use tokio::sync::RwLock;

use crate::conf::Conf;
use crate::sender::Sender;
use crate::signals::listen_signals;
use crate::watcher::Watcher;

mod conf;
mod events;
mod file;
mod requests;
mod sender;
mod signals;
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

    if !conf.is_debug {
        log::set_max_level(LevelFilter::Info);
    }

    let notify = listen_signals();

    let sub_svc_shutdown_notify = notify.clone();

    let sender = Arc::new(RwLock::new(Sender::new(conf.clone())));

    let mut watcher = Watcher::new(conf.clone(), sender.clone());

    let result = tokio::try_join!(
        tokio::task::spawn(async move { watcher.run().await }),
        tokio::task::spawn(async move { sender.read().await.run().await }),
    );

    match result {
        Ok(_) => log::info!("shutdown completed"),
        Err(e) => log::error!("thread join error {}", e),
    }

    Ok(())
}
