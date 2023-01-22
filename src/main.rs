// #![deny(warnings)]
#![forbid(unsafe_code)]

extern crate core;

use std::sync::Arc;

use json_env_logger2::builder;
use json_env_logger2::env_logger::Target;
use log::{warn, LevelFilter};

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

    let watcher_shutdown_notify = notify.clone();
    let sender_shutdown_notify = notify.clone();

    let sender = Arc::new(Sender::new(conf.clone()));

    let mut watcher = Watcher::new(conf.clone(), sender.clone());

    let result = tokio::try_join!(
        tokio::task::spawn(async move {
            watcher.run(watcher_shutdown_notify).await?;
            Ok::<(), notify::Error>(())
        }),
        tokio::task::spawn(async move {
            sender.run(sender_shutdown_notify).await?;
            Ok::<(), String>(())
        }),
    );

    match result {
        Ok((Err(e1), Err(e2))) => {
            log::error!("watcher and sender finished with errors: {}, {}", e1, e2)
        }
        Ok((Ok(()), Err(e2))) => log::error!("sender finished with error: {}", e2),
        Ok((Err(e1), Ok(()))) => log::error!("watcher finished with error: {}", e1),
        Ok(_) => log::info!("shutdown completed"),
        Err(e) => log::error!("thread join error {}", e),
    }

    Ok(())
}
