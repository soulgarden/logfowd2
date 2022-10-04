// #![deny(warnings)]
#![forbid(unsafe_code)]

mod conf;
mod file;
mod sender;
mod signals;
mod watcher;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use json_env_logger2::builder;
use json_env_logger2::env_logger::Target;
use log::LevelFilter;
use tokio::sync::RwLock;

use crate::conf::Conf;
use crate::sender::Sender;
use crate::signals::listen_signals;
use crate::watcher::Watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = builder();

    builder.target(Target::Stdout);
    builder.filter_level(LevelFilter::Debug);
    builder.try_init().unwrap();

    json_env_logger2::panic_hook();

    let conf = Conf::new().unwrap();

    let notify = listen_signals();

    let sub_svc_shutdown_notify = notify.clone();

    let mut watcher = Watcher::new(conf.clone(), Sender::new(conf.clone()));

    let result = tokio::try_join!(tokio::task::spawn(async move { watcher.run().await }),);

    match result {
        Ok(_) => log::info!("shutdown completed"),
        Err(e) => log::error!("thread join error {}", e),
    }

    Ok(())
}
