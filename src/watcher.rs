use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use futures::{
    // channel::mpsc::{channel, Receiver},
    SinkExt,
    StreamExt,
};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use tokio::sync::mpsc::{channel, Receiver};

use crate::file::File;
use crate::sender::Sender;
use crate::Conf;

pub struct Watcher {
    conf: Conf,
    sender: Sender,
    events: Vec<String>,
    files: HashMap<String, File>,
}

impl Watcher {
    pub fn new(conf: Conf, sender: Sender) -> Self {
        Watcher {
            conf,
            sender,
            events: Vec::new(),
            files: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        let log_path = self.conf.clone().log_path;

        let path = Path::new(log_path.as_str());

        println!("watching {}", path.clone().display());

        if let Err(e) = self.async_watch(path.clone()).await {
            println!("error: {:?}", e)
        }
    }

    async fn async_watch<P: AsRef<Path>>(&mut self, path: P) -> notify::Result<()> {
        let (mut tx, mut rx) = channel(1024);

        let mut watcher =
            RecommendedWatcher::new(move |res| tx.blocking_send(res).unwrap(), Config::default())?;

        watcher.watch(path.as_ref(), RecursiveMode::Recursive)?;

        while let Some(event) = rx.recv().await {
            match event {
                Ok(event) => {
                    println!("changed: {:?}", event.clone());

                    if event.kind.is_create() {
                        let mut file =
                            File::new(event.paths[0].to_str().unwrap().to_string()).await;

                        file.read_lines().await;

                        self.files
                            .insert(event.paths[0].to_str().unwrap().to_string(), file);
                    } else if event.kind.is_modify() {
                        // todo: read line
                    } else if event.kind.is_remove() {
                        // todo: close file
                    }
                }
                Err(e) => println!("watch error: {:?}", e),
            }
        }

        Ok(())
    }
}
