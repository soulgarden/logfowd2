use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use log::{debug, error, info, warn};
use notify::event::ModifyKind::{Data, Name};
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use regex::Regex;
use tokio::sync::mpsc::channel;
use tokio::sync::RwLock;

use crate::events::Meta;
use crate::file::File;
use crate::sender::Sender;
use crate::Conf;

const K8S_PODS_REGEXP: &str = "^/var/log/pods/(?P<namespace>[a-z0-9-]+)_(?P<pod_name>[a-z0-9-]+)_(?P<pod_id>[a-z0-9-]+)/(?P<container_name>[a-z-0-9]+)/(?P<num>[0-9]+).log$";

pub struct Watcher {
    conf: Conf,
    sender: Arc<RwLock<Sender>>,
    events: Vec<String>,
    files: HashMap<String, File>,
    regexp: Regex,
}

impl Watcher {
    pub fn new(conf: Conf, sender: Arc<RwLock<Sender>>) -> Self {
        Watcher {
            conf,
            sender,
            events: Vec::new(),
            files: HashMap::new(),
            regexp: Regex::new(K8S_PODS_REGEXP).unwrap(),
        }
    }

    pub async fn run(&mut self) {
        let log_path = self.conf.clone().log_path;

        let path = Path::new(log_path.as_str());

        // todo: read files from dir

        info!("watching {}", path.clone().display());

        if let Err(e) = self.async_watch(path.clone()).await {
            error!("error: {:?}", e)
        }
    }

    async fn async_watch<P: AsRef<Path>>(&mut self, path: P) -> notify::Result<()> {
        let (tx, mut rx) = channel(1024);

        let mut watcher =
            RecommendedWatcher::new(move |res| tx.blocking_send(res).unwrap(), Config::default())?;

        watcher.watch(path.as_ref(), RecursiveMode::Recursive)?;

        while let Some(event) = rx.recv().await {
            match event {
                Ok(event) => {
                    debug!("changed: {:?}", event.clone());

                    match event.kind {
                        EventKind::Create(_file) => {
                            let mut meta = Meta::default();

                            // todo: move parsing to files // check error
                            self.regexp
                                .captures(event.paths[0].to_str().unwrap())
                                .map(|caps| {
                                    meta.namespace =
                                        caps.name("namespace").unwrap().as_str().to_string();
                                    meta.pod_name =
                                        caps.name("pod_name").unwrap().as_str().to_string();
                                    meta.pod_id = caps.name("pod_id").unwrap().as_str().to_string();
                                    meta.container_name =
                                        caps.name("container_name").unwrap().as_str().to_string();
                                });

                            let file =
                                File::new(event.paths[0].to_str().unwrap().to_string(), meta).await;

                            match file {
                                Err(e) => {
                                    warn!("error opening file: {}", e);

                                    continue;
                                }
                                _ => {}
                            }

                            let mut file = file.unwrap();

                            let events: Vec<crate::events::Event> = file.read_line().await;

                            let sender = self.sender.read().await;

                            for event in events {
                                sender.send(event).await;
                            }

                            drop(sender);

                            self.files
                                .insert(event.paths[0].to_str().unwrap().to_string(), file);
                        }
                        EventKind::Modify(Data(_data_change)) => {
                            let file = self.files.get_mut(event.paths[0].to_str().unwrap());

                            match file {
                                Some(file) => {
                                    let events: Vec<crate::events::Event> = file.read_line().await;

                                    let sender = self.sender.read().await;

                                    for event in events {
                                        sender.send(event).await;
                                    }

                                    drop(sender);
                                }
                                None => {
                                    warn!("file not found");
                                }
                            }
                        }
                        EventKind::Modify(Name(_rename_mode)) => {
                            debug!("file was renamed");
                        }
                        EventKind::Remove(_) => {
                            debug!(
                                "file was removed, {}",
                                event.paths[0].to_str().unwrap().to_string()
                            );

                            self.files.remove(event.paths[0].to_str().unwrap());
                        }
                        _ => {}
                    }
                }
                Err(e) => info!("watch error: {:?}", e),
            }
        }

        Ok(())
    }
}
