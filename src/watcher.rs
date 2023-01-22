use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use log::{debug, error, info, warn};
use notify::event::ModifyKind::{Data, Name};
use notify::{
    Config, Error, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher,
};
use regex::Regex;
use tokio::sync::mpsc::channel;
use tokio::sync::Notify;

use crate::events::Meta;
use crate::file::File;
use crate::sender::Sender;
use crate::Conf;

const K8S_PODS_REGEXP: &str = r"^/var/log/pods/(?P<namespace>[a-z0-9-]+)_(?P<pod_name>[a-z0-9-]+)_(?P<pod_id>[a-z0-9-]+)/(?P<container_name>[a-z-0-9]+)/(?P<num>[0-9]+).log$";

pub struct Watcher {
    conf: Conf,
    sender: Arc<Sender>,
    files: HashMap<String, File>,
    regexp: Regex,
}

impl Watcher {
    pub fn new(conf: Conf, sender: Arc<Sender>) -> Self {
        Watcher {
            conf,
            sender,
            files: HashMap::new(),
            regexp: Regex::new(K8S_PODS_REGEXP).unwrap(),
        }
    }

    pub async fn run(&mut self, notify: Arc<Notify>) -> Result<(), Error> {
        let log_path = self.conf.clone().log_path;

        let path = Path::new(log_path.as_str());

        if !path.is_absolute() {
            return Err(Error::generic("log_path must be absolute path"));
        }

        info!("watching for {}", path.clone().display());

        self.sync_files(path.clone()).await;

        if let Err(e) = self.watch(path.clone(), notify).await {
            error!("error: {:?}", e);

            return Err(e);
        }

        Ok(())
    }

    async fn watch(&mut self, path: &Path, notify: Arc<Notify>) -> notify::Result<()> {
        let (tx, mut rx) = channel(1024);

        let mut watcher =
            RecommendedWatcher::new(move |res| tx.blocking_send(res).unwrap(), Config::default())?;

        watcher.watch(path.as_ref(), RecursiveMode::Recursive)?;

        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event {
                        Ok(event) => {
                            debug!("changed: {:?}", event.clone());

                            match event.kind {
                                EventKind::Create(_file) => {
                                    let file = self.create_file(&path).await;

                                    if file.is_err() {
                                        warn!("error opening file: {}", file.err().unwrap());

                                        continue;
                                    }

                                    let mut file = file.unwrap();

                                    let events: Vec<crate::events::Event> = file.read_line().await;

                                    for event in events {
                                        self.sender.send(event).await;
                                    }

                                    self.files.insert(event.paths[0].to_str().unwrap().to_string(), file);
                                }
                                EventKind::Modify(Data(_data_change)) => {
                                    let file = self.files.get_mut(event.paths[0].to_str().unwrap());

                                    match file {
                                        Some(file) => {
                                            let events: Vec<crate::events::Event> = file.read_line().await;

                                            for event in events {
                                                self.sender.send(event).await;
                                            }
                                        }
                                        None => {
                                            warn!("file not found, {}", event.paths[0].to_str().unwrap().to_string());
                                        }
                                    }
                                }
                                EventKind::Modify(Name(_rename_mode)) => {
                                    debug!("file was renamed, {}", event.paths[0].to_str().unwrap().to_string());
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
                _ = notify.notified() => {
                    log::info!("sender received shutdown signal");

                    return Ok(())
                }
            }
        }
    }

    fn sync_files<'a>(&'a mut self, path: &'a Path) -> BoxFuture<'a, ()> {
        async {
            if path.is_dir() {
                for entry in path.read_dir().unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();

                    // todo: is symlink?
                    if path.is_dir() {
                        self.sync_files(&path).await;
                    } else {
                        let file = self.create_file(&path).await.unwrap();

                        self.files.insert(path.to_str().unwrap().to_string(), file);

                        debug!("file inserted: {}", path.to_str().unwrap());

                        // seek to the end
                    }
                }
            }
        }
        .boxed()
    }

    async fn create_file(&mut self, path: &Path) -> Result<File, std::io::Error> {
        let mut meta = Meta::default();

        // todo: move parsing to files // check error
        self.regexp.captures(path.to_str().unwrap()).map(|caps| {
            meta.namespace = caps.name("namespace").unwrap().as_str().to_string();
            meta.pod_name = caps.name("pod_name").unwrap().as_str().to_string();
            meta.pod_id = caps.name("pod_id").unwrap().as_str().to_string();
            meta.container_name = caps.name("container_name").unwrap().as_str().to_string();
        });

        File::new(path.to_str().unwrap().to_string(), meta).await
    }
}
