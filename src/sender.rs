use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};

use tokio::sync::{Notify, RwLock};
use tokio::time::interval;

use crate::events::Event;
use crate::Conf;

pub struct Sender {
    conf: Conf,
    events: RwLock<Vec<Event>>,
    process_queue_receiver: async_channel::Receiver<Event>,
    es_queue_sender: async_channel::Sender<Vec<Event>>,
}

impl Sender {
    pub fn new(
        conf: Conf,
        process_queue: async_channel::Receiver<Event>,
        es_queue_sender: async_channel::Sender<Vec<Event>>,
    ) -> Self {
        Sender {
            conf,
            events: RwLock::new(Vec::new()),
            process_queue_receiver: process_queue,
            es_queue_sender,
        }
    }

    pub async fn run(&mut self, shutdown: Arc<Notify>) -> Result<(), String> {
        let mut ticker = interval(Duration::from_millis(self.conf.es.flush_interval));

        loop {
            tokio::select! {
                // send by timer
                 _ = ticker.tick() => {
                    debug!("ticker ticked");

                    if self.events.read().await.len() == 0 {
                        continue;
                    }

                    loop {
                        if self.events.read().await.len() == 0 {
                            break;
                        }

                        let mut events = Vec::new();

                        loop {
                            if events.len() == self.conf.es.bulk_size {
                                break;
                            }

                            match self.events.write().await.pop() {
                                Option::Some(event) => {
                                    events.push(event);
                                }
                                None => {
                                    warn!("no more events");
                                    break;
                                }
                            }
                        }

                        self.es_queue_sender.send(events).await.unwrap();
                     }
                }

                // send by limit
                event = self.process_queue_receiver.recv() => {
                    self.events.write().await.push(event.unwrap());

                    if self.events.read().await.len() >= self.conf.es.bulk_size {
                        let mut events = Vec::new();

                        loop {
                            if events.len() == self.conf.es.bulk_size {
                                break;
                            }

                            events.push(self.events.write().await.pop().unwrap());
                        }

                        self.es_queue_sender.send(events).await.unwrap();
                    }
                }
                 _ = shutdown.notified() => {
                    log::info!("sender received shutdown signal");

                    // todo: send remaining events, copy first statement

                    return Ok(())
                }
            }
        }
    }
}
