use bytes::BufMut;
use chrono::Utc;
use log::debug;
use reqwest::Client;

use crate::events::Event;
use crate::requests::{FieldsBody, Index};
use crate::Conf;

pub struct Es {
    conf: Conf,
    client: Client,
    es_queue_receiver: async_channel::Receiver<Vec<Event>>,
}

impl Es {
    pub fn new(conf: Conf, es_queue_receiver: async_channel::Receiver<Vec<Event>>) -> Self {
        Es {
            conf,
            client: Client::new(),
            es_queue_receiver,
        }
    }

    pub async fn run(&self) -> Result<(), String> {
        loop {
            tokio::select! {
                events = self.es_queue_receiver.recv() => {
                    let resp = self
                        .client
                        .post(
                            self.conf.es.host.clone()
                                + ":"
                                + self.conf.es.port.to_string().as_str()
                                + "/"
                                + self.conf.es.index_name.as_str()
                                + "-"
                                + Utc::now().format("%Y.%m.%d").to_string().as_str()
                                + "/_bulk",
                        )
                        .body(self.make_body(events.unwrap()).await)
                        .header("Content-Type", "application/json")
                        .send()
                        .await
                        .unwrap(); // todo: handle error

                    debug!(
                        "es resp: {}, {}",
                        resp.status().to_string(),
                        resp.text().await.unwrap().as_str()
                    );
                }
            }
        }
    }

    pub async fn make_body(&self, events: Vec<Event>) -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();

        let index = Index::new();

        serde_json::to_writer(&mut body, &index).unwrap();

        body.put_slice(b"\n");

        for event in events {
            let fields_body = FieldsBody::new(
                event.message,
                event.timestamp,
                event.meta.pod_name,
                event.meta.namespace,
                event.meta.container_name,
                event.meta.pod_id,
            );

            serde_json::to_writer(&mut body, &fields_body).unwrap();

            body.put_slice(b"\n");
        }

        body.put_slice(b"\n");

        debug!("body {}", std::str::from_utf8(&body).unwrap());

        body
    }
}
