use bytes::BufMut;
use chrono::Utc;
use log::warn;
use reqwest::Client;
use tokio::sync::broadcast;

use crate::events::Event;
use crate::requests::{FieldsBody, Index};
use crate::Conf;

pub struct Sender {
    conf: Conf,
    client: Client,
    sender: broadcast::Sender<Event>,
}

impl Sender {
    pub fn new(conf: Conf) -> Self {
        let (tx, _) = broadcast::channel(128);

        Sender {
            conf,
            client: reqwest::Client::new(),
            sender: tx,
        }
    }

    pub async fn run(&self) {
        let mut rx = self.sender.subscribe();

        while let event = rx.recv().await {
            // info!("send queue = {}", event.unwrap().message);

            let mut events = Vec::new();

            events.push(event.unwrap());

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
                .body(self.make_body(events).await)
                .header("Content-Type", "application/json")
                .send()
                .await
                .unwrap();

            warn!(
                "{}, {}",
                resp.status().to_string(),
                resp.text().await.unwrap().as_str()
            );
        }
    }

    pub async fn send(&self, event: Event) {
        self.sender.send(event).unwrap();
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

        warn!("{}", std::str::from_utf8(&body).unwrap());

        body
    }
}
