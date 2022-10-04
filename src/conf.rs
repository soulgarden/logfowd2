use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use serde::Deserialize;
use serde_json::Result;

#[derive(Deserialize, Clone)]
pub struct Conf {
    pub es: ES,
    pub log_path: String,
}

#[derive(Deserialize, Clone)]
pub struct ES {
    pub host: String,
    pub port: u16,
    pub index_name: String,
    pub flush_interval: u64,
    pub workers: u16,
}

impl Conf {
    pub fn new() -> Result<Self> {
        let file = File::open("config.json").expect("can't open config.json file");

        let mut buf_reader = BufReader::new(file);

        let mut contents = String::new();

        buf_reader
            .read_to_string(&mut contents)
            .expect("can't read config.json file");

        let conf: Conf = serde_json::from_str(contents.as_str())?;

        return Ok(conf);
    }
}
