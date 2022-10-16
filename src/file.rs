use log::{info, warn};
use tokio::fs::File as TokioFile;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, Lines};

use crate::events::{Event, Meta};

pub struct File {
    path: String,
    lines: Lines<BufReader<TokioFile>>,
    meta: Meta,
}

impl File {
    pub async fn new(path: String, meta: Meta) -> Result<Self, std::io::Error> {
        let file = TokioFile::open(path.clone()).await?;

        let reader: BufReader<TokioFile> = BufReader::new(file);

        let lines: Lines<BufReader<TokioFile>> = reader.lines();

        Ok(File { path, lines, meta })
    }

    // just skip all lines after file creation
    pub async fn read_lines(&mut self) {
        loop {
            let line = self.lines.next_line().await;

            match line {
                Ok(Some(line)) => {
                    info!("{}", line);

                    continue;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("error reading line: {}", e);

                    break;
                }
            }
        }
    }

    pub async fn read_line(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::new();

        loop {
            let line = self.lines.next_line().await;

            match line {
                Ok(Some(line)) => {
                    events.push(Event::new(line, self.meta.clone()));

                    continue;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("error reading line: {}", e);

                    break;
                }
            }
        }

        events
    }
}
