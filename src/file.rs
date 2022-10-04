use log::warn;
use tokio::fs::File as TokioFile;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, Lines};

pub struct File {
    path: String,
    lines: Lines<BufReader<TokioFile>>,
}

impl File {
    pub async fn new(path: String) -> Self {
        let file = TokioFile::open(path.clone())
            .await
            .expect("can't open file");

        let reader: BufReader<TokioFile> = BufReader::new(file);

        let lines: Lines<BufReader<TokioFile>> = reader.lines();

        File { path, lines }
    }

    // just skip all lines after file creation
    pub async fn read_lines(&mut self) {
        loop {
            let line = self.lines.next_line().await;

            match line {
                Ok(Some(line)) => {
                    println!("{}", line);

                    continue;
                }
                Ok(None) => {
                    warn!("empty line");

                    break;
                }
                Err(e) => {
                    warn!("error reading line: {}", e);
                }
            }
        }
    }
}
