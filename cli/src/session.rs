// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use databend_driver::{new_connection, Connection};
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{CompletionType, Editor};
use tokio::fs::{remove_file, File};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use crate::ast::{TokenKind, Tokenizer};
use crate::config::Settings;
use crate::display::{ChunkDisplay, FormatDisplay, ReplDisplay};
use crate::helper::CliHelper;

pub struct Session {
    dsn: String,
    conn: Box<dyn Connection>,
    is_repl: bool,

    settings: Settings,
    prompt: String,
}

impl Session {
    pub async fn try_new(dsn: String, settings: Settings, is_repl: bool) -> Result<Self> {
        let conn = new_connection(&dsn)?;
        let info = conn.info();
        if is_repl {
            println!("Welcome to BendSQL.");
            println!(
                "Trying connect to {}:{} as user {}.",
                info.host, info.port, info.user
            );
            let version = conn.version().await?;
            println!("Connected to {}", version);
            println!();
        }

        let mut prompt = settings.prompt.clone();

        {
            prompt = prompt.replace("{host}", &info.host);
            prompt = prompt.replace("{user}", &info.user);
        }

        Ok(Self {
            dsn,
            conn,
            is_repl,
            settings,
            prompt,
        })
    }

    pub async fn handle(&mut self) {
        if self.is_repl {
            self.handle_repl().await;
        } else {
            self.handle_stdin().await;
        }
    }

    pub async fn handle_repl(&mut self) {
        let mut query = "".to_owned();
        let config = Builder::new()
            .completion_prompt_limit(5)
            .completion_type(CompletionType::Circular)
            .build();
        let mut rl = Editor::<CliHelper, DefaultHistory>::with_config(config).unwrap();

        rl.set_helper(Some(CliHelper::new()));
        rl.load_history(&get_history_path()).ok();

        loop {
            match rl.readline(&self.prompt) {
                Ok(line) if line.starts_with("--") => {
                    continue;
                }
                Ok(line) => {
                    let line = line.trim_end();
                    query.push_str(&line.replace("\\\n", ""));
                }
                Err(e) => match e {
                    ReadlineError::Io(err) => {
                        eprintln!("io err: {err}");
                    }
                    ReadlineError::Interrupted => {
                        println!("^C");
                    }
                    ReadlineError::Eof => {
                        break;
                    }
                    _ => {}
                },
            }
            if !query.is_empty() {
                let _ = rl.add_history_entry(query.trim_end());
                match self.handle_query(true, &query).await {
                    Ok(true) => {
                        break;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        if e.to_string().contains("Unauthenticated") {
                            if let Err(e) = self.reconnect().await {
                                eprintln!("Reconnect error: {}", e);
                            } else if let Err(e) = self.handle_query(true, &query).await {
                                eprintln!("{}", e);
                            }
                        } else {
                            eprintln!("{}", e);
                        }
                    }
                }
            }
            query.clear();
        }

        println!("Bye");
        let _ = rl.save_history(&get_history_path());
    }

    pub async fn handle_stdin(&mut self) {
        let mut lines = std::io::stdin().lock().lines();
        // TODO support multi line
        while let Some(Ok(line)) = lines.next() {
            let line = line.trim_end();
            if line.is_empty() {
                continue;
            }
            if let Err(e) = self.handle_query(false, line).await {
                eprintln!("{}", e);
            }
        }
    }

    pub async fn handle_query(&mut self, is_repl: bool, query: &str) -> Result<bool> {
        if is_repl && (query == "exit" || query == "quit") {
            return Ok(true);
        }

        let start = Instant::now();

        let kind = QueryKind::from(query);

        match kind {
            QueryKind::Update => {
                let affected = self.conn.exec(query).await?;
                if is_repl {
                    if affected > 0 {
                        println!(
                            "{} rows affected in ({:.3} sec)",
                            affected,
                            start.elapsed().as_secs_f64()
                        );
                    } else {
                        println!("Processed in ({:.3} sec)", start.elapsed().as_secs_f64());
                    }
                    println!();
                }
                Ok(false)
            }
            QueryKind::Query | QueryKind::Explain => {
                let (schema, data) = self.conn.query_iter_ext(query).await?;

                if is_repl {
                    let mut displayer =
                        ReplDisplay::new(&self.settings, query, start, Arc::new(schema), data);
                    displayer.display().await?;
                } else {
                    let mut displayer =
                        FormatDisplay::new(&self.settings, start, Arc::new(schema), data);
                    displayer.display().await?;
                }
                Ok(false)
            }
        }
    }

    pub async fn stream_load_stdin(
        &mut self,
        query: &str,
        options: BTreeMap<&str, &str>,
    ) -> Result<()> {
        let dir = std::env::temp_dir();
        // TODO:(everpcpc) write by chunks
        let mut lines = std::io::stdin().lock().lines();
        let tmp_file = dir.join(format!("bendsql_{}", chrono::Utc::now().timestamp_nanos()));
        {
            let mut file = File::create(&tmp_file).await?;
            while let Some(Ok(line)) = lines.next() {
                file.write_all(line.as_bytes()).await?;
                file.write_all(b"\n").await?;
            }
            file.flush().await?;
        }
        self.stream_load_file(query, &tmp_file, options).await?;
        remove_file(tmp_file).await?;
        Ok(())
    }

    pub async fn stream_load_file(
        &mut self,
        query: &str,
        file: &Path,
        options: BTreeMap<&str, &str>,
    ) -> Result<()> {
        let _start = Instant::now();
        let file = File::open(file).await?;
        let metadata = file.metadata().await?;

        let _progress = self
            .conn
            .stream_load(query, Box::new(file), metadata.len(), Some(options), None)
            .await?;

        // TODO:(everpcpc) show progress
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.conn = new_connection(&self.dsn)?;
        if self.is_repl {
            let info = self.conn.info();
            println!(
                "Trying reconnect to {}:{} as user {}.",
                info.host, info.port, info.user
            );
            let version = self.conn.version().await?;
            println!("Connected to {}", version);
            println!();
        }
        Ok(())
    }
}

fn get_history_path() -> String {
    format!(
        "{}/.bendsql_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}

#[derive(PartialEq, Eq, Debug)]
pub enum QueryKind {
    Query,
    Update,
    Explain,
}

impl From<&str> for QueryKind {
    fn from(query: &str) -> Self {
        let mut tz = Tokenizer::new(query);
        match tz.next() {
            Some(Ok(t)) => match t.kind {
                TokenKind::EXPLAIN => QueryKind::Explain,
                TokenKind::ALTER
                | TokenKind::UPDATE
                | TokenKind::INSERT
                | TokenKind::CREATE
                | TokenKind::DROP
                | TokenKind::OPTIMIZE
                | TokenKind::COPY => QueryKind::Update,
                _ => QueryKind::Query,
            },
            _ => QueryKind::Query,
        }
    }
}
