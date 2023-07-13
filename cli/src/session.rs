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
use std::io::stdin;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
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
use crate::display::{format_write_progress, ChunkDisplay, FormatDisplay};
use crate::helper::CliHelper;
use crate::VERSION;

pub struct Session {
    dsn: String,
    conn: Box<dyn Connection>,
    is_repl: bool,

    settings: Settings,
    query: String,
    in_comment_block: bool,
}

impl Session {
    pub async fn try_new(dsn: String, settings: Settings, is_repl: bool) -> Result<Self> {
        let conn = new_connection(&dsn)?;
        let info = conn.info().await;
        if is_repl {
            println!("Welcome to BendSQL {}.", VERSION.as_str());
            println!(
                "Trying connect to {}:{} as user {}.",
                info.host, info.port, info.user
            );
            let version = conn.version().await?;
            println!("Connected to {}", version);
            println!();
        }

        Ok(Self {
            dsn,
            conn,
            is_repl,
            settings,
            query: String::new(),
            in_comment_block: false,
        })
    }

    pub async fn handle(&mut self) {
        if self.is_repl {
            self.handle_repl().await;
        } else {
            self.handle_reader(stdin().lock()).await;
        }
    }

    async fn prompt(&self) -> String {
        if !self.query.is_empty() {
            "> ".to_owned()
        } else {
            let info = self.conn.info().await;
            let mut prompt = self.settings.prompt.clone();
            prompt = prompt.replace("{host}", &info.host);
            prompt = prompt.replace("{user}", &info.user);
            prompt = prompt.replace("{port}", &info.port.to_string());
            if let Some(database) = &info.database {
                prompt = prompt.replace("{database}", database);
            } else {
                prompt = prompt.replace("{database}", "");
            }
            if let Some(warehouse) = &info.warehouse {
                prompt = prompt.replace("{warehouse}", &format!("({})", warehouse));
            } else {
                prompt = prompt.replace("{warehouse}", &format!("{}:{}", info.host, info.port));
            }
            format!("{} ", prompt.trim_end())
        }
    }

    pub async fn handle_repl(&mut self) {
        let config = Builder::new()
            .completion_prompt_limit(5)
            .completion_type(CompletionType::Circular)
            .build();
        let mut rl = Editor::<CliHelper, DefaultHistory>::with_config(config).unwrap();

        rl.set_helper(Some(CliHelper::new()));
        rl.load_history(&get_history_path()).ok();

        'F: loop {
            match rl.readline(&self.prompt().await) {
                Ok(line) => {
                    let queries = self.append_query(&line);
                    for query in queries {
                        let _ = rl.add_history_entry(&query);
                        match self.handle_query(true, &query).await {
                            Ok(true) => {
                                break 'F;
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
                                    self.query.clear();
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => match e {
                    ReadlineError::Io(err) => {
                        eprintln!("io err: {err}");
                    }
                    ReadlineError::Interrupted => {
                        self.query.clear();
                        println!("^C");
                    }
                    ReadlineError::Eof => {
                        break;
                    }
                    _ => {}
                },
            }
        }
        println!("Bye~");
        let _ = rl.save_history(&get_history_path());
    }

    pub async fn handle_reader<R: BufRead>(&mut self, r: R) {
        let start = Instant::now();
        let mut lines = r.lines();
        while let Some(Ok(line)) = lines.next() {
            let queries = self.append_query(&line);
            for query in queries {
                if let Err(e) = self.handle_query(false, &query).await {
                    eprintln!("{}", e);
                    return;
                }
            }
        }

        // if the last query is not finished with `;`, we need to execute it.
        let query = self.query.trim().to_owned();
        if !query.is_empty() {
            self.query.clear();
            if let Err(e) = self.handle_query(false, &query).await {
                eprintln!("{}", e);
            }
        }
        if self.settings.time {
            println!("{:.3}", start.elapsed().as_secs_f64());
        }
    }

    pub fn append_query(&mut self, line: &str) -> Vec<String> {
        let line = line.trim();
        if line.is_empty() {
            return vec![];
        }

        if self.query.is_empty() && (line.starts_with('.') || line == "exit" || line == "quit") {
            return vec![line.to_owned()];
        }

        if !self.settings.multi_line {
            if line.starts_with("--") {
                return vec![];
            } else {
                return vec![line.to_owned()];
            }
        }

        self.query.push(' ');

        let mut queries = Vec::new();
        let mut tokenizer = Tokenizer::new(line);
        let mut in_comment = false;
        let mut start = 0;
        let mut comment_block_start = 0;

        while let Some(Ok(token)) = tokenizer.next() {
            match token.kind {
                TokenKind::SemiColon => {
                    if in_comment || self.in_comment_block {
                        continue;
                    } else {
                        let mut sql = self.query.trim().to_owned();
                        if sql.is_empty() {
                            continue;
                        }
                        sql.push(';');

                        queries.push(sql);
                        self.query.clear();
                    }
                }
                TokenKind::Comment => {
                    in_comment = true;
                }
                TokenKind::EOI => {
                    in_comment = false;
                }
                TokenKind::Newline => {
                    in_comment = false;
                    self.query.push(' ');
                }
                TokenKind::CommentBlockStart => {
                    if !self.in_comment_block {
                        comment_block_start = token.span.start;
                    }
                    self.in_comment_block = true;
                }
                TokenKind::CommentBlockEnd => {
                    self.in_comment_block = false;
                    self.query
                        .push_str(&line[comment_block_start..token.span.end]);
                }
                _ => {
                    if !in_comment && !self.in_comment_block {
                        self.query.push_str(&line[start..token.span.end]);
                    }
                }
            }
            start = token.span.end;
        }

        if self.in_comment_block {
            self.query.push_str(&line[comment_block_start..]);
        }
        queries
    }

    pub async fn handle_query(&mut self, is_repl: bool, query: &str) -> Result<bool> {
        let query = query.trim_end_matches(';').trim();
        if is_repl && (query == "exit" || query == "quit") {
            return Ok(true);
        }

        if is_repl && query.starts_with('.') {
            let query = query
                .trim_start_matches('.')
                .split_whitespace()
                .collect::<Vec<_>>();
            if query.len() != 2 {
                return Err(anyhow!(
                    "Control command error, must be syntax of `.cmd_name cmd_value`."
                ));
            }
            self.settings.inject_ctrl_cmd(query[0], query[1])?;
            return Ok(false);
        }

        let start = Instant::now();
        let kind = QueryKind::from(query);
        match kind {
            QueryKind::Update => {
                let affected = self.conn.exec(query).await?;
                if is_repl {
                    if affected > 0 {
                        eprintln!(
                            "{} rows affected in ({:.3} sec)",
                            affected,
                            start.elapsed().as_secs_f64()
                        );
                    } else {
                        eprintln!("Processed in ({:.3} sec)", start.elapsed().as_secs_f64());
                    }
                    eprintln!();
                }
                Ok(false)
            }
            QueryKind::Query | QueryKind::Explain => {
                let replace_newline = replace_newline_in_box_display(query);
                let (schema, data) = self.conn.query_iter_ext(query).await?;
                let mut displayer = FormatDisplay::new(
                    &self.settings,
                    query,
                    replace_newline,
                    start,
                    Arc::new(schema),
                    data,
                );
                displayer.display().await?;
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
        file_path: &Path,
        options: BTreeMap<&str, &str>,
    ) -> Result<()> {
        let start = Instant::now();
        let file = File::open(file_path).await?;
        let metadata = file.metadata().await?;

        let progress = self
            .conn
            .stream_load(query, Box::new(file), metadata.len(), Some(options), None)
            .await?;

        // TODO:(everpcpc) show progress
        if self.settings.show_progress {
            eprintln!(
                "==> Stream Loaded {}:\n    {}",
                file_path.display(),
                format_write_progress(&progress, start.elapsed().as_secs_f64())
            );
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.conn = new_connection(&self.dsn)?;
        if self.is_repl {
            let info = self.conn.info().await;
            eprintln!(
                "Trying reconnect to {}:{} as user {}.",
                info.host, info.port, info.user
            );
            let version = self.conn.version().await?;
            eprintln!("Connected to {}", version);
            eprintln!();
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
                | TokenKind::DELETE
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

fn replace_newline_in_box_display(query: &str) -> bool {
    let mut tz = Tokenizer::new(query);
    match tz.next() {
        Some(Ok(t)) => match t.kind {
            TokenKind::EXPLAIN => false,
            TokenKind::SHOW => !matches!(tz.next(), Some(Ok(t)) if t.kind == TokenKind::CREATE),
            _ => true,
        },
        _ => true,
    }
}
