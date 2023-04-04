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

use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::root_as_message;

use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{CompletionType, Editor};
use std::io::BufRead;

use tokio::time::Instant;
use tonic::transport::Endpoint;

use crate::ast::{TokenKind, Tokenizer};
use crate::config::Config;
use crate::display::{format_error, ChunkDisplay, FormatDisplay, ReplDisplay};
use crate::helper::CliHelper;

pub struct Session {
    client: FlightSqlServiceClient,
    is_repl: bool,
    config: Config,
    prompt: String,
}

impl Session {
    pub async fn try_new(
        config: Config,
        endpoint: Endpoint,
        user: &str,
        password: &str,
        is_repl: bool,
    ) -> Result<Self, ArrowError> {
        let channel = endpoint
            .connect()
            .await
            .map_err(|err| ArrowError::IoError(err.to_string()))?;

        let mut client = FlightSqlServiceClient::new(channel);

        if is_repl {
            println!("Welcome to databend-cli.");
            println!("Connecting to {} as user {}.", endpoint.uri(), user);
            println!();
        }

        // enable progress
        client.set_header("bendsql", "1");
        let _token = client.handshake(user, password).await.unwrap();

        let mut prompt = config.settings.prompt.clone();

        {
            prompt = prompt.replace("{host}", endpoint.uri().host().unwrap());
            prompt = prompt.replace("{user}", user);
        }

        Ok(Self {
            config,
            client,
            is_repl,
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
                        eprintln!("{}", format_error(e));
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
            if let Err(e) = self.handle_query(false, line).await {
                eprintln!("{}", format_error(e));
            }
        }
    }

    pub async fn handle_query(&mut self, is_repl: bool, query: &str) -> Result<bool, ArrowError> {
        if is_repl && (query == "exit" || query == "quit") {
            return Ok(true);
        }

        let start = Instant::now();

        let kind = QueryKind::from(query);

        if kind == QueryKind::Update {
            let rows = self.client.execute_update(query.to_string()).await?;
            if is_repl {
                println!(
                    "{} affected in ({:.3} sec)",
                    rows,
                    start.elapsed().as_secs_f64()
                );
                println!();
            }
            return Ok(false);
        }

        let mut stmt = self.client.prepare(query.to_string()).await?;
        let flight_info = stmt.execute().await?;
        let ticket = flight_info.endpoint[0]
            .ticket
            .as_ref()
            .ok_or_else(|| ArrowError::IoError("Ticket is empty".to_string()))?;

        let mut flight_data = self.client.do_get(ticket.clone()).await?;
        let datum = flight_data.try_next().await.unwrap().unwrap();

        let message = root_as_message(&datum.data_header[..])
            .map_err(|_| ArrowError::CastError("Cannot get root as message".to_string()))?;
        let ipc_schema = message
            .header_as_schema()
            .ok_or_else(|| ArrowError::CastError("Cannot get header as Schema".to_string()))?;

        let schema = fb_to_schema(ipc_schema);

        if is_repl {
            let mut displayer = ReplDisplay::new(&self.config, query, &schema, start, flight_data);
            displayer.display().await?;
        } else {
            let mut displayer = FormatDisplay::new(schema, flight_data);
            displayer.display().await?;
        }

        Ok(false)
    }
}

fn get_history_path() -> String {
    format!(
        "{}/.databend_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}

#[derive(PartialEq, Eq)]
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
