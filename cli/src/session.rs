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

use arrow::array::{Array, ArrayDataBuilder, LargeBinaryArray, LargeStringArray};
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::FlightData;
use futures::TryStreamExt;
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{CompletionType, Editor};
use std::io::BufRead;
use std::sync::Arc;
use tokio::time::Instant;
use tonic::transport::Endpoint;

use crate::display::{format_error, print_batches};
use crate::helper::CliHelper;
use crate::token::{TokenKind, Tokenizer};

pub struct Session {
    client: FlightSqlServiceClient,
    is_repl: bool,
    prompt: String,
}

impl Session {
    pub async fn try_new(
        endpoint: Endpoint,
        user: &str,
        password: &str,
        is_repl: bool,
    ) -> Result<Self, ArrowError> {
        let channel = endpoint
            .connect()
            .await
            .map_err(|err| ArrowError::IoError(err.to_string()))?;

        if is_repl {
            println!("Welcome to databend-cli.");
            println!("Connecting to {} as user {}.", endpoint.uri(), user);
            println!();
        }
        let mut client = FlightSqlServiceClient::new(channel);
        let _token = client.handshake(user, password).await.unwrap();

        let prompt = format!("{} :) ", endpoint.uri().host().unwrap());
        Ok(Self {
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
        if is_repl {
            if query == "exit" || query == "quit" {
                return Ok(true);
            }
            println!("\n{}\n", query);
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

        let flight_data = self.client.do_get(ticket.clone()).await?;
        let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
        let batches = flight_data_to_batches(&flight_data)?;
        let batches = batches
            .iter()
            .map(normalize_record_batch)
            .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

        if is_repl {
            print_batches(batches.as_slice())?;

            println!();

            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!(
                "{} rows in set ({:.3} sec)",
                rows,
                start.elapsed().as_secs_f64()
            );
            println!();
        } else {
            let res = print_batches_with_sep(batches.as_slice(), b'\t')?;
            print!("{res}");
        }

        Ok(false)
    }
}

fn print_batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> Result<String, ArrowError> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .has_headers(false)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    let formatted = String::from_utf8(bytes).map_err(|e| ArrowError::CsvError(e.to_string()))?;
    Ok(formatted)
}

fn get_history_path() -> String {
    format!(
        "{}/.databend_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}

fn normalize_record_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let num_columns = batch.num_columns();
    let mut columns = Vec::with_capacity(num_columns);
    let mut fields = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let field = batch.schema().field(i).clone();
        let array = batch.column(i);
        if let Some(binary_array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
            let data = binary_array.data().clone();
            let builder = ArrayDataBuilder::from(data).data_type(DataType::LargeUtf8);
            let data = builder.build()?;

            let utf8_array = LargeStringArray::from(data);

            columns.push(Arc::new(utf8_array) as Arc<dyn Array>);
            fields.push(
                Field::new(field.name(), DataType::LargeUtf8, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            );
        } else {
            columns.push(array.clone());
            fields.push(field);
        }
    }

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns)
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
