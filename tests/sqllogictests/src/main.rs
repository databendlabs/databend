// Copyright 2021 Datafuse Labs.
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

use std::fs::ReadDir;
use std::future::Future;
use std::path::Path;
use std::time::Instant;

use clap::Parser;
use client::ClickhouseHttpClient;
use futures_util::stream;
use futures_util::StreamExt;
use sqllogictest::default_validator;
use sqllogictest::parse_file;
use sqllogictest::update_test_file;
use sqllogictest::DBOutput;
use sqllogictest::Record;
use sqllogictest::Runner;
use sqllogictest::TestError;

use crate::arg::SqlLogicTestArgs;
use crate::client::Client;
use crate::client::ClientType;
use crate::client::HttpClient;
use crate::client::MysqlClient;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
use crate::util::get_files;

mod arg;
mod client;
mod error;
mod util;

pub struct Databend {
    client: Client,
}

impl Databend {
    pub fn create(client: Client) -> Self {
        Databend { client }
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Databend {
    type Error = DSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        self.client.query(sql).await
    }

    fn engine_name(&self) -> &str {
        self.client.engine_name()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = SqlLogicTestArgs::parse();
    if let Some(handlers) = &args.handlers {
        for handler in handlers.iter() {
            match handler.as_str() {
                "mysql" => {
                    println!("Mysql client starts to run...");
                    run_mysql_client().await?;
                }
                "http" => {
                    println!("Http client starts to run...");
                    run_http_client().await?;
                }
                "clickhouse" => {
                    println!("Clickhouse http client starts to run...");
                    run_ck_http_client().await?;
                }
                _ => unreachable!(),
            }
        }
        return Ok(());
    }
    // If args don't set handler, run all handlers one by one.

    // First run databend with mysql client
    println!("Mysql client starts to run...");
    run_mysql_client().await?;

    // Second run databend with http client
    println!("Http client starts to run...");
    run_http_client().await?;

    // Third run databend with clickhouse http client
    println!("Clickhouse http client starts to run...");
    run_ck_http_client().await?;

    Ok(())
}

async fn run_mysql_client() -> Result<()> {
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::Mysql).await?;
    Ok(())
}

async fn run_http_client() -> Result<()> {
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::Http).await?;
    Ok(())
}

async fn run_ck_http_client() -> Result<()> {
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::Clickhouse).await?;
    Ok(())
}

// Create new databend with client type
async fn create_databend(client_type: &ClientType) -> Result<Databend> {
    let mut client: Client;
    match client_type {
        ClientType::Mysql => {
            client = Client::Mysql(MysqlClient::create().await?);
        }
        ClientType::Http => {
            client = Client::Http(HttpClient::create()?);
        }
        ClientType::Clickhouse => {
            client = Client::Clickhouse(ClickhouseHttpClient::create()?);
        }
    }
    client.create_sandbox().await?;
    Ok(Databend::create(client))
}

async fn run_suits(suits: ReadDir, client_type: ClientType) -> Result<()> {
    // Todo: set validator to process regex
    let args = SqlLogicTestArgs::parse();
    let mut tasks = vec![];
    // Walk each suit dir and read all files in it
    // After get a slt file, set the file name to databend
    let start = Instant::now();
    for suit in suits {
        // Get a suit and find all slt files in the suit
        let suit = suit.unwrap().path();
        // Parse the suit and find all slt files
        let files = get_files(suit)?;
        for file in files.into_iter() {
            // For each file, create new client to run.
            let mut runner = Runner::new(create_databend(&client_type).await?);
            let file_name = file
                .as_ref()
                .unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            if let Some(ref specific_file) = args.file {
                if !file_name.contains(specific_file) {
                    continue;
                }
            }
            if args.complete {
                let col_separator = " ";
                let validator = default_validator;
                update_test_file(file.unwrap().path(), &mut runner, col_separator, validator)
                    .await
                    .unwrap();
            } else {
                tasks.push(async move { run_file_async(&mut runner, file.unwrap().path()).await })
            }
        }
    }
    if args.complete {
        return Ok(());
    }
    // Run all tasks parallel
    run_parallel_async(tasks).await?;
    let duration = start.elapsed();
    println!("Run all tests using {} ms", duration.as_millis());

    Ok(())
}

async fn run_parallel_async(
    tasks: Vec<impl Future<Output = std::result::Result<Vec<TestError>, TestError>>>,
) -> Result<()> {
    let jobs = tasks.len();
    let tasks = stream::iter(tasks).buffer_unordered(jobs);
    let no_fail_fast = SqlLogicTestArgs::parse().no_fail_fast;
    if !no_fail_fast {
        let errors = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        print_error_info(errors);
    } else {
        let errors: Vec<Vec<TestError>> = tasks
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;
        print_error_info(errors.into_iter().flatten().collect());
    }
    Ok(())
}

async fn run_file_async(
    runner: &mut Runner<Databend>,
    filename: impl AsRef<Path>,
) -> std::result::Result<Vec<TestError>, TestError> {
    let mut error_records = vec![];
    let no_fail_fast = SqlLogicTestArgs::parse().no_fail_fast;
    let records = parse_file(filename).unwrap();
    for record in records.into_iter() {
        if let Record::Halt { .. } = record {
            break;
        }
        // Capture error record and continue to run next records
        if let Err(e) = runner.run_async(record).await {
            if no_fail_fast {
                error_records.push(e);
            } else {
                return Err(e);
            }
        }
    }
    Ok(error_records)
}

fn print_error_info(error_records: Vec<TestError>) {
    if error_records.is_empty() {
        return;
    }
    println!(
        "Test finished, Total {} records failed to run",
        error_records.len()
    );
    for (idx, error_record) in error_records.iter().enumerate() {
        println!("{idx}: {}", error_record.display(true));
    }
}
