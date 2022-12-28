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
use std::path::Path;

use clap::Parser;
use client::ClickhouseHttpClient;
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
    match client_type {
        ClientType::Mysql => {
            let mysql_client = MysqlClient::create().await?;
            Ok(Databend::create(Client::Mysql(mysql_client)))
        }
        ClientType::Http => {
            let http_client = HttpClient::create()?;
            Ok(Databend::create(Client::Http(http_client)))
        }
        ClientType::Clickhouse => {
            let ck_client = ClickhouseHttpClient::create()?;
            Ok(Databend::create(Client::Clickhouse(ck_client)))
        }
    }
}

async fn run_suits(suits: ReadDir, client_type: ClientType) -> Result<()> {
    // Todo: set validator to process regex
    let args = SqlLogicTestArgs::parse();
    let no_fail_fast = args.no_fail_fast;
    let mut error_records = Vec::new();
    // Walk each suit dir and read all files in it
    // After get a slt file, set the file name to databend
    for suit in suits {
        // Get a suit and find all slt files in the suit
        let suit = suit.unwrap().path();
        // Parse the suit and find all slt files
        let files = get_files(suit)?;
        for file in files.into_iter() {
            // For each file, create new client to run.
            let mut runner = sqllogictest::Runner::new(create_databend(&client_type).await?);
            let file_name = file
                .as_ref()
                .unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
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
                println!("test file: [{}] is running", file_name,);
                if no_fail_fast {
                    run_file_async(&mut runner, &mut error_records, file.unwrap().path()).await?;
                } else {
                    runner.run_file_async(file.unwrap().path()).await?;
                }
            }
        }
    }
    if no_fail_fast {
        print_error_info(error_records);
    }

    Ok(())
}

async fn run_file_async(
    runner: &mut Runner<Databend>,
    error_records: &mut Vec<TestError>,
    filename: impl AsRef<Path>,
) -> Result<()> {
    let records = parse_file(filename).unwrap();
    for record in records.into_iter() {
        if let Record::Halt { .. } = record {
            break;
        }
        // Capture error record and continue to run next records
        if let Err(e) = runner.run_async(record).await {
            error_records.push(e);
            continue;
        }
    }
    Ok(())
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
