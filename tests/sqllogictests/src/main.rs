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
use std::path::PathBuf;

use client::ClickhouseHttpClient;
use sqllogictest::DBOutput;
use walkdir::DirEntry;
use walkdir::WalkDir;

use crate::client::HttpClient;
use crate::client::MysqlClient;
use crate::error::DSqlLogicTestError;
use crate::error::Result;

mod client;
mod error;

const TEST_SUITS: &str = "tests/logictest/suites";

pub struct Databend {
    mysql_client: Option<MysqlClient>,
    http_client: Option<HttpClient>,
    ck_client: Option<ClickhouseHttpClient>,
}

impl Databend {
    pub fn create(
        mysql_client: Option<MysqlClient>,
        http_client: Option<HttpClient>,
        ck_client: Option<ClickhouseHttpClient>,
    ) -> Self {
        Databend {
            mysql_client,
            http_client,
            ck_client,
        }
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Databend {
    type Error = DSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        if let Some(mysql_client) = &mut self.mysql_client {
            return mysql_client.query(sql);
        }
        if let Some(http_client) = &mut self.http_client {
            return http_client.query(sql).await;
        }
        self.ck_client.as_mut().unwrap().query(sql).await
    }

    fn engine_name(&self) -> &str {
        "databend"
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // First run databend with mysql client
    run_mysql_client().await?;
    // Second run databend with http client
    run_http_client().await?;
    // Third run databend with clickhouse http client
    run_ck_http_client().await?;

    Ok(())
}

async fn run_mysql_client() -> Result<()> {
    let suits = std::fs::read_dir(TEST_SUITS).unwrap();
    let mysql_client = MysqlClient::create()?;
    let databend = Databend::create(Some(mysql_client), None, None);
    run_suits(suits, databend).await?;
    Ok(())
}

async fn run_http_client() -> Result<()> {
    let suits = std::fs::read_dir(TEST_SUITS).unwrap();
    let http_client = HttpClient::create()?;
    let databend = Databend::create(None, Some(http_client), None);
    run_suits(suits, databend).await?;
    Ok(())
}

async fn run_ck_http_client() -> Result<()> {
    let suits = std::fs::read_dir(TEST_SUITS).unwrap();
    let ck_client = ClickhouseHttpClient::create()?;
    let databend = Databend::create(None, None, Some(ck_client));
    run_suits(suits, databend).await?;
    Ok(())
}

async fn run_suits(suits: ReadDir, databend: Databend) -> Result<()> {
    let mut runner = sqllogictest::Runner::new(databend);
    // Walk each suit dir and read all files in it
    // After get a slt file, set the file name to databend
    for suit in suits {
        // Get a suit and find all slt files in the suit
        let suit = suit.unwrap().path();
        // Parse the suit and find all slt files
        let files = get_files(suit)?;
        for file in files.into_iter() {
            println!(
                "[{}] is running",
                file.as_ref()
                    .unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
            );
            runner.run_file_async(file.unwrap().path()).await?;
        }
    }

    Ok(())
}

fn get_files(suit: PathBuf) -> Result<Vec<walkdir::Result<DirEntry>>> {
    let mut files = vec![];
    for entry in WalkDir::new(suit)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter(|e| !e.as_ref().unwrap().file_type().is_dir())
    {
        // dbg!(entry.unwrap().path());
        files.push(entry);
    }
    Ok(files)
}
