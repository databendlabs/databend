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

use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;
use std::path::PathBuf;
use std::ptr::write;

use client::ClickhouseHttpClient;
use mysql::prelude::Queryable;
use mysql::serde_json;
use mysql::Conn;
use mysql::Pool;
use mysql::Row;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde_json::Value;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::client::HttpClient;
use crate::client::MysqlClient;
use crate::error::DSqlLogicTestError;
use crate::error::Result;

mod client;
mod error;

const TEST_SUITS: &str = "tests/sqllogictests/suits";

pub struct Databend {
    mysql_client: Option<MysqlClient>,
    http_client: Option<HttpClient>,
    ck_client: Option<ClickhouseHttpClient>,
    file_name: String,
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Databend {
    type Error = DSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        println!("[{}] running query: \"{}\"", self.file_name, sql);
        if let Some(mysql_client) = &mut self.mysql_client {
            return mysql_client.query(sql);
        }
        if let Some(http_client) = &mut self.http_client {
            return http_client.query(sql).await;
        }
        if let Some(ck_client) = &mut self.ck_client {
            return ck_client.query(sql).await;
        }
        Ok(DBOutput::StatementComplete(1))
    }

    fn engine_name(&self) -> &str {
        todo!()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let suits = std::fs::read_dir(TEST_SUITS).unwrap();

    for suit in suits {
        let suit = suit.unwrap().path();
        run_suit(&suit).await?;
    }

    Ok(())
}

async fn run_suit(suit: &Path) -> Result<()> {
    // Todo: walking dir util read files
    // let file_name = suit.file_name().unwrap().to_str().unwrap().to_string();
    // Create databend
    let mysql_client = MysqlClient::create()?;
    let file_name = suit.file_name().unwrap().to_str().unwrap();
    let mut databend = Databend {
        mysql_client: Some(mysql_client),
        http_client: None,
        ck_client: None,
        file_name: file_name.to_string(),
    };
    let mut runner = sqllogictest::Runner::new(databend);
    runner.run_file_async(suit).await?;

    let http_client = HttpClient::create()?;
    let mut databend = Databend {
        mysql_client: None,
        http_client: Some(http_client),
        ck_client: None,
        file_name: file_name.to_string(),
    };
    let mut runner = sqllogictest::Runner::new(databend);
    runner.run_file_async(suit).await?;

    let ck_client = ClickhouseHttpClient::create()?;
    let mut databend = Databend {
        mysql_client: None,
        http_client: None,
        ck_client: Some(ck_client),
        file_name: file_name.to_string(),
    };
    let mut runner = sqllogictest::Runner::new(databend);
    runner.run_file_async(suit).await?;
    Ok(())
}
