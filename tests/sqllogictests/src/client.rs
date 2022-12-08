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
use std::process::Output;
use mysql::Pool;
use mysql::PooledConn;
use mysql::prelude::Queryable;
use mysql::Row;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use crate::error::Result;

#[derive(Debug)]
pub struct MysqlClient {
    pub conn: PooledConn,
}

impl MysqlClient {
    pub fn create() -> Result<MysqlClient> {
        let url = "mysql://root:@127.0.0.1:3307/default";
        let pool = Pool::new(url)?;
        let mut conn = pool.get_conn()?;
        Ok(MysqlClient { conn })
    }

    pub fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let rows: Vec<Row> = self.conn.query(sql)?;
        let types = vec![ColumnType::Any; rows.len()];
        let mut parsed_rows = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let mut parsed_row = Vec::new();
            for i in 0..row.len() {
                let value: Option<Option<String>> = row.get(i);
                match value {
                    Some(v) => match v {
                        None => parsed_row.push("NULL".to_string()),
                        Some(s) if s.is_empty() => parsed_row.push("(empty)".to_string()),
                        Some(s) => parsed_row.push(s),
                    },
                    _ => {}
                }
            }
            parsed_rows.push(parsed_row);
        }
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    pub fn name(&self) -> &'static str {
        "mysql_client"
    }
}

pub struct HttpClient {
    pub client: Client,
    pub url: String,
}

impl HttpClient {
    pub fn create() -> Result<HttpClient> {
        let client = Client::new();
        let url = "http://127.0.0.1:8000/v1/query".to_string();
        Ok(HttpClient {
            client,
            url,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let mut header = HeaderMap::new();
        header.insert("Content-Type", HeaderValue::from_str("application/json").unwrap());
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        let mut query = HashMap::new();
        query.insert("sql", sql);
        let response = self.client.post(&self.url).json(&query).
            basic_auth("root", Some("")).headers(header).send().await?;
        let data: Value = serde_json::from_str(response.text().await?.as_str()).unwrap();
        let rows = data.as_object().unwrap().get("data").unwrap();
        let mut parsed_rows = Vec::new();
        for row in rows.as_array().unwrap() {
            let mut parsed_row = Vec::new();
            for col in row.as_array().unwrap() {
                parsed_row.push(col.as_str().unwrap().to_string())
            }
            parsed_rows.push(parsed_row);
        }
        let mut types = vec![];
        if !parsed_rows.is_empty() {
            types = vec![ColumnType::Any; parsed_rows[0].len()];
        }
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }
}

pub struct ClickhouseClient {}
