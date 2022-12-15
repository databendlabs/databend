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

use mysql::prelude::Queryable;
use mysql::Pool;
use mysql::PooledConn;
use mysql::Row;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde_json::Value;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::error::Result;
use crate::util::HttpSessionConf;

#[derive(Debug)]
pub struct MysqlClient {
    pub conn: PooledConn,
}

impl MysqlClient {
    pub fn create() -> Result<MysqlClient> {
        let url = "mysql://root:@127.0.0.1:3307/default";
        let pool = Pool::new(url)?;
        let conn = pool.get_conn()?;
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
                    _ => {
                        todo!()
                    }
                }
            }
            parsed_rows.push(parsed_row);
        }
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        "mysql_client"
    }
}

pub struct HttpClient {
    pub client: Client,
    pub url: String,
    pub session: Option<HttpSessionConf>,
}

impl HttpClient {
    pub fn create() -> Result<HttpClient> {
        let client = Client::new();
        let url = "http://127.0.0.1:8000/v1/query".to_string();
        Ok(HttpClient { client, url, session: None})
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let mut header = HeaderMap::new();
        header.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        let mut query = HashMap::new();
        query.insert("sql", serde_json::to_value(sql).unwrap());
        if let Some(session) = &self.session {
            query.insert("session", serde_json::to_value(session).unwrap());
        }
        let mut response = self
            .client
            .post(&self.url)
            .json(&query)
            .basic_auth("root", Some(""))
            .headers(header.clone())
            .send()
            .await?;
        let text = response.text().await?;
        let mut data: Value = serde_json::from_str(text.as_str()).unwrap();
        self.session = serde_json::from_value(data.as_object().unwrap().get("session").unwrap().clone()).unwrap();
        let rows = data.as_object().unwrap().get("data").unwrap();
        let mut parsed_rows = Vec::new();
        for row in rows.as_array().unwrap() {
            let mut parsed_row = Vec::new();
            for col in row.as_array().unwrap() {
                let mut cell = col.as_str().unwrap().to_string();
                if &cell == "inf" {
                    cell = "Infinity".to_string();
                }
                if &cell == "nan" {
                    cell = "NaN".to_string();
                }
                if cell.is_empty() {
                    parsed_row.push("(empty)".to_string());
                } else {
                    parsed_row.push(cell);
                }
            }
            parsed_rows.push(parsed_row);
        }
        while let Some(next_uri) = data.as_object().unwrap().get("next_uri").unwrap().as_str() {
            let mut url = "http://127.0.0.1:8000".to_string();
            url.push_str(next_uri);
            response = self
                .client
                .post(url)
                .json(&query)
                .basic_auth("root", Some(""))
                .headers(header.clone())
                .send().await?;
            let text = response.text().await?;
            data = serde_json::from_str(text.as_str()).unwrap();
            let rows = data.as_object().unwrap().get("data").unwrap();
            for row in rows.as_array().unwrap() {
                let mut parsed_row = Vec::new();
                for col in row.as_array().unwrap() {
                    let mut cell = col.as_str().unwrap().to_string();
                    if &cell == "inf" {
                        cell = "Infinity".to_string();
                    }
                    if &cell == "nan" {
                        cell = "NaN".to_string();
                    }
                    if cell.is_empty() {
                        parsed_row.push("(empty)".to_string());
                    } else {
                        parsed_row.push(cell);
                    }
                }
                parsed_rows.push(parsed_row);
            }
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

pub struct ClickhouseHttpClient {
    pub client: Client,
    pub url: String,
}

impl ClickhouseHttpClient {
    pub fn create() -> Result<ClickhouseHttpClient> {
        let client = Client::new();
        let url = "http://127.0.0.1:8124".to_string();
        Ok(ClickhouseHttpClient { client, url })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let mut query = HashMap::new();
        query.insert("query", sql);
        let response = self
            .client
            .post(&self.url)
            .query(&query)
            .basic_auth("root", Some(""))
            .send()
            .await?;
        let res = response.text().await?;
        let rows: Vec<Vec<String>> = res
            .lines()
            .map(|s| {
                s.split('\t')
                    .map(|s| {
                        if s == "\\N" {
                            "NULL".to_string()
                        } else {
                            if s.is_empty() {
                                return "(empty)".to_string();
                            }
                            // Maybe `s` contains "\\N", such as `[\N,'cc']`
                            // So we need to find it and replace with NULL (a little hack)
                            let s = str::replace(s, "\\N", "NULL");
                            str::replace(&s, r"\", "")
                        }
                    })
                    .collect()
            })
            .collect();
        let mut types = vec![];
        if !rows.is_empty() {
            types = vec![ColumnType::Any; rows[0].len()];
        }
        Ok(DBOutput::Rows { types, rows })
    }
}
