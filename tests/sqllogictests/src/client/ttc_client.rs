// Copyright 2021 Datafuse Labs
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

use std::time::Instant;

use regex::Regex;
use sqllogictest::DBOutput;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::error::Result;
use crate::util::ColumnType;

#[derive(Debug)]
pub struct TTCClient {
    pub debug: bool,
    pub bench: bool,
    pub image: String,
    stream: TcpStream,
}

impl TTCClient {
    pub async fn create(image: &str, conn: &str) -> Result<Self> {
        let stream = TcpStream::connect(conn).await?;

        Ok(Self {
            stream,
            debug: false,
            bench: false,
            image: image.to_string(),
        })
    }

    pub fn enable_bench(&mut self) {
        self.bench = true;
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<ColumnType>> {
        let start = Instant::now();
        let res = self.query_response(sql).await;

        let elapsed = start.elapsed();

        if self.bench
            && !(sql.trim_start().starts_with("set") || sql.trim_start().starts_with("analyze"))
        {
            println!("{elapsed:?}");
        }

        let rows: Vec<Vec<Option<String>>> = match res {
            Ok(response) => {
                if let Some(error) = response.error {
                    // APIError: QueryFailed: [1006]Fields in select statement is not equal
                    // Code: 1006, Text = Fields in select statement is not equal
                    let re = Regex::new(r"(?s).*(QueryFailed|AuthFailure): \[(\d+)\](.*)").unwrap();
                    if let Some(captures) = re.captures(&error) {
                        let error_code = captures[2].parse::<i32>().unwrap();
                        let error_text = captures[3].trim();
                        return Err(format!("Code: {}, Text = {}", error_code, error_text).into());
                    } else {
                        return Err(error.into());
                    }
                }

                if self.debug {
                    println!("Running sql with mysql client: [{sql}] ({elapsed:?})");
                };
                response.values
            }
            Err(err) => {
                if self.debug {
                    println!(
                        "Running sql with mysql client: [{sql}] ({elapsed:?}); error: ({err:?})"
                    );
                };
                return Err(err);
            }
        };

        let mut parsed_rows = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            let mut parsed_row = Vec::new();
            for i in 0..row.len() {
                let value = row.get(i);
                if let Some(v) = value {
                    match v {
                        None => parsed_row.push("NULL".to_string()),
                        Some(s) if s.is_empty() => parsed_row.push("(empty)".to_string()),
                        Some(s) => parsed_row.push(s.clone()),
                    }
                }
            }
            parsed_rows.push(parsed_row);
        }
        let mut types = vec![];
        if !parsed_rows.is_empty() {
            types = vec![ColumnType::Any; parsed_rows[0].len()];
        }
        // Todo: add types to compare
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    async fn query_response(&mut self, sql: &str) -> Result<Response> {
        let len = sql.len() as u32;
        let len_bytes = len.to_be_bytes();

        // Create a buffer with the length of the sql and the sql itself
        let mut buffer = Vec::with_capacity(4 + sql.len());
        buffer.extend_from_slice(&len_bytes);
        buffer.extend_from_slice(sql.as_bytes());

        // Send the sql
        self.stream.write_all(&buffer).await?;

        let mut len_bytes = [0; 4];
        self.stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Read the response
        let mut response = vec![0; len];
        self.stream.read_exact(&mut response).await?;

        Ok(serde_json::from_reader(response.as_slice()).unwrap())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Response {
    values: Vec<Vec<Option<String>>>,
    error: Option<String>,
}
