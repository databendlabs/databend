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

use mysql_async::Conn;
use mysql_async::Pool;
use mysql_async::Row;
use mysql_async::prelude::Queryable;
use sqllogictest::DBOutput;

use crate::error::Result;
use crate::util::ColumnType;

#[derive(Debug)]
pub struct MySQLClient {
    pub conn: Conn,
    pub debug: bool,
    pub bench: bool,
}

impl MySQLClient {
    pub async fn create(database: &str) -> Result<Self> {
        let url = format!("mysql://root:@127.0.0.1:3307/{database}");
        let pool = Pool::new(url.as_str());
        let conn = pool.get_conn().await?;
        Ok(Self {
            conn,
            debug: false,
            bench: false,
        })
    }

    pub fn enable_bench(&mut self) {
        self.bench = true;
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<ColumnType>> {
        let start = Instant::now();
        let res = self.conn.query(sql).await;

        let elapsed = start.elapsed();

        if self.bench
            && !(sql.trim_start().starts_with("set") || sql.trim_start().starts_with("analyze"))
        {
            println!("{elapsed:?}");
        }

        let rows: Vec<Row> = match res {
            Ok(rows) => {
                if self.debug {
                    println!("Running sql with mysql client: [{sql}] ({elapsed:?})");
                };
                rows
            }
            Err(err) => {
                if self.debug {
                    println!(
                        "Running sql with mysql client: [{sql}] ({elapsed:?}); error: ({err:?})"
                    );
                };
                return Err(err.into());
            }
        };

        let types = rows.first().map(|row| {
            row.columns()
                .iter()
                .map(|c| {
                    use mysql_async::consts::ColumnType::*;
                    match c.column_type() {
                        MYSQL_TYPE_TINY => ColumnType::Any,
                        MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG
                        | MYSQL_TYPE_INT24 => ColumnType::Integer,
                        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_DECIMAL => {
                            ColumnType::FloatingPoint
                        }
                        MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING | MYSQL_TYPE_VARCHAR => {
                            ColumnType::Text
                        }
                        _ => ColumnType::Any,
                    }
                })
                .collect::<Vec<_>>()
        });

        let mut parsed_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut parsed_row = Vec::new();
            for i in 0..row.len() {
                let value: Option<Option<String>> = row.get(i);
                if let Some(v) = value {
                    match v {
                        None => parsed_row.push("NULL".to_string()),
                        Some(s) if s.is_empty() => parsed_row.push("(empty)".to_string()),
                        Some(s) => parsed_row.push(s),
                    }
                }
            }
            parsed_rows.push(parsed_row);
        }

        Ok(DBOutput::Rows {
            types: types.unwrap_or_default(),
            rows: parsed_rows,
        })
    }
}
