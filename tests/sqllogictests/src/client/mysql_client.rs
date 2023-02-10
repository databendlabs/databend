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

use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use mysql_async::Pool;
use mysql_async::Row;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::error::Result;

#[derive(Debug)]
pub struct MySQLClient {
    pub conn: Conn,
    pub debug: bool,
    pub tpch: bool,
}

impl MySQLClient {
    pub async fn create() -> Result<Self> {
        let url = "mysql://root:@127.0.0.1:3307/default";
        let pool = Pool::new(url);
        let conn = pool.get_conn().await?;
        Ok(Self {
            conn,
            debug: false,
            tpch: false,
        })
    }

    pub fn enable_tpch(&mut self) {
        self.tpch = true;
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let start = Instant::now();
        let rows: Vec<Row> = self.conn.query(sql).await?;
        let elapsed = start.elapsed();
        if self.tpch
            && !(sql.trim_start().starts_with("set") || sql.trim_start().starts_with("analyze"))
        {
            println!("{elapsed:?}");
        }
        if self.debug {
            println!("Running sql with mysql client: [{sql}] ({elapsed:?})");
        };
        let types = vec![ColumnType::Any; rows.len()];
        let mut parsed_rows = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
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
        // Todo: add types to compare
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }
}
