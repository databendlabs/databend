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

use mysql::prelude::Queryable;
use mysql::Pool;
use mysql::PooledConn;
use mysql::Row;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::error::Result;

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
