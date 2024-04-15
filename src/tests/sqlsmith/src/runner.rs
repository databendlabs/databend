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

use std::future::Future;
use std::time::Duration;

use databend_client::error::Error as ClientError;
use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_sql::resolve_type_name;
use databend_driver::Client;
use databend_driver::Connection;
use databend_driver::Error;
use databend_sql::value::Value;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use tokio_stream::StreamExt;

use crate::reducer::try_reduce_query;
use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

const KNOWN_ERRORS: &[&str] = &[
    // Errors caused by illegal parameters
    "Overflow on date YMD",
    "timestamp is out of range",
    "unable to cast type",
    "unable to cast to type",
    "invalid digit found in string",
    "number overflowed",
    "date is out of range",
    "Odd number of digits",
    "The maximum sleep time is 3 seconds",
    "Too many times to repeat",
    "Incorrect arguments to",
    "divided by zero",
    "Decimal overflow at line",
    "cannot parse to type",
    "invalid resolution",
    "invalid cell index",
    "invalid directed edge index",
    "invalid coordinate range",
    "window function calls cannot be nested",
    // Unsupported features
    "Row format is not yet support for",
    "to_decimal not support this DataType",
    "AggregateSumFunction does not support type",
    "AggregateArrayMovingAvgFunction does not support type",
    "AggregateArrayMovingSumFunction does not support type",
    "The arguments of AggregateRetention should be an expression which returns a Boolean result",
    "AggregateWindowFunnelFunction does not support type",
    "nth_value should count from 1",
    "step must not be zero",
    "start must be less than or equal to end when step is positive",
    "start must be greater than or equal to end when step is negative",
    "Expected Number, Date or Timestamp type, but got",
    "Unsupported data type for generate_series",
    "Having clause can't contain window functions",
];

pub struct Runner {
    count: usize,
    seed: Option<u64>,
    client: Client,
    timeout: u64,
}

impl Runner {
    pub fn new(dsn: String, count: usize, seed: Option<u64>, timeout: u64) -> Self {
        let client = Client::new(dsn);

        Self {
            count,
            seed,
            client,
            timeout,
        }
    }

    fn generate_rng(seed: Option<u64>) -> impl Rng {
        if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed)
        } else {
            SmallRng::from_entropy()
        }
    }

    #[allow(clippy::borrowed_box)]
    async fn create_base_table(
        &self,
        conn: &Box<dyn Connection>,
        table_stmts: Vec<(DropTableStmt, CreateTableStmt)>,
    ) -> Vec<Table> {
        let mut tables = Vec::with_capacity(table_stmts.len());
        for (drop_table_stmt, create_table_stmt) in table_stmts {
            let drop_table_sql = drop_table_stmt.to_string();
            tracing::info!("drop_table_sql: {}", drop_table_sql);
            Self::check_res(conn.exec(&drop_table_sql).await);
            let create_table_sql = create_table_stmt.to_string();
            tracing::info!("create_table_sql: {}", create_table_sql);
            Self::check_res(conn.exec(&create_table_sql).await);

            let table_name = create_table_stmt.table.name.clone();
            let mut fields = Vec::new();
            if let CreateTableSource::Columns(columns) = create_table_stmt.source.unwrap() {
                for column in columns {
                    let data_type = resolve_type_name(&column.data_type, true).unwrap();
                    let field = TableField::new(&column.name.name, data_type);
                    fields.push(field);
                }
            }
            let schema = TableSchemaRefExt::create(fields);

            let table = Table::new(table_name, schema);
            tables.push(table);
        }
        tables
    }

    #[allow(clippy::borrowed_box)]
    async fn get_settings(&self, conn: &Box<dyn Connection>) -> Vec<(String, DataType)> {
        let mut settings = vec![];
        let show_settings = "show settings".to_string();
        let rows = conn.query_iter(&show_settings).await;
        match rows {
            Ok(mut rows) => {
                while let Some(row) = rows.next().await {
                    if let Ok(row) = row {
                        match (row.values().first(), row.values().get(5)) {
                            (Some(Value::String(name)), Some(Value::String(ty))) => {
                                let data_type = match ty.as_str() {
                                    "UInt64" => DataType::Number(NumberDataType::UInt64),
                                    "String" => DataType::String,
                                    _ => DataType::String,
                                };
                                settings.push((name.clone(), data_type));
                            }
                            (_, _) => {
                                continue;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let err = format!("show settings exec err: {}", e);
                tracing::error!(err);
            }
        }
        settings
    }

    async fn check_timeout<F>(future: F, sql: String, sec: u64)
    where F: Future {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(sec), future).await {
            tracing::info!("sql: {}", sql);
            let err = format!("sql timeout: {}", e);
            tracing::error!(err);
        }
    }

    fn check_res(res: databend_driver::Result<i64>) {
        if let Err(Error::Api(ClientError::InvalidResponse(err))) = res {
            if err.code == 1005 || err.code == 1065 {
                return;
            }
            if KNOWN_ERRORS
                .iter()
                .any(|known_err| err.message.starts_with(known_err))
            {
                return;
            }
            let err = format!("sql exec err: {}", err.message);
            tracing::error!(err);
        }
    }

    pub async fn run(&self) {
        let conn = self.client.get_conn().await.unwrap();
        let settings = self.get_settings(&conn).await;

        let mut rng = Self::generate_rng(self.seed);
        let mut generator = SqlGenerator::new(&mut rng, settings);
        let table_stmts = generator.gen_base_tables();
        let tables = self.create_base_table(&conn, table_stmts).await;
        let row_count = 10;

        let mut new_tables = tables.clone();
        for (i, table) in tables.iter().enumerate() {
            let insert_stmt = generator.gen_insert(table, row_count);
            let insert_sql = insert_stmt.to_string();
            tracing::info!("insert_sql: {}", insert_sql);
            Self::check_res(conn.exec(&insert_sql).await);

            let alter_stmt_opt = generator.gen_alter(table, row_count);
            if let Some((alter_stmt, new_table, insert_stmt_opt)) = alter_stmt_opt {
                if let AlterTableAction::RenameTable { ref new_table } = alter_stmt.action {
                    let drop_table_stmt = DropTableStmt {
                        if_exists: true,
                        catalog: None,
                        database: None,
                        table: new_table.clone(),
                        all: false,
                    };
                    let drop_table_sql = drop_table_stmt.to_string();
                    tracing::info!("drop_table_sql: {}", drop_table_sql);
                    Self::check_res(conn.exec(&drop_table_sql).await);
                }
                let alter_sql = alter_stmt.to_string();
                tracing::info!("alter_sql: {}", alter_sql);
                Self::check_res(conn.exec(&alter_sql).await);
                // save new table schema
                new_tables[i] = new_table;
                if let Some(insert_stmt) = insert_stmt_opt {
                    let insert_sql = insert_stmt.to_string();
                    tracing::info!("after alter insert_sql: {}", insert_sql);
                    Self::check_res(conn.exec(&insert_sql).await);
                }
            }
        }
        generator.tables = new_tables;

        let enable_merge = "set enable_experimental_merge_into = 1".to_string();
        Self::check_res(conn.exec(&enable_merge).await);
        // generate merge, replace, update, delete
        for _ in 0..20 {
            let sql = match generator.rng.gen_range(0..=20) {
                0..=10 => generator.gen_merge().to_string(),
                11..=15 => generator.gen_replace().to_string(),
                16..=19 => generator.gen_update().to_string(),
                20 => generator.gen_delete().to_string(),
                _ => unreachable!(),
            };
            tracing::info!("sql: {}", sql);
            Self::check_timeout(
                async { Self::check_res(conn.exec(&sql.clone()).await) },
                sql.clone(),
                self.timeout,
            )
            .await;
        }

        // generate query
        for _ in 0..self.count {
            let query = generator.gen_query();
            let query_sql = query.to_string();
            let mut try_reduce = false;
            let mut err_code = 0;
            let mut err = String::new();
            Self::check_timeout(
                async {
                    if let Err(e) = conn.exec(&query_sql).await {
                        if let Error::Api(ClientError::InvalidResponse(err)) = &e {
                            // TODO: handle Syntax, Semantic and InvalidArgument errors
                            if err.code == 1005
                                || err.code == 1065
                                || err.code == 2004
                                || err.code == 1010
                            {
                                return;
                            }
                            if KNOWN_ERRORS
                                .iter()
                                .any(|known_err| err.message.starts_with(known_err))
                            {
                                return;
                            }
                            err_code = err.code;
                        }
                        err = format!("error: {}", e);
                        try_reduce = true;
                    }
                },
                query_sql.clone(),
                self.timeout,
            )
            .await;
            if try_reduce {
                tracing::info!("query_sql: {}", query_sql);
                let reduced_query = try_reduce_query(conn.clone(), err_code, query).await;
                tracing::info!("reduced query_sql: {}", reduced_query.to_string());
                tracing::error!(err);
            }
        }
    }
}
