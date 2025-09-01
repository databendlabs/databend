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

use std::fs::create_dir_all;
use std::fs::File;
use std::future::Future;
use std::io::BufWriter;
use std::io::Write;
use std::time::Duration;

use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::CreateViewStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_ast::ast::DropViewStmt;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_sql::resolve_type_name;
use jiff::fmt::strtime;
use jiff::Zoned;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

use crate::http_client::HttpClient;
use crate::query_fuzzer::QueryFuzzer;
use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;
use crate::util::read_sql_from_test_dirs;

const LOG_FILE_FORMAT: &str = "%Y-%m-%d-%H-%M-%S-%f";

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
    "attempt to shift left with overflow",
    "attempt to shift right with overflow",
    "attempt to subtract with overflow",
    "attempt to add with overflow",
    "attempt to multiply with overflow",
    "map keys have to be unique",
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
    "Cannot find common type for",
    "null value in column",
];

pub struct Runner {
    count: usize,
    seed: Option<u64>,
    pub(crate) client: HttpClient,
    db: String,
    timeout: u64,
    file_buf: BufWriter<File>,
    err_file_buf: BufWriter<File>,
}

impl Runner {
    pub async fn try_new(
        host: String,
        username: String,
        password: String,
        db: String,
        log_dir: String,
        count: usize,
        seed: Option<u64>,
        timeout: u64,
    ) -> Result<Self> {
        let client = HttpClient::create(host, username, password).await?;

        // Create SQL log directory and file.
        create_dir_all(&log_dir)?;
        let now = Zoned::now();
        let time = strtime::format(LOG_FILE_FORMAT, &now).unwrap();
        let log_path = format!("{}/databend-sqlsmith.{}.sql", log_dir, time);
        let file = File::create(log_path)?;
        let file_buf = BufWriter::new(file);

        let err_log_path = format!("{}/databend-sqlsmith.{}.error.sql", log_dir, time);
        let err_file = File::create(err_log_path)?;
        let err_file_buf = BufWriter::new(err_file);

        Ok(Self {
            count,
            seed,
            client,
            db,
            timeout,
            file_buf,
            err_file_buf,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let create_db_sql = format!("CREATE OR REPLACE database {}", self.db);
        let _ = self.run_sql(create_db_sql, None).await;
        let use_db_sql = format!("USE {}", self.db);
        let _ = self.run_sql(use_db_sql, None).await;

        let settings = self.get_settings().await?;

        let mut rng = Self::generate_rng(self.seed);
        let mut generator = SqlGenerator::new(&mut rng, settings);
        let table_stmts = generator.gen_base_tables(&self.db);
        let tables = self.create_base_table(table_stmts).await?;
        let row_count = 10;

        let mut new_tables = tables.clone();
        for (i, table) in tables.iter().enumerate() {
            let insert_stmts = generator.gen_insert(table, row_count);
            for insert_stmt in insert_stmts.into_iter() {
                let insert_sql = insert_stmt.to_string();
                let _ = self.run_sql(insert_sql, None).await;
            }
            let alter_stmt_opt = generator.gen_alter(table, row_count);
            if let Some((alter_stmt, new_table, insert_stmt_opt)) = alter_stmt_opt {
                let alter_sql = alter_stmt.to_string();
                let _ = self.run_sql(alter_sql, None).await;
                // save new table schema
                new_tables[i] = new_table;
                if let Some(insert_stmt) = insert_stmt_opt {
                    let insert_sql = insert_stmt.to_string();
                    let _ = self.run_sql(insert_sql, None).await;
                }
            }
        }
        generator.tables = new_tables.clone();

        let dml_stmts = generator.gen_dml_stmt();
        for dml_stmt in dml_stmts.into_iter() {
            let dml_sql = dml_stmt.to_string();
            let _ = self.run_sql(dml_sql, None).await;
        }

        let view_stmts = generator.gen_view(&self.db).await?;
        let mut views = self.create_view(view_stmts).await?;
        generator.tables.append(&mut views);

        // generate query
        let mut conn_error_cnt = 0;
        for _ in 0..self.count {
            let query = generator.gen_query();
            let query_sql = query.to_string();
            let (_, is_conn_error) = self.run_sql(query_sql, Some(query)).await;
            if is_conn_error {
                conn_error_cnt += 1;
            }
            // Query server panic, exist
            if conn_error_cnt >= 3 {
                break;
            }
        }
        Ok(())
    }

    pub async fn run_fuzz(&mut self, fuzz_path: &str) -> Result<()> {
        let sqls = read_sql_from_test_dirs(fuzz_path)?;
        let mut conn_error_cnt = 0;
        let mut query_fuzzer = QueryFuzzer::new();
        for sql in sqls {
            tracing::info!("orig sql: {}", sql);
            let Ok(tokens) = tokenize_sql(&sql) else {
                tracing::error!("invalid sql: {}", sql);
                continue;
            };
            let Ok((stmt, _)) = parse_sql(&tokens, Dialect::PostgreSQL) else {
                tracing::error!("invalid sql: {}", sql);
                continue;
            };
            let fuzz_stmt = query_fuzzer.fuzz(stmt);
            let fuzz_sql = fuzz_stmt.to_string();
            tracing::info!("fuzz sql: {}", fuzz_sql);
            if let Statement::Query(_) = fuzz_stmt {
                let (_, is_conn_error) = self.run_sql(fuzz_sql, None).await;
                if is_conn_error {
                    conn_error_cnt += 1;
                }
                // Query server panic, exist
                if conn_error_cnt >= 3 {
                    break;
                }
            } else if let Err(err) = self.client.query(&fuzz_sql).await {
                tracing::error!("fuzz sql err: {}", err);
            }
        }

        Ok(())
    }

    async fn run_sql(&mut self, query_sql: String, query: Option<Query>) -> (bool, bool) {
        // Write SQL to log file for replay.
        writeln!(self.file_buf, "{};", query_sql).unwrap();

        let mut timeout_err = None;
        let mut is_error = false;
        let mut is_conn_error = false;
        let mut try_reduce = false;
        let mut err_code = 0;
        let mut err_message = String::new();
        Self::check_timeout(
            async {
                match self.client.query(&query_sql).await {
                    Ok(responses) => {
                        if let Some(error) = &responses[0].error {
                            let value = error.as_object().unwrap();
                            let code = value["code"].as_u64().unwrap();
                            let message = value["message"].as_str().unwrap();

                            writeln!(self.err_file_buf, "{};", query_sql).unwrap();
                            writeln!(self.err_file_buf, "{}-{};", code, message).unwrap();

                            is_error = true;
                            if code == 1005 || code == 1065 || code == 2004 || code == 1010 {
                                return;
                            }
                            if KNOWN_ERRORS
                                .iter()
                                .any(|known_err| message.starts_with(known_err))
                            {
                                return;
                            }
                            err_code = code;
                            err_message = format!("error: {}", message);
                            try_reduce = true;
                        }
                    }
                    Err(err) => {
                        // 1910 is ReqwestError, Query server may have panic, need exist
                        if err.code() == 1910 {
                            is_conn_error = true;
                        }
                        is_error = true;
                        err_message = format!("http err: {}", err);
                    }
                }
            },
            self.timeout,
            &mut timeout_err,
        )
        .await;

        if let Some(ref timeout_err) = timeout_err {
            tracing::info!("sql: {}", query_sql);
            tracing::error!("sql timeout: {}", timeout_err);
        } else if is_error && err_code > 0 {
            tracing::info!("sql: {}", query_sql);
            if try_reduce {
                if let Some(query) = query {
                    let reduced_query = self.try_reduce_query(err_code, query).await;
                    tracing::info!("reduced sql: {}", reduced_query.to_string());
                }
            }
            tracing::error!(err_message);
        }
        (is_error | timeout_err.is_some(), is_conn_error)
    }

    fn generate_rng(seed: Option<u64>) -> impl Rng {
        if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed)
        } else {
            SmallRng::from_entropy()
        }
    }

    async fn create_base_table(
        &mut self,
        table_stmts: Vec<(DropTableStmt, CreateTableStmt)>,
    ) -> Result<Vec<Table>> {
        let mut tables = Vec::with_capacity(table_stmts.len());
        for (drop_table_stmt, create_table_stmt) in table_stmts {
            let drop_table_sql = drop_table_stmt.to_string();
            let _ = self.run_sql(drop_table_sql, None).await;
            let create_table_sql = create_table_stmt.to_string();
            let _ = self.run_sql(create_table_sql, None).await;

            let db_name = create_table_stmt.database.clone();
            let table_name = create_table_stmt.table.clone();
            let mut fields = Vec::new();
            if let CreateTableSource::Columns { columns, .. } = create_table_stmt.source.unwrap() {
                for column in columns {
                    let data_type = resolve_type_name(&column.data_type, true).unwrap();
                    let field = TableField::new(&column.name.name, data_type);
                    fields.push(field);
                }
            }
            let schema = TableSchemaRefExt::create(fields);

            let table = Table::new(db_name, table_name, schema);
            tables.push(table);
        }
        Ok(tables)
    }

    async fn create_view(
        &mut self,
        view_stmts: Vec<(DropViewStmt, CreateViewStmt, Vec<DataType>)>,
    ) -> Result<Vec<Table>> {
        let mut views = Vec::with_capacity(view_stmts.len());
        for (drop_view_stmt, create_view_stmt, col_types) in view_stmts {
            let drop_view_sql = drop_view_stmt.to_string();
            let _ = self.run_sql(drop_view_sql, None).await;
            let create_view_sql = create_view_stmt.to_string();
            let (is_err, _) = self.run_sql(create_view_sql, None).await;

            if !is_err {
                let db_name = create_view_stmt.database.clone();
                let view_name = create_view_stmt.view.clone();
                let mut fields = Vec::with_capacity(create_view_stmt.columns.len());
                for (c_name, c_type) in create_view_stmt.columns.iter().zip(col_types) {
                    let table_dt = infer_schema_type(&c_type)?;
                    let field = TableField::new(&c_name.to_string(), table_dt);
                    fields.push(field);
                }
                let schema = TableSchemaRefExt::create(fields);
                let table = Table::new(db_name, view_name, schema);
                views.push(table);
            }
        }
        Ok(views)
    }

    async fn get_settings(&mut self) -> Result<Vec<(String, DataType)>> {
        let show_settings = "show settings".to_string();
        let responses = self.client.query(&show_settings).await?;

        let mut settings = vec![];
        for response in responses {
            if let Some(serde_json::Value::Array(arr)) = response.data {
                for row in arr {
                    let name = row.get(0);
                    let ty = row.get(6);
                    if let (
                        Some(serde_json::Value::String(name)),
                        Some(serde_json::Value::String(ty)),
                    ) = (name, ty)
                    {
                        let data_type = match ty.as_str() {
                            "UInt64" => DataType::Number(NumberDataType::UInt64),
                            "String" => DataType::String,
                            _ => DataType::String,
                        };
                        settings.push((name.clone(), data_type));
                    }
                }
            }
        }
        Ok(settings)
    }

    async fn check_timeout<F>(future: F, sec: u64, timeout_err: &mut Option<String>)
    where F: Future {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(sec), future).await {
            *timeout_err = Some(format!("{}", e));
        }
    }
}
