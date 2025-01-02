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

use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_sql::resolve_type_name;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

use crate::http_client::HttpClient;
use crate::http_client::QueryResponse;
use crate::query_fuzzer::QueryFuzzer;
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
    "Cannot find common type for",
    "null value in column",
];

pub struct Runner {
    count: usize,
    seed: Option<u64>,
    pub(crate) client: HttpClient,
    db: String,
    timeout: u64,
}

impl Runner {
    pub async fn try_new(
        host: String,
        username: String,
        password: String,
        db: String,
        count: usize,
        seed: Option<u64>,
        timeout: u64,
    ) -> Result<Self> {
        let client = HttpClient::create(host, username, password).await?;

        Ok(Self {
            count,
            seed,
            client,
            db,
            timeout,
        })
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
            tracing::info!("drop_table_sql: {}", drop_table_sql);
            Self::check_res(self.client.query(&drop_table_sql).await);
            let create_table_sql = create_table_stmt.to_string();
            tracing::info!("create_table_sql: {}", create_table_sql);
            Self::check_res(self.client.query(&create_table_sql).await);

            let table_name = create_table_stmt.table.name.clone();
            let mut fields = Vec::new();
            if let CreateTableSource::Columns(columns, _) = create_table_stmt.source.unwrap() {
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
        Ok(tables)
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

    fn check_res(responses: Result<Vec<QueryResponse>>) {
        match responses {
            Ok(responses) => {
                if let Some(error) = &responses[0].error {
                    let value = error.as_object().unwrap();
                    let code = value["code"].as_u64().unwrap();
                    let message = value["message"].as_str().unwrap();
                    if code == 1005 || code == 1065 {
                        return;
                    }
                    if KNOWN_ERRORS
                        .iter()
                        .any(|known_err| message.starts_with(known_err))
                    {
                        return;
                    }
                    let err = format!("sql exec err code: {} message: {}", code, message);
                    tracing::error!(err);
                }
            }
            Err(err) => {
                let err = format!("http err: {}", err);
                tracing::error!(err);
            }
        }
    }

    pub async fn run_fuzz(&mut self) -> Result<()> {
        let sqls = vec!["select 1, 'a'", "select 2, 'b'"];
        let mut query_fuzzer = QueryFuzzer::new();
        for sql in sqls {
            let tokens = tokenize_sql(sql).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            let fuzz_stmt = query_fuzzer.fuzz(stmt);
            let fuzz_sql = format!("{}", fuzz_stmt);
            // todo
            println!("fuzz_sql={:?}", fuzz_sql)
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        self.run_fuzz().await?;

        let create_db_sql = format!("CREATE OR REPLACE database {}", self.db);
        let _ = self.client.query(&create_db_sql).await?;
        let use_db_sql = format!("USE {}", self.db);
        let _ = self.client.query(&use_db_sql).await?;

        let settings = self.get_settings().await?;

        let mut rng = Self::generate_rng(self.seed);
        let mut generator = SqlGenerator::new(&mut rng, settings);
        let table_stmts = generator.gen_base_tables();
        let tables = self.create_base_table(table_stmts).await?;
        let row_count = 10;

        let mut new_tables = tables.clone();
        for (i, table) in tables.iter().enumerate() {
            let insert_stmt = generator.gen_insert(table, row_count);
            let insert_sql = insert_stmt.to_string();
            tracing::info!("insert_sql: {}", insert_sql);
            Self::check_res(self.client.query(&insert_sql).await);

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
                    Self::check_res(self.client.query(&drop_table_sql).await);
                }
                let alter_sql = alter_stmt.to_string();
                tracing::info!("alter_sql: {}", alter_sql);
                Self::check_res(self.client.query(&alter_sql).await);
                // save new table schema
                new_tables[i] = new_table;
                if let Some(insert_stmt) = insert_stmt_opt {
                    let insert_sql = insert_stmt.to_string();
                    tracing::info!("after alter insert_sql: {}", insert_sql);
                    Self::check_res(self.client.query(&insert_sql).await);
                }
            }
        }
        generator.tables = new_tables;

        let enable_merge = "set enable_experimental_merge_into = 1".to_string();
        Self::check_res(self.client.query(&enable_merge).await);
        // generate merge, replace, update, delete
        for _ in 0..20 {
            let sql = match generator.rng.gen_range(0..=20) {
                0..=10 => generator.gen_merge().to_string(),
                11..=15 => generator.gen_replace().to_string(),
                16..=19 => generator.gen_update().to_string(),
                20 => generator.gen_delete().to_string(),
                _ => unreachable!(),
            };
            let mut timeout_err = None;
            tracing::info!("dml sql: {}", sql);
            Self::check_timeout(
                async { Self::check_res(self.client.query(&sql).await) },
                self.timeout,
                &mut timeout_err,
            )
            .await;
            if let Some(timeout_err) = timeout_err {
                tracing::error!("sql timeout: {}", timeout_err);
            }
        }

        // generate query
        for _ in 0..self.count {
            let query = generator.gen_query();
            let query_sql = query.to_string();
            let mut timeout_err = None;
            let mut is_error = false;
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
                                if code == 1005 || code == 1065 || code == 2004 || code == 1010 {
                                    return;
                                }
                                if KNOWN_ERRORS
                                    .iter()
                                    .any(|known_err| message.starts_with(known_err))
                                {
                                    return;
                                }
                                is_error = true;
                                err_code = code;
                                err_message = format!("error: {}", message);
                                try_reduce = true;
                            }
                        }
                        Err(err) => {
                            is_error = true;
                            err_message = format!("http err: {}", err);
                        }
                    }
                },
                self.timeout,
                &mut timeout_err,
            )
            .await;

            if let Some(timeout_err) = timeout_err {
                tracing::info!("query_sql: {}", query_sql);
                tracing::error!("sql timeout: {}", timeout_err);
            } else if is_error {
                tracing::info!("query_sql: {}", query_sql);
                if try_reduce {
                    let reduced_query = self.try_reduce_query(err_code, query).await;
                    tracing::info!("reduced query_sql: {}", reduced_query.to_string());
                }
                tracing::error!(err_message);
            }
        }
        Ok(())
    }
}
