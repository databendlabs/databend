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

use common_ast::ast::CreateTableSource;
use common_ast::ast::CreateTableStmt;
use common_ast::ast::DropTableStmt;
use common_exception::Result;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::resolve_type_name;
use databend_client::error::Error as ClientError;
use databend_driver::Client;
use databend_driver::Error;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

const KNOWN_ERRORS: [&str; 19] = [
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
    // Unsupported features
    "Row format is not yet support for",
];

pub struct Runner {
    count: usize,
    seed: Option<u64>,
    client: Client,
}

impl Runner {
    pub fn new(dsn: String, count: usize, seed: Option<u64>) -> Self {
        let client = Client::new(dsn);

        Self {
            count,
            seed,
            client,
        }
    }

    fn generate_rng(seed: Option<u64>) -> impl Rng {
        if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed)
        } else {
            SmallRng::from_entropy()
        }
    }

    async fn create_base_table(
        &self,
        table_stmts: Vec<(DropTableStmt, CreateTableStmt)>,
    ) -> Result<Vec<Table>> {
        let conn = self.client.get_conn().await.unwrap();
        let mut tables = Vec::with_capacity(table_stmts.len());
        for (drop_table_stmt, create_table_stmt) in table_stmts {
            let drop_table_sql = drop_table_stmt.to_string();
            tracing::info!("drop_table_sql: {}", drop_table_sql);
            conn.exec(&drop_table_sql).await.unwrap();
            let create_table_sql = create_table_stmt.to_string();
            tracing::info!("create_table_sql: {}", create_table_sql);
            conn.exec(&create_table_sql).await.unwrap();

            let table_name = create_table_stmt.table.name.clone();
            let mut fields = Vec::new();
            if let CreateTableSource::Columns(columns) = create_table_stmt.source.unwrap() {
                for column in columns {
                    let data_type = resolve_type_name(&column.data_type, true)?;
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

    pub async fn run(&self) {
        let mut rng = Self::generate_rng(self.seed);
        let mut generater = SqlGenerator::new(&mut rng);
        let table_stmts = generater.gen_base_tables();
        let tables = self.create_base_table(table_stmts).await.unwrap();
        let conn = self.client.get_conn().await.unwrap();

        for table in &tables {
            let insert_stmt = generater.gen_insert(table, 50);
            let insert_sql = insert_stmt.to_string();
            tracing::info!("insert_sql: {}", insert_sql);
            conn.exec(&insert_sql).await.unwrap();
        }
        generater.tables = tables;

        for _ in 0..self.count {
            let query = generater.gen_query();
            let query_sql = query.to_string();
            tracing::info!("query_sql: {}", query_sql);
            if let Err(e) = conn.exec(&query_sql).await {
                if let Error::Api(ClientError::InvalidResponse(err)) = &e {
                    if KNOWN_ERRORS
                        .iter()
                        .any(|known_err| err.message.starts_with(known_err))
                    {
                        continue;
                    }
                }

                let err = format!("error: {}", e);
                tracing::error!(err);
            }
        }
    }
}
