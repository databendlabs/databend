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
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::token::Token;
use common_ast::parser::token::Tokenizer;
use common_ast::Dialect;
use common_exception::Result;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::resolve_type_name;
use databend_driver::Client;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

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

    fn gen_drop_base_table_sql() -> String {
        "DROP TABLE IF EXISTS t1".to_string()
    }

    fn gen_create_base_table_sql() -> String {
        "
        CREATE TABLE t1 (
            c1 boolean not null,
            c2 boolean null,
            c3 uint8 not null,
            c4 uint8 null,
            c5 uint16 not null,
            c6 uint16 null,
            c7 uint32 not null,
            c8 uint32 null,
            c9 uint64 not null,
            c10 uint64 null,
            c11 int8 not null,
            c12 int8 null,
            c13 int16 not null,
            c14 int16 null,
            c15 int32 not null,
            c16 int32 null,
            c17 int64 not null,
            c18 int64 null,
            c19 float32 not null,
            c20 float32 null,
            c21 float64 not null,
            c22 float64 null,
            c23 decimal(7, 2) not null,
            c24 decimal(7, 2) null,
            c25 decimal(15, 2) not null,
            c26 decimal(15, 2) null,
            c27 string not null,
            c28 string null,
            c29 date not null,
            c30 date null,
            c31 timestamp not null,
            c32 timestamp null
        );
        "
        .to_string()
    }

    fn parse_sql(sql: &str) -> Result<Statement> {
        let sql_dialect = Dialect::PostgreSQL;
        let mut tokenizer = Tokenizer::new(sql).peekable();
        let tokens: Vec<Token> = (&mut tokenizer).collect::<Result<_>>()?;
        let (stmt, _) = parse_sql(&tokens, sql_dialect)?;
        Ok(stmt)
    }

    async fn create_base_table(&self) -> Result<Table> {
        let conn = self.client.get_conn().await.unwrap();
        let drop_sql = Self::gen_drop_base_table_sql();
        conn.exec(&drop_sql).await.unwrap();

        let create_sql = Self::gen_create_base_table_sql();
        let stmt = Self::parse_sql(&create_sql)?;
        conn.exec(&stmt.to_string()).await.unwrap();

        if let Statement::CreateTable(create_table_stmt) = stmt {
            let table_name = create_table_stmt.table.name.clone();
            let mut fields = Vec::new();
            if let CreateTableSource::Columns(columns) = create_table_stmt.source.unwrap() {
                for column in columns {
                    let data_type = resolve_type_name(&column.data_type, false)?;
                    let field = TableField::new(&column.name.name, data_type);
                    fields.push(field);
                }
            }
            let schema = TableSchemaRefExt::create(fields);

            let table = Table::new(table_name, schema);
            Ok(table)
        } else {
            unreachable!()
        }
    }

    pub async fn run(&self) {
        let mut rng = Self::generate_rng(self.seed);

        let table = self.create_base_table().await.unwrap();
        let conn = self.client.get_conn().await.unwrap();
        let mut generater = SqlGenerator::new(&mut rng, vec![table.clone()]);

        let insert_stmt = generater.generate_insert(&table, 50);
        let insert_sql = insert_stmt.to_string();
        tracing::info!("insert_sql: {}", insert_sql);
        conn.exec(&insert_sql).await.unwrap();
        for _ in 0..self.count {
            let query = generater.gen_query();
            let query_sql = query.to_string();
            tracing::info!("query_sql: {}", query_sql);
            // TODO check query result
            if let Err(e) = conn.exec(&query_sql).await {
                let err = format!("error: {}", e);
                tracing::error!(err);
            }
        }
    }
}
