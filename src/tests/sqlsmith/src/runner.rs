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
            c1 boolean,
            c2 uint8,
            c3 uint16,
            c4 uint32,
            c5 uint64,
            c6 int8,
            c7 int16,
            c8 int32,
            c9 int64,
            c10 float32,
            c11 float64,
            c12 decimal(7, 2),
            c13 decimal(15, 2),
            c14 string,
            c15 date,
            c16 timestamp
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
                    let data_type = resolve_type_name(&column.data_type)?;
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
        let mut generater = SqlGenerator::new(&mut rng, vec![table]);
        for _ in 0..self.count {
            let query = generater.gen_query();
            println!("query={:?}", query.to_string());
            // TODO check query result
            conn.exec(&query.to_string()).await.unwrap();
        }
    }
}
