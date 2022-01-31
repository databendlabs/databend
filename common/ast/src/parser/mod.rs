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

pub mod ast;
pub mod expr;
pub mod rule;
pub mod token;
pub mod transformer;

use common_exception::Result;

use crate::parser::ast::Statement;
use crate::parser::transformer::AstTransformer;
use crate::parser::transformer::AstTransformerFactory;

pub struct Parser;

impl Parser {
    // Parse a SQL string into `Statement`s.
    #[allow(unused)]
    pub fn parse_sql(&self, _: &str) -> Result<Vec<Statement>> {
        todo!()
    }

    #[allow(unused)]
    pub fn parse_with_sqlparser(&self, sql: &str) -> Result<Vec<Statement>> {
        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)?;
        stmts
            .into_iter()
            .map(|stmt| {
                let transformer = AstTransformerFactory::new_sqlparser_transformer(stmt);
                transformer.transform()
            })
            .collect::<Result<_>>()
    }
}
