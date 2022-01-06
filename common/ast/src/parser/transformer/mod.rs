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

mod transform_sqlparser;

use common_exception::Result;

use crate::parser::ast::Statement;
use crate::parser::transformer::transform_sqlparser::TransformerSqlparser;

// `AstTransformer` is a utility to transform an SQL AST produced by other parsers(e.g. sqlparser-rs)
// into Databend `Statement`.
pub trait AstTransformer {
    fn transform(&self) -> Result<Statement>;
}

pub struct AstTransformerFactory;

impl AstTransformerFactory {
    #[allow(unused)]
    pub fn new_sqlparser_transformer(stmt: sqlparser::ast::Statement) -> impl AstTransformer {
        TransformerSqlparser::new(stmt)
    }
}
