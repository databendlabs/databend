// Copyright 2022 Datafuse Labs.
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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::write_comma_separated_list;
use crate::ast::CreateTableSource;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTabularFunctionStmt<'a> {
    pub if_not_exists: bool,
    pub name: Identifier<'a>,
    pub args: Vec<Expr<'a>>,
    pub source: Option<CreateTableSource<'a>>,
    pub as_query: Box<Query<'a>>,
}

impl Display for CreateTabularFunctionStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE FUNCTION ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        write!(f, " (")?;
        if !self.args.is_empty() {
            write_comma_separated_list(f, &self.args)?;
        }
        write!(f, ")")?;

        write!(f, " RETURNS TABLE")?;

        if let Some(source) = &self.source {
            write!(f, " {source}")?;
        }

        write!(f, " AS {}", self.as_query)?;

        Ok(())
    }
}
