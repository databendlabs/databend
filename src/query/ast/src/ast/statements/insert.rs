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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::With;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InsertStmt {
    pub hints: Option<Hint>,
    // With clause, common table expression
    pub with: Option<With>,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub source: InsertSource,
    pub overwrite: bool,
}

impl Display for InsertStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "INSERT ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        if self.overwrite {
            write!(f, "OVERWRITE ")?;
        } else {
            write!(f, "INTO ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        write!(f, " {}", self.source)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum InsertSource {
    Values { rows: Vec<Vec<Expr>> },
    RawValues { rest_str: String, start: usize },
    Select { query: Box<Query> },
}

impl Display for InsertSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            InsertSource::Values { rows } => {
                write!(f, "VALUES ")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    write_comma_separated_list(f, row)?;
                    write!(f, ")")?;
                }
                Ok(())
            }
            InsertSource::RawValues { rest_str, .. } => write!(f, "VALUES {rest_str}"),
            InsertSource::Select { query } => write!(f, "{query}"),
        }
    }
}
