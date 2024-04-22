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

use crate::ast::Expr;
use crate::ast::Hint;
use crate::ast::TableReference;
use crate::ast::With;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DeleteStmt {
    pub hints: Option<Hint>,
    pub table: TableReference,
    pub selection: Option<Expr>,
    // With clause, common table expression
    pub with: Option<With>,
}

impl Display for DeleteStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "DELETE ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, "FROM {}", self.table)?;
        if let Some(conditions) = &self.selection {
            write!(f, " WHERE {conditions}")?;
        }
        Ok(())
    }
}
