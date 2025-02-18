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
use crate::ast::MutationSource;
use crate::ast::MutationUpdateExpr;
use crate::ast::TableAlias;
use crate::ast::With;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UpdateStmt {
    pub hints: Option<Hint>,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub table_alias: Option<TableAlias>,
    pub update_list: Vec<MutationUpdateExpr>,
    pub from: Option<MutationSource>,
    pub selection: Option<Expr>,
    // With clause, common table expression
    pub with: Option<With>,
}

impl Display for UpdateStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "UPDATE ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if let Some(alias) = &self.table_alias {
            write!(f, " AS {}", alias.name)?;
        }
        write!(f, " SET ")?;
        write_comma_separated_list(f, &self.update_list)?;
        if let Some(from) = &self.from {
            write!(f, " FROM {} ", from)?;
        }
        if let Some(conditions) = &self.selection {
            write!(f, " WHERE {conditions}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UpdateExpr {
    pub name: Identifier,
    pub expr: Expr,
}

impl Display for UpdateExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} = {}", self.name, self.expr)
    }
}
