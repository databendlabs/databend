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

use databend_common_ast_visit_derive::Walk;
use databend_common_ast_visit_derive::WalkMut;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Expr;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::InsertSource;
use crate::ast::TableRef;
use crate::ast::write_comma_separated_list;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut, Walk, WalkMut)]
pub struct ReplaceStmt {
    pub hints: Option<Hint>,
    pub table: TableRef,
    pub is_conflict: bool,
    pub on_conflict_columns: Vec<Identifier>,
    pub columns: Vec<Identifier>,
    pub source: InsertSource,
    pub delete_when: Option<Expr>,
}

impl Display for ReplaceStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REPLACE")?;
        if let Some(hints) = &self.hints {
            write!(f, " {}", hints)?;
        }
        write!(f, " INTO ")?;
        write!(f, "{}", self.table)?;

        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }

        // on_conflict_columns must be non-empty
        write!(f, " ON")?;
        if self.is_conflict {
            write!(f, " CONFLICT")?;
        }
        write!(f, " (")?;
        write_comma_separated_list(f, &self.on_conflict_columns)?;
        write!(f, ")")?;

        if let Some(expr) = &self.delete_when {
            write!(f, " DELETE WHEN {expr}")?;
        }

        write!(f, " {}", self.source)
    }
}
