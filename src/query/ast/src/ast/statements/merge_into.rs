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

use super::UpdateExpr;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::TableReference;

#[derive(Debug, Clone, PartialEq)]
pub enum MatchOperation {
    Update { update_list: Vec<UpdateExpr> },
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchedClause {
    pub selection: Option<Expr>,
    pub operations: Vec<MatchOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertOperation {
    pub columns: Option<Vec<Identifier>>,
    pub values: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnmatchedClause {
    pub selection: Option<Expr>,
    pub insert_operation: InsertOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeOption {
    Match(MatchedClause),
    Unmatch(UnmatchedClause),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeIntoStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub source_reference: TableReference,
    pub join_expr: Expr,
    pub merge_options: Vec<MergeOption>,
}

impl Display for MergeIntoStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MERGE INTO  ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " USING {} ON {}", self.source_reference, self.join_expr)?;

        for clause in &self.merge_options {
            match clause {
                MergeOption::Match(match_clause) => {
                    write!(f, " WHEN MATCHED ")?;
                    if let Some(e) = &match_clause.selection {
                        write!(f, " AND {} ", e)?;
                    }
                    write!(f, " THEN ")?;
                    for operation in &match_clause.operations {
                        match operation {
                            MatchOperation::Update { update_list } => {
                                write!(f, " UPDATE SET ")?;
                                write_comma_separated_list(f, update_list)?;
                            }
                            MatchOperation::Delete => {
                                write!(f, " DELETE ")?;
                            }
                        }
                    }
                }
                MergeOption::Unmatch(unmatch_clause) => {
                    write!(f, " WHEN NOT MATCHED ")?;
                    if let Some(e) = &unmatch_clause.selection {
                        write!(f, " AND {} ", e)?;
                    }
                    write!(f, " THEN INSERT ")?;
                    if let Some(columns) = &unmatch_clause.insert_operation.columns {
                        if !columns.is_empty() {
                            write!(f, " (")?;
                            write_comma_separated_list(f, columns)?;
                            write!(f, ")")?;
                        }
                    }
                    write!(f, " VALUES {} ", unmatch_clause.insert_operation.values)?;
                }
            }
        }
        Ok(())
    }
}
