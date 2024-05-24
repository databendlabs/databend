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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::FileFormatOptions;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableAlias;
use crate::ast::TableReference;
use crate::ParseError;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct MergeUpdateExpr {
    pub table: Option<Identifier>,
    pub name: Identifier,
    pub expr: Expr,
}

impl Display for MergeUpdateExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.table.is_some() {
            write!(f, "{}.", self.table.clone().unwrap())?;
        }

        write!(f, "{} = {}", self.name, self.expr)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum MatchOperation {
    Update {
        update_list: Vec<MergeUpdateExpr>,
        #[drive(skip)]
        is_star: bool,
    },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct MatchedClause {
    pub selection: Option<Expr>,
    pub operation: MatchOperation,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InsertOperation {
    pub columns: Option<Vec<Identifier>>,
    pub values: Vec<Expr>,
    #[drive(skip)]
    pub is_star: bool,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UnmatchedClause {
    pub selection: Option<Expr>,
    pub insert_operation: InsertOperation,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum MergeOption {
    Match(MatchedClause),
    Unmatch(UnmatchedClause),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct MergeIntoStmt {
    pub hints: Option<Hint>,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table_ident: Identifier,
    pub source: MergeSource,
    // target_alias is belong to target
    pub target_alias: Option<TableAlias>,
    pub join_expr: Expr,
    pub merge_options: Vec<MergeOption>,
}

impl Display for MergeIntoStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MERGE INTO ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table_ident)),
        )?;
        if let Some(alias) = &self.target_alias {
            write!(f, " AS {}", alias.name)?;
        }
        write!(f, " USING {} ON {}", self.source, self.join_expr)?;

        for clause in &self.merge_options {
            match clause {
                MergeOption::Match(match_clause) => {
                    write!(f, " WHEN MATCHED ")?;
                    if let Some(e) = &match_clause.selection {
                        write!(f, "AND {} ", e)?;
                    }
                    write!(f, "THEN ")?;

                    match &match_clause.operation {
                        MatchOperation::Update {
                            update_list,
                            is_star,
                        } => {
                            if *is_star {
                                write!(f, "UPDATE *")?;
                            } else {
                                write!(f, "UPDATE SET ")?;
                                write_comma_separated_list(f, update_list)?;
                            }
                        }
                        MatchOperation::Delete => {
                            write!(f, "DELETE")?;
                        }
                    }
                }
                MergeOption::Unmatch(unmatch_clause) => {
                    write!(f, " WHEN NOT MATCHED ")?;
                    if let Some(e) = &unmatch_clause.selection {
                        write!(f, "AND {} ", e)?;
                    }
                    write!(f, "THEN INSERT")?;

                    if let Some(columns) = &unmatch_clause.insert_operation.columns {
                        if !columns.is_empty() {
                            write!(f, " (")?;
                            write_comma_separated_list(f, columns)?;
                            write!(f, ")")?;
                        }
                    }

                    if unmatch_clause.insert_operation.is_star {
                        write!(f, " *")?;
                    } else {
                        write!(f, " VALUES(")?;
                        write_comma_separated_list(
                            f,
                            unmatch_clause.insert_operation.values.clone(),
                        )?;
                        write!(f, ")")?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl MergeIntoStmt {
    pub fn split_clauses(&self) -> (Vec<MatchedClause>, Vec<UnmatchedClause>) {
        let mut match_clauses = Vec::with_capacity(self.merge_options.len());
        let mut unmatch_clauses = Vec::with_capacity(self.merge_options.len());
        for merge_operation in &self.merge_options {
            match merge_operation {
                MergeOption::Match(match_clause) => match_clauses.push(match_clause.clone()),
                MergeOption::Unmatch(unmatch_clause) => {
                    unmatch_clauses.push(unmatch_clause.clone())
                }
            }
        }
        (match_clauses, unmatch_clauses)
    }

    pub fn check_multi_match_clauses_semantic(clauses: &[MatchedClause]) -> Result<()> {
        // check match_clauses
        if clauses.len() > 1 {
            for (idx, clause) in clauses.iter().enumerate() {
                if clause.selection.is_none() && idx < clauses.len() - 1 {
                    return Err(ParseError(None,
                        "when there are multi matched clauses, we must have a condition for every one except the last one".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn check_multi_unmatch_clauses_semantic(clauses: &[UnmatchedClause]) -> Result<()> {
        // check unmatch_clauses
        if clauses.len() > 1 {
            for (idx, clause) in clauses.iter().enumerate() {
                if clause.selection.is_none() && idx < clauses.len() - 1 {
                    return Err(ParseError(None,
                        "when there are multi unmatched clauses, we must have a condition for every one except the last one".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum MergeSource {
    StreamingV2 {
        settings: FileFormatOptions,
        #[drive(skip)]
        on_error_mode: Option<String>,
        #[drive(skip)]
        start: usize,
    },

    Select {
        query: Box<Query>,
        source_alias: TableAlias,
    },
    Table {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
        alias: Option<TableAlias>,
    },
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct StreamingSource {
    #[drive(skip)]
    settings: BTreeMap<String, String>,
    #[drive(skip)]
    on_error_mode: Option<String>,
    #[drive(skip)]
    start: usize,
}

impl MergeSource {
    pub fn transform_table_reference(&self) -> TableReference {
        match self {
            Self::StreamingV2 {
                settings: _,
                on_error_mode: _,
                start: _,
            } => unimplemented!(),

            Self::Select {
                query,
                source_alias,
            } => TableReference::Subquery {
                span: None,
                lateral: false,
                subquery: query.clone(),
                alias: Some(source_alias.clone()),
            },
            Self::Table {
                catalog,
                database,
                table,
                alias,
            } => TableReference::Table {
                span: None,
                catalog: catalog.clone(),
                database: database.clone(),
                table: table.clone(),
                alias: alias.clone(),
                temporal: None,
                consume: false,
                pivot: None,
                unpivot: None,
            },
        }
    }
}

impl Display for MergeSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            MergeSource::StreamingV2 {
                settings,
                on_error_mode,
                start: _,
            } => {
                write!(f, " FILE_FORMAT = ({})", settings)?;
                write!(
                    f,
                    " ON_ERROR = '{}'",
                    on_error_mode.as_ref().unwrap_or(&"Abort".to_string())
                )
            }

            MergeSource::Select {
                query,
                source_alias,
            } => write!(f, "({query}) AS {source_alias}"),

            MergeSource::Table {
                catalog,
                database,
                table,
                alias,
            } => {
                write_dot_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(table)),
                )?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }
        }
    }
}
