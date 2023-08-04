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

use super::InsertSource;
use super::UpdateExpr;
use crate::ast::Expr;
use crate::ast::Identifier;

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
pub struct UnmatchedClause {
    pub selection: Option<Expr>,
    pub columns: Option<Vec<Identifier>>,
    pub values: String,
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
    pub source: InsertSource,
    pub join_expr: Expr,
    pub merge_options: Vec<MergeOption>,
}

impl Display for MergeIntoStmt {
    fn fmt(&self, _: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}
