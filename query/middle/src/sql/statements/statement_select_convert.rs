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

use std::convert::TryFrom;

use sqlparser::ast::Query;
use sqlparser::ast::Select;
use sqlparser::ast::SetExpr;
use sqlparser::parser::ParserError;

use crate::sql::statements::DfQueryStatement;

impl TryFrom<Query> for DfQueryStatement {
    type Error = ParserError;

    fn try_from(query: Query) -> Result<Self, Self::Error> {
        let query_body = Self::get_body(&query)?;

        if query.with.is_some() {
            return Err(ParserError::ParserError(String::from(
                "CTE is not yet implement",
            )));
        }

        if query.fetch.is_some() {
            return Err(ParserError::ParserError(String::from(
                "FETCH is not yet implement",
            )));
        }

        if query_body.top.is_some() {
            return Err(ParserError::ParserError(String::from(
                "TOP is not yet implement",
            )));
        }

        if !query_body.sort_by.is_empty() {
            return Err(ParserError::ParserError(String::from(
                "Sort by is unsupported",
            )));
        }

        if !query_body.cluster_by.is_empty() {
            return Err(ParserError::ParserError(String::from(
                "Cluster by is unsupported",
            )));
        }

        if !query_body.distribute_by.is_empty() {
            return Err(ParserError::ParserError(String::from(
                "Distribute by is unsupported",
            )));
        }

        Ok(DfQueryStatement {
            from: query_body.from.clone(),
            projection: query_body.projection.clone(),
            selection: query_body.selection.clone(),
            group_by: query_body.group_by.clone(),
            having: query_body.having.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit.clone(),
            offset: query.offset.clone(),
        })
    }
}

impl DfQueryStatement {
    fn get_body(query: &Query) -> Result<&Select, ParserError> {
        match &query.body {
            SetExpr::Select(query) => Ok(query),
            other => Err(ParserError::ParserError(format!(
                "Query {} is not yet implemented",
                other
            ))),
        }
    }
}
