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
