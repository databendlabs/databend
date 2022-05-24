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

use common_ast::ast::Identifier;
use common_ast::ast::TableAlias;
use common_ast::parser::error::DisplayError as _;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::AggregateInfo;
use crate::sql::common::IndexType;

#[derive(Clone, PartialEq, Debug)]
pub struct ColumnBinding {
    /// Table name of this `ColumnBinding` in current context
    pub table_name: Option<String>,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    /// Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: DataTypeImpl,

    /// Consider the sql: `select * from t join t1 using(a)`.
    /// The result should only contain one `a` column.
    /// So we need make `t.a` or `t1.a` invisible in unqualified wildcard.
    pub visible_in_unqualified_wildcard: bool,
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Default, Debug)]
pub struct BindContext {
    pub parent: Option<Box<BindContext>>,

    pub columns: Vec<ColumnBinding>,

    pub aggregate_info: AggregateInfo,

    /// True if there is aggregation in current context, which means
    /// non-grouping columns cannot be referenced outside aggregation
    /// functions, otherwise a grouping error will be raised.
    pub in_grouping: bool,
}

impl BindContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_parent(parent: Box<BindContext>) -> Self {
        BindContext {
            parent: Some(parent),
            columns: vec![],
            aggregate_info: Default::default(),
            in_grouping: false,
        }
    }

    /// Generate a new BindContext and take current BindContext as its parent.
    pub fn push(self) -> Self {
        Self::with_parent(Box::new(self))
    }

    /// Returns all column bindings in current scope.
    pub fn all_column_bindings(&self) -> &[ColumnBinding] {
        &self.columns
    }

    pub fn add_column_binding(&mut self, column_binding: ColumnBinding) {
        self.columns.push(column_binding);
    }

    /// Apply table alias like `SELECT * FROM t AS t1(a, b, c)`.
    /// This method will rename column bindings according to table alias.
    pub fn apply_table_alias(&mut self, alias: &TableAlias) -> Result<()> {
        for column in self.columns.iter_mut() {
            column.table_name = Some(alias.name.to_string());
        }

        if alias.columns.len() > self.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                self.columns.len(),
                alias.columns.len()
            )));
        }
        for (index, column_name) in alias.columns.iter().map(ToString::to_string).enumerate() {
            self.columns[index].column_name = column_name;
        }
        Ok(())
    }

    /// Try to find a column binding with given table name and column name.
    /// This method will return error if the given names are ambiguous or invalid.
    pub fn resolve_column(
        &self,
        table: Option<String>,
        column: &Identifier,
    ) -> Result<ColumnBinding> {
        let mut result = vec![];

        let mut bind_context: &BindContext = self;
        // Lookup parent context to support correlated subquery
        loop {
            for column_binding in self.columns.iter() {
                match (&table, &column_binding.table_name) {
                    // No qualified table name specified
                    (None, None) | (None, Some(_))
                        if column.name.to_lowercase() == column_binding.column_name =>
                    {
                        result.push(column_binding.clone());
                    }

                    // Qualified column reference
                    (Some(table), Some(table_name))
                        if table == table_name
                            && column.name.to_lowercase() == column_binding.column_name =>
                    {
                        result.push(column_binding.clone());
                    }
                    _ => {}
                }
            }

            if !result.is_empty() {
                break;
            }

            if let Some(ref parent) = bind_context.parent {
                bind_context = parent;
            } else {
                break;
            }
        }

        if result.is_empty() {
            Err(ErrorCode::SemanticError(
                column
                    .span
                    .display_error("column doesn't exist".to_string()),
            ))
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(
                column
                    .span
                    .display_error("column reference is ambiguous".to_string()),
            ))
        } else {
            Ok(result.remove(0))
        }
    }

    /// Get result columns of current context in order.
    /// For example, a query `SELECT b, a AS b FROM t` has `[(index_of(b), "b"), index_of(a), "b"]` as
    /// its result columns.
    ///
    /// This method is used to retrieve the physical representation of result set of
    /// a query.
    pub fn result_columns(&self) -> Vec<(IndexType, String)> {
        self.columns
            .iter()
            .map(|col| (col.index, col.column_name.clone()))
            .collect()
    }
}
