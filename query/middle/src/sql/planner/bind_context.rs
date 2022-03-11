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

use common_ast::parser::ast::TableAlias;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::common::IndexType;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::scalar::ScalarExpr;

#[derive(Clone)]
pub struct ColumnBinding {
    /// Table name of this `ColumnBinding` in current context
    pub table_name: String,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    pub index: IndexType,
    pub data_type: DataTypePtr,
    pub nullable: bool,
    pub expr: Option<ScalarExpr>,
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Default)]
pub struct BindContext {
    _parent: Option<Box<BindContext>>,
    columns: Vec<ColumnBinding>,

    /// The relational operator in current context
    pub expression: Option<SExpr>,
}

impl BindContext {
    pub fn create() -> Self {
        Self::default()
    }

    pub fn new_with_parent(parent: Box<BindContext>) -> Self {
        BindContext {
            _parent: Some(parent),
            columns: vec![],
            expression: None,
        }
    }

    /// Generate a new BindContext and take current BindContext as its parent.
    #[must_use]
    pub fn push(self) -> Self {
        Self::new_with_parent(Box::new(self))
    }

    pub fn column_bindings_by_table_name(&self, name: &str) -> Vec<ColumnBinding> {
        let mut column_bindings = vec![];
        for column in self.columns.iter() {
            if column.table_name.as_str() == name {
                column_bindings.push(column.clone());
            }
        }
        column_bindings
    }

    pub fn column_indices_by_table_name(&self, name: &str) -> Vec<IndexType> {
        let mut indices = vec![];
        for column in self.columns.iter() {
            if column.table_name.as_str() == name {
                indices.push(column.index);
            }
        }
        indices
    }

    /// Returns all column bindings in current scope.
    pub fn all_column_bindings(&self) -> &Vec<ColumnBinding> {
        &self.columns
    }

    pub fn add_column_binding(
        &mut self,
        index: IndexType,
        table_name: String,
        column_name: String,
        data_type: DataTypePtr,
        nullable: bool,
        expr: Option<ScalarExpr>,
    ) {
        self.columns.push(ColumnBinding {
            index,
            table_name,
            column_name,
            data_type,
            nullable,
            expr,
        })
    }

    /// Apply table alias like `SELECT * FROM t AS t1(a, b, c)`.
    /// This method will rename column bindings according to table alias.
    pub fn apply_table_alias(&mut self, original_name: &str, alias: &TableAlias) -> Result<()> {
        for column in self.columns.iter_mut() {
            if column.table_name == *original_name {
                column.table_name = alias.name.to_string();
            }
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
    pub fn resolve_column(&self, table: Option<String>, column: String) -> Result<ColumnBinding> {
        // TODO: lookup parent context to support correlated subquery
        let mut result = vec![];
        if let Some(table) = table {
            for column_binding in self.columns.iter() {
                if column_binding.table_name.eq(&table) && column_binding.column_name.eq(&column) {
                    result.push(column_binding.clone());
                }
            }
        } else {
            for column_binding in self.columns.iter() {
                if column_binding.column_name.eq(&column) {
                    result.push(column_binding.clone());
                }
            }
        }

        if result.is_empty() {
            Err(ErrorCode::SemanticError(format!(
                "column \"{}\" doesn't exist",
                column
            )))
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(format!(
                "column reference \"{}\" is ambiguous",
                column
            )))
        } else {
            Ok(result.remove(0))
        }
    }
}
