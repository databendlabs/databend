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

use common_ast::parser::ast::Expr;
use common_exception::Result;

use crate::sql::planner::bind_context::BindContext;
use crate::sql::planner::scalar::ScalarExpr;
use crate::sql::BoundVariable;

/// Helper to build `ScalarExpr` with AST and `BindContext`.
pub struct ExpressionBinder {}

impl ExpressionBinder {
    pub fn create() -> Self {
        ExpressionBinder {}
    }

    pub fn bind_expr(&self, expr: &Expr, bind_context: &BindContext) -> Result<ScalarExpr> {
        match expr {
            Expr::ColumnRef {
                database: _,
                table,
                column,
            } => {
                let table_name = table.as_ref().map(|t| (t.name).to_lowercase());
                let column_name = column.name.to_lowercase();
                let column = bind_context.resolve_column(table_name, column_name)?;
                Ok(ScalarExpr::BoundVariable(BoundVariable {
                    index: column.index,
                    data_type: column.data_type,
                    nullable: column.nullable,
                }))
            }
            _ => todo!(),
        }
    }
}
