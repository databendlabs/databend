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

use std::any::Any;
use std::sync::Arc;

use common_ast::ast::Expr;
use common_datavalues::DataTypePtr;
use common_exception::Result;

use crate::sql::planner::binder::BindContext;

/// Helper for binding scalar expression with `BindContext`.
pub struct ScalarBinder;

impl ScalarBinder {
    pub fn new() -> Self {
        ScalarBinder {}
    }

    pub fn bind_expr(&self, expr: &Expr, bind_context: &BindContext) -> Result<ScalarExprRef> {
        match expr {
            Expr::ColumnRef { table, column, .. } => {
                let table_name: Option<String> = table.clone().map(|ident| ident.name);
                let column_name = column.name.clone();
                let _column_binding = bind_context.resolve_column(table_name, column_name)?;

                todo!()
            }
            _ => todo!(),
        }
    }
}

pub type ScalarExprRef = Arc<dyn ScalarExpr>;

pub trait ScalarExpr: Any {
    /// Get return type and nullability
    fn data_type(&self) -> (DataTypePtr, bool);

    // TODO: implement this in the future
    // fn used_columns(&self) -> ColumnSet;

    // TODO: implement this in the future
    // fn outer_columns(&self) -> ColumnSet;

    fn contains_aggregate(&self) -> bool;

    fn contains_subquery(&self) -> bool;

    fn as_any(&self) -> &dyn Any;
}
