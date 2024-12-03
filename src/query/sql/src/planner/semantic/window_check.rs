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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::ColumnBindingBuilder;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::ScalarExpr;
use crate::Visibility;

pub struct WindowChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> WindowChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }
}

impl<'a> VisitorMut<'_> for WindowChecker<'a> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::WindowFunction(win) = expr {
            if let Some(column) = self
                .bind_context
                .windows
                .window_functions_map
                .get(&win.display_name)
            {
                let window_info = &self.bind_context.windows.window_functions[*column];
                let column_binding = ColumnBindingBuilder::new(
                    win.display_name.clone(),
                    window_info.index,
                    Box::new(window_info.func.return_type()),
                    Visibility::Visible,
                )
                .build();
                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();
                return Ok(());
            }

            return Err(ErrorCode::Internal("Window Check: Invalid window function"));
        }

        walk_expr_mut(self, expr)
    }

    fn visit_subquery_expr(&mut self, _: &mut SubqueryExpr) -> Result<()> {
        // TODO(leiysky): check subquery in the future
        Ok(())
    }
}
