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

use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::SetExpr;
use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;

impl Binder {
    pub(crate) fn bind_set_expr(
        &mut self,
        bind_context: &mut BindContext,
        set_expr: &SetExpr,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
    ) -> Result<(SExpr, BindContext)> {
        match set_expr {
            SetExpr::Select(stmt) => self.bind_select(bind_context, stmt, order_by, limit),
            SetExpr::Query(stmt) => self.bind_query(bind_context, stmt),
            SetExpr::SetOperation(set_operation) => self.bind_set_operator(
                bind_context,
                &set_operation.left,
                &set_operation.right,
                &set_operation.op,
                &set_operation.all,
                None,
            ),
            SetExpr::Values { span, values } => self.bind_values(bind_context, *span, values),
        }
    }
}
