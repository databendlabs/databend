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

use common_ast::ast::OrderByExpr;
use common_ast::parser::error::DisplayError;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::SortItem;
use crate::sql::plans::SortPlan;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(super) async fn bind_order_by(
        &mut self,
        child: SExpr,
        order_by: &[OrderByExpr<'a>],
        input_context: &BindContext,
        output_context: &BindContext,
    ) -> Result<SExpr> {
        let select_scalar_binder = ScalarBinder::new(output_context, self.ctx.clone());
        let from_scalar_binder = ScalarBinder::new(input_context, self.ctx.clone());
        let mut order_by_items = vec![];
        for order in order_by {
            // First we try to resolve sort item with `SELECT` context
            let mut res = select_scalar_binder.bind_expr(&order.expr).await;
            // If failed, we will try to resolve sort item with `FROM` context
            if let Err(e) = res.clone() {
                // If still failed, the previous error will be raised
                res = from_scalar_binder.bind_expr(&order.expr).await.or(Err(e));
            }
            let (scalar, _) = res?;
            if !matches!(scalar, Scalar::BoundColumnRef(_)) {
                return Err(ErrorCode::SemanticError(
                    order
                        .expr
                        .span()
                        .display_error("can only order by column".to_string()),
                ));
            }
            let order_by_item = SortItem {
                expr: scalar,
                asc: order.asc,
                nulls_first: order.nulls_first,
            };
            order_by_items.push(order_by_item);
        }

        let sort_plan = SortPlan {
            items: order_by_items,
        };
        let new_expr = SExpr::create_unary(sort_plan.into(), child);
        Ok(new_expr)
    }
}
