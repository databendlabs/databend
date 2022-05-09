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
use common_datavalues::DataTypeImpl;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::OrderExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::Scalar::Order;
use crate::sql::plans::SortPlan;
use crate::sql::BindContext;

impl Binder {
    pub(super) fn bind_order_by(
        &mut self,
        order_by: &[OrderByExpr],
        bind_context: &mut BindContext,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new(bind_context);
        let order_by_exprs = order_by
            .iter()
            .map(|expr| scalar_binder.bind_expr(&expr.expr))
            .collect::<Result<Vec<(Scalar, DataTypeImpl)>>>()?;

        let mut order_by_items = Vec::with_capacity(order_by_exprs.len());
        for (idx, order_by_expr) in order_by_exprs.iter().enumerate() {
            let order_by_item = OrderExpr {
                expr: Box::new(order_by_expr.0.clone()),
                asc: order_by[idx].asc,
                nulls_first: order_by[idx].nulls_first,
            };
            order_by_items.push(Order(order_by_item));
        }
        let sort_plan = SortPlan {
            items: order_by_items,
        };
        let new_expr =
            SExpr::create_unary(sort_plan.into(), bind_context.expression.clone().unwrap());
        bind_context.expression = Some(new_expr);
        Ok(())
    }
}
