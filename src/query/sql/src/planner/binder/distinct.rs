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

use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_exception::Result;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::project::SelectOutputAnalysis;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::EvalScalar;
impl Binder {
    pub fn bind_distinct(
        &self,
        span: Span,
        _bind_context: &mut BindContext,
        projection: &mut SelectOutputAnalysis,
        child: SExpr,
    ) -> Result<SExpr> {
        let distinct_input = projection.take_distinct_plan(span);
        let pre_distinct_items = distinct_input.pre_distinct_items;
        let group_items = distinct_input.group_items;

        let mut new_expr = child;
        if !pre_distinct_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: pre_distinct_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let distinct_plan = Aggregate {
            mode: AggregateMode::Initial,
            group_items,
            from_distinct: true,
            ..Default::default()
        };

        Ok(SExpr::create_unary(
            Arc::new(distinct_plan.into()),
            Arc::new(new_expr),
        ))
    }
}
