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

use std::collections::HashMap;

use common_exception::Result;
use common_exception::Span;

use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::optimizer::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::BindContext;
use crate::IndexType;

impl Binder {
    pub(super) fn bind_distinct(
        &self,
        span: Span,
        bind_context: &BindContext,
        projections: &[ColumnBinding],
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let scalar_items: Vec<ScalarItem> = scalar_items
            .drain()
            .map(|(_, item)| {
                if bind_context.in_grouping {
                    let group_checker = GroupingChecker::new(bind_context);
                    let scalar = group_checker.resolve(&item.scalar, None)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                } else {
                    Ok(item)
                }
            })
            .collect::<Result<_>>()?;

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(eval_scalar.into(), new_expr);
        }

        // Like aggregate, we just use scalar directly.
        let group_items: Vec<ScalarItem> = projections
            .iter()
            .map(|v| ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span,
                    column: v.clone(),
                }),
                index: v.index,
            })
            .collect();

        let distinct_plan = Aggregate {
            mode: AggregateMode::Initial,
            group_items,
            aggregate_functions: vec![],
            from_distinct: true,
            limit: None,
            grouping_id_index: 0,
            grouping_sets: vec![],
        };

        Ok(SExpr::create_unary(distinct_plan.into(), new_expr))
    }
}
