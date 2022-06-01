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

use std::collections::HashMap;

use common_exception::Result;

use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarItem;
use crate::sql::BindContext;
use crate::sql::IndexType;

impl Binder {
    pub(super) fn bind_distinct(
        &self,
        bind_context: &BindContext,
        projections: &[ColumnBinding],
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let scalar_items: Vec<ScalarItem> = scalar_items
            .drain()
            .map(|(_, item)| {
                if bind_context.in_grouping {
                    let mut group_checker = GroupingChecker::new(bind_context);
                    let scalar = group_checker.resolve(&item.scalar)?;
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
                scalar: Scalar::BoundColumnRef(BoundColumnRef { column: v.clone() }),
                index: v.index,
            })
            .collect();

        let distinct_plan = AggregatePlan {
            group_items,
            aggregate_functions: vec![],
            from_distinct: true,
        };

        Ok(SExpr::create_unary(distinct_plan.into(), new_expr))
    }
}
