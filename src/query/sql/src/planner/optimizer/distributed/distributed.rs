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

use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::property::require_property;
use crate::optimizer::Distribution;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;
use crate::plans::Sort;

pub fn optimize_distributed_query(ctx: Arc<dyn TableContext>, s_expr: &SExpr) -> Result<SExpr> {
    let required = RequiredProperty {
        distribution: Distribution::Any,
    };
    let mut result = require_property(ctx, &required, s_expr)?;
    (result, _) = push_down_topk_to_merge(&result, None)?;
    let rel_expr = RelExpr::with_s_expr(&result);
    let physical_prop = rel_expr.derive_physical_prop()?;
    let root_required = RequiredProperty {
        distribution: Distribution::Serial,
    };
    if !root_required.satisfied_by(&physical_prop) {
        // Manually enforce serial distribution.
        result = SExpr::create_unary(Arc::new(Exchange::Merge.into()), Arc::new(result));
    }

    Ok(result)
}

// Traverse the SExpr tree to find top_k, if find, push down it to Exchange::Merge
fn push_down_topk_to_merge(s_expr: &SExpr, mut top_k: Option<Sort>) -> Result<(SExpr, bool)> {
    if let RelOperator::Exchange(Exchange::Merge) = *s_expr.plan {
        // A quick fix for Merge child is aggregate.
        // Todo: consider to push down topk to the above of aggregate.
        if let RelOperator::Aggregate(_) = *s_expr.child(0)?.plan {
            return Ok((s_expr.clone(), false));
        }
        if let Some(sort) = &top_k {
            if sort.limit.is_some() {
                let child = Arc::new(SExpr::create_unary(
                    Arc::new(sort.clone().into()),
                    s_expr.children[0].clone(),
                ));
                let children = if s_expr.children.len() == 2 {
                    vec![child, s_expr.children[1].clone()]
                } else {
                    vec![child]
                };
                return Ok((s_expr.replace_children(children.into_iter()), true));
            }
        }
    }

    if let RelOperator::Sort(sort) = s_expr.plan.as_ref() {
        if sort.limit.is_some() {
            top_k = Some(sort.clone());
        }
    }

    let mut pushed_down_to_exchange = false;
    let mut new_children = Vec::with_capacity(s_expr.children.len());
    for child in s_expr.children.iter() {
        let (child, pushed_down) = push_down_topk_to_merge(child, top_k.clone())?;
        pushed_down_to_exchange = pushed_down_to_exchange || pushed_down;
        new_children.push(Arc::new(child));
    }
    let mut res = s_expr.replace_children(new_children);
    if let RelOperator::Sort(sort) = s_expr.plan.as_ref() {
        if pushed_down_to_exchange {
            let mut sort = sort.clone();
            sort.after_exchange = true;
            res = res.replace_plan(Arc::new(sort.into()));
        }
    }
    Ok((res, pushed_down_to_exchange))
}
