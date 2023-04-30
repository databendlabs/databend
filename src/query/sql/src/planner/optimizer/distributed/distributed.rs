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

use crate::optimizer::distributed::topk::TopK;
use crate::optimizer::property::require_property;
use crate::optimizer::Distribution;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;

pub fn optimize_distributed_query(ctx: Arc<dyn TableContext>, s_expr: &SExpr) -> Result<SExpr> {
    let required = RequiredProperty {
        distribution: Distribution::Any,
    };
    let mut result = require_property(ctx, &required, s_expr)?;
    result = push_down_topk_to_merge(&result, None)?;
    let rel_expr = RelExpr::with_s_expr(&result);
    let physical_prop = rel_expr.derive_physical_prop()?;
    let root_required = RequiredProperty {
        distribution: Distribution::Serial,
    };
    if !root_required.satisfied_by(&physical_prop) {
        // Manually enforce serial distribution.
        result = SExpr::create_unary(Exchange::Merge.into(), result);
    }

    Ok(result)
}

// Traverse the SExpr tree to find top_k, if find, push down it to Exchange::Merge
fn push_down_topk_to_merge(s_expr: &SExpr, mut top_k: Option<TopK>) -> Result<SExpr> {
    if let RelOperator::Exchange(Exchange::Merge) = s_expr.plan {
        // A quick fix for Merge child is aggregate.
        // Todo: consider to push down topk to the above of aggregate.
        if let RelOperator::Aggregate(_) = s_expr.child(0)?.plan {
            return Ok(s_expr.clone());
        }
        if let Some(top_k) = top_k {
            let mut child = s_expr.children[0].clone();
            child = SExpr::create_unary(top_k.sort.into(), child.clone());
            let children = if s_expr.children.len() == 2 {
                vec![child, s_expr.children[1].clone()]
            } else {
                vec![child]
            };
            return Ok(s_expr.replace_children(children));
        }
    }

    let mut s_expr_children = vec![];
    for child in s_expr.children.iter() {
        top_k = None;
        if let RelOperator::Sort(sort) = &child.plan {
            if sort.limit.is_some() {
                top_k = Some(TopK { sort: sort.clone() });
            }
        }
        let mut new_children = vec![];
        for child_child in child.children.iter() {
            new_children.push(push_down_topk_to_merge(child_child, top_k.clone())?);
        }
        s_expr_children.push(child.replace_children(new_children));
    }
    Ok(s_expr.replace_children(s_expr_children))
}
