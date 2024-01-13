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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::property::Distribution;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::Exchange;

/// Require and enforce physical property from a physical `SExpr`
pub fn require_property(
    ctx: Arc<dyn TableContext>,
    required: &RequiredProperty,
    s_expr: &SExpr,
) -> Result<SExpr> {
    // First, we will require the child SExpr with input `RequiredProperty`
    let optimized_children = s_expr
        .children()
        .iter()
        .map(|child| Ok(Arc::new(require_property(ctx.clone(), required, child)?)))
        .collect::<Result<Vec<_>>>()?;
    let optimized_expr = SExpr::create(
        Arc::new(s_expr.plan().clone()),
        optimized_children,
        None,
        None,
        None,
    );

    let rel_expr = RelExpr::with_s_expr(&optimized_expr);
    let mut children = Vec::with_capacity(s_expr.arity());
    let plan = optimized_expr.plan.as_ref().clone();
    for index in 0..optimized_expr.arity() {
        let required = rel_expr.compute_required_prop_child(ctx.clone(), index, required)?;
        let physical = rel_expr.derive_physical_prop_child(index)?;

        if required.satisfied_by(&physical) {
            children.push(Arc::new(optimized_expr.child(index)?.clone()));
            continue;
        }

        // Enforce required property on the child.
        let enforced_child = enforce_property(optimized_expr.child(index)?, &required)?;
        children.push(Arc::new(enforced_child));
    }

    Ok(SExpr::create(Arc::new(plan), children, None, None, None))
}

/// Try to enforce physical property from a physical `SExpr`
fn enforce_property(s_expr: &SExpr, required: &RequiredProperty) -> Result<SExpr> {
    let enforced_distribution = enforce_distribution(&required.distribution, s_expr)?;
    Ok(enforced_distribution)
}

pub fn enforce_distribution(distribution: &Distribution, s_expr: &SExpr) -> Result<SExpr> {
    match distribution {
        Distribution::Serial => Ok(SExpr::create_unary(
            Arc::new(Exchange::Merge.into()),
            Arc::new(s_expr.clone()),
        )),

        Distribution::Broadcast => Ok(SExpr::create_unary(
            Arc::new(Exchange::Broadcast.into()),
            Arc::new(s_expr.clone()),
        )),

        Distribution::Hash(hash_keys) => Ok(SExpr::create_unary(
            Arc::new(Exchange::Hash(hash_keys.clone()).into()),
            Arc::new(s_expr.clone()),
        )),

        Distribution::Random | Distribution::Any => Err(ErrorCode::Internal(
            "Cannot enforce random or any distribution",
        )),
    }
}
