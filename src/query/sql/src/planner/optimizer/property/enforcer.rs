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

use crate::optimizer::property::Distribution;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;

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
    for index in 0..optimized_expr.arity() {
        let required = rel_expr.compute_required_prop_child(ctx.clone(), index, required)?;
        let physical = rel_expr.derive_physical_prop_child(index)?;
        if let RelOperator::Join(_) = s_expr.plan.as_ref() {
            if index == 0 && required.distribution == Distribution::Broadcast {
                // If the child is join probe side and join type is broadcast join
                // We should wrap the child with Random exchange to make it partition to all nodes
                if optimized_expr
                    .child(0)?
                    .children()
                    .iter()
                    .any(|v| check_partition(v.as_ref()))
                {
                    children.push(Arc::new(optimized_expr.child(index)?.clone()));
                    continue;
                }
                let enforced_child =
                    enforce_property(optimized_expr.child(index)?, &RequiredProperty {
                        distribution: Distribution::Any,
                    })?;
                children.push(Arc::new(enforced_child));
                continue;
            } else if index == 1 && required.distribution == Distribution::Broadcast {
                // If the child is join build side and join type is broadcast join
                // We should wrap the child with Broadcast exchange to make it available to all nodes.
                let enforced_child = enforce_property(optimized_expr.child(index)?, &required)?;
                children.push(Arc::new(enforced_child));
            }
        }
        if let RelOperator::UnionAll(_) = s_expr.plan.as_ref() {
            // Wrap the child with Random exchange to make it partition to all nodes
            // Check if exists `Merge` in child, if not exits, wrap it with `Exchange`
            if optimized_expr
                .children()
                .iter()
                .all(|child| !check_merge(child))
            {
                let enforced_child =
                    enforce_property(optimized_expr.child(index)?, &RequiredProperty {
                        distribution: Distribution::Any,
                    })?;
                children.push(Arc::new(enforced_child));
                continue;
            }
        }

        if required.satisfied_by(&physical) {
            children.push(Arc::new(optimized_expr.child(index)?.clone()));
            continue;
        }

        // Enforce required property on the child.
        let enforced_child = enforce_property(optimized_expr.child(index)?, &required)?;
        children.push(Arc::new(enforced_child));
    }

    Ok(SExpr::create(
        Arc::new(optimized_expr.plan().clone()),
        children,
        None,
        None,
        None,
    ))
}

/// Try to enforce physical property from a physical `SExpr`
fn enforce_property(s_expr: &SExpr, required: &RequiredProperty) -> Result<SExpr> {
    let enforced_distribution = enforce_distribution(&required.distribution, s_expr)?;
    Ok(enforced_distribution)
}

pub fn enforce_distribution(distribution: &Distribution, s_expr: &SExpr) -> Result<SExpr> {
    match distribution {
        Distribution::Random | Distribution::Any => Ok(SExpr::create_unary(
            Arc::new(Exchange::Random.into()),
            Arc::new(s_expr.clone()),
        )),

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
    }
}

fn check_merge(s_expr: &SExpr) -> bool {
    // Todo: support cluster for materialized cte
    if let RelOperator::CteScan(_) = s_expr.plan.as_ref() {
        return true;
    }
    if let RelOperator::Exchange(op) = s_expr.plan.as_ref() {
        if op == &Exchange::Merge {
            return true;
        }
    }
    for child in s_expr.children() {
        if check_merge(child) {
            return true;
        }
    }
    false
}

fn check_partition(s_expr: &SExpr) -> bool {
    if let RelOperator::Exchange(op) = s_expr.plan.as_ref() {
        if matches!(op, Exchange::Random | Exchange::Hash(_)) {
            return true;
        }
    }
    for child in s_expr.children() {
        if check_partition(child) {
            return true;
        }
    }
    false
}
