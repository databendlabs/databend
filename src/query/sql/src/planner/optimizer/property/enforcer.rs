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
        .map(|child| require_property(ctx.clone(), required, child))
        .collect::<Result<Vec<SExpr>>>()?;
    let optimized_expr = SExpr::create(s_expr.plan().clone(), optimized_children, None, None, None);

    let rel_expr = RelExpr::with_s_expr(&optimized_expr);
    let mut children = Vec::with_capacity(s_expr.arity());
    for index in 0..optimized_expr.arity() {
        let required = rel_expr.compute_required_prop_child(ctx.clone(), index, required)?;
        let physical = rel_expr.derive_physical_prop_child(index)?;
        if let RelOperator::Join(_) = &s_expr.plan {
            // If the child is join probe side and join type is broadcast join
            // We should wrap the child with Random exchange to make it partition to all nodes
            if index == 0 && required.distribution == Distribution::Broadcast {
                if optimized_expr
                    .child(0)?
                    .children()
                    .iter()
                    .any(check_partition)
                {
                    children.push(optimized_expr.child(index)?.clone());
                    continue;
                }
                let enforced_child =
                    enforce_property(optimized_expr.child(index)?, &RequiredProperty {
                        distribution: Distribution::Any,
                    })?;
                children.push(enforced_child);
                continue;
            }
        }
        if let RelOperator::UnionAll(_) = &s_expr.plan {
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
                children.push(enforced_child);
                continue;
            }
        }

        if required.satisfied_by(&physical) {
            children.push(optimized_expr.child(index)?.clone());
            continue;
        }

        // Enforce required property on the child.
        let enforced_child = enforce_property(optimized_expr.child(index)?, &required)?;
        children.push(enforced_child);
    }

    Ok(SExpr::create(
        optimized_expr.plan().clone(),
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
        Distribution::Random | Distribution::Any => {
            Ok(SExpr::create_unary(Exchange::Random.into(), s_expr.clone()))
        }

        Distribution::Serial => Ok(SExpr::create_unary(Exchange::Merge.into(), s_expr.clone())),

        Distribution::Broadcast => Ok(SExpr::create_unary(
            Exchange::Broadcast.into(),
            s_expr.clone(),
        )),

        Distribution::Hash(hash_keys) => Ok(SExpr::create_unary(
            Exchange::Hash(hash_keys.clone()).into(),
            s_expr.clone(),
        )),
    }
}

fn check_merge(s_expr: &SExpr) -> bool {
    if let RelOperator::Exchange(op) = &s_expr.plan {
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
    if let RelOperator::Exchange(op) = &s_expr.plan {
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
