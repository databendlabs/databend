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

use common_exception::Result;

use crate::sql::optimizer::property::Distribution;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Exchange;

/// Require and enforce physical property from a physical `SExpr`
pub fn require_property(required: &RequiredProperty, s_expr: &SExpr) -> Result<SExpr> {
    // First, we will require the child SExpr with input `RequiredProperty`
    let optimized_children = s_expr
        .children()
        .iter()
        .map(|child| require_property(required, child))
        .collect::<Result<Vec<SExpr>>>()?;
    let optimized_expr = SExpr::create(s_expr.plan().clone(), optimized_children, None, None);

    let rel_expr = RelExpr::with_s_expr(&optimized_expr);
    let mut children = Vec::with_capacity(s_expr.arity());
    for index in 0..optimized_expr.arity() {
        let required = rel_expr.compute_required_prop_child(index, required)?;
        let physical = rel_expr.derive_physical_prop_child(index)?;
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
    ))
}

/// Try to enforce physical property from a physical `SExpr`
fn enforce_property(s_expr: &SExpr, required: &RequiredProperty) -> Result<SExpr> {
    let enforced_distribution = enforce_distribution(&required.distribution, s_expr)?;
    Ok(enforced_distribution)
}

pub fn enforce_distribution(distribution: &Distribution, s_expr: &SExpr) -> Result<SExpr> {
    match distribution {
        Distribution::Random | Distribution::Any => Ok(s_expr.clone()),

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
