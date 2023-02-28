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

use std::collections::BTreeMap;
use std::collections::HashMap;

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RuntimeFilterId;
use crate::plans::RuntimeFilterSource;
use crate::ScalarExpr;

pub struct RuntimeFilterResult {
    pub runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
    // Used by join probe side
    pub predicates: Vec<ScalarExpr>,
}

fn create_runtime_filters(join: &Join) -> Result<RuntimeFilterResult> {
    let mut runtime_filters = HashMap::with_capacity(join.right_conditions.len());
    for (idx, expr) in join.right_conditions.iter().enumerate() {
        runtime_filters.insert(RuntimeFilterId::new(idx), expr);
        let probe_condition = &join.left_conditions[idx];
        // todo: create a new function to represent predicate for join probe side?
    }

    todo!()
}

fn wrap_filter_to_probe(s_expr: &SExpr, predicates: Vec<ScalarExpr>) -> Result<SExpr> {
    let mut probe_side = s_expr.child(0)?.clone();
    let new_filter = Filter {
        predicates,
        is_having: false,
    };
    probe_side = SExpr::create_unary(new_filter.into(), probe_side);
    Ok(s_expr
        .child(0)?
        .replace_children(vec![probe_side, s_expr.child(1)?.clone()]))
}

fn wrap_runtime_filter_source_to_build(
    s_expr: &SExpr,
    runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
) -> Result<SExpr> {
    let source_node = RuntimeFilterSource { runtime_filters };
    let mut build_side = s_expr.child(1)?.clone();
    build_side = SExpr::create_unary(source_node.into(), build_side);
    Ok(s_expr
        .child(0)?
        .replace_children(vec![s_expr.child(0)?.clone(), build_side]))
}

// Traverse plan tree and check if exists join
// Currently, only support inner join.
pub fn try_add_runtime_filter_nodes(expr: &SExpr) -> Result<SExpr> {
    if expr.plan.rel_op() == RelOp::Join {
        return add_runtime_filter_nodes(expr);
    }

    let mut children = vec![];

    for child in expr.children.iter() {
        children.push(try_add_runtime_filter_nodes(child)?);
    }
    Ok(expr.replace_children(children))
}

fn add_runtime_filter_nodes(expr: &SExpr) -> Result<SExpr> {
    assert_eq!(expr.plan.rel_op(), RelOp::Join);
    let join: Join = expr.plan().clone().try_into()?;
    if join.join_type != JoinType::Inner {
        return Ok(expr.clone());
    }
    let runtime_filter_result = create_runtime_filters(&join)?;
    // Add a filter node to probe side, the predicates contain runtime filter info
    let expr = wrap_filter_to_probe(expr, runtime_filter_result.predicates)?;
    // Add RuntimeFilterSource node to build side
    Ok(wrap_runtime_filter_source_to_build(&expr, runtime_filter_result.runtime_filters)?)
}
