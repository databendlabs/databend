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

use std::collections::BTreeMap;

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::RuntimeFilterId;
use crate::plans::RuntimeFilterSource;
use crate::ScalarExpr;

pub struct RuntimeFilterResult {
    pub left_runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
    pub right_runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
}

fn create_runtime_filters(join: &Join) -> Result<RuntimeFilterResult> {
    let mut left_runtime_filters = BTreeMap::new();
    let mut right_runtime_filters = BTreeMap::new();
    for (idx, exprs) in join
        .right_conditions
        .iter()
        .zip(join.left_conditions.iter())
        .enumerate()
    {
        right_runtime_filters.insert(RuntimeFilterId::new(idx), exprs.0.clone());
        left_runtime_filters.insert(RuntimeFilterId::new(idx), exprs.1.clone());
    }
    Ok(RuntimeFilterResult {
        left_runtime_filters,
        right_runtime_filters,
    })
}

fn wrap_runtime_filter_source(
    s_expr: &SExpr,
    runtime_filter_result: RuntimeFilterResult,
) -> Result<SExpr> {
    let source_node = RuntimeFilterSource {
        left_runtime_filters: runtime_filter_result.left_runtime_filters,
        right_runtime_filters: runtime_filter_result.right_runtime_filters,
    };
    let build_side = s_expr.child(1)?.clone();
    let mut probe_side = s_expr.child(0)?.clone();
    probe_side = SExpr::create_binary(source_node.into(), probe_side, build_side.clone());
    let mut join: Join = s_expr.plan().clone().try_into()?;
    join.contain_runtime_filter = true;
    let s_expr = s_expr.replace_plan(RelOperator::Join(join));
    Ok(s_expr.replace_children(vec![probe_side, build_side]))
}

// Traverse plan tree and check if exists join
// Currently, only support inner join.
pub fn try_add_runtime_filter_nodes(expr: &SExpr) -> Result<SExpr> {
    if expr.children().len() == 1 && expr.children()[0].is_pattern() {
        return Ok(expr.clone());
    }
    let mut new_expr = expr.clone();
    if expr.plan.rel_op() == RelOp::Join {
        // Todo(xudong): develop a strategy to decide whether to add runtime filter node
        new_expr = add_runtime_filter_nodes(expr)?;
    }

    let mut children = vec![];

    for child in new_expr.children.iter() {
        children.push(try_add_runtime_filter_nodes(child)?);
    }
    Ok(new_expr.replace_children(children))
}

fn add_runtime_filter_nodes(expr: &SExpr) -> Result<SExpr> {
    assert_eq!(expr.plan.rel_op(), RelOp::Join);
    let join: Join = expr.plan().clone().try_into()?;
    if join.join_type != JoinType::Inner {
        return Ok(expr.clone());
    }
    let runtime_filter_result = create_runtime_filters(&join)?;
    wrap_runtime_filter_source(expr, runtime_filter_result)
}
