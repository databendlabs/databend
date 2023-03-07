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

use common_catalog::plan::RuntimeFilterId;
use common_exception::Result;

use crate::binder::JoinPredicate;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::RelOperator::Scan as RelScan;
use crate::plans::Scan;
use crate::IndexType;
use crate::ScalarExpr;

pub struct RuntimeFilterResult {
    pub target_exprs: BTreeMap<RuntimeFilterId, ScalarExpr>,
    pub source_exprs: BTreeMap<RuntimeFilterId, ScalarExpr>,
}

fn create_runtime_filters(id: &mut IndexType, join: &Join) -> Result<RuntimeFilterResult> {
    let mut target_exprs = BTreeMap::new();
    let mut source_exprs = BTreeMap::new();
    for (source_expr, target_expr) in join
        .right_conditions
        .iter()
        .zip(join.left_conditions.iter())
    {
        source_exprs.insert(RuntimeFilterId::new(*id), source_expr.clone());
        target_exprs.insert(RuntimeFilterId::new(*id), target_expr.clone());
        *id += 1;
    }
    Ok(RuntimeFilterResult {
        target_exprs,
        source_exprs,
    })
}

// Traverse plan tree and check if exists join
// Currently, only support inner join.
// If find join, then add runtime_filter to join probe scan node
pub fn try_add_runtime_filter_to_scan(id: &mut IndexType, expr: &SExpr) -> Result<SExpr> {
    if expr.plan().is_pattern() {
        return Ok(expr.clone());
    }
    let mut new_expr = expr.clone();
    if expr.plan.rel_op() == RelOp::Join {
        // Todo(xudong): develop a strategy to decide whether to add runtime filter node
        let join: Join = expr.plan().clone().try_into()?;
        if join.join_type == JoinType::Inner {
            let runtime_filter_result = create_runtime_filters(id, &join)?;
            new_expr = expr.replace_plan(RelOperator::Join(Join {
                left_conditions: join.left_conditions,
                right_conditions: join.right_conditions,
                non_equi_conditions: join.non_equi_conditions,
                join_type: join.join_type,
                marker_index: join.marker_index,
                from_correlated_subquery: join.from_correlated_subquery,
                source_exprs: runtime_filter_result.source_exprs,
            }));
            let new_left_child =
                add_runtime_filter_to_scan(runtime_filter_result.target_exprs, new_expr.child(0)?)?;
            new_expr = new_expr.replace_children(vec![new_left_child, expr.child(1)?.clone()]);
        }
    }
    let mut children = vec![];

    for child in new_expr.children.iter() {
        children.push(try_add_runtime_filter_to_scan(id, child)?);
    }
    Ok(new_expr.replace_children(children))
}

fn add_runtime_filter_to_scan(
    target_exprs: BTreeMap<RuntimeFilterId, ScalarExpr>,
    expr: &SExpr,
) -> Result<SExpr> {
    match expr.plan() {
        RelOperator::Scan(op) => {
            let new_expr = expr.replace_plan(RelScan(Scan {
                table_index: op.table_index,
                columns: op.columns.clone(),
                push_down_predicates: op.push_down_predicates.clone(),
                limit: op.limit.clone(),
                order_by: op.order_by.clone(),
                prewhere: op.prewhere.clone(),
                runtime_filter_exprs: Some(target_exprs),
                statistics: op.statistics.clone(),
            }));
            Ok(new_expr)
        }
        RelOperator::Join(_) => {
            let left_child = expr.child(0)?;
            let right_child = expr.child(1)?;
            let rel_expr = RelExpr::with_s_expr(expr);
            let left_prop = rel_expr.derive_relational_prop_child(0)?;
            let right_prop = rel_expr.derive_relational_prop_child(1)?;
            let mut left_target_exprs = BTreeMap::new();
            let mut right_target_exprs = BTreeMap::new();
            for target_expr in target_exprs.iter() {
                let pred = JoinPredicate::new(target_expr.1, &left_prop, &right_prop);
                match pred {
                    JoinPredicate::Left(_) => {
                        left_target_exprs.insert(target_expr.0.clone(), target_expr.1.clone());
                    }
                    JoinPredicate::Right(_) => {
                        right_target_exprs.insert(target_expr.0.clone(), target_expr.1.clone());
                    }
                    _ => unreachable!(),
                }
            }
            let new_left_child = add_runtime_filter_to_scan(left_target_exprs, left_child)?;
            let new_right_child = add_runtime_filter_to_scan(right_target_exprs, right_child)?;
            Ok(expr.replace_children(vec![new_left_child, new_right_child]))
        }
        RelOperator::EvalScalar(_)
        | RelOperator::Filter(_)
        | RelOperator::Aggregate(_)
        | RelOperator::Sort(_)
        | RelOperator::Limit(_) => {
            let child_expr = add_runtime_filter_to_scan(target_exprs, expr.child(0)?)?;
            Ok(expr.replace_children(vec![child_expr]))
        }
        RelOperator::UnionAll(_) | RelOperator::DummyTableScan(_) => Ok(expr.clone()),
        RelOperator::Exchange(_) | RelOperator::Pattern(_) => {
            unreachable!()
        }
    }
}
