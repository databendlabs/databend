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

use common_exception::ErrorCode;
use common_exception::Result;

use super::Cost;
use super::CostModel;
use crate::sql::optimizer::MExpr;
use crate::sql::optimizer::Memo;
use crate::sql::plans::PhysicalHashJoin;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::RelOperator;

static COST_FACTOR_COMPUTE_PER_ROW: f64 = 1.0;
static COST_FACTOR_HASH_TABLE_PER_ROW: f64 = 10.0;

#[derive(Default)]
pub struct DefaultCostModel;

impl CostModel for DefaultCostModel {
    fn compute_cost(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        compute_cost_impl(memo, m_expr)
    }
}

fn compute_cost_impl(memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
    match &m_expr.plan {
        RelOperator::PhysicalScan(plan) => compute_cost_physical_scan(memo, m_expr, plan),
        RelOperator::PhysicalHashJoin(plan) => compute_cost_hash_join(memo, m_expr, plan),
        RelOperator::UnionAll(_) => compute_cost_union_all(memo, m_expr),

        RelOperator::Project(_)
        | RelOperator::EvalScalar(_)
        | RelOperator::Filter(_)
        | RelOperator::Aggregate(_)
        | RelOperator::Sort(_)
        | RelOperator::Limit(_) => compute_cost_unary_common_operator(memo, m_expr),

        _ => Err(ErrorCode::LogicalError(
            "Cannot compute cost from logical plan",
        )),
    }
}

fn compute_cost_physical_scan(memo: &Memo, m_expr: &MExpr, _plan: &PhysicalScan) -> Result<Cost> {
    // Since we don't have alternations(e.g. index scan) for table scan for now, we just ignore
    // the I/O cost and treat `PhysicalScan` as normal computation.
    let group = memo.group(m_expr.group_index)?;
    let prop = &group.relational_prop;
    let cost = prop.cardinality * COST_FACTOR_COMPUTE_PER_ROW;
    Ok(Cost(cost))
}

fn compute_cost_hash_join(memo: &Memo, m_expr: &MExpr, _plan: &PhysicalHashJoin) -> Result<Cost> {
    let build_group = m_expr.child_group(memo, 1)?;
    let probe_group = m_expr.child_group(memo, 0)?;
    let build_card = build_group.relational_prop.cardinality;
    let probe_card = probe_group.relational_prop.cardinality;

    let cost =
        build_card * COST_FACTOR_HASH_TABLE_PER_ROW + probe_card * COST_FACTOR_COMPUTE_PER_ROW;
    Ok(Cost(cost))
}

/// Compute cost for the unary operators that perform simple computation(e.g. `Project`, `Filter`, `EvalScalar`).
///
/// TODO(leiysky): Since we don't have alternation for `Aggregate` for now, we just
/// treat `Aggregate` as normal computation.
fn compute_cost_unary_common_operator(memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
    let group = m_expr.child_group(memo, 0)?;
    let card = group.relational_prop.cardinality;
    let cost = card * COST_FACTOR_COMPUTE_PER_ROW;
    Ok(Cost(cost))
}

fn compute_cost_union_all(memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
    let left_group = m_expr.child_group(memo, 0)?;
    let right_group = m_expr.child_group(memo, 0)?;
    let card = left_group.relational_prop.cardinality + right_group.relational_prop.cardinality;
    let cost = card * COST_FACTOR_COMPUTE_PER_ROW;
    Ok(Cost(cost))
}
