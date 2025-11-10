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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::binder::is_range_join_condition;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;

enum PhysicalJoinType {
    Hash,
    // The first arg is range conditions, the second arg is other conditions
    RangeJoin(Vec<ScalarExpr>, Vec<ScalarExpr>),
}

// Choose physical join type by join conditions
fn physical_join(join: &Join, s_expr: &SExpr) -> Result<PhysicalJoinType> {
    if join.equi_conditions.is_empty() && join.join_type.is_any_join() {
        return Err(ErrorCode::SemanticError(
            "ANY JOIN only supports equality-based hash joins",
        ));
    }

    if !join.equi_conditions.is_empty() {
        // Contain equi condition, use hash join
        return Ok(PhysicalJoinType::Hash);
    }

    if join.build_side_cache_info.is_some() {
        // There is a build side cache, use hash join.
        return Ok(PhysicalJoinType::Hash);
    }

    let left_rel_expr = RelExpr::with_s_expr(s_expr.child(0)?);
    let right_rel_expr = RelExpr::with_s_expr(s_expr.child(1)?);
    let right_stat_info = right_rel_expr.derive_cardinality()?;
    if matches!(right_stat_info.statistics.precise_cardinality, Some(1))
        || right_stat_info.cardinality == 1.0
    {
        // If the output rows of build side is equal to 1, we use CROSS JOIN + FILTER instead of RANGE JOIN.
        return Ok(PhysicalJoinType::Hash);
    }

    let left_prop = left_rel_expr.derive_relational_prop()?;
    let right_prop = right_rel_expr.derive_relational_prop()?;
    let (range_conditions, other_conditions) = join
        .non_equi_conditions
        .iter()
        .cloned()
        .partition::<Vec<_>, _>(|condition| {
            is_range_join_condition(condition, &left_prop, &right_prop).is_some()
        });

    if !range_conditions.is_empty() && matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
        return Ok(PhysicalJoinType::RangeJoin(
            range_conditions,
            other_conditions,
        ));
    }
    // Leverage hash join to execute nested loop join
    Ok(PhysicalJoinType::Hash)
}

impl PhysicalPlanBuilder {
    pub async fn build_join(
        &mut self,
        s_expr: &SExpr,
        join: &databend_common_sql::plans::Join,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut others_required = join
            .non_equi_conditions
            .iter()
            .fold(required.clone(), |acc, v| {
                acc.union(&v.used_columns()).cloned().collect()
            });
        if let Some(cache_info) = &join.build_side_cache_info {
            for column in &cache_info.columns {
                others_required.insert(*column);
            }
        }

        // Include columns referenced in left conditions and right conditions.
        let left_required: ColumnSet = join
            .equi_conditions
            .iter()
            .fold(required.clone(), |acc, v| {
                acc.union(&v.left.used_columns()).cloned().collect()
            })
            .union(&others_required)
            .cloned()
            .collect();
        let right_required: ColumnSet = join
            .equi_conditions
            .iter()
            .fold(required.clone(), |acc, v| {
                acc.union(&v.right.used_columns()).cloned().collect()
            })
            .union(&others_required)
            .cloned()
            .collect();
        let left_required = left_required.union(&others_required).cloned().collect();
        let right_required = right_required.union(&others_required).cloned().collect();

        // 2. Build physical plan.
        // Choose physical join type by join conditions
        if join.join_type.is_asof_join() {
            let left_prop = s_expr.left_child().derive_relational_prop()?;
            let right_prop = s_expr.right_child().derive_relational_prop()?;

            let (range_conditions, other_conditions) = join
                .non_equi_conditions
                .iter()
                .cloned()
                .chain(join.equi_conditions.iter().cloned().map(|condition| {
                    FunctionCall {
                        span: condition.left.span(),
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![condition.left, condition.right],
                    }
                    .into()
                }))
                .partition(|condition| {
                    is_range_join_condition(condition, &left_prop, &right_prop).is_some()
                });

            self.build_range_join(
                join.join_type,
                s_expr,
                left_required,
                right_required,
                range_conditions,
                other_conditions,
            )
            .await
        } else {
            match physical_join(join, s_expr)? {
                PhysicalJoinType::Hash => {
                    self.build_hash_join(
                        join,
                        s_expr,
                        required,
                        others_required,
                        left_required,
                        right_required,
                        stat_info,
                    )
                    .await
                }
                PhysicalJoinType::RangeJoin(range, other) => {
                    self.build_range_join(
                        join.join_type,
                        s_expr,
                        left_required,
                        right_required,
                        range,
                        other,
                    )
                    .await
                }
            }
        }
    }
}
