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
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::binder::is_range_join_condition;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::physical_plan::PhysicalPlan;

fn is_single_row(stat_info: &databend_common_sql::optimizer::ir::StatInfo) -> bool {
    matches!(stat_info.statistics.precise_cardinality, Some(1)) || stat_info.cardinality == 1.0
}

fn is_precise_single_row(stat_info: &databend_common_sql::optimizer::ir::StatInfo) -> bool {
    matches!(stat_info.statistics.precise_cardinality, Some(1))
}

fn single_join_scalar_side_is_precise_single_row(join: &Join, s_expr: &SExpr) -> Result<bool> {
    match join.single_to_inner {
        Some(JoinType::LeftSingle) => {
            let right_rel_expr = RelExpr::with_s_expr(s_expr.right_child());
            let right_stat_info = right_rel_expr.derive_cardinality()?;
            Ok(is_precise_single_row(&right_stat_info))
        }
        Some(JoinType::RightSingle) => {
            let left_rel_expr = RelExpr::with_s_expr(s_expr.left_child());
            let left_stat_info = left_rel_expr.derive_cardinality()?;
            Ok(is_precise_single_row(&left_stat_info))
        }
        _ => Ok(false),
    }
}

enum PhysicalJoinType {
    Hash,
    // The first arg is range conditions, the second arg is other conditions
    RangeJoin {
        range: Vec<ScalarExpr>,
        other: Vec<ScalarExpr>,
    },
}

fn asof_hash_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Asof => JoinType::Inner,
        // ASOF rewrite swaps children to:
        //   probe = window(original right)
        //   build = original left
        //
        // HashJoin preserves the probe side for LEFT joins and the build side
        // for RIGHT joins, so outer ASOF joins need the opposite mapping here
        // to preserve the original SQL null-preserving side.
        JoinType::LeftAsof => JoinType::Right,
        JoinType::RightAsof => JoinType::Left,
        JoinType::FullAsof => JoinType::Full,
        _ => join_type,
    }
}

// Choose physical join type by join conditions
fn physical_join(join: &Join, s_expr: &SExpr) -> Result<PhysicalJoinType> {
    if join.equi_conditions.is_empty() && join.join_type.is_any_join() {
        return Err(ErrorCode::SemanticError(
            "ANY JOIN only supports equality-based hash joins",
        ));
    }

    let left_rel_expr = RelExpr::with_s_expr(s_expr.left_child());
    let right_rel_expr = RelExpr::with_s_expr(s_expr.right_child());
    let left_stat_info = left_rel_expr.derive_cardinality()?;
    let right_stat_info = right_rel_expr.derive_cardinality()?;

    if !join.equi_conditions.is_empty() {
        // Contain equi condition, use hash join
        return Ok(PhysicalJoinType::Hash);
    }

    if join.build_side_cache_info.is_some() {
        // There is a build side cache, use hash join.
        return Ok(PhysicalJoinType::Hash);
    }

    if is_single_row(&left_stat_info) || is_single_row(&right_stat_info) {
        // Prefer CROSS JOIN + FILTER when statistics prove or estimate one side at one row.
        // HashJoin remains correct if the estimate is wrong and avoids the result-block
        // overhead that RangeJoin can incur for this shape after join commutation.
        return Ok(PhysicalJoinType::Hash);
    }

    if matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
        let left_prop = left_rel_expr.derive_relational_prop()?;
        let right_prop = right_rel_expr.derive_relational_prop()?;
        let (range, other) = join
            .non_equi_conditions
            .iter()
            .cloned()
            .partition::<Vec<_>, _>(|condition| {
                is_range_join_condition(condition, &left_prop, &right_prop).is_some()
            });

        if !range.is_empty() {
            return Ok(PhysicalJoinType::RangeJoin { range, other });
        }
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
        let left_required: ColumnSet = left_required.union(&others_required).cloned().collect();
        let right_required: ColumnSet = right_required.union(&others_required).cloned().collect();

        // 2. Try Build physical spatial join plan.
        if let Some(candidate) = join.spatial_join.clone() {
            if let Some(plan) = self
                .try_build_spatial_join(
                    *candidate,
                    s_expr,
                    required.clone(),
                    left_required.clone(),
                    right_required.clone(),
                )
                .await?
            {
                return Ok(plan);
            }
        }

        // 3. Build physical plan.
        // Choose physical join type by join conditions
        if join.join_type.is_asof_join() {
            if !join.equi_conditions.is_empty() {
                // Binder rewrites ASOF into:
                //   1. the original inequality; and
                //   2. a window-derived boundary that guarantees at most one build row
                //      matches inside each equi-key partition.
                //
                // When equi conditions are present, we can therefore reuse the existing
                // hash join path to first shrink candidates by the equi keys, then apply
                // the ASOF residual predicates as post-join filters.
                let mut hash_join = join.clone();
                hash_join.join_type = asof_hash_join_type(hash_join.join_type);

                return self
                    .build_hash_join(
                        &hash_join,
                        s_expr,
                        required,
                        others_required,
                        left_required,
                        right_required,
                        stat_info,
                    )
                    .await;
            }

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
                    // When a LeftSingle/RightSingle join (scalar subquery) has no
                    // equi-conditions, the hash join executes as a cross join + filter.
                    // The single-join runtime check can fire during the cross-product
                    // matching phase before the filter is applied. It is only safe to
                    // clear the marker when the scalar side itself is proven to produce
                    // exactly one row. A precise one-row cardinality on the other input
                    // does not prove that.
                    let join = if join.equi_conditions.is_empty()
                        && join.single_to_inner.is_some()
                        && single_join_scalar_side_is_precise_single_row(join, s_expr)?
                    {
                        let mut j = join.clone();
                        j.single_to_inner = None;
                        j
                    } else {
                        join.clone()
                    };
                    self.build_hash_join(
                        &join,
                        s_expr,
                        required,
                        others_required,
                        left_required,
                        right_required,
                        stat_info,
                    )
                    .await
                }
                PhysicalJoinType::RangeJoin { range, other } => {
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
