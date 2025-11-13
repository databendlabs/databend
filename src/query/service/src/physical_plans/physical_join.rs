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
use databend_common_expression::types::DataType;
use databend_common_settings::Settings;
use databend_common_sql::binder::is_range_join_condition;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;

enum PhysicalJoinType {
    Hash,
    // The first arg is range conditions, the second arg is other conditions
    RangeJoin {
        range: Vec<ScalarExpr>,
        other: Vec<ScalarExpr>,
    },
    LoopJoin {
        conditions: Vec<ScalarExpr>,
    },
}

// Choose physical join type by join conditions
fn physical_join(join: &Join, s_expr: &SExpr, settings: &Settings) -> Result<PhysicalJoinType> {
    if join.equi_conditions.is_empty() && join.join_type.is_any_join() {
        return Err(ErrorCode::SemanticError(
            "ANY JOIN only supports equality-based hash joins",
        ));
    }

    let left_rel_expr = RelExpr::with_s_expr(s_expr.left_child());
    let right_rel_expr = RelExpr::with_s_expr(s_expr.right_child());
    let right_stat_info = right_rel_expr.derive_cardinality()?;
    let nested_loop_join_threshold = settings.get_nested_loop_join_threshold()?;
    if matches!(join.join_type, JoinType::Inner | JoinType::Cross)
        && (right_stat_info
            .statistics
            .precise_cardinality
            .map(|n| n < nested_loop_join_threshold)
            .unwrap_or(false)
            || right_stat_info.cardinality < nested_loop_join_threshold as _)
    {
        let conditions = join
            .non_equi_conditions
            .iter()
            .map(|c| Ok(c.clone()))
            .chain(join.equi_conditions.iter().map(condition_to_expr))
            .collect::<Result<_>>()?;
        return Ok(PhysicalJoinType::LoopJoin { conditions });
    };

    if !join.equi_conditions.is_empty() {
        // Contain equi condition, use hash join
        return Ok(PhysicalJoinType::Hash);
    }

    if join.build_side_cache_info.is_some() {
        // There is a build side cache, use hash join.
        return Ok(PhysicalJoinType::Hash);
    }

    if matches!(right_stat_info.statistics.precise_cardinality, Some(1))
        || right_stat_info.cardinality == 1.0
    {
        // If the output rows of build side is equal to 1, we use CROSS JOIN + FILTER instead of RANGE JOIN.
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
            let settings = self.ctx.get_settings();
            match physical_join(join, s_expr, &settings)? {
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
                PhysicalJoinType::LoopJoin { conditions } => {
                    self.build_loop_join(
                        join.join_type,
                        s_expr,
                        left_required,
                        right_required,
                        conditions,
                    )
                    .await
                }
            }
        }
    }
}

fn condition_to_expr(condition: &JoinEquiCondition) -> Result<ScalarExpr> {
    let left_type = condition.left.data_type()?;
    let right_type = condition.right.data_type()?;

    let arguments = match (&left_type, &right_type) {
        (DataType::Nullable(left), right) if **left == *right => vec![
            condition.left.clone(),
            condition.right.clone().unify_to_data_type(&left_type),
        ],
        (left, DataType::Nullable(right)) if *left == **right => vec![
            condition.left.clone().unify_to_data_type(&right_type),
            condition.right.clone(),
        ],
        _ => vec![condition.left.clone(), condition.right.clone()],
    };

    Ok(FunctionCall {
        span: condition.left.span(),
        func_name: "eq".to_string(),
        params: vec![],
        arguments,
    }
    .into())
}
