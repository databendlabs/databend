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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_cast;
use common_expression::type_check::common_super_type;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::Exchange;
use crate::executor::HashJoin;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::TypeCheck;

impl PhysicalPlanBuilder {
    pub async fn build_hash_join(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let mut probe_side = Box::new(self.build(s_expr.child(0)?).await?);
        let mut build_side = Box::new(self.build(s_expr.child(1)?).await?);

        // Unify the data types of the left and right exchange keys.
        if let (
            PhysicalPlan::Exchange(Exchange {
                keys: probe_keys, ..
            }),
            PhysicalPlan::Exchange(Exchange {
                keys: build_keys, ..
            }),
        ) = (probe_side.as_mut(), build_side.as_mut())
        {
            for (probe_key, build_key) in probe_keys.iter_mut().zip(build_keys.iter_mut()) {
                let probe_expr = probe_key.as_expr(&BUILTIN_FUNCTIONS);
                let build_expr = build_key.as_expr(&BUILTIN_FUNCTIONS);
                let common_ty = common_super_type(
                    probe_expr.data_type().clone(),
                    build_expr.data_type().clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
                .ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Cannot find common type for probe key {:?} and build key {:?}",
                        &probe_expr, &build_expr
                    ))
                })?;
                *probe_key = check_cast(
                    probe_expr.span(),
                    false,
                    probe_expr,
                    &common_ty,
                    &BUILTIN_FUNCTIONS,
                )?
                .as_remote_expr();
                *build_key = check_cast(
                    build_expr.span(),
                    false,
                    build_expr,
                    &common_ty,
                    &BUILTIN_FUNCTIONS,
                )?
                .as_remote_expr();
            }
        }

        let build_schema = match join.join_type {
            JoinType::Left | JoinType::Full => {
                let build_schema = build_side.output_schema()?;
                // Wrap nullable type for columns in build side.
                let build_schema = DataSchemaRefExt::create(
                    build_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                build_schema
            }

            _ => build_side.output_schema()?,
        };

        let probe_schema = match join.join_type {
            JoinType::Right | JoinType::Full => {
                let probe_schema = probe_side.output_schema()?;
                // Wrap nullable type for columns in probe side.
                let probe_schema = DataSchemaRefExt::create(
                    probe_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                probe_schema
            }

            _ => probe_side.output_schema()?,
        };

        assert_eq!(join.left_conditions.len(), join.right_conditions.len());
        let mut left_join_conditions = Vec::new();
        let mut right_join_conditions = Vec::new();
        for (left_condition, right_condition) in join
            .left_conditions
            .iter()
            .zip(join.right_conditions.iter())
        {
            let left_expr = left_condition
                .resolve_and_check(probe_schema.as_ref())?
                .project_column_ref(|index| probe_schema.index_of(&index.to_string()).unwrap());
            let right_expr = right_condition
                .resolve_and_check(build_schema.as_ref())?
                .project_column_ref(|index| build_schema.index_of(&index.to_string()).unwrap());

            // Unify the data types of the left and right expressions.
            let left_type = left_expr.data_type();
            let right_type = right_expr.data_type();
            let common_ty = common_super_type(
                left_type.clone(),
                right_type.clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            )
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!(
                    "Cannot find common type for {:?} and {:?}",
                    left_type, right_type
                ))
            })?;
            let left_expr = check_cast(
                left_expr.span(),
                false,
                left_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;
            let right_expr = check_cast(
                right_expr.span(),
                false,
                right_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;

            let (left_expr, _) =
                ConstantFolder::fold(&left_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let (right_expr, _) =
                ConstantFolder::fold(&right_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

            left_join_conditions.push(left_expr.as_remote_expr());
            right_join_conditions.push(right_expr.as_remote_expr());
        }

        let merged_schema = DataSchemaRefExt::create(
            probe_schema
                .fields()
                .iter()
                .chain(build_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: self.next_plan_id(),
            build: build_side,
            probe: probe_side,
            join_type: join.join_type.clone(),
            build_keys: right_join_conditions,
            probe_keys: left_join_conditions,
            non_equi_conditions: join
                .non_equi_conditions
                .iter()
                .map(|scalar| {
                    let expr = scalar
                        .resolve_and_check(merged_schema.as_ref())?
                        .project_column_ref(|index| {
                            merged_schema.index_of(&index.to_string()).unwrap()
                        });
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    Ok(expr.as_remote_expr())
                })
                .collect::<Result<_>>()?,
            marker_index: join.marker_index,
            from_correlated_subquery: join.from_correlated_subquery,

            contain_runtime_filter: join.contain_runtime_filter,
            stat_info: Some(stat_info),
        }))
    }
}
