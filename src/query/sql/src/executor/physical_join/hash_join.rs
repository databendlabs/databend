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
        let probe_side = Box::new(self.build(s_expr.child(0)?).await?);
        let build_side = Box::new(self.build(s_expr.child(1)?).await?);

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
        let merged_schema = DataSchemaRefExt::create(
            probe_schema
                .fields()
                .iter()
                .chain(build_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        assert_eq!(join.left_conditions.len(), join.right_conditions.len());

        let probe_keys = join
            .left_conditions
            .iter()
            .map(|scalar| {
                let expr = scalar
                    .resolve_and_check(probe_schema.as_ref())?
                    .project_column_ref(|index| probe_schema.index_of(&index.to_string()).unwrap());
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok(expr.as_remote_expr())
            })
            .collect::<Result<_>>()?;
        let build_keys = join
            .right_conditions
            .iter()
            .map(|scalar| {
                let expr = scalar
                    .resolve_and_check(build_schema.as_ref())?
                    .project_column_ref(|index| build_schema.index_of(&index.to_string()).unwrap());
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok(expr.as_remote_expr())
            })
            .collect::<Result<_>>()?;
        let non_equi_conditions = join
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
            .collect::<Result<_>>()?;

        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: self.next_plan_id(),
            build: build_side,
            probe: probe_side,
            join_type: join.join_type.clone(),
            build_keys,
            probe_keys,
            non_equi_conditions,
            marker_index: join.marker_index,
            from_correlated_subquery: join.from_correlated_subquery,

            contain_runtime_filter: join.contain_runtime_filter,
            stat_info: Some(stat_info),
        }))
    }
}
