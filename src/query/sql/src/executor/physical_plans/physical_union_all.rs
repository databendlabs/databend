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

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub left_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub right_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub schema: DataSchemaRef,
    pub cte_name: Option<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_union_all(
        &mut self,
        s_expr: &SExpr,
        union_all: &crate::plans::UnionAll,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let left_required = union_all
            .left_outputs
            .iter()
            .fold(required.clone(), |mut acc, v| {
                acc.insert(v.0);
                acc
            });
        let right_required = union_all.right_outputs.iter().fold(required, |mut acc, v| {
            acc.insert(v.0);
            acc
        });

        // 2. Build physical plan.
        let left_plan = self.build(s_expr.child(0)?, left_required).await?;
        let right_plan = self.build(s_expr.child(1)?, right_required).await?;

        let left_schema = left_plan.output_schema()?;
        let right_schema = right_plan.output_schema()?;

        let fields = union_all
            .left_outputs
            .iter()
            .map(|(index, expr)| {
                if let Some(expr) = expr {
                    Ok(DataField::new(&index.to_string(), expr.data_type()?))
                } else {
                    Ok(left_schema.field_with_name(&index.to_string())?.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let left_outputs = union_all
            .left_outputs
            .iter()
            .map(|(index, scalar_expr)| {
                if let Some(scalar_expr) = scalar_expr {
                    let expr = scalar_expr
                        .type_check(left_schema.as_ref())?
                        .project_column_ref(|idx| left_schema.index_of(&idx.to_string()).unwrap());
                    Ok((*index, Some(expr.as_remote_expr())))
                } else {
                    Ok((*index, None))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let right_outputs = union_all
            .right_outputs
            .iter()
            .map(|(index, scalar_expr)| {
                if let Some(scalar_expr) = scalar_expr {
                    let expr = scalar_expr
                        .type_check(right_schema.as_ref())?
                        .project_column_ref(|idx| right_schema.index_of(&idx.to_string()).unwrap());
                    Ok((*index, Some(expr.as_remote_expr())))
                } else {
                    Ok((*index, None))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: 0,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            left_outputs,
            right_outputs,
            schema: DataSchemaRefExt::create(fields),

            cte_name: union_all.cte_name.clone(),
            stat_info: Some(stat_info),
        }))
    }
}
