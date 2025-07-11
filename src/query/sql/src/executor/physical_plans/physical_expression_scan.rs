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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plan_builder::BuildPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::SExpr;
use crate::ColumnSet;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExpressionScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub values: Vec<Vec<RemoteExpr>>,
    pub input: Box<PhysicalPlan>,
    pub output_schema: DataSchemaRef,
}

impl ExpressionScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

#[async_trait::async_trait]
impl BuildPhysicalPlan for ExpressionScan {
    async fn build(
        builder: &mut PhysicalPlanBuilder,
        s_expr: &SExpr,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let plan = s_expr
            .plan()
            .as_any()
            .downcast_ref::<crate::plans::ExpressionScan>()
            .unwrap();
        builder
            .build_expression_scan(s_expr, plan, required, stat_info)
            .await
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_expression_scan(
        &mut self,
        s_expr: &SExpr,
        scan: &crate::plans::ExpressionScan,
        required: ColumnSet,
        _stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;

        let values = scan
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|scalar| {
                        let expr = scalar
                            .type_check(input_schema.as_ref())?
                            .project_column_ref(|index| {
                                input_schema.index_of(&index.to_string()).unwrap()
                            });
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        Ok(expr.as_remote_expr())
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::ExpressionScan(ExpressionScan {
            plan_id: 0,
            values,
            input: Box::new(input),
            output_schema: scan.schema.clone(),
        }))
    }
}
