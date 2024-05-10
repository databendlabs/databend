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
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsyncFunction {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub func_name: String,
    pub display_name: String,
    pub arguments: Vec<String>,
    pub return_type: DataType,
    pub schema: DataSchemaRef,
    pub input: Box<PhysicalPlan>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AsyncFunction {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_async_func(
        &mut self,
        s_expr: &SExpr,
        async_func: &crate::plans::AsyncFunction,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let child = s_expr.child(0)?;
        let input = self.build(child, required.clone()).await?;

        let input_schema = input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        fields.push(DataField::new(
            &async_func.index.to_string(),
            async_func.return_type.clone(),
        ));
        let schema = DataSchemaRefExt::create(fields);

        Ok(PhysicalPlan::AsyncFunction(AsyncFunction {
            plan_id: 0,
            func_name: async_func.func_name.clone(),
            display_name: async_func.display_name.clone(),
            arguments: async_func.arguments.clone(),
            return_type: async_func.return_type.clone(),
            schema,
            input: Box::new(input),
            stat_info: Some(stat_info),
        }))
    }
}
