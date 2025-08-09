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

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

/// This is a leaf operator that consumes the result of a materialized CTE.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializeCTERef {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub cte_name: String,
    pub cte_schema: DataSchemaRef,
}

impl MaterializeCTERef {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.cte_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cte_consumer(
        &mut self,
        cte_consumer: &crate::plans::MaterializedCTERef,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let mut fields = Vec::new();
        let metadata = self.metadata.read();
        for index in &cte_consumer.output_columns {
            let column = metadata.column(*index);
            fields.push(DataField::new(&index.to_string(), column.data_type()));
        }
        let cte_schema = DataSchemaRefExt::create(fields);
        Ok(PhysicalPlan::MaterializeCTERef(Box::new(
            MaterializeCTERef {
                plan_id: 0,
                stat_info: Some(stat_info),
                cte_name: cte_consumer.cte_name.clone(),
                cte_schema,
            },
        )))
    }
}
