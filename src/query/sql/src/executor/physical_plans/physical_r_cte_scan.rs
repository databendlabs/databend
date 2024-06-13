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

use std::fmt::Display;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecursiveCteScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub output_schema: DataSchemaRef,
    pub cte_name: String,
    pub stat: PlanStatsInfo,
}

impl RecursiveCteScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_recursive_cte_scan(
        &mut self,
        recursive_cte_scan: &crate::plans::RecursiveCteScan,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::RecursiveCteScan(RecursiveCteScan {
            plan_id: 0,
            output_schema: DataSchemaRefExt::create(recursive_cte_scan.fields.clone()),
            cte_name: recursive_cte_scan.cte_name.clone(),
            stat: stat_info,
        }))
    }
}

impl Display for RecursiveCteScan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RecursiveCTEScan")
    }
}
