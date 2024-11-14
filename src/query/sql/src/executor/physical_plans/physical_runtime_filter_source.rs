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
use databend_common_expression::DataSchemaRef;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterSource {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub hash_join_id: usize,
    pub output_schema: DataSchemaRef,
}

impl RuntimeFilterSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) fn build_runtime_filter_source(
        &mut self,
        hash_join_id: usize,
        output_schema: DataSchemaRef,
    ) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::RuntimeFilterSource(RuntimeFilterSource {
            plan_id: 0,
            hash_join_id,
            output_schema,
        }))
    }
}
