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

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterSink {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub hash_join_id: usize,
    pub input: Box<PhysicalPlan>,
}

impl PhysicalPlanBuilder {
    pub(crate) fn build_runtime_filter_sink(
        &mut self,
        hash_join_id: usize,
        input: Box<PhysicalPlan>,
    ) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::RuntimeFilterSink(RuntimeFilterSink {
            plan_id: 0,
            hash_join_id,
            input,
        }))
    }
}
