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

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecursiveCte {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
}

impl RecursiveCte {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        todo!()
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_recursive_cte(
        &mut self,
        recursive_cte: &crate::plans::RecursiveCte,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        todo!()
    }
}


impl Display for RecursiveCte {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }
}
