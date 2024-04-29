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
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::optimizer::ColumnSet;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Project {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub projections: Vec<usize>,
    pub ignore_result: bool,

    // Only used for display
    pub columns: ColumnSet,
    pub stat_info: Option<PlanStatsInfo>,
}

impl Project {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::new();
        for i in self.projections.iter() {
            fields.push(input_schema.field(*i).clone());
        }
        Ok(DataSchemaRefExt::create(fields))
    }

    pub fn from_columns_binding(
        plan_id: u32,
        input: Box<PhysicalPlan>,
        columns: Vec<ColumnBinding>,
        ignore_result: bool,
    ) -> Result<Project> {
        let input_schema = input.output_schema()?;
        let mut projections = Vec::with_capacity(columns.len());

        for column_binding in &columns {
            let index = column_binding.index;
            projections.push(input_schema.index_of(index.to_string().as_str())?);
        }

        Ok(Project {
            plan_id,
            input,
            projections,
            ignore_result,
            columns: Default::default(),
            stat_info: None,
        })
    }
}
