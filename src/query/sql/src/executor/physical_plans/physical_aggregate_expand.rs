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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::plans::GroupingSets;
use crate::IndexType;

/// Add dummy data before `GROUPING SETS`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregateExpand {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub group_bys: Vec<IndexType>,
    pub grouping_sets: GroupingSets,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregateExpand {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut output_fields = input_schema.fields().clone();
        // Add virtual columns to group by.
        output_fields.reserve(self.group_bys.len() + 1);

        for (group_by, (actual, ty)) in self
            .group_bys
            .iter()
            .zip(self.grouping_sets.dup_group_items.iter())
        {
            // All group by columns will wrap nullable.
            let i = input_schema.index_of(&group_by.to_string())?;
            let f = &mut output_fields[i];
            debug_assert!(f.data_type() == ty || f.data_type().wrap_nullable() == *ty);
            *f = DataField::new(f.name(), f.data_type().wrap_nullable());
            let new_field = DataField::new(&actual.to_string(), ty.clone());
            output_fields.push(new_field);
        }

        output_fields.push(DataField::new(
            &self.grouping_sets.grouping_id_index.to_string(),
            DataType::Number(NumberDataType::UInt32),
        ));
        Ok(DataSchemaRefExt::create(output_fields))
    }
}
