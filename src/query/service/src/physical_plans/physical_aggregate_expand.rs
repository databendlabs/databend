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

use std::any::Any;

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::IndexType;
use databend_common_sql::plans::GroupingSets;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::AggregateExpandFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::aggregator::TransformExpandGroupingSets;

/// Add dummy data before `GROUPING SETS`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregateExpand {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub group_bys: Vec<IndexType>,
    pub grouping_sets: GroupingSets,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for AggregateExpand {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[stacksafe::stacksafe]
    fn output_schema(&self) -> Result<DataSchemaRef> {
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

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(AggregateExpandFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .grouping_sets
            .sets
            .iter()
            .map(|set| {
                set.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .map(|s| format!("({})", s))
            .collect::<Vec<_>>()
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);

        PhysicalPlan::new(AggregateExpand {
            meta: self.meta.clone(),
            input: children.remove(0),
            group_bys: self.group_bys.clone(),
            grouping_sets: self.grouping_sets.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let group_bys = self
            .group_bys
            .iter()
            .take(self.group_bys.len() - 1) // The last group-by will be virtual column `_grouping_id`
            .map(|i| input_schema.index_of(&i.to_string()))
            .collect::<Result<Vec<_>>>()?;
        let grouping_sets = self
            .grouping_sets
            .sets
            .iter()
            .map(|sets| {
                sets.iter()
                    .map(|i| {
                        let i = input_schema.index_of(&i.to_string())?;
                        let offset = group_bys.iter().position(|j| *j == i).unwrap();
                        Ok(offset)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut grouping_ids = Vec::with_capacity(grouping_sets.len());
        let mask = (1 << group_bys.len()) - 1;
        for set in grouping_sets {
            let mut id = 0;
            for i in set {
                id |= 1 << i;
            }
            // For element in `group_bys`,
            // if it is in current grouping set: set 0, else: set 1. (1 represents it will be NULL in grouping)
            // Example: GROUP BY GROUPING SETS ((a, b), (a), (b), ())
            // group_bys: [a, b]
            // grouping_sets: [[0, 1], [0], [1], []]
            // grouping_ids: 00, 01, 10, 11
            grouping_ids.push(!id & mask);
        }

        builder.main_pipeline.add_accumulating_transformer(|| {
            TransformExpandGroupingSets::new(group_bys.clone(), grouping_ids.clone())
        });
        Ok(())
    }
}
