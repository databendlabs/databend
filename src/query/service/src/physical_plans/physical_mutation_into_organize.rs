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
use databend_common_sql::binder::MutationStrategy;

use crate::physical_plans::format::MutationOrganizeFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationOrganize {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub strategy: MutationStrategy,
}

impl IPhysicalPlan for MutationOrganize {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MutationOrganizeFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(MutationOrganize {
            meta: self.meta.clone(),
            input,
            strategy: self.strategy.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        // The complete pipeline:
        // -----------------------------------------------------------------------------------------
        // row_id port0_1               row_id port0_1              row_id port0_1
        // matched data port0_2              .....                  row_id port1_1         row_id port
        // unmatched port0_3            data port0_2                    ......
        // row_id port1_1       ====>   row_id port1_1    ====>     data port0_2    ====>  data port0
        // matched data port1_2              .....                  data port1_2           data port1
        // unmatched port1_3            data port1_2                    ......
        // ......                            .....
        // -----------------------------------------------------------------------------------------
        // 1. matched only or complete pipeline are same with above
        // 2. for unmatched only, there are no row_id port

        let mut ranges = Vec::with_capacity(builder.main_pipeline.output_len());
        let mut rules = Vec::with_capacity(builder.main_pipeline.output_len());
        match self.strategy {
            MutationStrategy::MixedMatched => {
                assert_eq!(builder.main_pipeline.output_len() % 3, 0);
                // merge matched update ports and not matched ports ===> data ports
                for idx in (0..builder.main_pipeline.output_len()).step_by(3) {
                    ranges.push(vec![idx]);
                    ranges.push(vec![idx + 1, idx + 2]);
                }
                builder.main_pipeline.resize_partial_one(ranges.clone())?;
                assert_eq!(builder.main_pipeline.output_len() % 2, 0);
                let row_id_len = builder.main_pipeline.output_len() / 2;
                for idx in 0..row_id_len {
                    rules.push(idx);
                    rules.push(idx + row_id_len);
                }
                builder.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2, builder)?;
            }
            MutationStrategy::MatchedOnly => {
                assert_eq!(builder.main_pipeline.output_len() % 2, 0);
                let row_id_len = builder.main_pipeline.output_len() / 2;
                for idx in 0..row_id_len {
                    rules.push(idx);
                    rules.push(idx + row_id_len);
                }
                builder.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2, builder)?;
            }
            MutationStrategy::NotMatchedOnly => {}
            MutationStrategy::Direct => unreachable!(),
        }
        Ok(())
    }
}

impl MutationOrganize {
    fn resize_row_id(&self, step: usize, builder: &mut PipelineBuilder) -> Result<()> {
        // resize row_id
        let row_id_len = builder.main_pipeline.output_len() / step;
        let mut ranges = Vec::with_capacity(builder.main_pipeline.output_len());
        let mut vec = Vec::with_capacity(row_id_len);
        for idx in 0..row_id_len {
            vec.push(idx);
        }
        ranges.push(vec.clone());

        // data ports
        for idx in 0..row_id_len {
            ranges.push(vec![idx + row_id_len]);
        }

        builder.main_pipeline.resize_partial_one(ranges.clone())
    }
}

crate::register_physical_plan!(MutationOrganize => crate::physical_plans::physical_mutation_into_organize::MutationOrganize);
