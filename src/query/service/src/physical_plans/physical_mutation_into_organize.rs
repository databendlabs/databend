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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_sql::binder::MutationStrategy;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationOrganize {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub strategy: MutationStrategy,
}

#[typetag::serde]
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

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        _: &mut FormatContext<'_>,
        mut children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        // ignore self
        assert_eq!(children.len(), 1);
        Ok(children.pop().unwrap())
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
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
    fn resize_row_id(&mut self, step: usize, builder: &mut PipelineBuilder) -> Result<()> {
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
