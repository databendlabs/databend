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
use databend_common_pipeline::core::Pipe;
use databend_common_sql::IndexType;
use databend_common_storages_fuse::operations::MutationSplitProcessor;

use crate::physical_plans::format::MutationSplitFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationSplit {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub split_index: IndexType,
}

impl IPhysicalPlan for MutationSplit {
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
        Ok(MutationSplitFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(MutationSplit {
            meta: self.meta.clone(),
            input,
            split_index: self.split_index,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        builder
            .main_pipeline
            .try_resize(builder.settings.get_max_threads()? as usize)?;

        // The MutationStrategy is FullOperation, use row_id_idx to split
        let mut items = Vec::with_capacity(builder.main_pipeline.output_len());
        let output_len = builder.main_pipeline.output_len();
        for _ in 0..output_len {
            let merge_into_split_processor =
                MutationSplitProcessor::create(self.split_index as u32)?;
            items.push(merge_into_split_processor.into_pipe_item());
        }

        builder
            .main_pipeline
            .add_pipe(Pipe::create(output_len, output_len * 2, items));
        Ok(())
    }
}
