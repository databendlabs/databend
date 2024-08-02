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
use databend_common_pipeline_core::Pipe;
use databend_common_sql::executor::physical_plans::MutationSplit;
use databend_common_storages_fuse::operations::MutationSplitProcessor;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_mutation_split(&mut self, merge_into_split: &MutationSplit) -> Result<()> {
        self.build_pipeline(&merge_into_split.input)?;
        self.main_pipeline
            .try_resize(self.ctx.get_settings().get_max_threads()? as usize)?;

        // The MutationStrategy is FullOperation, use row_id_idx to split
        let mut items = Vec::with_capacity(self.main_pipeline.output_len());
        let output_len = self.main_pipeline.output_len();
        for _ in 0..output_len {
            let merge_into_split_processor =
                MutationSplitProcessor::create(merge_into_split.split_index as u32)?;
            items.push(merge_into_split_processor.into_pipe_item());
        }
        self.main_pipeline
            .add_pipe(Pipe::create(output_len, output_len * 2, items));

        Ok(())
    }
}
