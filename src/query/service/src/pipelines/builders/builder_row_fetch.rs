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
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_sql::executor::physical_plans::RowFetch;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storages_fuse::operations::row_fetch_processor;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_row_fetch(&mut self, row_fetch: &RowFetch) -> Result<()> {
        debug_assert!(matches!(
            &*row_fetch.input,
            PhysicalPlan::Limit(_)
                | PhysicalPlan::HashJoin(_)
                | PhysicalPlan::MergeIntoSplit(_)
                | PhysicalPlan::Exchange(_)
        ));
        self.build_pipeline(&row_fetch.input)?;
        let processor = row_fetch_processor(
            self.ctx.clone(),
            row_fetch.row_id_col_offset,
            &row_fetch.source,
            row_fetch.cols_to_fetch.clone(),
            row_fetch.need_wrap_nullable,
        )?;
        if !matches!(&*row_fetch.input, PhysicalPlan::MergeIntoSplit(_)) {
            self.main_pipeline.add_transform(processor)?;
        } else {
            let output_len = self.main_pipeline.output_len();
            let mut pipe_items = Vec::with_capacity(output_len);
            for i in 0..output_len {
                if i % 2 == 0 {
                    let input = InputPort::create();
                    let output = OutputPort::create();
                    let processor_ptr = processor(input.clone(), output.clone())?;
                    pipe_items.push(PipeItem::create(processor_ptr, vec![input], vec![output]));
                } else {
                    pipe_items.push(create_dummy_item());
                }
            }
            self.main_pipeline
                .add_pipe(Pipe::create(output_len, output_len, pipe_items));
        }
        Ok(())
    }
}
