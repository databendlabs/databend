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
use databend_common_pipeline_transforms::processors::TransformLocalShuffle;
use databend_common_sql::executor::physical_plans::ShuffleStrategy;

use crate::pipelines::PipelineBuilder;
use crate::sql::executor::physical_plans::LocalShuffle;

impl PipelineBuilder {
    pub(crate) fn build_local_shuffle_pipeline(
        &mut self,
        local_shuffle: &LocalShuffle,
    ) -> Result<()> {
        self.build_pipeline(&local_shuffle.input)?;

        let output_len = self.main_pipeline.output_len();

        // We don't add shuffle if output_len = 1
        if output_len == 1 {
            return Ok(());
        }

        self.main_pipeline.duplicate(true, output_len)?;
        assert_eq!(self.main_pipeline.output_len() % output_len, 0);

        let strategy = ShuffleStrategy::Transpose(output_len);
        self.main_pipeline
            .reorder_inputs(strategy.shuffle(self.main_pipeline.output_len())?);

        let widths = vec![output_len; self.main_pipeline.output_len() / output_len];
        self.main_pipeline.resize_partial_one_with_width(widths)?;

        let mut pipe_items = Vec::with_capacity(output_len);
        for idx in 0..output_len {
            let input = InputPort::create();
            let output = OutputPort::create();
            let processor = TransformLocalShuffle::create(
                input.clone(),
                output.clone(),
                idx,
                output_len,
                local_shuffle.shuffle_by.clone(),
            )?;
            pipe_items.push(PipeItem::create(processor, vec![input], vec![output]));
        }
        self.main_pipeline
            .add_pipe(Pipe::create(output_len, output_len, pipe_items));
        Ok(())
    }
}
