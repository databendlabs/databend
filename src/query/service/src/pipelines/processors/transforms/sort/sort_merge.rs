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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::MergePartitionProcessor;
use databend_common_pipeline_core::processors::MultiwayStrategy;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;

pub struct TransformSortRangeMerge {}

impl Exchange for TransformSortRangeMerge {
    const NAME: &'static str = "SortRangeMerge";
    const STRATEGY: MultiwayStrategy = MultiwayStrategy::Custom;

    fn partition(&self, block: DataBlock, _: usize) -> Result<Vec<DataBlock>> {
        Ok(vec![block])
    }

    fn multiway_pick(&self, partitions: &[Option<DataBlock>]) -> Result<usize> {
        Ok(partitions.iter().position(Option::is_some).unwrap())
    }
}

pub fn add_range_shuffle_merge(pipeline: &mut Pipeline) -> Result<()> {
    let inputs = pipeline.output_len();
    let inputs_port = (0..inputs).map(|_| InputPort::create()).collect::<Vec<_>>();
    let output = OutputPort::create();

    let processor = MergePartitionProcessor::create(
        inputs_port.clone(),
        output.clone(),
        Arc::new(TransformSortRangeMerge {}),
    );

    let pipe = Pipe::create(inputs, 1, vec![PipeItem::create(
        processor,
        inputs_port,
        vec![output],
    )]);

    pipeline.add_pipe(pipe);
    Ok(())
}
