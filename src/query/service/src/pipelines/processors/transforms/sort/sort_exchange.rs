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

use std::iter;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::PartitionProcessor;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;

use super::SortScatteredMeta;

struct SortRangeExchange;

impl Exchange for SortRangeExchange {
    const NAME: &'static str = "SortRange";
    fn partition(&self, mut data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let Some(meta) = data.take_meta() else {
            unreachable!();
        };

        let Some(SortScatteredMeta(scattered)) = SortScatteredMeta::downcast_from(meta) else {
            unreachable!();
        };

        assert!(scattered.len() <= n);

        let blocks = scattered
            .into_iter()
            .map(|meta| DataBlock::empty_with_meta(Box::new(meta)))
            .collect();

        Ok(blocks)
    }
}

fn create_exchange_pipe(num_input: usize, num_output: usize) -> Pipe {
    let items = iter::repeat_with(|| {
        let input = InputPort::create();
        let outputs = iter::repeat_with(OutputPort::create)
            .take(num_output)
            .collect::<Vec<_>>();

        PipeItem::create(
            PartitionProcessor::create(input.clone(), outputs.clone(), Arc::new(SortRangeExchange)),
            vec![input],
            outputs,
        )
    })
    .take(num_input)
    .collect::<Vec<_>>();

    Pipe::create(num_input, num_input * num_output, items)
}

pub fn add_range_shuffle_exchange(pipeline: &mut Pipeline, num_output: usize) -> Result<()> {
    let num_input = pipeline.output_len();

    pipeline.add_pipe(create_exchange_pipe(num_input, num_output));

    let n = num_output;
    let reorder_edges = (0..num_input * n)
        .map(|i| (i % n) * num_input + (i / n))
        .collect::<Vec<_>>();

    pipeline.reorder_inputs(reorder_edges);

    Ok(())
}
