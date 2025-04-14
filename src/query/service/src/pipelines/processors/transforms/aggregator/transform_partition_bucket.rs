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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::DataOperator;

use super::TransformFinalAggregate;
use super::TransformPartitionRestore;
use crate::pipelines::processors::transforms::aggregator::transform_partition_align::TransformPartitionAlign;
use crate::pipelines::processors::transforms::aggregator::transform_partition_dispatch::TransformPartitionDispatch;
use crate::pipelines::processors::transforms::aggregator::transform_partition_exchange::ExchangePartition;
use crate::pipelines::processors::transforms::aggregator::transform_partition_resorting::ResortingPartition;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::sessions::QueryContext;

pub static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

pub fn build_final_aggregate(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let settings = ctx.get_settings();
    let pipe_size = settings.get_max_threads()? as usize;

    // 1. resorting partition
    pipeline.exchange(1, Arc::new(ResortingPartition::create()))?;

    // 2. align partitions
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(Box::new(
            TransformPartitionAlign::create(ctx.clone(), params.clone(), input, output)?,
        )))
    })?;

    // 3. dispatch partition
    let processor = TransformPartitionDispatch::create(pipe_size);
    let inputs_port = processor.get_inputs();
    let outputs_port = processor.get_outputs();
    pipeline.add_pipe(Pipe::create(inputs_port.len(), outputs_port.len(), vec![
        PipeItem::create(
            ProcessorPtr::create(Box::new(processor)),
            inputs_port,
            outputs_port,
        ),
    ]));

    // 4. restore partition
    let operator = DataOperator::instance().spill_operator();
    pipeline.add_transform(|input, output| {
        TransformPartitionRestore::create(input, output, operator.clone(), params.clone())
    })?;

    // 5. exchange local
    let pipe_size = pipeline.output_len();
    pipeline.new_exchange(
        pipe_size,
        ExchangePartition::create(pipe_size, params.clone()),
    )?;

    // 6. final aggregate
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(TransformFinalAggregate::try_create(
            input.clone(),
            output.clone(),
            params.clone(),
        )?))
    })
}
