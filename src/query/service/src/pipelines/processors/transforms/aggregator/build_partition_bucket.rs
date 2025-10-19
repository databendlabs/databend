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
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::TransformPipeBuilder;
use databend_common_storage::DataOperator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::transform_meta_dispatcher::TransformMetaDispatcher;
use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::TransformPartitionBucket;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewTransformAggregateFinal;
use crate::pipelines::processors::transforms::aggregator::SharedRestoreState;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::TransformFinalAggregate;
use crate::sessions::QueryContext;

pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    max_restore_worker: u64,
    after_worker: usize,
    experiment_aggregate_final: bool,
    ctx: Arc<QueryContext>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = TransformPartitionBucket::create(input_nums, params.clone())?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        inputs_port,
        vec![output],
    )]));

    let operator = DataOperator::instance().spill_operator();

    if experiment_aggregate_final {
        let semaphore = Arc::new(Semaphore::new(params.max_spill_io_requests));

        // PartitionedPayload only accept power of two partitions
        let normalized = std::cmp::max(1, after_worker);
        let mut partition_count = normalized.next_power_of_two();
        const MAX_PARTITION_COUNT: usize = 128;
        if partition_count > MAX_PARTITION_COUNT {
            partition_count = MAX_PARTITION_COUNT;
        }

        let shared_state = SharedRestoreState::new(partition_count);

        let dispatcher = TransformMetaDispatcher::create(partition_count, shared_state.clone())?;
        let dispatcher_input = dispatcher.input_port();
        let dispatcher_outputs: Vec<Arc<OutputPort>> = dispatcher.output_ports();

        pipeline.add_pipe(Pipe::create(1, partition_count, vec![PipeItem::create(
            ProcessorPtr::create(Box::new(dispatcher)),
            vec![dispatcher_input],
            dispatcher_outputs,
        )]));

        let mut builder = TransformPipeBuilder::create();
        for id in 0..partition_count {
            let input_port = InputPort::create();
            let output_port = OutputPort::create();
            let processor = NewTransformAggregateFinal::create(
                ctx.clone(),
                input_port.clone(),
                output_port.clone(),
                operator.clone(),
                semaphore.clone(),
                params.clone(),
                shared_state.clone(),
                partition_count,
                id,
            )?;
            builder.add_transform(input_port, output_port, processor);
        }

        pipeline.add_pipe(builder.finalize());
        pipeline.try_resize(after_worker)?;
    } else {
        pipeline.try_resize(std::cmp::min(input_nums, max_restore_worker as usize))?;
        let semaphore = Arc::new(Semaphore::new(params.max_spill_io_requests));
        pipeline.add_transform(|input, output| {
            let operator = operator.clone();
            TransformAggregateSpillReader::create(input, output, operator, semaphore.clone())
        })?;
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformFinalAggregate::try_create(
                input,
                output,
                params.clone(),
            )?))
        })?;
        pipeline.try_resize(after_worker)?;
    }

    Ok(())
}
