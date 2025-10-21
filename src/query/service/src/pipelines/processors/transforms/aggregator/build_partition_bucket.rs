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

use crate::pipelines::processors::transforms::aggregator::new_final_aggregate::FinalAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::new_final_aggregate::NewFinalAggregateTransform;
use crate::pipelines::processors::transforms::aggregator::new_final_aggregate::TransformPartitionBucketScatter;
use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::TransformPartitionBucket;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
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
    let operator = DataOperator::instance().spill_operator();

    if experiment_aggregate_final {
        let semaphore = Arc::new(Semaphore::new(params.max_spill_io_requests));

        // PartitionedPayload only accept power of two partitions
        let mut output_num = after_worker.next_power_of_two();
        const MAX_PARTITION_COUNT: usize = 128;
        if output_num > MAX_PARTITION_COUNT {
            output_num = MAX_PARTITION_COUNT;
        }

        let input_num = pipeline.output_len();
        let scatter =
            TransformPartitionBucketScatter::create(input_num, output_num, params.clone())?;
        let scatter_inputs = scatter.get_inputs();
        let scatter_outputs = scatter.get_outputs();

        pipeline.add_pipe(Pipe::create(
            scatter_inputs.len(),
            scatter_outputs.len(),
            vec![PipeItem::create(
                ProcessorPtr::create(Box::new(scatter)),
                scatter_inputs,
                scatter_outputs,
            )],
        ));

        let mut builder = TransformPipeBuilder::create();

        for id in 0..output_num {
            let spiller = FinalAggregateSpiller::try_create(ctx.clone(), operator.clone())?;
            let input_port = InputPort::create();
            let output_port = OutputPort::create();
            let processor = NewFinalAggregateTransform::try_create(
                input_port.clone(),
                output_port.clone(),
                id,
                params.clone(),
                output_num,
                ctx.clone(),
            )?;
            builder.add_transform(input_port, output_port, ProcessorPtr::create(processor));
        }

        pipeline.add_pipe(builder.finalize());
        pipeline.try_resize(after_worker)?;
    } else {
        let input_nums = pipeline.output_len();
        let transform = TransformPartitionBucket::create(input_nums, params.clone())?;

        let output = transform.get_output();
        let inputs_port = transform.get_inputs();

        pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
            ProcessorPtr::create(Box::new(transform)),
            inputs_port,
            vec![output],
        )]));

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
