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
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::TransformPipeBuilder;
use databend_common_storage::DataOperator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::transform_partition_bucket::TransformPartitionBucket;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewTransformFinalAggregate;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::TransformFinalAggregate;
use crate::sessions::QueryContext;

fn build_partition_bucket_experimental(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    after_worker: usize,
    ctx: Arc<QueryContext>,
) -> Result<()> {
    let mut builder = TransformPipeBuilder::create();
    for id in 0..pipeline.output_len() {
        let input_port = InputPort::create();
        let output_port = OutputPort::create();
        let processor = NewTransformFinalAggregate::try_create(
            input_port.clone(),
            output_port.clone(),
            params.clone(),
            id,
            ctx.clone(),
        )?;
        builder.add_transform(input_port, output_port, ProcessorPtr::create(processor));
    }

    pipeline.add_pipe(builder.finalize());

    pipeline.resize(after_worker, true)?;

    Ok(())
}

fn build_partition_bucket_legacy(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    max_restore_worker: u64,
    after_worker: usize,
) -> Result<()> {
    let operator = DataOperator::instance().spill_operator();

    let input_num = pipeline.output_len();
    let transform = TransformPartitionBucket::create(input_num, params.clone())?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        inputs_port,
        vec![output],
    )]));

    pipeline.try_resize(std::cmp::min(input_num, max_restore_worker as usize))?;
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

    Ok(())
}

/// Build partition bucket pipeline based on the experiment_aggregate flag.
/// Dispatches to either experimental or legacy implementation.
pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    max_restore_worker: u64,
    after_worker: usize,
    ctx: Arc<QueryContext>,
) -> Result<()> {
    if params.enable_experiment_aggregate {
        build_partition_bucket_experimental(pipeline, params, after_worker, ctx)
    } else {
        build_partition_bucket_legacy(pipeline, params, max_restore_worker, after_worker)
    }
}
