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
use std::sync::atomic::AtomicU64;

use databend_common_exception::Result;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::TransformPipeBuilder;

use crate::pipelines::processors::transforms::aggregator::AggregateScatter;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::LocalScatter;
use crate::pipelines::processors::transforms::aggregator::LocalScatterTransform;
use crate::pipelines::processors::transforms::aggregator::TransformFinalAggregate;
use crate::servers::flight::v1::exchange::ExchangeShuffleTransform;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

/// Build the final aggregates. Unless `global_shuffled`, where the hash shuffle already delivered
/// to each output of `pipeline` the input of one final aggregate, the partial aggregate output is
/// scattered here to the final aggregates of this node.
pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    after_worker: usize,
    ctx: Arc<QueryContext>,
    global_shuffled: bool,
) -> Result<()> {
    let final_parallelism = if global_shuffled {
        pipeline.output_len()
    } else {
        let final_parallelism = ctx.get_settings().get_max_threads()? as usize;
        let scatter: Arc<Box<dyn LocalScatter>> = Arc::new(Box::new(AggregateScatter::local(
            final_parallelism,
            params.clone(),
        )));
        pipeline.add_transform(|input, output| {
            Ok(LocalScatterTransform::create(
                input,
                output,
                scatter.clone(),
            ))
        })?;

        let input_len = pipeline.output_len();
        let transform =
            ExchangeShuffleTransform::create(input_len, final_parallelism, final_parallelism);
        let inputs = transform.get_inputs();
        let outputs = transform.get_outputs();
        pipeline.add_pipe(Pipe::create(input_len, final_parallelism, vec![
            PipeItem::create(ProcessorPtr::create(Box::new(transform)), inputs, outputs),
        ]));
        final_parallelism
    };

    let mut builder = TransformPipeBuilder::create();
    let (tx, rx) = async_channel::unbounded();
    let next_task_id = Arc::new(AtomicU64::new(1));
    for id in 0..final_parallelism {
        let input_port = InputPort::create();
        let output_port = OutputPort::create();
        let processor = TransformFinalAggregate::try_create(
            input_port.clone(),
            output_port.clone(),
            params.clone(),
            id,
            ctx.clone(),
            tx.clone(),
            rx.clone(),
            next_task_id.clone(),
        )?;
        builder.add_transform(input_port, output_port, ProcessorPtr::create(processor));
    }

    pipeline.add_pipe(builder.finalize());

    pipeline.resize(after_worker, true)?;

    Ok(())
}
