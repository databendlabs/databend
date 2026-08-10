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

use crate::clusters::ClusterHelper;
use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::AggregateBucketScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateRowScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::LocalScatterTransform;
use crate::pipelines::processors::transforms::aggregator::RowShuffleReaderTransform;
use crate::pipelines::processors::transforms::aggregator::TransformFinalAggregate;
use crate::servers::flight::v1::exchange::ExchangeShuffleTransform;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    after_worker: usize,
    ctx: Arc<QueryContext>,
    shuffle_mode: AggregateShuffleMode,
) -> Result<()> {
    let mut final_parallelism = ctx.get_settings().get_max_threads()? as usize;
    let base_consumed_bits = shuffle_mode.determine_radix_bits();
    match shuffle_mode {
        AggregateShuffleMode::Row => {
            let schema = params.spill_schema();
            pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(RowShuffleReaderTransform::create(
                    input,
                    output,
                    AggregateSpillReader::try_create(ctx.clone(), schema.clone())?,
                )))
            })?;

            pipeline.add_transform(|input, output| {
                Ok(LocalScatterTransform::create(
                    input,
                    output,
                    Arc::new(Box::new(AggregateRowScatter {
                        buckets: final_parallelism,
                        aggregate_params: params.clone(),
                    })),
                ))
            })?;
        }
        AggregateShuffleMode::Bucket(hint) => {
            let hint = hint as usize;
            final_parallelism = if params.cluster_aggregator {
                let cluster = ctx.get_cluster();
                let local_pos = cluster.ordered_index();
                let nodes_num = cluster.nodes.len();
                let base = hint / nodes_num;
                let rem = hint % nodes_num;
                if local_pos < rem { base + 1 } else { base }
            } else {
                hint
            };

            pipeline.add_transform(|input, output| {
                Ok(LocalScatterTransform::create(
                    input,
                    output,
                    Arc::new(Box::new(AggregateBucketScatter {
                        buckets: final_parallelism,
                    })),
                ))
            })?;
        }
    }

    let input_len = pipeline.output_len();
    let transform =
        ExchangeShuffleTransform::create(input_len, final_parallelism, final_parallelism);
    let inputs = transform.get_inputs();
    let outputs = transform.get_outputs();
    pipeline.add_pipe(Pipe::create(input_len, final_parallelism, vec![
        PipeItem::create(ProcessorPtr::create(Box::new(transform)), inputs, outputs),
    ]));

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
            base_consumed_bits,
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
