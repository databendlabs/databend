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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::Transform;
use databend_common_pipeline_transforms::Transformer;
use databend_common_storage::DataOperator;

use super::AggregateMeta;
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
    pipeline.exchange(1, Arc::new(ResortingPartition::create()));

    // 2. align partitions
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(AccumulatingTransformer::create(
            input,
            output,
            TransformPartitionAlign::create(ctx.clone(), params.clone())?,
        )))
    })?;

    pipeline.add_transform(|input, output| {
        CheckPartition::create(input, output, String::from("after align"))
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

    pipeline.add_transform(|input, output| {
        CheckPartition::create(input, output, String::from("after dispatch"))
    })?;

    // 4. restore partition
    let operator = DataOperator::instance().spill_operator();
    pipeline.add_transform(|input, output| {
        TransformPartitionRestore::create(input, output, operator.clone(), params.clone())
    })?;

    pipeline.add_transform(|input, output| {
        CheckPartition::create(input, output, String::from("after restore"))
    })?;

    // 5. exchange local
    let pipe_size = pipeline.output_len();
    pipeline.exchange(pipe_size, ExchangePartition::create(params.clone()));

    pipeline.add_transform(|input, output| {
        CheckPartition::create(input, output, String::from("after exchange"))
    })?;

    // 6. final aggregate
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(TransformFinalAggregate::try_create(
            input.clone(),
            output.clone(),
            params.clone(),
        )?))
    })
}

pub struct CheckPartition {
    name: String,
    cur_partition: Option<isize>,
}

impl CheckPartition {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        name: String,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            CheckPartition {
                name,
                cur_partition: None,
            },
        )))
    }
}

impl Transform for CheckPartition {
    const NAME: &'static str = "CheckPartition";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let Some(meta) = data.get_meta() else {
            unreachable!();
        };

        let Some(meta) = AggregateMeta::downcast_ref_from(meta) else {
            unreachable!();
        };

        if let AggregateMeta::FinalPartition = meta {
            self.cur_partition = None;
            return Ok(data);
        }

        let partition = meta.get_partition();
        assert!(
            self.cur_partition.is_none() || matches!(self.cur_partition, Some(v) if v == partition),
            "{:?} assert failure partition({}) != current_partition({:?})",
            self.name,
            partition,
            self.cur_partition
        );
        self.cur_partition = Some(partition);
        Ok(data)
    }
}
