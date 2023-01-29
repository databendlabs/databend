// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::with_hash_method;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipe;
use common_pipeline_core::Pipeline;
use strength_reduce::StrengthReducedU64;

use super::AggregatorTransformParams;
use super::TransformAggregator;
use crate::pipelines::processors::transforms::aggregator::AggregateInfo;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

pub struct TransformConvertGroupingV2<
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static,
> {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    method: Method,
    params: Arc<AggregatorParams>,
    output_num: u64,
    blocks: VecDeque<DataBlock>,
    partition_expr: StrengthReducedU64,
    counts: BTreeMap<isize, usize>,
}

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static>
    TransformConvertGroupingV2<Method>
{
    pub fn create(
        method: Method,
        params: Arc<AggregatorParams>,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        output_num: usize,
    ) -> Result<ProcessorPtr> {
        let transform = TransformConvertGroupingV2 {
            method,
            params,
            input_port,
            output_port,
            output_num: output_num as u64,
            blocks: VecDeque::new(),
            partition_expr: StrengthReducedU64::new(output_num as u64),
            counts: BTreeMap::new(),
        };

        Ok(ProcessorPtr::create(Box::new(transform)))
    }

    fn add_block(&mut self, data_block: DataBlock) -> Result<()> {
        let data_block_meta = data_block
            .get_meta()
            .and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>());

        if let Some(info) = data_block_meta {
            if info.bucket > SINGLE_LEVEL_BUCKET_NUM {
                // max bucket num is defaults to 256
                // assume that the output num is far less than 256
                let bucket = info.bucket % self.output_num as isize;
                let data_block = data_block
                    .clone()
                    .add_meta(Some(AggregateInfo::create(bucket)))?;

                self.counts
                    .entry(1)
                    .and_modify(|e| *e += data_block.num_rows())
                    .or_insert(data_block.num_rows());
                self.blocks.push_back(data_block);
                return Ok(());
            }
        }
        self.counts
            .entry(-1)
            .and_modify(|e| *e += data_block.num_rows())
            .or_insert(data_block.num_rows());

        let blocks = self.partition(data_block).unwrap();

        for (bucket, block) in blocks.into_iter().enumerate() {
            if block.num_rows() == 0 {
                continue;
            }
            let block = block.add_meta(Some(AggregateInfo::create(bucket as isize)))?;
            self.blocks.push_back(block);
        }
        Ok(())
    }

    fn partition(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = data_block
            .get_by_offset(aggregate_function_len)
            .value
            .as_column()
            .unwrap();
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        let indices: Vec<u64> = keys_iter
            .iter()
            .map(|key_item| {
                let hash = self.method.get_hash(key_item);
                hash % self.partition_expr
            })
            .collect();

        DataBlock::scatter(&data_block, &indices, self.output_num as usize)
    }
}

#[async_trait::async_trait]
impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static + Send + 'static> Processor
    for TransformConvertGroupingV2<Method>
{
    fn name(&self) -> String {
        String::from("TransformConvertGroupingV2")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            self.blocks.clear();
            return Ok(Event::Finished);
        }

        if !self.blocks.is_empty() {
            if !self.output_port.can_push() {
                self.input_port.set_not_need_data();
                return Ok(Event::NeedConsume);
            }

            let block = self.blocks.pop_front().unwrap();
            self.output_port.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            return Ok(Event::Sync);
        }

        if self.input_port.is_finished() {
            println!("counts {:?}", self.counts);
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        if !self.input_port.has_data() {
            self.input_port.set_need_data();
            return Ok(Event::NeedData);
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        let data_block = self.input_port.pull_data().unwrap()?;
        self.add_block(data_block)?;
        Ok(())
    }
}

fn build_convert_grouping_v2<
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static + Send + 'static,
>(
    method: Method,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    ctx: Arc<QueryContext>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    pipeline.add_transform(|input, output| {
        TransformConvertGroupingV2::create(
            method.clone(),
            params.clone(),
            input,
            output,
            input_nums,
        )
    })?;

    pipeline.resize(1)?;

    let input = InputPort::create();
    let output_ports: Vec<Arc<OutputPort>> =
        (0..input_nums).map(|_| OutputPort::create()).collect();
    let coordinator = BucketAggregatorCoodinator {
        input: input.clone(),
        output: output_ports.clone(),
        input_block: None,
    };

    pipeline.add_pipe(Pipe::ResizePipe {
        inputs_port: vec![input],
        outputs_port: output_ports,
        processor: ProcessorPtr::create(Box::new(coordinator)),
    });

    pipeline.add_transform(|input, output| {
        TransformAggregator::try_create_final(
            ctx.clone(),
            AggregatorTransformParams::try_create(input, output, &params)?,
            false,
        )
    })
}

struct BucketAggregatorCoodinator {
    input: Arc<InputPort>,
    output: Vec<Arc<OutputPort>>,
    input_block: Option<(DataBlock, isize)>,
}

impl Processor for BucketAggregatorCoodinator {
    fn name(&self) -> String {
        "BucketAggregatorCoodinator".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        let mut datablock = self.input.pull_data().unwrap()?;
        let meta = datablock.take_meta().unwrap();
        let bucket = meta
            .as_any()
            .downcast_ref::<AggregateInfo>()
            .unwrap()
            .bucket;

        self.input_block = Some((datablock, bucket));
        Ok(())
    }

    fn event(&mut self) -> Result<Event> {
        if let Some((block, bucket)) = self.input_block.take() {
            let output = &self.output[bucket as usize];

            if output.is_finished() {
                return Ok(Event::NeedData);
            }

            if !output.can_push() {
                self.input.set_not_need_data();
                self.input_block = Some((block, bucket));
                return Ok(Event::NeedConsume);
            }
            output.push_data(Ok(block));
            return Ok(Event::NeedData);
        }

        if self.input.has_data() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            for output in self.output.iter() {
                output.finish();
            }

            return Ok(Event::Finished);
        }
        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}

pub fn efficiently_memory_final_aggregator_v2(
    params: Arc<AggregatorParams>,
    pipeline: &mut Pipeline,
    ctx: Arc<QueryContext>,
) -> Result<()> {
    let group_cols = &params.group_columns;
    let schema_before_group_by = params.input_schema.clone();
    let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
    let method = DataBlock::choose_hash_method(&sample_block, group_cols)?;

    with_hash_method!(|T| match method {
        HashMethodKind::T(v) => build_convert_grouping_v2(v, pipeline, params.clone(), ctx),
    })
}
