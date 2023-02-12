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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_expression::with_hash_method;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::aggregator::AggregateHashStateInfo;
use super::group_by::BUCKETS_LG2;
use crate::pipelines::processors::transforms::aggregator::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::BucketAggregator;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::AggregatorParams;

// Overflow to object storage data block
#[allow(dead_code)]
static OVERFLOW_BUCKET_NUM: isize = -2;
// Single level data block
static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

///
#[derive(Debug)]
struct ConvertGroupingMetaInfo {
    #[allow(dead_code)]
    pub bucket: isize,
    pub blocks: Vec<DataBlock>,
}

impl Serialize for ConvertGroupingMetaInfo {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unreachable!("ConvertGroupingMetaInfo does not support exchanging between multiple nodes")
    }
}

impl<'de> Deserialize<'de> for ConvertGroupingMetaInfo {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unreachable!("ConvertGroupingMetaInfo does not support exchanging between multiple nodes")
    }
}

impl ConvertGroupingMetaInfo {
    pub fn create(bucket: isize, blocks: Vec<DataBlock>) -> BlockMetaInfoPtr {
        Box::new(ConvertGroupingMetaInfo { bucket, blocks })
    }
}

#[typetag::serde(name = "convert_grouping")]
impl BlockMetaInfo for ConvertGroupingMetaInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for ConvertGroupingMetaInfo")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for ConvertGroupingMetaInfo")
    }
}

struct InputPortState {
    port: Arc<InputPort>,
    bucket: isize,
}

/// A helper class that  Map
/// AggregateInfo/AggregateHashStateInfo  --->  ConvertGroupingMetaInfo { meta: blocks with Option<AggregateHashStateInfo> }
pub struct TransformConvertGrouping<Method: HashMethod + PolymorphicKeysHelper<Method>> {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,

    method: Method,
    working_bucket: isize,
    pushing_bucket: isize,
    initialized_all_inputs: bool,
    params: Arc<AggregatorParams>,
    buckets_blocks: HashMap<isize, Vec<DataBlock>>,
    unsplitted_blocks: Vec<DataBlock>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method>> TransformConvertGrouping<Method> {
    pub fn create(
        method: Method,
        params: Arc<AggregatorParams>,
        input_nums: usize,
    ) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState {
                bucket: -1,
                port: InputPort::create(),
            });
        }

        Ok(TransformConvertGrouping {
            method,
            params,
            inputs,
            working_bucket: 0,
            pushing_bucket: 0,
            output: OutputPort::create(),
            buckets_blocks: HashMap::new(),
            unsplitted_blocks: vec![],
            initialized_all_inputs: false,
        })
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        let mut inputs = Vec::with_capacity(self.inputs.len());

        for input_state in &self.inputs {
            inputs.push(input_state.port.clone());
        }

        inputs
    }

    pub fn get_output(&self) -> Arc<OutputPort> {
        self.output.clone()
    }

    fn initialize_all_inputs(&mut self) -> Result<bool> {
        self.initialized_all_inputs = true;

        for index in 0..self.inputs.len() {
            if self.inputs[index].port.is_finished() {
                continue;
            }

            // We pull the first unsplitted data block
            if self.inputs[index].bucket > SINGLE_LEVEL_BUCKET_NUM {
                continue;
            }

            self.inputs[index].port.set_need_data();

            if !self.inputs[index].port.has_data() {
                self.initialized_all_inputs = false;
                continue;
            }

            self.inputs[index].bucket =
                self.add_bucket(self.inputs[index].port.pull_data().unwrap()?);
        }

        Ok(self.initialized_all_inputs)
    }

    fn add_bucket(&mut self, data_block: DataBlock) -> isize {
        if let Some(info) = data_block
            .get_meta()
            .and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>())
        {
            if info.overflow.is_none() && info.bucket > SINGLE_LEVEL_BUCKET_NUM {
                let bucket = info.bucket;
                match self.buckets_blocks.entry(bucket) {
                    Entry::Vacant(v) => {
                        v.insert(vec![data_block]);
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(data_block);
                    }
                };

                return bucket;
            }
        }

        // check if it's local state
        if let Some(info) = data_block
            .get_meta()
            .and_then(|meta| meta.as_any().downcast_ref::<AggregateHashStateInfo>())
        {
            let bucket = info.bucket as isize;
            match self.buckets_blocks.entry(bucket) {
                Entry::Vacant(v) => {
                    v.insert(vec![data_block]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(data_block);
                }
            };
            return bucket;
        }

        self.unsplitted_blocks.push(data_block);
        SINGLE_LEVEL_BUCKET_NUM
    }

    fn try_push_data_block(&mut self) -> bool {
        match self.buckets_blocks.is_empty() {
            true => self.try_push_single_level(),
            false => self.try_push_two_level(),
        }
    }

    fn try_push_two_level(&mut self) -> bool {
        while self.pushing_bucket < self.working_bucket {
            if let Some(bucket_blocks) = self.buckets_blocks.remove(&self.pushing_bucket) {
                let meta = ConvertGroupingMetaInfo::create(self.pushing_bucket, bucket_blocks);
                self.output.push_data(Ok(DataBlock::empty_with_meta(meta)));
                self.pushing_bucket += 1;
                return true;
            }

            self.pushing_bucket += 1;
        }

        false
    }

    fn try_push_single_level(&mut self) -> bool {
        if self.unsplitted_blocks.is_empty() {
            return false;
        }

        let unsplitted_blocks = std::mem::take(&mut self.unsplitted_blocks);
        let meta = ConvertGroupingMetaInfo::create(SINGLE_LEVEL_BUCKET_NUM, unsplitted_blocks);
        self.output.push_data(Ok(DataBlock::empty_with_meta(meta)));
        true
    }

    fn convert_to_two_level(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = data_block
            .get_by_offset(aggregate_function_len)
            .value
            .as_column()
            .unwrap();
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        let mut indices = Vec::with_capacity(data_block.num_rows());

        for key_item in keys_iter.iter() {
            let hash = self.method.get_hash(key_item);
            indices.push((hash as usize >> (64u32 - BUCKETS_LG2)) as u16);
        }

        DataBlock::scatter(&data_block, &indices, 1 << BUCKETS_LG2)
    }
}

#[async_trait::async_trait]
impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> Processor
    for TransformConvertGrouping<Method>
{
    fn name(&self) -> String {
        String::from("TransformConvertGrouping")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input_state in &self.inputs {
                input_state.port.finish();
            }

            self.buckets_blocks.clear();
            return Ok(Event::Finished);
        }

        // We pull the first unsplitted data block
        if !self.initialized_all_inputs && !self.initialize_all_inputs()? {
            return Ok(Event::NeedData);
        }

        if !self.buckets_blocks.is_empty() && !self.unsplitted_blocks.is_empty() {
            // Split data blocks if it's unsplitted.
            return Ok(Event::Sync);
        }

        if !self.output.can_push() {
            for input_state in &self.inputs {
                input_state.port.set_not_need_data();
            }

            return Ok(Event::NeedConsume);
        }

        let pushed_data_block = self.try_push_data_block();

        loop {
            // Try to pull the next data or until the port is closed
            let mut all_inputs_is_finished = true;
            let mut all_port_prepared_data = true;

            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                all_inputs_is_finished = false;
                if self.inputs[index].bucket >= self.working_bucket {
                    continue;
                }

                self.inputs[index].port.set_need_data();
                if !self.inputs[index].port.has_data() {
                    all_port_prepared_data = false;
                    continue;
                }

                self.inputs[index].bucket =
                    self.add_bucket(self.inputs[index].port.pull_data().unwrap()?);
                debug_assert!(self.unsplitted_blocks.is_empty());
            }

            if all_inputs_is_finished {
                break;
            }

            if !all_port_prepared_data {
                return Ok(Event::NeedData);
            }

            self.working_bucket += 1;
        }

        if pushed_data_block || self.try_push_data_block() {
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.unsplitted_blocks.pop() {
            let data_block_meta: Option<&AggregateInfo> = data_block
                .get_meta()
                .and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>());

            let data_blocks = match data_block_meta {
                None => self.convert_to_two_level(data_block)?,
                Some(meta) => match &meta.overflow {
                    None => self.convert_to_two_level(data_block)?,
                    Some(_overflow_info) => unimplemented!(),
                },
            };

            for (bucket, block) in data_blocks.into_iter().enumerate() {
                if !block.is_empty() {
                    match self.buckets_blocks.entry(bucket as isize) {
                        Entry::Vacant(v) => {
                            v.insert(vec![block]);
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().push(block);
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

fn build_convert_grouping<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static>(
    method: Method,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = TransformConvertGrouping::create(method.clone(), params.clone(), input_nums)?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        inputs_port,
        vec![output],
    )]));

    pipeline.resize(input_nums)?;

    pipeline.add_transform(|input, output| {
        MergeBucketTransform::try_create(input, output, method.clone(), params.clone())
    })
}

pub fn efficiently_memory_final_aggregator(
    params: Arc<AggregatorParams>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let group_cols = &params.group_columns;
    let schema_before_group_by = params.input_schema.clone();
    let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
    let method = DataBlock::choose_hash_method(&sample_block, group_cols)?;

    with_hash_method!(|T| match method {
        HashMethodKind::T(v) => build_convert_grouping(v, pipeline, params.clone()),
    })
}

struct MergeBucketTransform<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> {
    method: Method,
    params: Arc<AggregatorParams>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_block: Option<DataBlock>,
    output_blocks: Vec<DataBlock>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static>
    MergeBucketTransform<Method>
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MergeBucketTransform {
            input,
            output,
            method,
            params,
            input_block: None,
            output_blocks: vec![],
        })))
    }
}

#[async_trait::async_trait]
impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> Processor
    for MergeBucketTransform<Method>
{
    fn name(&self) -> String {
        String::from("MergeBucketTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input_block.take();
            self.output_blocks.clear();
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_blocks.pop() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_block.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_block = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_block.take() {
            let mut blocks = vec![];
            if let Some(mut meta) = data_block.take_meta() {
                if let Some(meta) = meta.as_mut_any().downcast_mut::<ConvertGroupingMetaInfo>() {
                    std::mem::swap(&mut blocks, &mut meta.blocks);
                }
            }

            match self.params.aggregate_functions.is_empty() {
                true => {
                    let mut bucket_merger = BucketAggregator::<false, _>::create(
                        self.method.clone(),
                        self.params.clone(),
                    )?;

                    self.output_blocks
                        .extend(bucket_merger.merge_blocks(blocks)?);
                }
                false => {
                    let mut bucket_merger = BucketAggregator::<true, _>::create(
                        self.method.clone(),
                        self.params.clone(),
                    )?;

                    self.output_blocks
                        .extend(bucket_merger.merge_blocks(blocks)?);
                }
            };
        }

        Ok(())
    }
}
