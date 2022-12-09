use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::replace;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKind;
use common_datavalues::Series;
use common_datavalues::StringColumn;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::ResizeProcessor;
use common_pipeline_core::Pipe;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::Transform;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tracing::info;

use crate::pipelines::processors::transforms::aggregator::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::BucketAggregator;
use crate::pipelines::processors::transforms::aggregator::OverflowInfo;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::TransformMarkJoin;
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::MarkJoinCompactor;
use crate::sessions::QueryContext;

static MAX_BUCKET_NUM: isize = 256;

///
#[derive(Debug, PartialEq)]
struct ConvertGroupingMetaInfo {
    pub bucket: isize,
    pub blocks: Vec<DataBlock>,
}

impl Serialize for ConvertGroupingMetaInfo {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unreachable!()
    }
}

impl<'de> Deserialize<'de> for ConvertGroupingMetaInfo {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unreachable!()
    }
}

impl ConvertGroupingMetaInfo {
    pub fn create(bucket: isize, blocks: Vec<DataBlock>) -> BlockMetaInfoPtr {
        Arc::new(Box::new(ConvertGroupingMetaInfo { bucket, blocks }))
    }
}

#[typetag::serde(name = "convert_grouping")]
impl BlockMetaInfo for ConvertGroupingMetaInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<ConvertGroupingMetaInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

enum InputPortState {
    Active { port: Arc<InputPort>, bucket: isize },
    Finished,
}

pub struct TransformConvertGrouping<Method: HashMethod + PolymorphicKeysHelper<Method>> {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,

    working_bucket: isize,
    method: Method,
    params: Arc<AggregatorParams>,
    buckets_blocks: HashMap<isize, Vec<DataBlock>>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method>> TransformConvertGrouping<Method> {
    pub fn create(
        method: Method,
        params: Arc<AggregatorParams>,
        input_nums: usize,
    ) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState::Active {
                bucket: 0,
                port: InputPort::create(),
            });
        }

        Ok(TransformConvertGrouping {
            method,
            params,
            inputs,
            working_bucket: 0,
            output: OutputPort::create(),
            buckets_blocks: HashMap::new(),
        })
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        let mut inputs = Vec::with_capacity(self.inputs.len());

        for input in self.inputs {
            if let InputPortState::Active { port, .. } = input {
                inputs.push(port.clone());
            }
        }

        inputs
    }

    pub fn get_output(&self) -> Arc<OutputPort> {
        self.output.clone()
    }

    fn convert_to_two_level(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        // let instant = Instant::now();
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = data_block.column(aggregate_function_len);
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        let mut indices = Vec::with_capacity(data_block.num_rows());

        for key_item in keys_iter.iter() {
            let hash = self.method.get_hash(key_item);
            indices.push(hash as usize >> (64u32 - 8));
        }

        DataBlock::scatter_block(&data_block, &indices, 256)
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
        if self.working_bucket == MAX_BUCKET_NUM || self.output.is_finished() {
            for input in &self.inputs {
                if let InputPortState::Active { port, .. } = input {
                    port.finish();
                }
            }

            self.buckets_blocks.clear();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            for input in &self.inputs {
                if let InputPortState::Active { port, .. } = input {
                    port.set_not_need_data();
                }
            }

            return Ok(Event::NeedConsume);
        }

        if self.working_bucket == 1 {
            if self.buckets_blocks.contains_key(&-2) || self.buckets_blocks.contains_key(&-1) {
                return Ok(Event::Sync);
            }

            if self.buckets_blocks.contains_key(&0) {
                if let Some(bucket_blocks) = self.buckets_blocks.remove(&0) {
                    self.output.push_data(Ok(DataBlock::empty_with_meta(
                        ConvertGroupingMetaInfo::create(0, bucket_blocks),
                    )));

                    return Ok(Event::NeedConsume);
                }
            }
        }

        let mut next_working_bucket = self.working_bucket + 1;

        for input in self.inputs.iter_mut() {
            match input {
                InputPortState::Active { port, .. } if port.is_finished() => {
                    port.finish();
                    *input = InputPortState::Finished;
                }
                InputPortState::Active { port, bucket } if *bucket == self.working_bucket => {
                    port.set_need_data();

                    if !port.has_data() {
                        next_working_bucket = self.working_bucket;
                        continue;
                    }

                    let data_block = port.pull_data().unwrap()?;
                    let data_block_meta: Option<&AggregateInfo> = data_block
                        .get_meta()
                        .and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>());

                    match data_block_meta {
                        // XXX: None | Some(info) if info.bucket == -1 is compile failure.
                        None => {
                            port.finish();
                            *input = InputPortState::Finished;
                            match self.buckets_blocks.entry(-1) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                            };
                        }
                        Some(info) if info.bucket == -1 => {
                            port.finish();
                            *input = InputPortState::Finished;
                            match self.buckets_blocks.entry(-1) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                            };
                        }
                        Some(info) => match info.overflow {
                            None => {
                                *bucket = info.bucket + 1;
                                match self.buckets_blocks.entry(info.bucket) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![data_block]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(data_block);
                                    }
                                };
                            }
                            Some(_) => {
                                // Skipping overflow block.
                                next_working_bucket = self.working_bucket;
                                match self.buckets_blocks.entry(-2) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![data_block]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(data_block);
                                    }
                                };
                            }
                        },
                    };
                }
                _ => { /* finished or done current bucket, do nothing */ }
            }
        }

        if self.working_bucket + 1 == next_working_bucket {
            // current working bucket is process completed.

            if self.working_bucket == 0 {
                // all single level data block
                if self.buckets_blocks.len() == 1 && self.buckets_blocks.contains_key(&-1) {
                    self.working_bucket = 256;

                    if let Some(bucket_blocks) = self.buckets_blocks.remove(&-1) {
                        self.output.push_data(Ok(DataBlock::empty_with_meta(
                            ConvertGroupingMetaInfo::create(-1, bucket_blocks),
                        )));
                    }

                    return Ok(Event::NeedConsume);
                }

                // need convert to two level data block
                self.working_bucket = next_working_bucket;
                return Ok(Event::Sync);
            }

            if let Some(bucket_blocks) = self.buckets_blocks.remove(&self.working_bucket) {
                self.output.push_data(Ok(DataBlock::empty_with_meta(
                    ConvertGroupingMetaInfo::create(self.working_bucket, bucket_blocks),
                )));
            }

            self.working_bucket = next_working_bucket;
            return Ok(Event::NeedConsume);
        }

        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(overflow_blocks) = self.buckets_blocks.get_mut(&-2) {
            match overflow_blocks.pop() {
                None => {
                    self.buckets_blocks.remove(&-2);
                }
                Some(data_block) => {
                    // TODO:
                    // data_block
                }
            };
        }

        if let Some(single_level_blocks) = self.buckets_blocks.get_mut(&-1) {
            match single_level_blocks.pop() {
                None => {
                    self.buckets_blocks.remove(&-1);
                }
                Some(data_block) => {
                    let blocks = self.convert_to_two_level(data_block)?;

                    for (bucket, block) in blocks.into_iter().enumerate() {
                        if !block.is_empty() {
                            match self.buckets_blocks.entry(bucket as isize) {
                                Entry::Vacant(mut v) => {
                                    v.insert(vec![block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(block);
                                }
                            };
                        }
                    }
                }
            };
        }

        Ok(())
    }
}

fn build_convert_grouping<Method: HashMethod + PolymorphicKeysHelper<Method>>(
    method: Method,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = TransformConvertGrouping::create(method.clone(), params.clone(), input_nums)?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::ResizePipe {
        inputs_port,
        outputs_port: vec![output],
        processor: ProcessorPtr::create(Box::new(transform)),
    });

    pipeline.resize(input_nums)?;

    pipeline.add_transform(|input, output| {
        MergeBucketTransform::try_Pipcreate(input, output, method.clone(), params.clone())
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

    match method {
        HashMethodKind::KeysU8(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU16(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU32(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU64(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU128(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU256(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::KeysU512(v) => build_convert_grouping(v, pipeline, params.clone()),
        HashMethodKind::Serializer(v) => build_convert_grouping(v, pipeline, params.clone()),
    }
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
            self.input_block.clear();
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
        if let Some(data_block) = self.input_block.take() {
            let mut blocks = vec![];
            if let Some(meta) = data_block.get_meta() {
                if let Some(meta) = meta.as_any().downcast_ref::<ConvertGroupingMetaInfo>() {
                    blocks.extend(meta.blocks.iter().cloned());
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
            }
        }

        Ok(())
    }
}
