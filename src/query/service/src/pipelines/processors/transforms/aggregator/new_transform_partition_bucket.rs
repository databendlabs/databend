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

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::DataOperator;
use tokio::sync::Semaphore;

use super::AggregatePayload;
use super::TransformAggregateSpillReader;
use super::TransformFinalAggregate;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;
static MAX_PARTITION_COUNT: usize = 128;

struct InputPortState {
    port: Arc<InputPort>,
    bucket: isize,
    max_partition_count: usize,
}
pub struct NewTransformPartitionBucket {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,
    params: Arc<AggregatorParams>,
    working_bucket: isize,
    pushing_bucket: isize,
    initialized_all_inputs: bool,
    all_inputs_init: bool,
    buckets_blocks: BTreeMap<isize, Vec<DataBlock>>,
    flush_state: PayloadFlushState,
    unpartitioned_blocks: Vec<DataBlock>,
    max_partition_count: usize,
}

impl NewTransformPartitionBucket {
    pub fn create(input_nums: usize, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState {
                bucket: -1,
                port: InputPort::create(),
                max_partition_count: 0,
            });
        }

        Ok(NewTransformPartitionBucket {
            params,
            inputs,
            working_bucket: 0,
            pushing_bucket: 0,
            output: OutputPort::create(),
            buckets_blocks: BTreeMap::new(),
            unpartitioned_blocks: vec![],
            flush_state: PayloadFlushState::default(),
            initialized_all_inputs: false,
            all_inputs_init: false,
            max_partition_count: 0,
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
        // in a cluster where partitions are only 8 and 128,
        // we need to pull all data where the partition equals 8 until the partition changes to 128 or there is no data available.
        if self.params.cluster_aggregator {
            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                // We pull all the data that are not the max_partition_count and all spill data
                if self.inputs[index].max_partition_count == MAX_PARTITION_COUNT
                    && self.inputs[index].bucket > SINGLE_LEVEL_BUCKET_NUM
                {
                    continue;
                }

                if !self.inputs[index].port.has_data() {
                    self.inputs[index].port.set_need_data();
                    self.initialized_all_inputs = false;
                    continue;
                }

                let data_block = self.inputs[index].port.pull_data().unwrap()?;

                (
                    self.inputs[index].bucket,
                    self.inputs[index].max_partition_count,
                ) = self.add_bucket(data_block)?;

                // we need pull all spill data in init, and data less than max partition
                if self.inputs[index].bucket <= SINGLE_LEVEL_BUCKET_NUM
                    || self.inputs[index].max_partition_count < MAX_PARTITION_COUNT
                {
                    self.inputs[index].port.set_need_data();
                    self.initialized_all_inputs = false;
                }
            }
        } else {
            // in singleton, the partition is 8, 32, 128.
            // We pull the first data to ensure the max partition,
            // and then pull all data that is less than the max partition
            let mut refresh_index = 0;
            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                // We pull all the data that are not the max_partition_count
                if self.inputs[index].max_partition_count > 0
                    && self.inputs[index].bucket > SINGLE_LEVEL_BUCKET_NUM
                    && self.inputs[index].max_partition_count == self.max_partition_count
                {
                    continue;
                }

                if !self.inputs[index].port.has_data() {
                    self.inputs[index].port.set_need_data();
                    self.initialized_all_inputs = false;
                    continue;
                }

                let data_block = self.inputs[index].port.pull_data().unwrap()?;

                let before_max_partition_count = self.max_partition_count;
                (
                    self.inputs[index].bucket,
                    self.inputs[index].max_partition_count,
                ) = self.add_bucket(data_block)?;

                // we need pull all spill data in init, and data less than max partition
                if self.inputs[index].bucket <= SINGLE_LEVEL_BUCKET_NUM
                    || self.inputs[index].max_partition_count < self.max_partition_count
                {
                    self.inputs[index].port.set_need_data();
                    self.initialized_all_inputs = false;
                }

                // max partition count change
                if before_max_partition_count > 0
                    && before_max_partition_count != self.max_partition_count
                {
                    // set need data for inputs which is less than the max partition
                    for i in refresh_index..index {
                        if !self.inputs[i].port.is_finished()
                            && !self.inputs[i].port.has_data()
                            && self.inputs[i].max_partition_count != self.max_partition_count
                        {
                            self.inputs[i].port.set_need_data();
                            self.initialized_all_inputs = false;
                        }
                    }
                    refresh_index = index;
                }
            }
        }

        if self.initialized_all_inputs {
            self.all_inputs_init = true;
        }

        Ok(self.initialized_all_inputs)
    }

    #[allow(unused_assignments)]
    fn add_bucket(&mut self, mut data_block: DataBlock) -> Result<(isize, usize)> {
        let (mut bucket, mut partition_count) = (0, 0);
        let mut is_empty_block = false;
        if let Some(block_meta) = data_block.get_meta() {
            if let Some(block_meta) = AggregateMeta::downcast_ref_from(block_meta) {
                (bucket, partition_count) = match block_meta {
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => {
                        let meta = data_block.take_meta().unwrap();

                        if let Some(AggregateMeta::BucketSpilled(payload)) =
                            AggregateMeta::downcast_from(meta)
                        {
                            let bucket = payload.bucket;
                            let partition_count = payload.max_partition_count;
                            self.max_partition_count =
                                self.max_partition_count.max(partition_count);

                            let data_block = DataBlock::empty_with_meta(
                                AggregateMeta::create_bucket_spilled(payload),
                            );
                            match self.buckets_blocks.entry(bucket) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                            };

                            return Ok((SINGLE_LEVEL_BUCKET_NUM, partition_count));
                        }
                        unreachable!()
                    }
                    AggregateMeta::Spilled(_) => {
                        let meta = data_block.take_meta().unwrap();

                        if let Some(AggregateMeta::Spilled(buckets_payload)) =
                            AggregateMeta::downcast_from(meta)
                        {
                            let partition_count = if !buckets_payload.is_empty() {
                                buckets_payload[0].max_partition_count
                            } else {
                                MAX_PARTITION_COUNT
                            };
                            self.max_partition_count =
                                self.max_partition_count.max(partition_count);

                            for bucket_payload in buckets_payload {
                                let bucket = bucket_payload.bucket;
                                let data_block = DataBlock::empty_with_meta(
                                    AggregateMeta::create_bucket_spilled(bucket_payload),
                                );
                                match self.buckets_blocks.entry(bucket) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![data_block]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(data_block);
                                    }
                                };
                            }

                            return Ok((SINGLE_LEVEL_BUCKET_NUM, partition_count));
                        }
                        unreachable!()
                    }
                    AggregateMeta::Serialized(payload) => {
                        is_empty_block = payload.data_block.is_empty();
                        self.max_partition_count =
                            self.max_partition_count.max(payload.max_partition_count);

                        (payload.bucket, payload.max_partition_count)
                    }
                    AggregateMeta::AggregatePayload(payload) => {
                        is_empty_block = payload.payload.len() == 0;
                        self.max_partition_count =
                            self.max_partition_count.max(payload.max_partition_count);

                        (payload.bucket, payload.max_partition_count)
                    }
                };
            } else {
                return Err(ErrorCode::Internal(format!(
                    "Internal, TransformPartitionBucket only recv AggregateMeta, but got {:?}",
                    block_meta
                )));
            }
        } else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv DataBlock with meta.",
            ));
        }

        if !is_empty_block {
            if self.all_inputs_init {
                if partition_count != self.max_partition_count {
                    return Err(ErrorCode::Internal(
                        "Internal, the partition count does not equal the max partition count on TransformPartitionBucket.
                    ",
                    ));
                }
                match self.buckets_blocks.entry(bucket) {
                    Entry::Vacant(v) => {
                        v.insert(vec![data_block]);
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(data_block);
                    }
                };
            } else {
                self.unpartitioned_blocks.push(data_block);
            }
        }

        Ok((bucket, partition_count))
    }

    fn try_push_data_block(&mut self) -> bool {
        while self.pushing_bucket < self.working_bucket {
            if let Some(bucket_blocks) = self.buckets_blocks.remove(&self.pushing_bucket) {
                let data_block = Self::convert_blocks(self.pushing_bucket, bucket_blocks);
                self.output.push_data(Ok(data_block));
                self.pushing_bucket += 1;
                return true;
            }

            self.pushing_bucket += 1;
        }

        false
    }

    fn partition_block(&mut self, payload: SerializedPayload) -> Result<Vec<Option<DataBlock>>> {
        // already is max partition
        if payload.max_partition_count == self.max_partition_count {
            let bucket = payload.bucket;
            let data_block =
                DataBlock::empty_with_meta(Box::new(AggregateMeta::Serialized(payload)));
            match self.buckets_blocks.entry(bucket) {
                Entry::Vacant(v) => {
                    v.insert(vec![data_block]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(data_block);
                }
            };
            return Ok(vec![]);
        }

        // need repartition
        let mut blocks = Vec::with_capacity(self.max_partition_count);
        let p = payload.convert_to_partitioned_payload(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.params.num_states(),
            0,
            Arc::new(Bump::new()),
        )?;

        let mut partitioned_payload = PartitionedPayload::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.max_partition_count as u64,
            p.arenas.clone(),
        );
        partitioned_payload.combine(p, &mut self.flush_state);

        for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
            blocks.push(Some(DataBlock::empty_with_meta(
                AggregateMeta::create_agg_payload(
                    bucket as isize,
                    payload,
                    self.max_partition_count,
                ),
            )));
        }

        Ok(blocks)
    }

    fn partition_payload(&mut self, payload: AggregatePayload) -> Result<Vec<Option<DataBlock>>> {
        // already is max partition
        if payload.max_partition_count == self.max_partition_count {
            let bucket = payload.bucket;
            let data_block =
                DataBlock::empty_with_meta(Box::new(AggregateMeta::AggregatePayload(payload)));
            match self.buckets_blocks.entry(bucket) {
                Entry::Vacant(v) => {
                    v.insert(vec![data_block]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(data_block);
                }
            };
            return Ok(vec![]);
        }

        // need repartition
        let mut blocks = Vec::with_capacity(self.max_partition_count);
        let mut partitioned_payload = PartitionedPayload::new(
            self.params.group_data_types.clone(),
            self.params.aggregate_functions.clone(),
            self.max_partition_count as u64,
            vec![payload.payload.arena.clone()],
        );

        partitioned_payload.combine_single(payload.payload, &mut self.flush_state, None);

        for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
            blocks.push(Some(DataBlock::empty_with_meta(
                AggregateMeta::create_agg_payload(
                    bucket as isize,
                    payload,
                    self.max_partition_count,
                ),
            )));
        }

        Ok(blocks)
    }

    fn convert_blocks(bucket: isize, data_blocks: Vec<DataBlock>) -> DataBlock {
        let mut data = Vec::with_capacity(data_blocks.len());
        for mut data_block in data_blocks.into_iter() {
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                    data.push(block_meta);
                }
            }
        }

        DataBlock::empty_with_meta(AggregateMeta::create_partitioned(bucket, data))
    }
}

#[async_trait::async_trait]
impl Processor for NewTransformPartitionBucket {
    fn name(&self) -> String {
        String::from("TransformPartitionBucket")
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

        if !self.unpartitioned_blocks.is_empty() {
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
                if self.inputs[index].bucket > self.working_bucket {
                    continue;
                }

                if !self.inputs[index].port.has_data() {
                    all_port_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                    continue;
                }

                let data_block = self.inputs[index].port.pull_data().unwrap()?;
                (self.inputs[index].bucket, _) = self.add_bucket(data_block)?;

                if self.inputs[index].bucket <= self.working_bucket {
                    all_port_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                }
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

        if let Some((bucket, bucket_blocks)) = self.buckets_blocks.pop_first() {
            let data_block = Self::convert_blocks(bucket, bucket_blocks);
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }

    fn process(&mut self) -> Result<()> {
        let block_meta = self
            .unpartitioned_blocks
            .pop()
            .and_then(|mut block| block.take_meta())
            .and_then(AggregateMeta::downcast_from);

        if let Some(agg_block_meta) = block_meta {
            let data_blocks = match agg_block_meta {
                AggregateMeta::Spilled(_) => unreachable!(),
                AggregateMeta::Partitioned { .. } => unreachable!(),
                AggregateMeta::AggregateSpilling(_) => unreachable!(),
                AggregateMeta::BucketSpilled(_) => unreachable!(),
                AggregateMeta::Serialized(payload) => self.partition_block(payload)?,
                AggregateMeta::AggregatePayload(payload) => self.partition_payload(payload)?,
            };

            for (bucket, block) in data_blocks.into_iter().enumerate() {
                if let Some(data_block) = block {
                    match self.buckets_blocks.entry(bucket as isize) {
                        Entry::Vacant(v) => {
                            v.insert(vec![data_block]);
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().push(data_block);
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

pub fn build_partition_bucket(
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
    max_restore_worker: u64,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = NewTransformPartitionBucket::create(input_nums, params.clone())?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
        ProcessorPtr::create(Box::new(transform)),
        inputs_port,
        vec![output],
    )]));

    pipeline.try_resize(std::cmp::min(input_nums, max_restore_worker as usize))?;

    let semaphore = Arc::new(Semaphore::new(params.max_spill_io_requests));
    let operator = DataOperator::instance().spill_operator();
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

    pipeline.try_resize(input_nums)?;

    Ok(())
}
