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
use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use crate::pipelines::processors::transforms::aggregator::AggregateBucketInput;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionItem;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;
static MAX_PARTITION_COUNT: usize = 128;

struct InputPortState {
    port: Arc<InputPort>,
    bucket: isize,
    max_partition_count: usize,
}
pub struct TransformPartitionBucket {
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

impl TransformPartitionBucket {
    pub fn create(input_nums: usize, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState {
                bucket: -1,
                port: InputPort::create(),
                max_partition_count: 0,
            });
        }

        Ok(TransformPartitionBucket {
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

    #[allow(unused)]
    fn add_bucket(&mut self, mut data_block: DataBlock) -> Result<(isize, usize)> {
        let Some(block_meta) = data_block.take_meta() else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv DataBlock with meta.",
            ));
        };
        let Some(block_meta) = AggregateMeta::downcast_from(block_meta) else {
            return Err(ErrorCode::Internal(
                "Internal, TransformPartitionBucket only recv AggregateMeta",
            ));
        };

        match block_meta.into_bucket_input(MAX_PARTITION_COUNT) {
            AggregateBucketInput::Spilled {
                max_partition_count,
                metas,
            } => {
                self.max_partition_count = self.max_partition_count.max(max_partition_count);
                for (bucket, meta) in metas {
                    self.push_bucket_block(bucket, meta.into_datablock());
                }
                Ok((SINGLE_LEVEL_BUCKET_NUM, max_partition_count))
            }
            AggregateBucketInput::Direct {
                bucket,
                max_partition_count,
                is_empty,
                meta,
            } => {
                self.max_partition_count = self.max_partition_count.max(max_partition_count);
                if !is_empty {
                    let data_block =
                        data_block.add_meta(Some(Box::new(AggregateMeta::from(meta))))?;
                    if self.all_inputs_init {
                        if max_partition_count != self.max_partition_count {
                            return Err(ErrorCode::Internal(
                                "Internal, the partition count does not equal the max partition count on TransformPartitionBucket.
                            ",
                            ));
                        }
                        self.push_bucket_block(bucket, data_block);
                    } else {
                        self.unpartitioned_blocks.push(data_block);
                    }
                }

                Ok((bucket, max_partition_count))
            }
        }
    }

    fn push_bucket_block(&mut self, bucket: isize, data_block: DataBlock) {
        self.buckets_blocks
            .entry(bucket)
            .or_default()
            .push(data_block)
    }

    fn try_push_data_block(&mut self) -> Result<bool> {
        while self.pushing_bucket < self.working_bucket {
            if let Some(bucket_blocks) = self.buckets_blocks.remove(&self.pushing_bucket) {
                let data_block = Self::convert_blocks(self.pushing_bucket, bucket_blocks)?;
                self.output.push_data(Ok(data_block));
                self.pushing_bucket += 1;
                return Ok(true);
            }

            self.pushing_bucket += 1;
        }

        Ok(false)
    }

    fn convert_blocks(bucket: isize, data_blocks: Vec<DataBlock>) -> Result<DataBlock> {
        let mut data = Vec::with_capacity(data_blocks.len());
        for mut data_block in data_blocks.into_iter() {
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                    let item = match block_meta {
                        AggregateMeta::Serialized(payload) => PartitionItem::Serialized(payload),
                        AggregateMeta::AggregatePayload(payload) => {
                            PartitionItem::AggregatePayload(payload)
                        }
                        AggregateMeta::BucketSpilled(payload) => {
                            PartitionItem::BucketSpilled(payload)
                        }
                        AggregateMeta::NewBucketSpilled(payload) => {
                            PartitionItem::NewBucketSpilled(payload)
                        }
                        _ => Err(ErrorCode::Internal(format!(
                            "Unexpected aggregate meta in partitioned aggregate batch: {block_meta:?}"
                        )))?,
                    };
                    data.push(item);
                }
            }
        }
        Ok(DataBlock::empty_with_meta(
            AggregateMeta::create_partitioned(Some(bucket), PartitionedData::Mixed(data)),
        ))
    }
}

#[async_trait::async_trait]
impl Processor for TransformPartitionBucket {
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

        let pushed_data_block = self.try_push_data_block()?;

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

        if pushed_data_block || self.try_push_data_block()? {
            return Ok(Event::NeedConsume);
        }

        if let Some((bucket, bucket_blocks)) = self.buckets_blocks.pop_first() {
            let data_block = Self::convert_blocks(bucket, bucket_blocks)?;
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
            match agg_block_meta {
                AggregateMeta::Serialized(payload) => {
                    if payload.max_partition_count == self.max_partition_count {
                        self.push_bucket_block(
                            payload.bucket,
                            PartitionItem::Serialized(payload).into_datablock(),
                        );
                    } else {
                        for payload in payload.repartition_to_payloads(
                            self.max_partition_count,
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            self.params.num_states(),
                            self.params.enable_experiment_hash_index,
                            &mut self.flush_state,
                        )? {
                            self.push_bucket_block(
                                payload.bucket,
                                PartitionItem::AggregatePayload(payload).into_datablock(),
                            );
                        }
                    }
                }
                AggregateMeta::AggregatePayload(payload) => {
                    if payload.max_partition_count == self.max_partition_count {
                        self.push_bucket_block(
                            payload.bucket,
                            PartitionItem::AggregatePayload(payload).into_datablock(),
                        );
                    } else {
                        for payload in payload.repartition_to_payloads(
                            self.max_partition_count,
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            &mut self.flush_state,
                        ) {
                            self.push_bucket_block(
                                payload.bucket,
                                PartitionItem::AggregatePayload(payload).into_datablock(),
                            );
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }
}
