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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::WindowPartitionMeta;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

struct InputPortState {
    port: Arc<InputPort>,
    bucket: isize,
}

pub struct TransformWindowPartitionBucket {
    inputs: Vec<InputPortState>,
    output: Arc<OutputPort>,
    working_bucket: isize,
    pushing_bucket: isize,
    initialized_all_inputs: bool,
    buckets_blocks: BTreeMap<isize, Vec<DataBlock>>,
}

impl TransformWindowPartitionBucket {
    pub fn create(input_nums: usize) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _ in 0..input_nums {
            inputs.push(InputPortState {
                bucket: -1,
                port: InputPort::create(),
            })
        }

        Ok(TransformWindowPartitionBucket {
            inputs,
            output: OutputPort::create(),
            working_bucket: 0,
            pushing_bucket: 0,
            initialized_all_inputs: false,
            buckets_blocks: BTreeMap::new(),
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

            if self.inputs[index].bucket > SINGLE_LEVEL_BUCKET_NUM {
                continue;
            }

            if !self.inputs[index].port.has_data() {
                self.inputs[index].port.set_need_data();
                self.initialized_all_inputs = false;
                continue;
            }

            let data_block = self.inputs[index].port.pull_data().unwrap()?;
            self.inputs[index].bucket = self.add_bucket(data_block)?;

            if self.inputs[index].bucket <= SINGLE_LEVEL_BUCKET_NUM {
                self.inputs[index].port.set_need_data();
                self.initialized_all_inputs = false;
            }
        }

        Ok(self.initialized_all_inputs)
    }

    fn add_bucket(&mut self, mut data_block: DataBlock) -> Result<isize> {
        if let Some(block_meta) = data_block.get_meta() {
            if let Some(block_meta) = WindowPartitionMeta::downcast_ref_from(block_meta) {
                let (bucket, res) = match block_meta {
                    WindowPartitionMeta::Spilling(_) => unreachable!(),
                    WindowPartitionMeta::BucketSpilled(_) => unreachable!(),
                    WindowPartitionMeta::Partitioned { .. } => unreachable!(),
                    WindowPartitionMeta::Payload(p) => (p.bucket, p.bucket),
                    WindowPartitionMeta::Spilled(_) => {
                        let meta = data_block.take_meta().unwrap();

                        if let Some(WindowPartitionMeta::Spilled(buckets_payload)) =
                            WindowPartitionMeta::downcast_from(meta)
                        {
                            for bucket_payload in buckets_payload {
                                match self.buckets_blocks.entry(bucket_payload.bucket) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![DataBlock::empty_with_meta(
                                            WindowPartitionMeta::create_bucket_spilled(
                                                bucket_payload,
                                            ),
                                        )]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(DataBlock::empty_with_meta(
                                            WindowPartitionMeta::create_bucket_spilled(
                                                bucket_payload,
                                            ),
                                        ));
                                    }
                                };
                            }

                            return Ok(SINGLE_LEVEL_BUCKET_NUM);
                        }

                        unreachable!()
                    }
                };

                match self.buckets_blocks.entry(bucket) {
                    Entry::Vacant(v) => {
                        v.insert(vec![data_block]);
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(data_block);
                    }
                };

                return Ok(res);
            }
        }

        unreachable!()
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

    fn convert_blocks(bucket: isize, data_blocks: Vec<DataBlock>) -> DataBlock {
        let mut data = Vec::with_capacity(data_blocks.len());
        for mut data_block in data_blocks.into_iter() {
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = WindowPartitionMeta::downcast_from(block_meta) {
                    data.push(block_meta);
                }
            }
        }

        DataBlock::empty_with_meta(WindowPartitionMeta::create_partitioned(bucket, data))
    }
}

#[async_trait::async_trait]
impl Processor for TransformWindowPartitionBucket {
    fn name(&self) -> String {
        String::from("TransformWindowPartitionBucket")
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

        if !self.initialized_all_inputs && !self.initialize_all_inputs()? {
            return Ok(Event::NeedData);
        }

        if !self.output.can_push() {
            for input_state in &self.inputs {
                input_state.port.set_not_need_data();
            }

            return Ok(Event::NeedConsume);
        }

        let pushed_data_block = self.try_push_data_block();

        loop {
            let mut all_inputs_is_finished = true;
            let mut all_ports_prepared_data = true;

            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                all_inputs_is_finished = false;
                if self.inputs[index].bucket > self.working_bucket {
                    continue;
                }

                if !self.inputs[index].port.has_data() {
                    all_ports_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                    continue;
                }

                let data_block = self.inputs[index].port.pull_data().unwrap()?;
                self.inputs[index].bucket = self.add_bucket(data_block)?;

                if self.inputs[index].bucket <= self.working_bucket {
                    all_ports_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                }
            }

            if all_inputs_is_finished {
                break;
            }

            if !all_ports_prepared_data {
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
        Ok(())
    }
}
