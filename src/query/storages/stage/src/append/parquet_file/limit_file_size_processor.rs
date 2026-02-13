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
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::block_batch::BlockBatch;
use crate::append::partition::partition_from_block;

struct PartitionBucket {
    blocks: Vec<DataBlock>,
    size: usize,
}

impl PartitionBucket {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            size: 0,
        }
    }

    fn drain_ready_batches(&mut self, threshold: usize) -> Vec<DataBlock> {
        let mut batches = Vec::new();
        while self.size > threshold && !self.blocks.is_empty() {
            let mut accumulated = 0usize;
            let mut split_idx = None;
            for (idx, block) in self.blocks.iter().enumerate() {
                accumulated += block.memory_size();
                if accumulated > threshold {
                    split_idx = Some(idx);
                    break;
                }
            }
            let Some(idx) = split_idx else {
                break;
            };
            let remain = self.blocks.split_off(idx + 1);
            let emitted = mem::replace(&mut self.blocks, remain);
            let emitted_size = emitted.iter().map(|b| b.memory_size()).sum::<usize>();
            self.size = self.blocks.iter().map(|b| b.memory_size()).sum::<usize>();
            batches.push(BlockBatch::create_block(emitted));
            if emitted_size == 0 {
                break;
            }
        }
        batches
    }
}

pub(super) struct LimitFileSizeProcessor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    threshold: usize,

    input_data: Option<DataBlock>,

    partitions: HashMap<Option<Arc<str>>, PartitionBucket>,
    pending_batches: VecDeque<DataBlock>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        threshold: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            threshold,
            input_data: None,
            partitions: HashMap::new(),
            pending_batches: VecDeque::new(),
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }

    fn queue_flush_bucket(&mut self, bucket: PartitionBucket) {
        if bucket.blocks.is_empty() {
            return;
        }
        self.pending_batches
            .push_back(BlockBatch::create_block(bucket.blocks));
    }

    fn flush_all_buckets(&mut self) {
        let buckets = std::mem::take(&mut self.partitions);
        for (_, bucket) in buckets {
            self.queue_flush_bucket(bucket);
        }
    }
}

impl Processor for LimitFileSizeProcessor {
    fn name(&self) -> String {
        String::from("LimitFileSizeProcessor")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(batch) = self.pending_batches.pop_front() {
            self.output.push_data(Ok(batch));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if self.partitions.is_empty() && self.pending_batches.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            self.flush_all_buckets();
            if let Some(batch) = self.pending_batches.pop_front() {
                self.output.push_data(Ok(batch));
                return Ok(Event::NeedConsume);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let Some(block) = self.input_data.take() else {
            return Ok(());
        };
        let partition = partition_from_block(&block);
        let bucket = self
            .partitions
            .entry(partition.clone())
            .or_insert_with(PartitionBucket::new);
        bucket.size += block.memory_size();
        bucket.blocks.push(block);
        if bucket.size > self.threshold {
            let batches = bucket.drain_ready_batches(self.threshold);
            self.pending_batches.extend(batches);
        }
        Ok(())
    }
}
