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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::append::row_based_file::buffers::FileOutputBuffer;
use crate::append::row_based_file::buffers::FileOutputBuffers;

struct PartitionBuffer {
    buffers: Vec<FileOutputBuffer>,
    size: usize,
    partition: Option<Arc<str>>,
}

impl PartitionBuffer {
    fn new(partition: Option<Arc<str>>) -> Self {
        Self {
            buffers: Vec::new(),
            size: 0,
            partition,
        }
    }

    fn drain_ready_batches(&mut self, threshold: usize) -> Vec<DataBlock> {
        let mut batches = Vec::new();
        while self.size > threshold {
            let mut accumulated = 0usize;
            let mut split_idx = None;
            for (idx, buf) in self.buffers.iter().enumerate() {
                accumulated += buf.buffer.len();
                if accumulated > threshold {
                    split_idx = Some(idx);
                    break;
                }
            }
            let Some(idx) = split_idx else {
                break;
            };
            let rest = self.buffers.split_off(idx + 1);
            let prefix = mem::replace(&mut self.buffers, rest);
            let flushed_size = prefix.iter().map(|b| b.buffer.len()).sum::<usize>();
            self.size = self.buffers.iter().map(|b| b.buffer.len()).sum::<usize>();
            batches.push(FileOutputBuffers::create_block(
                prefix,
                self.partition.clone(),
            ));
            // guard against unexpected size drift
            if flushed_size == 0 {
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

    partitions: HashMap<Option<Arc<str>>, PartitionBuffer>,
    pending: VecDeque<DataBlock>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        max_file_size: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            threshold: max_file_size,
            input_data: None,
            partitions: HashMap::new(),
            pending: VecDeque::new(),
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }

    fn enqueue_flush(&mut self, bucket: PartitionBuffer) {
        if bucket.buffers.is_empty() {
            return;
        }
        let partition = bucket.partition.clone();
        self.pending
            .push_back(FileOutputBuffers::create_block(bucket.buffers, partition));
    }

    fn flush_all(&mut self) {
        let buckets = std::mem::take(&mut self.partitions);
        for (_, bucket) in buckets {
            self.enqueue_flush(bucket);
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

    fn event(&mut self) -> databend_common_exception::Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.pending.pop_front() {
            self.output.push_data(Ok(data));
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
            if self.partitions.is_empty() && self.pending.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            self.flush_all();
            if let Some(data) = self.pending.pop_front() {
                self.output.push_data(Ok(data));
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
        let block_meta = block.get_owned_meta().unwrap();
        let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
        let partition = buffers.partition.clone();
        let bucket = self
            .partitions
            .entry(partition.clone())
            .or_insert_with(|| PartitionBuffer::new(partition.clone()));

        bucket.size += buffers
            .buffers
            .iter()
            .map(|b| b.buffer.len())
            .sum::<usize>();
        bucket.buffers.extend(buffers.buffers);

        if bucket.size > self.threshold {
            let batches = bucket.drain_ready_batches(self.threshold);
            self.pending.extend(batches);
        }
        Ok(())
    }
}
