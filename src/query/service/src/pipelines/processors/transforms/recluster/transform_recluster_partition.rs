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
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storages_fuse::io::StreamBlockBuilder;
use databend_common_storages_fuse::io::StreamBlockProperties;

use crate::pipelines::processors::transforms::WindowPartitionMeta;

enum Step {
    Consume,
    Collect,
    Flush,
}

struct PartitionData {
    builder: Option<StreamBlockBuilder>,
    data_blocks: Vec<DataBlock>,
    block_size: usize,
    block_rows: usize,
}

impl PartitionData {
    fn new() -> Self {
        Self {
            builder: None,
            data_blocks: vec![],
            block_size: 0,
            block_rows: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.builder.as_ref().is_none_or(|v| v.is_empty()) && self.data_blocks.is_empty()
    }
}

pub struct TransformReclusterPartition {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    properties: Arc<StreamBlockProperties>,

    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    partition_data: Vec<PartitionData>,
    output_data: VecDeque<DataBlock>,

    start: Instant,
    cnt: usize,

    step: Step,
}

impl TransformReclusterPartition {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        properties: Arc<StreamBlockProperties>,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
    ) -> Result<ProcessorPtr> {
        let partitions = (0..num_partitions)
            .filter(|&partition| (partition * num_processors) / num_partitions == processor_id)
            .collect::<Vec<_>>();
        let mut partition_id = vec![0; num_partitions];
        let mut partition_data = Vec::with_capacity(num_partitions);
        for (new_partition_id, partition) in partitions.iter().enumerate() {
            partition_id[*partition] = new_partition_id;
            partition_data.push(PartitionData::new());
        }
        Ok(ProcessorPtr::create(Box::new(
            TransformReclusterPartition {
                input,
                output,
                properties,
                partition_id,
                partition_data,
                output_data: VecDeque::new(),
                step: Step::Consume,
                start: Instant::now(),
                cnt: 0,
            },
        )))
    }
}

impl Processor for TransformReclusterPartition {
    fn name(&self) -> String {
        "TransformReclusterPartition".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.step, Step::Collect | Step::Flush) {
            return Ok(Event::Sync);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            if !self.partition_data.is_empty() {
                if self.cnt == 0 {
                    log::info!("Recluster: start flush: {:?}", self.start.elapsed());
                }
                self.cnt += 1;
                self.step = Step::Flush;
                return Ok(Event::Sync);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.step = Step::Collect;
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.step, Step::Consume) {
            Step::Collect => {
                let data_block = self.input.pull_data().unwrap()?;
                if let Some(meta) = data_block
                    .get_owned_meta()
                    .and_then(WindowPartitionMeta::downcast_from)
                {
                    for (partition_id, data_block) in meta.partitioned_data.into_iter() {
                        if data_block.is_empty() {
                            continue;
                        }

                        let new_id = self.partition_id[partition_id];
                        let partition_data =
                            unsafe { self.partition_data.get_unchecked_mut(new_id) };
                        if partition_data.builder.is_none() {
                            partition_data.builder = Some(StreamBlockBuilder::try_new_with_config(
                                self.properties.clone(),
                            )?);
                        }
                        let builder = partition_data.builder.as_mut().unwrap();
                        if !builder.need_flush() {
                            builder.write(data_block)?;
                        } else {
                            partition_data.block_size += data_block.estimate_block_size();
                            partition_data.block_rows += data_block.num_rows();
                            partition_data.data_blocks.push(data_block);

                            if self.properties.check_large_enough(
                                partition_data.block_rows,
                                partition_data.block_size,
                            ) {
                                let builder = partition_data.builder.take().unwrap();
                                let serialized = builder.finish()?;
                                self.output_data
                                    .push_back(DataBlock::empty_with_meta(Box::new(serialized)));

                                let mut builder = StreamBlockBuilder::try_new_with_config(
                                    self.properties.clone(),
                                )?;
                                for block in
                                    std::mem::take(&mut partition_data.data_blocks).into_iter()
                                {
                                    builder.write(block)?;
                                }
                                partition_data.builder = Some(builder);
                                partition_data.block_rows = 0;
                                partition_data.block_size = 0;
                            }
                        }
                    }
                }
            }
            Step::Flush => {
                while let Some(mut partition_data) = self.partition_data.pop() {
                    if partition_data.is_empty() {
                        continue;
                    }

                    let mut builder = if partition_data.builder.is_none() {
                        StreamBlockBuilder::try_new_with_config(self.properties.clone())?
                    } else {
                        partition_data.builder.take().unwrap()
                    };
                    for block in partition_data.data_blocks {
                        builder.write(block)?;
                    }
                    let serialized = builder.finish()?;
                    self.output_data
                        .push_back(DataBlock::empty_with_meta(Box::new(serialized)));
                    break;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
