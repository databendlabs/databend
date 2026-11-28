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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::slice::IndexPartitionSlices;

/// Replays the partitions in id order and hands each writer the slices of the outputs it owns
/// (`output % writers`), the way the k-way merge sort combiner replays its tasks.
///
/// Every reader receives partitions in increasing id order, so the partition the scatter waits
/// for sits at the head of exactly one reader, or is still being read there. One block per
/// reader is held; a reader whose held block is not next is not pulled from, which stalls it
/// after its one buffered block and bounds the partitions in flight to about three per reader.
/// The next partition is taken only when every writer accepted the current one, so writers see
/// the partitions in order without buffering. The outputs finish once every reader finished
/// and nothing is held.
pub struct ReclusterIndexSliceScatter {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,
    /// One block per reader, with its partition id.
    heads: Vec<Option<(u32, DataBlock)>>,
    next: u32,
    /// The block being split, between `event` and `process`.
    splitting: Option<DataBlock>,
    /// The current partition's part for every writer, until the writer takes it.
    parts: Vec<Option<DataBlock>>,
}

impl ReclusterIndexSliceScatter {
    pub fn create(inputs: Vec<Arc<InputPort>>, outputs: Vec<Arc<OutputPort>>) -> ProcessorPtr {
        let mut heads = Vec::with_capacity(inputs.len());
        heads.resize_with(inputs.len(), || None);
        let mut parts = Vec::with_capacity(outputs.len());
        parts.resize_with(outputs.len(), || None);
        ProcessorPtr::create(Box::new(Self {
            inputs,
            outputs,
            heads,
            next: 0,
            splitting: None,
            parts,
        }))
    }

    fn partition_id(block: &DataBlock) -> Result<u32> {
        let Some(meta) = block.get_meta() else {
            return Err(ErrorCode::Internal(
                "missing recluster index partition slices",
            ));
        };
        let Some(result) = IndexPartitionSlices::downcast_ref_from(meta) else {
            return Err(ErrorCode::Internal(
                "unexpected recluster index partition slices",
            ));
        };
        Ok(result.id)
    }

    /// Pushes the parts whose writer can take them; true if any part is still held.
    fn push_parts(&mut self) -> bool {
        let mut held = false;
        for (index, output) in self.outputs.iter().enumerate() {
            if output.is_finished() {
                self.parts[index] = None;
                continue;
            }
            if self.parts[index].is_none() {
                continue;
            }
            if output.can_push() {
                let part = self.parts[index].take().expect("checked above");
                output.push_data(Ok(part));
            } else {
                held = true;
            }
        }
        held
    }

    /// Takes the head blocks the readers pushed and asks the readers without one for more.
    fn pull_heads(&mut self) -> Result<()> {
        for (index, input) in self.inputs.iter().enumerate() {
            if self.heads[index].is_some() {
                continue;
            }
            if input.has_data() {
                let block = input.pull_data().unwrap()?;
                let id = Self::partition_id(&block)?;
                if id < self.next {
                    return Err(ErrorCode::Internal(format!(
                        "recluster index partition {id} arrived after partition {}",
                        self.next
                    )));
                }
                self.heads[index] = Some((id, block));
            } else if !input.is_finished() {
                input.set_need_data();
            }
        }
        Ok(())
    }

    /// The reader holding the next partition, if it arrived.
    fn next_head(&self) -> Option<usize> {
        for (index, head) in self.heads.iter().enumerate() {
            if let Some((id, _)) = head
                && *id == self.next
            {
                return Some(index);
            }
        }
        None
    }

    fn all_inputs_finished(&self) -> bool {
        for (index, input) in self.inputs.iter().enumerate() {
            if self.heads[index].is_some() || !input.is_finished() {
                return false;
            }
        }
        true
    }
}

impl Processor for ReclusterIndexSliceScatter {
    fn name(&self) -> String {
        "ReclusterIndexSliceScatter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let mut all_outputs_finished = true;
        for output in &self.outputs {
            if !output.is_finished() {
                all_outputs_finished = false;
            }
        }
        if all_outputs_finished {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }
        if self.splitting.is_some() {
            return Ok(Event::Sync);
        }
        if self.push_parts() {
            return Ok(Event::NeedConsume);
        }

        self.pull_heads()?;
        if let Some(index) = self.next_head() {
            let (_, block) = self.heads[index].take().expect("found above");
            self.next += 1;
            if !self.inputs[index].is_finished() {
                self.inputs[index].set_need_data();
            }
            self.splitting = Some(block);
            return Ok(Event::Sync);
        }
        if self.all_inputs_finished() {
            for output in &self.outputs {
                output.finish();
            }
            return Ok(Event::Finished);
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let Some(mut block) = self.splitting.take() else {
            return Ok(());
        };
        let Some(meta) = block.take_meta() else {
            return Err(ErrorCode::Internal(
                "missing recluster index partition slices",
            ));
        };
        let Some(result) = IndexPartitionSlices::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "unexpected recluster index partition slices",
            ));
        };
        let writers = self.outputs.len();
        let mut parts = Vec::with_capacity(writers);
        for _ in 0..writers {
            parts.push(IndexPartitionSlices {
                id: result.id,
                index: result.index,
                slices: Vec::new(),
            });
        }
        for slice in result.slices {
            parts[slice.output as usize % writers].slices.push(slice);
        }
        for (slot, part) in self.parts.iter_mut().zip(parts) {
            *slot = Some(DataBlock::empty_with_meta(Box::new(part)));
        }
        Ok(())
    }
}
