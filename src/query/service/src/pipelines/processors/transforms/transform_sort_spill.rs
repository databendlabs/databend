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

use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::Event;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;

// use crate::spillers::Spiller;

/// The partition id used for the [`Spiller`] is always 0 for spill sorted data.
// const SPILL_PID: u8 = 0;

enum State {
    /// The initial state of the processor.
    Init,
    /// This state means the processor will never spill incoming blocks.
    NoSpill,
    /// This state means the processor will spill incoming blocks except the last block.
    Spill,
    /// This state means the processor is doing external merge sort.
    Merging,
    /// Finish the process.
    MergeFinished,
}

pub struct TransformSortSpill {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    state: State,
    // spiller: Spiller,
}

#[inline(always)]
fn need_spill(block: &DataBlock) -> bool {
    block
        .get_meta()
        .and_then(SortSpillMeta::downcast_ref_from)
        .is_some()
}

#[async_trait::async_trait]
impl Processor for TransformSortSpill {
    fn name(&self) -> String {
        String::from("TransformSortSpill")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if matches!(self.state, State::MergeFinished) && self.output_data.is_none() {
            debug_assert!(self.input.is_finished());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            debug_assert!(matches!(self.state, State::Merging | State::MergeFinished));
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            return match &self.state {
                State::Init => {
                    if need_spill(&block) {
                        // Need to spill this block.
                        self.input_data = Some(block);
                        self.state = State::Spill;
                        Ok(Event::Async)
                    } else {
                        // If we get a memory block at initial state, it means we will never spill data.
                        // debug_assert_eq!(self.spiller.spilled_files_num(SPILL_PID), 0);
                        self.output.push_data(Ok(block));
                        self.state = State::NoSpill;
                        Ok(Event::NeedConsume)
                    }
                }
                State::NoSpill => {
                    debug_assert!(!need_spill(&block));
                    self.output.push_data(Ok(block));
                    self.state = State::NoSpill;
                    Ok(Event::NeedConsume)
                }
                State::Spill => {
                    if need_spill(&block) {
                        // It means we get the last block.
                        // We can launch external merge sort now.
                        self.state = State::Merging;
                    }
                    self.input_data = Some(block);
                    Ok(Event::Async)
                }
                _ => unreachable!(),
            };
        }

        if self.input.is_finished() {
            return match &self.state {
                State::Init | State::NoSpill | State::MergeFinished => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                State::Spill => {
                    // No more input data, we can launch external merge sort now.
                    self.state = State::Merging;
                    Ok(Event::Async)
                }
                State::Merging => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.state {
            State::Spill => {
                let block = self.input_data.take().unwrap();
                self.spill(block).await?;
            }
            State::Merging => {
                let block = self.input_data.take();
                self.merge_sort(block).await?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl TransformSortSpill {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        // spiller: Spiller
    ) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            input_data: None,
            output_data: None,
            // spiller,
            state: State::Init,
        })
    }

    async fn spill(&mut self, _block: DataBlock) -> Result<()> {
        // TODO(spill)
        // pid and worker id is not used in current processor.
        // self.spiller.spill_with_partition(0, block, 0).await
        Ok(())
    }

    /// Do an external merge sort. If `block` is not [None], we need to merge it with spilled files.
    async fn merge_sort(&mut self, _block: Option<DataBlock>) -> Result<()> {
        // TODO(spill)
        self.state = State::MergeFinished;
        Ok(())
    }
}

/// Mark a partially sorted [`DataBlock`] as a block needs to be spilled.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SortSpillMeta {}

#[typetag::serde(name = "sort_spill")]
impl BlockMetaInfo for SortSpillMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals SortSpillMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone SortSpillMeta")
    }
}
