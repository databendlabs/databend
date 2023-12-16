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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

pub type Aborting = Arc<Box<dyn Fn() -> bool + Send + Sync + 'static>>;

pub struct TransformCompact<T: Compactor + Send + 'static> {
    state: ProcessorState,
    compactor: T,
}

/// Compactor is a trait that defines how to compact blocks.
pub trait Compactor {
    fn name() -> &'static str;

    /// `use_partial_compact` enable the compactor to compact the blocks when a new block is pushed
    fn use_partial_compact(&self) -> bool {
        false
    }

    fn interrupt(&self) {}

    /// `compact_partial` is called when a new block is pushed and `use_partial_compact` is enabled
    fn compact_partial(&mut self, _blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        Ok(vec![])
    }

    /// `compact_final` is called when all the blocks are pushed to finish the compaction
    fn compact_final(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>>;
}

impl<T: Compactor + Send + 'static> TransformCompact<T> {
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        compactor: T,
    ) -> Result<Box<dyn Processor>> {
        let state = ProcessorState::Consume(ConsumeState {
            input_port,
            output_port,
            input_data_blocks: vec![],
            output_data_blocks: VecDeque::new(),
        });

        Ok(Box::new(Self { state, compactor }))
    }

    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let ProcessorState::Consume(state) = &mut self.state {
            if !state.output_data_blocks.is_empty() {
                if state.output_port.can_push() {
                    let block = state.output_data_blocks.pop_front().unwrap();
                    state.output_port.push_data(Ok(block));
                }
                return Ok(Event::NeedConsume);
            }

            if state.input_port.has_data() {
                let data_block = state.input_port.pull_data().unwrap()?;
                state.input_data_blocks.push(data_block);

                if T::use_partial_compact(&self.compactor) {
                    return Ok(Event::Sync);
                }
            }

            if state.input_port.is_finished() {
                let mut temp_state = ProcessorState::Finished;
                std::mem::swap(&mut self.state, &mut temp_state);
                temp_state = temp_state.convert_to_compacting_state()?;
                std::mem::swap(&mut self.state, &mut temp_state);
                return Ok(Event::Sync);
            }

            state.input_port.set_need_data();
            return Ok(Event::NeedData);
        }

        Err(ErrorCode::Internal("It's a bug"))
    }
}

#[async_trait::async_trait]
impl<T: Compactor + Send + 'static> Processor for TransformCompact<T> {
    fn name(&self) -> String {
        T::name().to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match &mut self.state {
            ProcessorState::Finished => Ok(Event::Finished),
            ProcessorState::Consume(_) => self.consume_event(),
            ProcessorState::Compacting(_) => Err(ErrorCode::Internal("It's a bug.")),
            ProcessorState::Compacted(state) => {
                if state.output_port.is_finished() {
                    debug_assert!(state.input_port.is_finished());
                    return Ok(Event::Finished);
                }

                if !state.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                match state.compacted_blocks.pop_front() {
                    None => {
                        state.output_port.finish();
                        Ok(Event::Finished)
                    }
                    Some(data) => {
                        state.output_port.push_data(Ok(data));
                        Ok(Event::NeedConsume)
                    }
                }
            }
        }
    }

    fn interrupt(&self) {
        self.compactor.interrupt();
    }

    fn process(&mut self) -> Result<()> {
        match &mut self.state {
            ProcessorState::Consume(state) => {
                let compacted_blocks = self
                    .compactor
                    .compact_partial(&mut state.input_data_blocks)?;

                for b in compacted_blocks {
                    state.output_data_blocks.push_back(b);
                }
                Ok(())
            }
            ProcessorState::Compacting(state) => {
                let compacted_blocks = self
                    .compactor
                    .compact_final(std::mem::take(&mut state.blocks))?;

                let mut temp_state = ProcessorState::Finished;
                std::mem::swap(&mut self.state, &mut temp_state);
                temp_state = temp_state.convert_to_compacted_state(compacted_blocks)?;
                std::mem::swap(&mut self.state, &mut temp_state);
                debug_assert!(matches!(temp_state, ProcessorState::Finished));
                Ok(())
            }
            _ => Err(ErrorCode::Internal("State invalid. it's a bug.")),
        }
    }
}

enum ProcessorState {
    Consume(ConsumeState),
    Compacting(CompactingState),
    Compacted(CompactedState),
    Finished,
}

pub struct CompactedState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    compacted_blocks: VecDeque<DataBlock>,
}

pub struct ConsumeState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data_blocks: Vec<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
}

pub struct CompactingState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    blocks: Vec<DataBlock>,
}

impl ProcessorState {
    #[inline(always)]
    fn convert_to_compacting_state(self) -> Result<Self> {
        match self {
            ProcessorState::Consume(state) => Ok(ProcessorState::Compacting(CompactingState {
                input_port: state.input_port,
                output_port: state.output_port,
                blocks: state.input_data_blocks,
            })),
            _ => Err(ErrorCode::Internal("State invalid, must be consume state")),
        }
    }

    #[inline(always)]
    fn convert_to_compacted_state(self, compacted_blocks: Vec<DataBlock>) -> Result<Self> {
        match self {
            ProcessorState::Compacting(state) => {
                let compacted_blocks = VecDeque::from(compacted_blocks);
                Ok(ProcessorState::Compacted(CompactedState {
                    input_port: state.input_port,
                    output_port: state.output_port,
                    compacted_blocks,
                }))
            }
            _ => Err(ErrorCode::Internal(
                "State invalid, must be compacted state",
            )),
        }
    }
}
