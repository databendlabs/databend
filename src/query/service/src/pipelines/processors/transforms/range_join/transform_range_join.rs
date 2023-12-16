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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_sinks::Sink;

use crate::pipelines::processors::transforms::range_join::RangeJoinState;

enum RangeJoinStep {
    Sink,
    Merging,
    // Execute ie_join algo,
    Execute,
}

pub struct TransformRangeJoinLeft {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    state: Arc<RangeJoinState>,
    step: RangeJoinStep,
    execute_finished: bool,
}

impl TransformRangeJoinLeft {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ie_join_state: Arc<RangeJoinState>,
    ) -> Box<dyn Processor> {
        ie_join_state.left_attach();
        Box::new(TransformRangeJoinLeft {
            input_port,
            output_port,
            input_data: None,
            output_data_blocks: Default::default(),
            state: ie_join_state,
            step: RangeJoinStep::Sink,
            execute_finished: false,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRangeJoinLeft {
    fn name(&self) -> String {
        if self.state.ie_join_state.is_some() {
            "TransformIEJoinLeft".to_string()
        } else {
            "TransformMergeJoinLeft".to_string()
        }
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            RangeJoinStep::Sink => {
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }
                if self.input_port.is_finished() {
                    self.state.left_detach()?;
                    self.step = RangeJoinStep::Merging;
                    return Ok(Event::Async);
                }
                match self.input_port.has_data() {
                    true => {
                        self.input_data = Some(self.input_port.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    }
                    false => {
                        self.input_port.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
            }
            RangeJoinStep::Execute => {
                if self.output_port.is_finished() {
                    return Ok(Event::Finished);
                }

                if !self.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                if !self.output_data_blocks.is_empty() {
                    let data = self.output_data_blocks.pop_front().unwrap();
                    self.output_port.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }

                if !self.execute_finished {
                    Ok(Event::Sync)
                } else {
                    self.output_port.finish();
                    Ok(Event::Finished)
                }
            }
            _ => unreachable!(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            RangeJoinStep::Sink => {
                if let Some(data_block) = self.input_data.take() {
                    self.state.sink_left(data_block)?;
                }
            }
            RangeJoinStep::Execute => {
                let task_id = self.state.task_id();
                if let Some(task_id) = task_id {
                    let res = match self.state.ie_join_state {
                        Some(ref _ie_join_state) => self.state.ie_join(task_id)?,
                        None => self.state.merge_join(task_id)?,
                    };
                    for block in res {
                        if !block.is_empty() {
                            self.output_data_blocks.push_back(block);
                        }
                    }
                } else {
                    self.execute_finished = true;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let RangeJoinStep::Merging = self.step {
            self.state.wait_merge_finish().await?;
            self.step = RangeJoinStep::Execute;
        }
        Ok(())
    }
}

pub struct TransformRangeJoinRight {
    state: Arc<RangeJoinState>,
}

impl TransformRangeJoinRight {
    pub fn create(ie_join_state: Arc<RangeJoinState>) -> Self {
        ie_join_state.right_attach();
        TransformRangeJoinRight {
            state: ie_join_state,
        }
    }
}

impl Sink for TransformRangeJoinRight {
    const NAME: &'static str = "TransformRangeJoinRight";

    fn on_finish(&mut self) -> Result<()> {
        self.state.right_detach()?;
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.state.sink_right(data_block)
    }
}
