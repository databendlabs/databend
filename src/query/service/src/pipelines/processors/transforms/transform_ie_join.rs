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

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_pipeline_sinks::Sink;

use crate::pipelines::processors::transforms::IEJoinState;

enum IEJoinStep {
    Sink,
    Merging,
    // Execute ie_join algo,
    Execute,
}

pub struct TransformIEJoinLeft {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,
    state: Arc<IEJoinState>,
    step: IEJoinStep,
    execute_finished: bool,
}

impl TransformIEJoinLeft {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ie_join_state: Arc<IEJoinState>,
    ) -> Box<dyn Processor> {
        ie_join_state.left_attach();
        Box::new(TransformIEJoinLeft {
            input_port,
            output_port,
            input_data: None,
            output_data_blocks: Default::default(),
            state: ie_join_state,
            step: IEJoinStep::Sink,
            execute_finished: false,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformIEJoinLeft {
    fn name(&self) -> String {
        "TransformIEJoinLeft".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            IEJoinStep::Sink => {
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }
                if self.input_port.is_finished() {
                    self.state.left_detach()?;
                    self.step = IEJoinStep::Merging;
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
            IEJoinStep::Execute => {
                if !self.output_data_blocks.is_empty() {
                    let data = self.output_data_blocks.pop_front().unwrap();
                    self.output_port.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                } else if !self.execute_finished {
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
            IEJoinStep::Sink => {
                if let Some(data_block) = self.input_data.take() {
                    self.state.sink_left(data_block)?;
                }
            }
            IEJoinStep::Execute => {
                let task_id = self.state.task_id();
                if let Some(task_id) = task_id {
                    let res = self.state.ie_join(task_id)?;
                    if !res.is_empty() {
                        self.output_data_blocks.push_back(res);
                    }
                } else {
                    self.execute_finished = true;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let IEJoinStep::Merging = self.step {
            self.state.wait_merge_finish().await?;
            self.step = IEJoinStep::Execute;
        }
        Ok(())
    }
}

pub struct TransformIEJoinRight {
    state: Arc<IEJoinState>,
}

impl TransformIEJoinRight {
    pub fn create(ie_join_state: Arc<IEJoinState>) -> Self {
        ie_join_state.right_attach();
        TransformIEJoinRight {
            state: ie_join_state,
        }
    }
}

impl Sink for TransformIEJoinRight {
    const NAME: &'static str = "TransformIEJoinRight";

    fn on_finish(&mut self) -> Result<()> {
        self.state.right_detach();
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.state.sink_right(data_block)
    }
}
