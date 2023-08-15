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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::FunctionContext;
use common_sql::optimizer::ColumnSet;
use common_sql::plans::JoinType;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::hash_join::HashJoinState;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::Processor;

pub(crate) enum HashJoinStep {
    Build,
    Finalize,
    Probe,
    FinalScan,
    FastReturn,
}

pub struct TransformHashJoinBuild {
    input_port: Arc<InputPort>,

    input_data: Option<DataBlock>,
    step: HashJoinStep,
    join_state: Arc<dyn HashJoinState>,
    finalize_finished: bool,
}

impl TransformHashJoinBuild {
    pub fn create(
        input_port: Arc<InputPort>,
        join_state: Arc<dyn HashJoinState>,
    ) -> Box<dyn Processor> {
        Box::new(TransformHashJoinBuild {
            input_port,
            input_data: None,
            step: HashJoinStep::Build,
            join_state,
            finalize_finished: false,
        })
    }

    pub fn attach(join_state: Arc<dyn HashJoinState>) -> Result<Arc<dyn HashJoinState>> {
        join_state.build_attach()?;
        Ok(join_state)
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoinBuild {
    fn name(&self) -> String {
        "HashJoinBuild".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            HashJoinStep::Build => {
                if self.input_data.is_some() {
                    return Ok(Event::Sync);
                }

                if self.input_port.is_finished() {
                    self.join_state.build_done()?;
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
            HashJoinStep::Finalize => match self.finalize_finished {
                false => Ok(Event::Sync),
                true => Ok(Event::Finished),
            },
            HashJoinStep::Probe => unreachable!(),
            HashJoinStep::FinalScan => unreachable!(),
            HashJoinStep::FastReturn => Ok(Event::Finished),
        }
    }

    fn interrupt(&self) {
        self.join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            HashJoinStep::Build => {
                if let Some(data_block) = self.input_data.take() {
                    self.join_state.build(data_block)?;
                }
                Ok(())
            }
            HashJoinStep::Finalize => {
                if let Some(task) = self.join_state.finalize_task() {
                    self.join_state.finalize(task)
                } else {
                    self.finalize_finished = true;
                    self.join_state.finalize_done()
                }
            }
            HashJoinStep::Probe | HashJoinStep::FinalScan | HashJoinStep::FastReturn => {
                unreachable!()
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let HashJoinStep::Build = &self.step {
            self.join_state.wait_build_finish().await?;
            if self.join_state.fast_return()? {
                self.step = HashJoinStep::FastReturn;
                return Ok(());
            }
            self.step = HashJoinStep::Finalize;
        }
        Ok(())
    }
}
