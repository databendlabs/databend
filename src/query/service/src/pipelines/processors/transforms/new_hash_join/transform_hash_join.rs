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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::ColumnSet;
use tokio::sync::Barrier;

use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;

pub struct TransformHashJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,

    joined_port: Arc<OutputPort>,

    stage: Stage,
    join: Box<dyn Join>,
    joined_data: Option<DataBlock>,
    stage_sync_barrier: Arc<Barrier>,
    projection: ColumnSet,
    initialize: bool,
}

impl TransformHashJoin {
    pub fn create(
        build_port: Arc<InputPort>,
        probe_port: Arc<InputPort>,
        joined_port: Arc<OutputPort>,
        join: Box<dyn Join>,
        stage_sync_barrier: Arc<Barrier>,
        projection: ColumnSet,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(TransformHashJoin {
            build_port,
            probe_port,
            joined_port,
            join,
            joined_data: None,
            stage_sync_barrier,
            projection,
            initialize: false,
            stage: Stage::Build(BuildState {
                finished: false,
                build_data: None,
            }),
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoin {
    fn name(&self) -> String {
        String::from("TransformHashJoin")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.joined_port.is_finished() {
            self.build_port.finish();
            self.probe_port.finish();

            return match &self.stage {
                Stage::Finished => Ok(Event::Finished),
                _ => Ok(Event::Async),
            };
        }

        if !self.joined_port.can_push() {
            match self.stage {
                Stage::Build(_) => self.build_port.set_not_need_data(),
                Stage::Probe(_) => self.probe_port.set_not_need_data(),
                Stage::BuildFinal(_) | Stage::ProbeFinal(_) | Stage::Finished => (),
            }

            return Ok(Event::NeedConsume);
        }

        if let Some(joined_data) = self.joined_data.take() {
            let joined_data = joined_data.project(&self.projection);
            self.joined_port.push_data(Ok(joined_data));
            return Ok(Event::NeedConsume);
        }

        match &mut self.stage {
            Stage::Build(state) => match state.event(&self.build_port)? {
                Event::NeedData if !self.initialize => {
                    self.initialize = true;
                    self.probe_port.set_need_data();
                    Ok(Event::NeedData)
                }
                other => Ok(other),
            },
            Stage::BuildFinal(state) => state.event(),
            Stage::Probe(state) => state.event(&self.probe_port),
            Stage::ProbeFinal(state) => state.event(&self.joined_port),
            Stage::Finished => {
                self.joined_port.finish();
                Ok(Event::Finished)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match &mut self.stage {
            Stage::Finished => Ok(()),
            Stage::Build(state) => {
                let Some(data_block) = state.build_data.take() else {
                    if !state.finished {
                        state.finished = true;
                        self.join.add_block(None)?;
                    }
                    return Ok(());
                };

                if !data_block.is_empty() {
                    self.join.add_block(Some(data_block))?;
                }

                Ok(())
            }
            Stage::BuildFinal(state) => {
                state.finished = self.join.final_build()?.is_none();
                Ok(())
            }
            Stage::Probe(state) => {
                if let Some(probe_data) = state.input_data.take() {
                    let stream = self.join.probe_block(probe_data)?;
                    state.stream = Some(stream);
                }

                if let Some(mut stream) = state.stream.take() {
                    if let Some(joined_data) = stream.next()? {
                        self.joined_data = Some(joined_data);
                        state.stream = Some(stream);
                    }
                }

                Ok(())
            }
            Stage::ProbeFinal(state) => {
                if !state.initialized {
                    state.initialized = true;
                    state.stream = Some(self.join.final_probe()?);
                }

                if let Some(mut stream) = state.stream.take() {
                    if let Some(joined_data) = stream.next()? {
                        self.joined_data = Some(joined_data);
                        state.stream = Some(stream);
                    }
                }

                Ok(())
            }
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        let _wait_res = self.stage_sync_barrier.wait().await;

        self.stage = match self.stage {
            Stage::Build(_) => Stage::BuildFinal(BuildFinalState::new()),
            Stage::BuildFinal(_) => Stage::Probe(ProbeState::new()),
            Stage::Probe(_) => Stage::ProbeFinal(ProbeFinalState::new()),
            Stage::ProbeFinal(_) => Stage::Finished,
            Stage::Finished => Stage::Finished,
        };

        Ok(())
    }
}

#[derive(Debug)]
enum Stage {
    Build(BuildState),
    BuildFinal(BuildFinalState),
    Probe(ProbeState),
    ProbeFinal(ProbeFinalState),
    Finished,
}

#[derive(Debug)]
struct BuildState {
    finished: bool,
    build_data: Option<DataBlock>,
}

impl BuildState {
    pub fn event(&mut self, input: &InputPort) -> Result<Event> {
        if self.build_data.is_some() {
            return Ok(Event::Sync);
        }

        if input.has_data() {
            self.build_data = Some(input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if input.is_finished() {
            return match self.finished {
                true => Ok(Event::Async),
                false => Ok(Event::Sync),
            };
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }
}

#[derive(Debug)]
struct BuildFinalState {
    finished: bool,
}

impl BuildFinalState {
    pub fn new() -> BuildFinalState {
        BuildFinalState { finished: false }
    }

    pub fn event(&mut self) -> Result<Event> {
        match self.finished {
            true => Ok(Event::Async),
            false => Ok(Event::Sync),
        }
    }
}

struct ProbeState {
    input_data: Option<DataBlock>,
    stream: Option<Box<dyn JoinStream>>,
}

impl Debug for ProbeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeState").finish()
    }
}

impl ProbeState {
    pub fn new() -> ProbeState {
        ProbeState {
            input_data: None,
            stream: None,
        }
    }

    pub fn event(&mut self, input: &InputPort) -> Result<Event> {
        if self.input_data.is_some() || self.stream.is_some() {
            return Ok(Event::Sync);
        }

        if input.has_data() {
            self.input_data = Some(input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if input.is_finished() {
            return Ok(Event::Async);
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }
}

struct ProbeFinalState {
    initialized: bool,
    stream: Option<Box<dyn JoinStream>>,
}

impl Debug for ProbeFinalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeFinalState")
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl ProbeFinalState {
    pub fn new() -> ProbeFinalState {
        ProbeFinalState {
            initialized: false,
            stream: None,
        }
    }

    pub fn event(&mut self, output_port: &OutputPort) -> Result<Event> {
        if self.stream.is_some() {
            return Ok(Event::Sync);
        }

        if self.initialized {
            output_port.finish();
            return Ok(Event::Async);
        }

        Ok(Event::Sync)
    }
}
