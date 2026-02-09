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
use std::marker::PhantomPinned;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Barrier;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::ColumnSet;
use log::info;

use crate::pipelines::processors::transforms::RuntimeFilterLocalBuilder;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::runtime_filter::RuntimeFiltersDesc;

pub struct TransformHashJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,
    joined_port: Arc<OutputPort>,

    stage: Stage,
    join: Box<dyn Join>,
    joined_data: Option<DataBlock>,
    stage_sync_barrier: Arc<Barrier>,
    projection: ColumnSet,
    rf_desc: Arc<RuntimeFiltersDesc>,
    runtime_filter_builder: Option<RuntimeFilterLocalBuilder>,
    instant: Instant,
    _p: PhantomPinned,
}

impl TransformHashJoin {
    pub fn create(
        build_port: Arc<InputPort>,
        probe_port: Arc<InputPort>,
        joined_port: Arc<OutputPort>,
        join: Box<dyn Join>,
        stage_sync_barrier: Arc<Barrier>,
        projection: ColumnSet,
        rf_desc: Arc<RuntimeFiltersDesc>,
    ) -> Result<ProcessorPtr> {
        let runtime_filter_builder = RuntimeFilterLocalBuilder::try_create(
            &rf_desc.func_ctx,
            rf_desc.filters_desc.clone(),
            rf_desc.inlist_threshold,
            rf_desc.bloom_threshold,
            rf_desc.min_max_threshold,
        )?;

        Ok(ProcessorPtr::create(Box::new(TransformHashJoin {
            build_port,
            probe_port,
            joined_port,
            join,
            rf_desc,
            projection,
            stage_sync_barrier,
            joined_data: None,
            runtime_filter_builder,
            stage: Stage::Build(BuildState {
                finished: false,
                build_data: None,
            }),
            instant: Instant::now(),
            _p: PhantomPinned,
        })))
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

            if !matches!(self.stage, Stage::Finished) {
                self.stage = Stage::Finished;
                self.stage_sync_barrier.reduce_quorum(1);
            }

            return Ok(Event::Finished);
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
            Stage::Build(state) => state.event(&self.build_port),
            Stage::BuildFinal(state) => state.event(),
            Stage::Probe(state) => state.event(&self.probe_port),
            Stage::ProbeFinal(state) => state.event(&self.joined_port),
            Stage::Finished => Ok(Event::Finished),
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
                    if let Some(builder) = self.runtime_filter_builder.as_mut() {
                        builder.add_block(&data_block)?;
                    }
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
                    // This is safe because both join and stream are properties of the struct.
                    state.stream = Some(unsafe {
                        std::mem::transmute::<Box<dyn JoinStream + '_>, Box<dyn JoinStream>>(stream)
                    });
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
                if state.stream.is_none() {
                    if let Some(final_stream) = self.join.final_probe()? {
                        state.initialize = true;
                        // This is safe because both join and stream are properties of the struct.
                        state.stream = Some(unsafe {
                            std::mem::transmute::<Box<dyn JoinStream + '_>, Box<dyn JoinStream>>(
                                final_stream,
                            )
                        });
                    } else {
                        state.finished = true;
                    }
                }

                if let Some(mut stream) = state.stream.take() {
                    if let Some(joined_data) = stream.next()? {
                        self.joined_data = Some(joined_data);
                        state.stream = Some(stream);
                    } else {
                        state.initialize = false;
                    }
                }

                Ok(())
            }
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        let elapsed = self.instant.elapsed();
        let wait_res = self.stage_sync_barrier.wait().await;

        self.stage = match &mut self.stage {
            Stage::Build(_) => {
                if let Some(builder) = self.runtime_filter_builder.take() {
                    let spill_happened = self.join.is_spill_happened();
                    // Disable runtime filters once spilling occurs to avoid partial-build filters
                    // being globalized across the cluster, which can prune valid probe rows.
                    let packet = builder.finish(spill_happened)?;
                    self.join.add_runtime_filter_packet(packet);
                }

                let rf_build_elapsed = self.instant.elapsed() - elapsed;
                let _wait_res = self.stage_sync_barrier.wait().await;
                let before_wait = self.instant.elapsed();

                if wait_res.is_leader() {
                    let spilled = self.join.is_spill_happened();
                    let packet = self.join.build_runtime_filter()?;
                    if let Some(packets) = &packet.packets {
                        info!(
                            "spilled: {}, globalize runtime filter: total {}, disable_all_due_to_spill: {}",
                            spilled,
                            packets.len(),
                            packet.disable_all_due_to_spill
                        );
                    };

                    self.rf_desc.globalization(packet).await?;
                }

                let _wait_res = self.stage_sync_barrier.wait().await;
                let wait_rf_elapsed = self.instant.elapsed() - before_wait;

                log::info!(
                    "HashJoin build stage, sync work elapsed: {:?}, build rf elapsed: {:?}, wait other node rf elapsed: {:?}",
                    elapsed,
                    rf_build_elapsed,
                    wait_rf_elapsed
                );

                self.instant = Instant::now();
                Stage::BuildFinal(BuildFinalState::new())
            }
            Stage::BuildFinal(_) => {
                let wait_elapsed = self.instant.elapsed() - elapsed;
                log::info!(
                    "HashJoin build final stage, sync work elapsed: {:?}, wait elapsed: {:?}",
                    elapsed,
                    wait_elapsed
                );

                self.instant = Instant::now();
                Stage::Probe(ProbeState::new())
            }
            Stage::Probe(_) => {
                let wait_elapsed = self.instant.elapsed() - elapsed;
                log::info!(
                    "HashJoin probe stage, sync work elapsed: {:?}, wait elapsed: {:?}",
                    elapsed,
                    wait_elapsed
                );

                self.instant = Instant::now();
                Stage::ProbeFinal(ProbeFinalState::new())
            }
            Stage::ProbeFinal(state) => match state.finished {
                true => {
                    let wait_elapsed = self.instant.elapsed() - elapsed;
                    log::info!(
                        "HashJoin probe final stage, sync work elapsed: {:?}, wait elapsed: {:?}",
                        elapsed,
                        wait_elapsed
                    );

                    self.instant = Instant::now();
                    Stage::Finished
                }
                false => {
                    let wait_elapsed = self.instant.elapsed() - elapsed;
                    log::info!(
                        "HashJoin probe final stage, sync work elapsed: {:?}, wait elapsed: {:?}",
                        elapsed,
                        wait_elapsed
                    );

                    self.instant = Instant::now();
                    Stage::ProbeFinal(ProbeFinalState {
                        initialize: true,
                        finished: state.finished,
                        stream: state.stream.take(),
                    })
                }
            },
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
    finished: bool,
    initialize: bool,
    stream: Option<Box<dyn JoinStream>>,
}

impl Debug for ProbeFinalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeFinalState")
            .field("initialized", &self.finished)
            .finish()
    }
}

impl ProbeFinalState {
    pub fn new() -> ProbeFinalState {
        ProbeFinalState {
            stream: None,
            finished: false,
            initialize: false,
        }
    }

    pub fn event(&mut self, output_port: &OutputPort) -> Result<Event> {
        if self.stream.is_some() {
            return Ok(Event::Sync);
        }

        if self.finished {
            output_port.finish();
            return Ok(Event::Async);
        }

        match self.initialize {
            true => Ok(Event::Sync),
            false => Ok(Event::Async),
        }
    }
}
