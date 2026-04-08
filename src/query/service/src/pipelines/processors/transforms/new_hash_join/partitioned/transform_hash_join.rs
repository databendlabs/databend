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
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Instant;

use databend_common_base::base::Barrier;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::plans::JoinType;
use log::info;

use super::PartitionedInnerJoin;
use super::PartitionedLeftAntiJoin;
use super::PartitionedLeftJoin;
use super::PartitionedLeftSemiJoin;
use super::PartitionedRightAntiJoin;
use super::PartitionedRightJoin;
use super::PartitionedRightSemiJoin;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFilterLocalBuilder;
use crate::pipelines::processors::transforms::merge_two_runtime_filter_packets;
use crate::pipelines::processors::transforms::new_hash_join::common::join::FinishedJoin;
use crate::pipelines::processors::transforms::new_hash_join::common::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::common::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::common::runtime_filter::RuntimeFiltersDesc;

pub struct SharedRuntimeFilterPackets {
    packets: Mutex<Vec<JoinRuntimeFilterPacket>>,
}

impl SharedRuntimeFilterPackets {
    pub fn create() -> Arc<Self> {
        Arc::new(SharedRuntimeFilterPackets {
            packets: Mutex::new(Vec::new()),
        })
    }

    pub fn merge_packet(&self, mut my_packet: JoinRuntimeFilterPacket) -> Result<()> {
        loop {
            let locked = self.packets.lock();
            let mut guard = locked.unwrap_or_else(PoisonError::into_inner);

            if guard.is_empty() {
                guard.push(my_packet);
                return Ok(());
            }

            let other = guard.pop().unwrap();
            drop(guard);
            my_packet = merge_two_runtime_filter_packets(my_packet, other)?;
        }
    }

    pub fn take_packet(&self) -> Option<JoinRuntimeFilterPacket> {
        let mut guard = self.packets.lock().unwrap_or_else(PoisonError::into_inner);
        guard.pop()
    }
}

pub struct TransformPartitionedHashJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,
    joined_port: Arc<OutputPort>,

    stage: Stage,
    join: Box<dyn Join>,
    joined_data: Option<DataBlock>,

    stage_sync_barrier: Arc<Barrier>,
    projection: BTreeSet<usize>,
    rf_desc: Arc<RuntimeFiltersDesc>,
    runtime_filter_builder: Option<RuntimeFilterLocalBuilder>,
    shared_rf_packets: Arc<SharedRuntimeFilterPackets>,
    instant: Instant,
}

impl TransformPartitionedHashJoin {
    pub fn create(
        build_port: Arc<InputPort>,
        probe_port: Arc<InputPort>,
        joined_port: Arc<OutputPort>,
        join: Box<dyn Join>,
        stage_sync_barrier: Arc<Barrier>,
        projection: BTreeSet<usize>,
        rf_desc: Arc<RuntimeFiltersDesc>,
        shared_rf_packets: Arc<SharedRuntimeFilterPackets>,
    ) -> Result<ProcessorPtr> {
        let runtime_filter_builder = RuntimeFilterLocalBuilder::try_create(
            &rf_desc.func_ctx,
            rf_desc.filters_desc.clone(),
            rf_desc.inlist_threshold,
            rf_desc.bloom_threshold,
            rf_desc.min_max_threshold,
            rf_desc.spatial_threshold,
        )?;

        Ok(ProcessorPtr::create(Box::new(
            TransformPartitionedHashJoin {
                build_port,
                probe_port,
                joined_port,
                join,
                rf_desc,
                projection,
                stage_sync_barrier,
                shared_rf_packets,
                joined_data: None,
                runtime_filter_builder,
                stage: Stage::Build(BuildState {
                    finished: false,
                    initialized: false,
                    build_data: None,
                }),
                instant: Instant::now(),
            },
        )))
    }

    pub fn create_join(
        typ: JoinType,
        hash_method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Box<dyn Join> {
        match typ {
            JoinType::Inner => Box::new(PartitionedInnerJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::Left => Box::new(PartitionedLeftJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::LeftAnti => Box::new(PartitionedLeftAntiJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::LeftSemi => Box::new(PartitionedLeftSemiJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::Right => Box::new(PartitionedRightJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::RightSemi => Box::new(PartitionedRightSemiJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            JoinType::RightAnti => Box::new(PartitionedRightAntiJoin::create(
                hash_method,
                desc,
                function_ctx,
                max_block_size,
            )),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformPartitionedHashJoin {
    fn name(&self) -> String {
        String::from("TransformPartitionedHashJoin")
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

                let mut finished = FinishedJoin::create();
                std::mem::swap(&mut finished, &mut self.join);
                drop(finished);

                if self.stage_sync_barrier.reduce_quorum(1) {
                    self.rf_desc.close_broadcast();
                }
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
            Stage::Build(state) => state.event(&self.build_port, &self.probe_port),
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

                        if let Some(builder) = self.runtime_filter_builder.take() {
                            let spill_happened = self.join.is_spill_happened();
                            let packet = builder.finish(spill_happened)?;
                            self.shared_rf_packets.merge_packet(packet)?;
                        }
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

        self.stage = match &mut self.stage {
            Stage::Build(_) => {
                let wait_res = self.stage_sync_barrier.wait().await;

                let rf_build_elapsed = self.instant.elapsed() - elapsed;
                let _wait_res = self.stage_sync_barrier.wait().await;
                let before_wait = self.instant.elapsed();

                if wait_res.is_leader() {
                    let packet = self
                        .shared_rf_packets
                        .take_packet()
                        .unwrap_or_else(|| JoinRuntimeFilterPacket::complete_without_filters(0));
                    info!(
                        "spilled: false, globalize runtime filter: total {}, disable_all_due_to_spill: {}",
                        packet.packets.as_ref().map_or(0, |p| p.len()),
                        packet.disable_all_due_to_spill
                    );
                    self.rf_desc.globalization(packet).await?;
                }

                let _wait_res = self.stage_sync_barrier.wait().await;
                let wait_rf_elapsed = self.instant.elapsed() - before_wait;

                log::info!(
                    "PartitionedHashJoin build stage, sync work elapsed: {:?}, build rf elapsed: {:?}, wait other node rf elapsed: {:?}",
                    elapsed,
                    rf_build_elapsed,
                    wait_rf_elapsed
                );

                self.instant = Instant::now();
                Stage::BuildFinal(BuildFinalState::new())
            }
            // BuildFinal → Probe: barrier
            Stage::BuildFinal(_) => {
                let _wait_res = self.stage_sync_barrier.wait().await;
                let wait_elapsed = self.instant.elapsed() - elapsed;
                log::info!(
                    "PartitionedHashJoin build final stage, sync work elapsed: {:?}, wait elapsed: {:?}",
                    elapsed,
                    wait_elapsed
                );

                self.instant = Instant::now();
                Stage::Probe(ProbeState::new())
            }
            // Probe → ProbeFinal: no barrier
            Stage::Probe(_) => {
                log::info!("PartitionedHashJoin probe stage elapsed: {:?}", elapsed);
                self.instant = Instant::now();
                Stage::ProbeFinal(ProbeFinalState::new())
            }
            // ProbeFinal → Finished or continue: no barrier
            Stage::ProbeFinal(state) => match state.finished {
                true => {
                    log::info!(
                        "PartitionedHashJoin probe final stage elapsed: {:?}",
                        elapsed
                    );
                    self.instant = Instant::now();

                    let mut finished = FinishedJoin::create();
                    std::mem::swap(&mut finished, &mut self.join);
                    drop(finished);

                    Stage::Finished
                }
                false => {
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
    initialized: bool,
    build_data: Option<DataBlock>,
}

impl BuildState {
    pub fn event(&mut self, build: &InputPort, probe: &InputPort) -> Result<Event> {
        if self.build_data.is_some() {
            return Ok(Event::Sync);
        }

        if build.has_data() {
            self.build_data = Some(build.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if build.is_finished() {
            return match self.finished {
                true => Ok(Event::Async),
                false => Ok(Event::Sync),
            };
        }

        if !self.initialized {
            self.initialized = true;
            if !probe.is_finished() && !probe.has_data() {
                probe.set_need_data();
            }
        }

        build.set_need_data();
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
