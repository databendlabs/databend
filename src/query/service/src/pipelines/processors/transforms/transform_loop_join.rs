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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_sql::plans::JoinType;

use super::Join;
use super::JoinStream;
use crate::physical_plans::NestedLoopJoin;
use crate::sessions::QueryContext;

pub struct TransformLoopJoin {
    build_port: Arc<InputPort>,
    probe_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    joined_data: Option<DataBlock>,
    join: Box<dyn Join>,
    stage: Stage,
}

impl TransformLoopJoin {
    pub fn create(
        build_port: Arc<InputPort>,
        probe_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        join: Box<dyn Join>,
    ) -> Box<dyn Processor> {
        Box::new(TransformLoopJoin {
            build_port,
            probe_port,
            output_port,
            join,
            joined_data: None,
            stage: Stage::Build(BuildState {
                finished: false,
                build_data: None,
            }),
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformLoopJoin {
    fn name(&self) -> String {
        String::from("TransformLoopJoinLeft")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.build_port.finish();
            self.probe_port.finish();
            return Ok(Event::Finished);
        }

        if !self.output_port.can_push() {
            match self.stage {
                Stage::Build(_) => self.build_port.set_not_need_data(),
                Stage::Probe(_) => self.probe_port.set_not_need_data(),
                Stage::BuildFinal(_) | Stage::Finished => (),
            }
            return Ok(Event::NeedConsume);
        }

        if let Some(joined_data) = self.joined_data.take() {
            self.output_port.push_data(Ok(joined_data));
            return Ok(Event::NeedConsume);
        }

        match &mut self.stage {
            Stage::Build(state) => {
                self.probe_port.set_not_need_data();
                state.event(&self.build_port)
            }
            Stage::BuildFinal(state) => state.event(),
            Stage::Probe(state) => state.event(&self.probe_port),
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
                    }
                    self.stage = Stage::BuildFinal(BuildFinalState::new());
                    return Ok(());
                };

                if !data_block.is_empty() {
                    self.join.add_block(Some(data_block))?;
                }

                Ok(())
            }
            Stage::BuildFinal(state) => {
                if self.join.final_build()?.is_none() {
                    state.finished = true;
                    self.stage = Stage::Probe(ProbeState::new());
                }
                Ok(())
            }
            Stage::Probe(state) => {
                if let Some(probe_data) = state.input_data.take() {
                    let _stream = self.join.probe_block(probe_data)?;
                    todo!();
                    // join and stream share the same lifetime within processor
                    // #[allow(clippy::missing_transmute_annotations)]
                    // state.stream = Some(unsafe { std::mem::transmute(stream) });
                }

                if let Some(mut stream) = state.stream.take() {
                    if let Some(joined_data) = stream.next()? {
                        self.joined_data = Some(joined_data);
                        state.stream = Some(stream);
                    } else if self.probe_port.is_finished() {
                        self.output_port.finish();
                        self.stage = Stage::Finished;
                    }
                } else if self.probe_port.is_finished() {
                    self.output_port.finish();
                    self.stage = Stage::Finished;
                }

                Ok(())
            }
        }
    }
}

enum Stage {
    Build(BuildState),
    BuildFinal(BuildFinalState),
    Probe(ProbeState),
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
                true => Ok(Event::Finished),
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
            return Ok(Event::Sync);
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }
}

#[derive(Clone)]
pub struct LoopJoinState {
    build_table: Arc<RwLock<Vec<DataBlock>>>,
    build_progress: Arc<Mutex<Option<ProgressValues>>>,
    build_finished: Arc<AtomicBool>,
    join_type: JoinType,
}

impl LoopJoinState {
    pub fn new(_ctx: Arc<QueryContext>, join: &NestedLoopJoin) -> Self {
        Self {
            build_table: Arc::new(RwLock::new(vec![])),
            build_progress: Arc::new(Mutex::new(None)),
            build_finished: Arc::new(AtomicBool::new(false)),
            join_type: join.join_type,
        }
    }

    fn push_build_block(&self, right_block: DataBlock) -> Result<()> {
        let right = if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            let rows = right_block.num_rows();
            let entries = right_block
                .take_columns()
                .into_iter()
                .map(|entry| entry.into_nullable())
                .collect::<Vec<_>>();
            DataBlock::new(entries, rows)
        } else {
            right_block
        };
        self.build_table.write()?.push(right);
        Ok(())
    }
}

impl Join for LoopJoinState {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        let Some(right_block) = data else {
            return Ok(());
        };

        self.push_build_block(right_block)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        if self.build_finished.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }

        let progress = self.build_table.read()?.iter().fold(
            ProgressValues::default(),
            |mut progress, block| {
                progress.rows += block.num_rows();
                progress.bytes += block.memory_size();
                progress
            },
        );

        let mut guard = self.build_progress.lock()?;
        *guard = Some(progress.clone());
        Ok(Some(progress))
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        let left = if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let rows = data.num_rows();
            let entries = data
                .take_columns()
                .into_iter()
                .map(|entry| entry.into_nullable())
                .collect::<Vec<_>>();
            DataBlock::new(entries, rows)
        } else {
            data
        };

        let right_table = self.build_table.read()?.clone();
        Ok(Box::new(LoopJoinStream::new(left, right_table)))
    }
}

struct LoopJoinStream {
    left_rows: Vec<Vec<Scalar>>,
    left_types: Vec<DataType>,
    right_blocks: Vec<DataBlock>,
    left_row: usize,
    right_index: usize,
}

impl LoopJoinStream {
    fn new(left: DataBlock, right_blocks: Vec<DataBlock>) -> Self {
        let mut left_rows = vec![Vec::new(); left.num_rows()];
        for entry in left.columns().iter() {
            match entry {
                BlockEntry::Const(scalar, _, _) => {
                    for row in &mut left_rows {
                        row.push(scalar.to_owned());
                    }
                }
                BlockEntry::Column(column) => {
                    for (row, scalar) in left_rows.iter_mut().zip(column.iter()) {
                        row.push(scalar.to_owned());
                    }
                }
            }
        }

        let left_types = left
            .columns()
            .iter()
            .map(|entry| entry.data_type())
            .collect();

        LoopJoinStream {
            left_rows,
            left_types,
            right_blocks,
            left_row: 0,
            right_index: 0,
        }
    }
}

impl JoinStream for LoopJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        if self.right_blocks.is_empty() || self.left_row >= self.left_rows.len() {
            return Ok(None);
        }

        let right_block = &self.right_blocks[self.right_index];
        let left_entries = &self.left_rows[self.left_row];

        let entries = left_entries
            .iter()
            .zip(self.left_types.iter())
            .map(|(scalar, data_type)| {
                BlockEntry::Const(scalar.clone(), data_type.clone(), right_block.num_rows())
            })
            .chain(right_block.columns().iter().cloned())
            .collect();

        self.right_index += 1;
        if self.right_index >= self.right_blocks.len() {
            self.right_index = 0;
            self.left_row += 1;
        }

        Ok(Some(DataBlock::new(entries, right_block.num_rows())))
    }
}
