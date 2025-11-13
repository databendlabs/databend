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
use std::sync::Mutex;
use std::sync::RwLock;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::sinks::Sink;
use databend_common_sql::plans::JoinType;

use super::Join;
use super::JoinStream;
use crate::physical_plans::NestedLoopJoin;
use crate::pipelines::executor::WatchNotify;
use crate::sessions::QueryContext;

pub struct TransformLoopJoinLeft {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    output_data_blocks: VecDeque<DataBlock>,
    state: Arc<LoopJoinState>,
}

impl TransformLoopJoinLeft {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        state: Arc<LoopJoinState>,
    ) -> Box<dyn Processor> {
        Box::new(TransformLoopJoinLeft {
            input_port,
            output_port,
            output_data_blocks: Default::default(),
            state,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformLoopJoinLeft {
    fn name(&self) -> String {
        "TransformLoopJoinLeft".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            return Ok(Event::Finished);
        }
        if !self.output_port.can_push() {
            return Ok(Event::NeedConsume);
        }
        if let Some(data) = self.output_data_blocks.pop_front() {
            self.output_port.push_data(Ok(data));
            return Ok(Event::NeedConsume);
        }

        if !self.state.is_right_finish()? {
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            // todo!()
            self.output_port.finish();
            return Ok(Event::Finished);
        }
        if !self.input_port.has_data() {
            self.input_port.set_need_data();
            return Ok(Event::NeedData);
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data) = self.input_port.pull_data() {
            self.output_data_blocks = self.state.loop_join(data?)?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        self.state.wait_right_finish().await?;
        Ok(())
    }
}

pub struct TransformLoopJoinRight {
    state: Arc<LoopJoinState>,
}

impl TransformLoopJoinRight {
    pub fn create(state: Arc<LoopJoinState>) -> Result<Self> {
        state.attach_right()?;
        Ok(TransformLoopJoinRight { state })
    }
}

impl Sink for TransformLoopJoinRight {
    const NAME: &'static str = "TransformLoopJoinRight";

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.state.sink_right(data_block)
    }

    fn on_finish(&mut self) -> Result<()> {
        self.state.detach_right()?;
        Ok(())
    }
}

pub struct LoopJoinState {
    right_table: RwLock<Vec<DataBlock>>,
    right_finished: Mutex<bool>,
    finished_notify: Arc<WatchNotify>,

    right_sinker_count: RwLock<usize>,

    join_type: JoinType,
}

impl LoopJoinState {
    pub fn new(_ctx: Arc<QueryContext>, join: &NestedLoopJoin) -> Self {
        Self {
            right_table: RwLock::new(vec![]),
            right_finished: Mutex::new(false),
            finished_notify: Arc::new(WatchNotify::new()),
            right_sinker_count: RwLock::new(0),
            join_type: join.join_type,
        }
    }

    fn attach_right(&self) -> Result<()> {
        let mut right_sinker_count = self.right_sinker_count.write()?;
        *right_sinker_count += 1;
        Ok(())
    }

    fn sink_right(&self, right_block: DataBlock) -> Result<()> {
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
        self.right_table.write()?.push(right);
        Ok(())
    }

    fn detach_right(&self) -> Result<()> {
        let finished = {
            let mut right_sinker_count = self.right_sinker_count.write()?;
            *right_sinker_count -= 1;
            *right_sinker_count == 0
        };
        if finished {
            // todo
            *self.right_finished.lock()? = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    fn is_right_finish(&self) -> Result<bool> {
        Ok(*self.right_finished.lock()?)
    }

    async fn wait_right_finish(&self) -> Result<()> {
        if !*self.right_finished.lock()? {
            self.finished_notify.notified().await
        }
        Ok(())
    }

    fn loop_join(&self, left_block: DataBlock) -> Result<VecDeque<DataBlock>> {
        let left = if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let rows = left_block.num_rows();
            let entries = left_block
                .take_columns()
                .into_iter()
                .map(|entry| entry.into_nullable())
                .collect::<Vec<_>>();
            DataBlock::new(entries, rows)
        } else {
            left_block
        };

        let right_table = self.right_table.read()?;
        let mut blocks = VecDeque::with_capacity(right_table.len() * left.num_rows());
        for right in right_table.iter() {
            blocks.extend(self.single_loop_join(&left, right)?);
        }
        Ok(blocks)
    }

    fn single_loop_join<'a>(
        &self,
        left: &'a DataBlock,
        right: &'a DataBlock,
    ) -> Result<impl Iterator<Item = DataBlock> + use<'a>> {
        let mut left_blocks = vec![Vec::new(); left.num_rows()];
        for entry in left.columns().iter() {
            match entry {
                BlockEntry::Const(scalar, _, _) => {
                    for left_entries in &mut left_blocks {
                        left_entries.push(scalar.as_ref());
                    }
                }
                BlockEntry::Column(column) => {
                    for (left_entries, scalar) in left_blocks.iter_mut().zip(column.iter()) {
                        left_entries.push(scalar);
                    }
                }
            }
        }

        let iter = left_blocks.into_iter().map(|left_entries| {
            let entries = left_entries
                .iter()
                .zip(left.columns().iter().map(|entry| entry.data_type()))
                .map(|(scalar, data_type)| {
                    BlockEntry::Const(scalar.to_owned(), data_type, right.num_rows())
                })
                .chain(right.columns().iter().cloned())
                .collect();
            DataBlock::new(entries, right.num_rows())
        });
        Ok(iter)
    }
}

impl Join for LoopJoinState {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        let Some(right_block) = data else {
            return Ok(());
        };

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
        self.right_table.write()?.push(right);
        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        let progress = self.right_table.read()?.iter().fold(
            ProgressValues::default(),
            |mut progress, block| {
                progress.rows += block.num_rows();
                progress.bytes += block.memory_size();
                progress
            },
        );
        Ok(Some(progress))
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        todo!();
    }
}

struct LoopJoinStream {}

impl JoinStream for LoopJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        todo!()
    }
}
