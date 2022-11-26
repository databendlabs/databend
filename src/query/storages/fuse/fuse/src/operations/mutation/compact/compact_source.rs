// Copyright 2022 Datafuse Labs.
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

use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;

use super::compact_meta::CompactSourceMeta;
use super::compact_part::CompactPartInfo;
use super::compact_part::CompactTask;
use crate::metrics::metrics_set_selected_blocks_memory_usage;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;

enum State {
    ReadData(Option<PartInfoPtr>),
    Generate {
        order: usize,
        tasks: VecDeque<CompactTask>,
    },
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

// Select the row_count >= min_rows_per_block or block_size >= max_bytes_per_block
// as the perfect_block condition(N for short). CompactSource gets a set of segments,
// iterates through the blocks, and finds the blocks >= N and blocks < 2N as a CompactTask.
pub struct CompactSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
    thresholds: BlockCompactThresholds,
}

impl CompactSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        thresholds: BlockCompactThresholds,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(CompactSource {
            state: State::ReadData(None),
            ctx,
            output,
            thresholds,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CompactSource {
    fn name(&self) -> String {
        "CompactSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadData(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadData(Some(part)),
            }
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Output(_, _)) {
            if let State::Output(part, data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadData(Some(part)),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let part = CompactPartInfo::from_part(&part)?;
                let mut builder = CompactTaskBuilder::default();
                let mut tasks = VecDeque::new();
                // The order of the compact is from old to new.
                for segment in part.segments.iter().rev() {
                    for block in segment.blocks.iter() {
                        // todo: add real metrics
                        metrics_set_selected_blocks_memory_usage(0.0);

                        let res = builder.add(block, self.thresholds);
                        tasks.extend(res);
                    }
                }
                if !builder.is_empty() {
                    let task = tasks.pop_back();
                    tasks.push_back(builder.finalize(task));
                }
                self.state = State::Generate {
                    order: part.order,
                    tasks,
                }
            }
            State::Generate { order, tasks } => {
                let meta = CompactSourceMeta::create(order, tasks);
                let new_part = self.ctx.try_get_part();
                self.state = State::Output(new_part, DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}

#[derive(Default)]
struct CompactTaskBuilder {
    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
}

impl CompactTaskBuilder {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn add(
        &mut self,
        block: &Arc<BlockMeta>,
        thresholds: BlockCompactThresholds,
    ) -> Vec<CompactTask> {
        self.total_rows += block.row_count as usize;
        self.total_size += block.block_size as usize;

        if !thresholds.check_large_enough(self.total_rows, self.total_size) {
            // blocks < N
            self.blocks.push(block.clone());
            return vec![];
        }

        let tasks = if !thresholds.check_for_compact(self.total_rows, self.total_size) {
            // blocks > 2N
            let trival_task = CompactTask::Trival(block.clone());
            if !self.blocks.is_empty() {
                let compact_task = Self::create_task(std::mem::take(&mut self.blocks));
                vec![compact_task, trival_task]
            } else {
                vec![trival_task]
            }
        } else {
            // N <= blocks < 2N
            self.blocks.push(block.clone());
            vec![Self::create_task(std::mem::take(&mut self.blocks))]
        };

        self.total_rows = 0;
        self.total_size = 0;
        tasks
    }

    fn finalize(&mut self, task: Option<CompactTask>) -> CompactTask {
        let mut blocks = task.map_or(vec![], |t| t.get_block_metas());
        blocks.extend(std::mem::take(&mut self.blocks));
        Self::create_task(blocks)
    }

    fn create_task(blocks: Vec<Arc<BlockMeta>>) -> CompactTask {
        match blocks.len() {
            0 => panic!("the blocks is empty"),
            1 => CompactTask::Trival(blocks[0].clone()),
            _ => CompactTask::Normal(blocks),
        }
    }
}
