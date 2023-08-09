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
use std::sync::Arc;
use std::time::Instant;

use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::metrics::*;
use crate::operations::mutation::mutation_meta::ClusterStatsGenType;
use crate::operations::mutation::CompactPartInfo;
use crate::operations::mutation::SerializeDataMeta;
use crate::operations::BlockMetaIndex;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::FuseStorageFormat;

enum State {
    ReadData(Option<PartInfoPtr>),
    Concat(Vec<DataBlock>, BlockMetaIndex),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct CompactSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    storage_format: FuseStorageFormat,
    output: Arc<OutputPort>,
}

impl CompactSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(CompactSource {
            state: State::ReadData(None),
            ctx,
            block_reader,
            storage_format,
            output,
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
            self.state = self
                .ctx
                .get_partition()
                .map_or(State::Finish, |part| State::ReadData(Some(part)));
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.state {
            State::ReadData(_) => Ok(Event::Async),
            State::Concat(_, _) => Ok(Event::Sync),
            State::Output(_, _) => {
                if let State::Output(part, data_block) =
                    std::mem::replace(&mut self.state, State::Finish)
                {
                    self.state = part.map_or(State::Finish, |part| State::ReadData(Some(part)));

                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                } else {
                    Err(ErrorCode::Internal("It's a bug."))
                }
            }
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Concat(blocks, index) => {
                // concat blocks.
                let block = if blocks.len() == 1 {
                    blocks[0].convert_to_full()
                } else {
                    DataBlock::concat(&blocks)?
                };

                let meta = SerializeDataMeta::create(index, ClusterStatsGenType::Generally);
                let new_block = block.add_meta(Some(meta))?;

                let progress_values = ProgressValues {
                    rows: new_block.num_rows(),
                    bytes: new_block.memory_size(),
                };
                self.ctx.get_write_progress().incr(&progress_values);

                self.state = State::Output(self.ctx.get_partition(), new_block);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let block_reader = self.block_reader.as_ref();
                let storage_format = self.storage_format;

                // block read tasks.
                let mut task_futures = Vec::new();
                let part = CompactPartInfo::from_part(&part)?;
                let mut stats = Vec::with_capacity(part.blocks.len());
                for block in &part.blocks {
                    stats.push(block.col_stats.clone());

                    let settings = ReadSettings::from_ctx(&self.ctx)?;
                    // read block in parallel.
                    task_futures.push(async move {
                        // Perf
                        {
                            metrics_inc_compact_block_read_nums(1);
                            metrics_inc_compact_block_read_bytes(block.block_size);
                        }

                        block_reader
                            .read_by_meta(&settings, block.as_ref(), &storage_format)
                            .await
                    });
                }

                let start = Instant::now();

                let blocks = futures::future::try_join_all(task_futures).await?;
                // Perf.
                {
                    metrics_inc_compact_block_read_milliseconds(start.elapsed().as_millis() as u64);
                }
                self.state = State::Concat(blocks, part.index.clone());
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
