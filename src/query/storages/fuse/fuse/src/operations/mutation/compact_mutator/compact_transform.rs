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
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_cache::Cache;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::SegmentInfo;
use opendal::Operator;

use super::compact_meta::CompactSourceMeta;
use super::CompactSinkMeta;
use crate::io::BlockReader;
use crate::io::BlockWriter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reducers::reduce_block_metas;

enum State {
    Consume,
    Compact(DataBlock),
    Generate(Vec<Arc<BlockMeta>>),
    Serialized {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Output {
        location: String,
        segment: Arc<SegmentInfo>,
    },
}

pub struct CompactTransform {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    output_data: Option<DataBlock>,

    block_reader: Arc<BlockReader>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,

    order: usize,
    thresholds: BlockCompactThresholds,
    abort_operation: AbortOperation,
}

impl CompactTransform {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        scan_progress: Arc<Progress>,
        block_reader: Arc<BlockReader>,
        location_gen: TableMetaLocationGenerator,
        dal: Operator,
        thresholds: BlockCompactThresholds,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(CompactTransform {
            state: State::Consume,
            input,
            output,
            scan_progress,
            output_data: None,
            block_reader,
            location_gen,
            dal,
            order: 0,
            thresholds,
            abort_operation: AbortOperation::default(),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CompactTransform {
    fn name(&self) -> String {
        "CompactTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(&self.state, State::Generate { .. } | State::Output { .. }) {
            return Ok(Event::Sync);
        }

        if matches!(&self.state, State::Serialized { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.state = State::Compact(self.input.pull_data().unwrap()?);
        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Generate(metas) => {
                let stats = reduce_block_metas(&metas, self.thresholds)?;
                let segment_info = SegmentInfo::new(metas, stats);
                let location = self.location_gen.gen_segment_info_location();
                self.abort_operation.add_segment(location.clone());
                self.state = State::Serialized {
                    data: serde_json::to_vec(&segment_info)?,
                    location,
                    segment: Arc::new(segment_info),
                };
            }
            State::Output { location, segment } => {
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    let cache = &mut segment_cache.write();
                    cache.put(location.clone(), segment.clone());
                }

                let meta = CompactSinkMeta::create(
                    self.order,
                    location,
                    segment,
                    std::mem::take(&mut self.abort_operation),
                );
                self.output_data = Some(DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Compact(block) => {
                let meta = block.get_meta().unwrap();
                let task_meta = CompactSourceMeta::from_meta(meta)?;
                self.order = task_meta.order;
                let mut new_metas = Vec::with_capacity(task_meta.tasks.len());
                for task in &task_meta.tasks {
                    let metas = task.get_block_metas();
                    if metas.len() == 1 {
                        new_metas.push(metas[0].clone());
                        continue;
                    }

                    let mut blocks = Vec::with_capacity(metas.len());
                    let mut stats_of_columns = Vec::with_capacity(metas.len());
                    for meta in metas {
                        let meta = meta.as_ref();
                        stats_of_columns.push(meta.col_stats.clone());
                        let block = self.block_reader.read_with_block_meta(meta).await?;
                        let progress_values = ProgressValues {
                            rows: block.num_rows(),
                            bytes: block.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        blocks.push(block);
                    }
                    let new_block = DataBlock::concat_blocks(&blocks)?;
                    let col_stats = reduce_block_statistics(&stats_of_columns)?;

                    let block_writer = BlockWriter::new(&self.dal, &self.location_gen);
                    let new_meta = block_writer.write(new_block, col_stats, None).await?;
                    self.abort_operation.add_block(&new_meta);
                    new_metas.push(Arc::new(new_meta));
                }
                self.state = State::Generate(new_metas);
            }
            State::Serialized {
                data,
                location,
                segment,
            } => {
                self.dal.object(&location).write(data).await?;
                self.state = State::Output { location, segment };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
