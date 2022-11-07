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
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::SegmentInfo;
use opendal::Operator;

use super::CompactMetaInfo;
use super::CompactPartInfo;
use crate::io::BlockReader;
use crate::io::BlockWriter;
use crate::io::TableMetaLocationGenerator;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reducers::reduce_block_metas;

enum State {
    Compact(Option<PartInfoPtr>),
    Generate {
        order: usize,
        metas: Vec<Arc<BlockMeta>>,
    },
    Serialized {
        order: usize,
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Output {
        order: usize,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Generated(Option<PartInfoPtr>, DataBlock),
    Finished,
}

pub struct CompactSource {
    ctx: Arc<dyn TableContext>,
    state: State,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,

    block_reader: Arc<BlockReader>,
    location_generator: TableMetaLocationGenerator,
    dal: Operator,
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
        if matches!(self.state, State::Compact(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finished,
                Some(part) => State::Compact(Some(part)),
            }
        }

        if matches!(self.state, State::Finished) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let State::Generated(part, data_block) =
                std::mem::replace(&mut self.state, State::Finished)
            {
                self.state = match part {
                    None => State::Finished,
                    Some(part) => State::Compact(Some(part)),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        if matches!(
            &self.state,
            State::Compact { .. } | State::Serialized { .. }
        ) {
            return Ok(Event::Async);
        }

        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finished) {
            State::Generate { order, metas } => {
                let stats = reduce_block_metas(&metas)?;
                let segment_info = SegmentInfo::new(metas, stats);
                self.state = State::Serialized {
                    order,
                    data: serde_json::to_vec(&segment_info)?,
                    location: self.location_generator.gen_segment_info_location(),
                    segment: Arc::new(segment_info),
                };
            }
            State::Output {
                order,
                location,
                segment,
            } => {
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    let cache = &mut segment_cache.write();
                    cache.put(location.clone(), segment.clone());
                }

                let meta = CompactMetaInfo::create(order, location, segment);
                let new_part = self.ctx.try_get_part();
                self.state = State::Generated(new_part, DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finished) {
            State::Compact(Some(part)) => {
                let part = CompactPartInfo::from_part(&part)?;
                let mut new_metas = Vec::with_capacity(part.tasks.len());
                for task in &part.tasks {
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

                    let block_writer = BlockWriter::new(&self.dal, &self.location_generator);
                    let new_meta = block_writer.write(new_block, col_stats, None).await?;
                    new_metas.push(Arc::new(new_meta));
                }
                self.state = State::Generate {
                    order: part.order,
                    metas: new_metas,
                };
            }
            State::Serialized {
                order,
                data,
                location,
                segment,
            } => {
                self.dal.object(&location).write(data).await?;
                self.state = State::Output {
                    order,
                    location,
                    segment,
                };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
