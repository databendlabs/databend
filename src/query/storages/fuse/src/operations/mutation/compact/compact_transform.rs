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
use std::time::Instant;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use opendal::Operator;
use storages_common_blocks::blocks_to_parquet;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::StatisticsOfColumns;
use storages_common_table_meta::table::TableCompression;

use super::compact_meta::CompactSourceMeta;
use super::compact_part::CompactTask;
use super::CompactSinkMeta;
use crate::io;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::metrics::*;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::SerializeState;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::gen_col_stats_lite;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reducers::reduce_block_metas;

enum State {
    Consume,
    ReadBlocks,
    CompactBlocks {
        blocks: Vec<DataBlock>,
        stats_of_columns: Vec<Vec<StatisticsOfColumns>>,
        trivals: VecDeque<Arc<BlockMeta>>,
    },
    SerializedBlocks(Vec<SerializeState>),
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Output {
        location: String,
        segment: Arc<SegmentInfo>,
    },
}

// Gets a set of CompactTask, only merge but not split, generate a new segment.
pub struct CompactTransform {
    ctx: Arc<dyn TableContext>,
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    output_data: Option<DataBlock>,

    block_reader: Arc<BlockReader>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    schema: TableSchemaRef,

    // Limit the memory size of the block read.
    max_memory: u64,
    max_io_requests: usize,
    compact_tasks: VecDeque<CompactTask>,
    block_metas: Vec<Arc<BlockMeta>>,
    order: usize,
    thresholds: BlockThresholds,
    write_settings: WriteSettings,
    abort_operation: AbortOperation,
}

impl CompactTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        scan_progress: Arc<Progress>,
        block_reader: Arc<BlockReader>,
        location_gen: TableMetaLocationGenerator,
        dal: Operator,
        schema: TableSchemaRef,
        thresholds: BlockThresholds,
        write_settings: WriteSettings,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let max_memory_usage = (settings.get_max_memory_usage()? as f64 * 0.8) as u64;
        let max_threads = settings.get_max_threads()?;
        let max_memory = max_memory_usage / max_threads;
        let max_io_requests = settings.get_max_storage_io_requests()? as usize;
        Ok(ProcessorPtr::create(Box::new(CompactTransform {
            ctx,
            state: State::Consume,
            input,
            output,
            scan_progress,
            output_data: None,
            block_reader,
            location_gen,
            dal,
            schema,
            max_memory,
            max_io_requests,
            compact_tasks: VecDeque::new(),
            block_metas: Vec::new(),
            order: 0,
            thresholds,
            write_settings,
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
        if matches!(
            &self.state,
            State::CompactBlocks { .. } | State::GenerateSegment { .. } | State::Output { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::ReadBlocks | State::SerializedBlocks(_) | State::SerializedSegment { .. }
        ) {
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

        let input_data = self.input.pull_data().unwrap()?;
        let meta = input_data
            .get_meta()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
        let task_meta = CompactSourceMeta::from_meta(meta)?;
        self.order = task_meta.order;
        self.compact_tasks = task_meta.tasks.clone();

        self.state = State::ReadBlocks;
        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::CompactBlocks {
                mut blocks,
                stats_of_columns,
                mut trivals,
            } => {
                let mut serialize_states = Vec::new();
                for stats in stats_of_columns {
                    let block_num = stats.len();
                    if block_num == 0 {
                        self.block_metas.push(trivals.pop_front().unwrap());
                        continue;
                    }

                    // concat blocks.
                    let compact_blocks: Vec<_> = blocks.drain(0..block_num).collect();
                    let new_block = DataBlock::concat(&compact_blocks)?;

                    let col_stats_lites = gen_col_stats_lite(
                        &new_block,
                        self.block_reader.schema().fields(),
                        &self.block_reader.default_vals,
                    )?;
                    // generate block statistics.
                    let col_stats = reduce_block_statistics(&stats, Some(&col_stats_lites))?;
                    let row_count = new_block.num_rows() as u64;
                    let block_size = new_block.memory_size() as u64;
                    let (block_location, block_id) = self.location_gen.gen_block_location();

                    // build block index.
                    let func_ctx = self.ctx.get_function_context()?;
                    let maybe_bloom_index = BloomIndex::try_create(
                        func_ctx,
                        self.schema.clone(),
                        block_location.1,
                        &[&new_block],
                    )?;

                    let (index_data, index_size, index_location) = match maybe_bloom_index {
                        Some(bloom_index) => {
                            // write index
                            let index_block = bloom_index.serialize_to_data_block()?;
                            let index_block_schema = &bloom_index.filter_schema;
                            let location = self.location_gen.block_bloom_index_location(&block_id);
                            let mut data = Vec::with_capacity(100 * 1024);
                            let (size, _) = blocks_to_parquet(
                                index_block_schema,
                                vec![index_block],
                                &mut data,
                                TableCompression::None,
                            )?;
                            (Some(data), size, Some(location))
                        }
                        None => (None, 0u64, None),
                    };

                    // serialize data block.
                    let mut block_data = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
                    let (file_size, col_metas) = io::serialize_block(
                        &self.write_settings,
                        &self.schema,
                        new_block,
                        &mut block_data,
                    )?;

                    // new block meta.
                    let new_meta = BlockMeta::new(
                        row_count,
                        block_size,
                        file_size,
                        col_stats,
                        col_metas,
                        None,
                        block_location.clone(),
                        index_location.clone(),
                        index_size,
                        self.write_settings.table_compression.into(),
                    );
                    self.abort_operation.add_block(&new_meta);
                    self.block_metas.push(Arc::new(new_meta));

                    serialize_states.push(SerializeState {
                        block_data,
                        block_location: block_location.0,
                        index_data,
                        index_location: index_location.map(|l| l.0),
                    });
                }
                self.state = State::SerializedBlocks(serialize_states);
            }
            State::GenerateSegment => {
                let metas = std::mem::take(&mut self.block_metas);
                let stats = reduce_block_metas(&metas, self.thresholds)?;
                let segment_info = SegmentInfo::new(metas, stats);
                let location = self.location_gen.gen_segment_info_location();
                self.abort_operation.add_segment(location.clone());
                self.state = State::SerializedSegment {
                    data: serde_json::to_vec(&segment_info)?,
                    location,
                    segment: Arc::new(segment_info),
                };
            }
            State::Output { location, segment } => {
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    segment_cache.put(location.clone(), segment.clone())
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
            State::ReadBlocks => {
                // block read tasks.
                let mut task_futures = Vec::new();
                // The no need compact blockmetas.
                let mut trivals = VecDeque::new();
                let mut memory_usage = 0;
                let mut stats_of_columns = Vec::new();

                let block_reader = self.block_reader.as_ref();
                while let Some(task) = self.compact_tasks.pop_front() {
                    let metas = task.get_block_metas();
                    // Only one block, no need to do a compact.
                    if metas.len() == 1 {
                        stats_of_columns.push(vec![]);
                        trivals.push_back(metas[0].clone());
                        continue;
                    }

                    memory_usage += metas.iter().fold(0, |acc, meta| {
                        let memory = meta.bloom_filter_index_size + meta.block_size;
                        acc + memory
                    });

                    if (memory_usage > self.max_memory
                        || task_futures.len() + metas.len() > self.max_io_requests)
                        && !task_futures.is_empty()
                    {
                        self.compact_tasks.push_front(task);
                        break;
                    }

                    let mut meta_stats = Vec::with_capacity(metas.len());
                    for meta in metas {
                        let progress_values = ProgressValues {
                            rows: meta.row_count as usize,
                            bytes: meta.block_size as usize,
                        };
                        self.scan_progress.incr(&progress_values);

                        meta_stats.push(meta.col_stats.clone());
                        let settings = ReadSettings::from_ctx(&self.ctx)?;
                        let storage_format = self.write_settings.storage_format;
                        // read block in parallel.
                        task_futures.push(async move {
                            // Perf
                            {
                                metrics_inc_compact_block_read_nums(1);
                                metrics_inc_compact_block_read_bytes(meta.block_size);
                            }

                            block_reader
                                .read_by_meta(&settings, meta.as_ref(), &storage_format)
                                .await
                        });
                    }
                    stats_of_columns.push(meta_stats);
                }

                let start = Instant::now();

                let blocks = futures::future::try_join_all(task_futures).await?;

                // Perf.
                {
                    metrics_inc_compact_block_read_milliseconds(start.elapsed().as_millis() as u64);
                }

                self.state = State::CompactBlocks {
                    blocks,
                    stats_of_columns,
                    trivals,
                }
            }
            State::SerializedBlocks(mut serialize_states) => {
                let mut handles = Vec::with_capacity(serialize_states.len());
                let dal = &self.dal;
                while let Some(state) = serialize_states.pop() {
                    handles.push(async move {
                        // Perf.
                        {
                            metrics_inc_compact_block_write_nums(1);
                            metrics_inc_compact_block_write_bytes(state.block_data.len() as u64);
                        }

                        // write index data.
                        if let (Some(index_data), Some(index_location)) =
                            (state.index_data, state.index_location)
                        {
                            write_data(index_data, dal, &index_location).await?;
                        }
                        // write block data.
                        write_data(state.block_data, dal, &state.block_location).await
                    });
                }

                let start = Instant::now();

                futures::future::try_join_all(handles).await?;

                // Perf
                {
                    metrics_inc_compact_block_write_milliseconds(start.elapsed().as_millis() as u64);
                }

                if self.compact_tasks.is_empty() {
                    self.state = State::GenerateSegment;
                } else {
                    self.state = State::ReadBlocks;
                }
            }
            State::SerializedSegment {
                data,
                location,
                segment,
            } => {
                self.dal.write(&location, data).await?;
                self.state = State::Output { location, segment };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
