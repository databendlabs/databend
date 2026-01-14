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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::IndexType;
use databend_storages_common_io::ReadSettings;
use log::debug;
use log::info;

use super::parquet_data_source::ParquetDataSource;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::CachedReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;

pub struct ReadStats {
    pub blocks_total: AtomicU64,
    pub blocks_pruned: AtomicU64,
}

/// State for processing a single part
struct PartProcessState {
    part: PartInfoPtr,
    cached_reader: CachedReader,
}

/// Pending work state
enum PendingWork {
    /// Cache hit - can be processed synchronously
    CacheHit(Vec<PartProcessState>),
    /// Cache miss - need async IO
    CacheMiss(Vec<PartProcessState>),
}

pub struct NewReadParquetDataTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    read_settings: ReadSettings,
    stats: Arc<ReadStats>,
    unfinished_processors_count: Arc<AtomicU64>,

    called_on_finish: bool,
    output_data: Option<DataBlock>,
    pending_work: Option<PendingWork>,
}

impl NewReadParquetDataTransform {
    pub fn create(
        table_index: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        stats: Arc<ReadStats>,
        unfinished_processors_count: Arc<AtomicU64>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        Ok(ProcessorPtr::create(Box::new(
            NewReadParquetDataTransform {
                input,
                output,
                func_ctx,
                block_reader,
                index_reader,
                virtual_reader,
                table_schema,
                scan_id: table_index,
                context: ctx,
                read_settings,
                stats,
                unfinished_processors_count,
                called_on_finish: false,
                output_data: None,
                pending_work: None,
            },
        )))
    }

    fn prepare_parts(&mut self, parts: Vec<PartInfoPtr>) -> Result<PendingWork> {
        let runtime_filter = ExprRuntimePruner::new(
            self.context
                .get_runtime_filters(self.scan_id)
                .into_iter()
                .flat_map(|entry| {
                    let mut exprs = Vec::new();
                    if let Some(expr) = entry.inlist.clone() {
                        exprs.push(RuntimeFilterExpr {
                            filter_id: entry.id,
                            expr,
                            stats: entry.stats.clone(),
                        });
                    }
                    if let Some(expr) = entry.min_max.clone() {
                        exprs.push(RuntimeFilterExpr {
                            filter_id: entry.id,
                            expr,
                            stats: entry.stats.clone(),
                        });
                    }
                    exprs
                })
                .collect(),
        );

        let mut states = Vec::with_capacity(parts.len());
        let mut any_need_async = false;

        for part in parts.into_iter() {
            let prune_start = Instant::now();
            self.stats.blocks_total.fetch_add(1, Ordering::Relaxed);

            if runtime_filter.prune(&self.func_ctx, self.table_schema.clone(), &part)? {
                self.stats.blocks_pruned.fetch_add(1, Ordering::Relaxed);
                let prune_duration = prune_start.elapsed();
                Profile::record_usize_profile(
                    ProfileStatisticsName::RuntimeFilterInlistMinMaxTime,
                    prune_duration.as_nanos() as usize,
                );
                continue;
            }

            let prune_duration = prune_start.elapsed();
            Profile::record_usize_profile(
                ProfileStatisticsName::RuntimeFilterInlistMinMaxTime,
                prune_duration.as_nanos() as usize,
            );

            // Sync read from cache
            let fuse_part = FuseBlockPartInfo::from_part(&part)?;
            let cached_reader = self.block_reader.sync_read_columns_data(
                &fuse_part.location,
                &fuse_part.columns_meta,
                &None, // TODO: handle ignore_column_ids from virtual columns
            );

            let need_async = !cached_reader.ranges_to_read.is_empty()
                || self.index_reader.is_some()
                || self.virtual_reader.is_some();

            any_need_async = any_need_async || need_async;

            states.push(PartProcessState {
                part,
                cached_reader,
            });
        }

        if any_need_async {
            Ok(PendingWork::CacheMiss(states))
        } else {
            Ok(PendingWork::CacheHit(states))
        }
    }

    fn process_cache_hit(&mut self, states: Vec<PartProcessState>) -> Result<DataBlock> {
        let mut fuse_part_infos = Vec::with_capacity(states.len());
        let mut sources = Vec::with_capacity(states.len());

        for state in states {
            fuse_part_infos.push(state.part);
            let block_read_result = self
                .block_reader
                .build_block_result_from_cache(state.cached_reader);
            sources.push(ParquetDataSource::Normal((block_read_result, None)));
        }

        debug!("ReadParquetDataSource (cache hit) parts: {}", sources.len());
        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            fuse_part_infos,
            sources,
        )))
    }

    async fn process_cache_miss(&mut self, states: Vec<PartProcessState>) -> Result<DataBlock> {
        let mut fuse_part_infos = Vec::with_capacity(states.len());
        let mut chunks = Vec::with_capacity(states.len());

        for state in states {
            fuse_part_infos.push(state.part.clone());

            let block_reader = self.block_reader.clone();
            let settings = self.read_settings;
            let index_reader = self.index_reader.clone();
            let virtual_reader = self.virtual_reader.clone();
            let part = state.part;
            let cached_reader = state.cached_reader;

            chunks.push(async move {
                databend_common_base::runtime::spawn(async move {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;

                    if let Some(index_reader) = index_reader.as_ref() {
                        let loc =
                            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                &fuse_part.location,
                                index_reader.index_id(),
                            );
                        if let Some(data) = index_reader
                            .read_parquet_data_by_merge_io(&settings, &loc)
                            .await
                        {
                            return Ok::<_, ErrorCode>(ParquetDataSource::AggIndex(data));
                        }
                    }

                    let virtual_source = if let Some(virtual_reader) = virtual_reader.as_ref() {
                        let virtual_block_meta = fuse_part
                            .block_meta_index
                            .as_ref()
                            .and_then(|b| b.virtual_block_meta.as_ref());
                        virtual_reader
                            .read_parquet_data_by_merge_io(
                                &settings,
                                &virtual_block_meta,
                                fuse_part.nums_rows,
                            )
                            .await
                    } else {
                        None
                    };

                    let source = if cached_reader.ranges_to_read.is_empty() {
                        // All columns cached
                        block_reader.build_block_result_from_cache(cached_reader)
                    } else {
                        // Need async read for cache miss columns
                        block_reader
                            .async_read_columns_data(
                                &settings,
                                &fuse_part.location,
                                &fuse_part.columns_meta,
                                cached_reader,
                            )
                            .await?
                    };

                    Ok(ParquetDataSource::Normal((source, virtual_source)))
                })
                .await
                .unwrap()
            });
        }

        debug!("ReadParquetDataSource (cache miss) parts: {}", chunks.len());
        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            fuse_part_infos,
            futures::future::try_join_all(chunks).await?,
        )))
    }
}

#[async_trait::async_trait]
impl Processor for NewReadParquetDataTransform {
    fn name(&self) -> String {
        "NewReadParquetDataTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.output_data.take() {
            self.output.push_data(Ok(data));
        }

        // Check if we have pending work
        if let Some(pending) = &self.pending_work {
            return match pending {
                PendingWork::CacheHit(_) => Ok(Event::Sync),
                PendingWork::CacheMiss(_) => Ok(Event::Async),
            };
        }

        // Pull new data
        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            if let Some(partition_meta) = BlockPartitionMeta::downcast_block(data_block) {
                if !partition_meta.part_ptr.is_empty() {
                    self.pending_work = Some(self.prepare_parts(partition_meta.part_ptr)?);
                    return match &self.pending_work {
                        Some(PendingWork::CacheHit(_)) => Ok(Event::Sync),
                        Some(PendingWork::CacheMiss(_)) => Ok(Event::Async),
                        None => unreachable!(),
                    };
                }
            }
        }

        if self.input.is_finished() {
            if !self.called_on_finish {
                self.called_on_finish = true;
                let unfinished = self
                    .unfinished_processors_count
                    .fetch_sub(1, Ordering::Relaxed);
                if unfinished == 1 {
                    let blocks_total = self.stats.blocks_total.load(Ordering::Relaxed);
                    let blocks_pruned = self.stats.blocks_pruned.load(Ordering::Relaxed);
                    info!(
                        "RUNTIME-FILTER: NewReadParquetDataTransform finished, scan_id: {}, blocks_total: {}, blocks_pruned: {}",
                        self.scan_id, blocks_total, blocks_pruned
                    );
                }
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(PendingWork::CacheHit(states)) = self.pending_work.take() {
            self.output_data = Some(self.process_cache_hit(states)?);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(PendingWork::CacheMiss(states)) = self.pending_work.take() {
            self.output_data = Some(self.process_cache_miss(states).await?);
        }
        Ok(())
    }
}
