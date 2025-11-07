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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::IndexType;
use databend_storages_common_io::ReadSettings;
use log::debug;
use log::info;

use super::parquet_data_source::ParquetDataSource;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;

pub struct ReadStats {
    pub blocks_total: AtomicU64,
    pub blocks_pruned: AtomicU64,
}

pub struct ReadParquetDataTransform<const BLOCKING_IO: bool> {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    stats: Arc<ReadStats>,
    unfinished_processors_count: Arc<AtomicU64>,
}

impl ReadParquetDataTransform<true> {
    pub fn create(
        scan_id: IndexType,
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
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            ReadParquetDataTransform::<true> {
                func_ctx,
                block_reader,
                index_reader,
                virtual_reader,
                table_schema,
                scan_id,
                context: ctx,
                stats,
                unfinished_processors_count,
            },
        )))
    }
}

impl ReadParquetDataTransform<false> {
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
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadParquetDataTransform::<false> {
                func_ctx,
                block_reader,
                index_reader,
                virtual_reader,
                table_schema,
                scan_id: table_index,
                context: ctx,
                stats,
                unfinished_processors_count,
            },
        )))
    }
}

impl Transform for ReadParquetDataTransform<true> {
    const NAME: &'static str = "SyncReadParquetDataTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let mut partitions = block_part_meta.part_ptr.clone();
                debug_assert!(partitions.len() == 1);
                let part = partitions.pop().unwrap();
                let prune_start = Instant::now();
                let mut filters = self.context.get_inlist_runtime_filter_with_id(self.scan_id);
                filters.extend(
                    self.context
                        .get_min_max_runtime_filter_with_id(self.scan_id),
                );

                let exists_runtime_filter = !filters.is_empty();

                let runtime_filter = ExprRuntimePruner::new(filters);

                self.stats.blocks_total.fetch_add(1, Ordering::Relaxed);
                let prune_result =
                    runtime_filter.prune(&self.func_ctx, self.table_schema.clone(), &part)?;
                let prune_duration = prune_start.elapsed();
                if exists_runtime_filter {
                    Profile::record_usize_profile(
                        ProfileStatisticsName::RuntimeFilterInlistMinMaxTime,
                        prune_duration.as_nanos() as usize,
                    );
                }
                if prune_result {
                    self.stats.blocks_pruned.fetch_add(1, Ordering::Relaxed);
                    return Ok(DataBlock::empty());
                }

                if let Some(index_reader) = self.index_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let loc =
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            &fuse_part.location,
                            index_reader.index_id(),
                        );
                    if let Some(data) = index_reader.sync_read_parquet_data_by_merge_io(
                        &ReadSettings::from_ctx(&self.context)?,
                        &loc,
                    ) {
                        // Read from aggregating index.
                        return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                            vec![part.clone()],
                            vec![ParquetDataSource::AggIndex(data)],
                        )));
                    }
                }

                // If virtual column file exists, read the data from the virtual columns directly.
                let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let virtual_block_meta = fuse_part
                        .block_meta_index
                        .as_ref()
                        .and_then(|b| b.virtual_block_meta.as_ref());
                    virtual_reader.sync_read_parquet_data_by_merge_io(
                        &ReadSettings::from_ctx(&self.context)?,
                        &virtual_block_meta,
                        fuse_part.nums_rows,
                    )
                } else {
                    None
                };
                let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                    &virtual_source.ignore_column_ids
                } else {
                    &None
                };

                let source = self.block_reader.sync_read_columns_data_by_merge_io(
                    &ReadSettings::from_ctx(&self.context)?,
                    &part,
                    ignore_column_ids,
                )?;

                return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                    vec![part],
                    vec![ParquetDataSource::Normal((source, virtual_source))],
                )));
            }
        }
        Err(ErrorCode::Internal(
            "ReadParquetDataTransform get wrong meta data",
        ))
    }

    fn on_finish(&mut self) -> Result<()> {
        let unfinished_processors_count = self
            .unfinished_processors_count
            .fetch_sub(1, Ordering::Relaxed);
        if unfinished_processors_count == 1 {
            let blocks_total = self.stats.blocks_total.load(Ordering::Relaxed);
            let blocks_pruned = self.stats.blocks_pruned.load(Ordering::Relaxed);
            info!("RUNTIME-FILTER: ReadParquetDataTransform finished, scan_id: {}, blocks_total: {}, blocks_pruned: {}", self.scan_id, blocks_total, blocks_pruned);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadParquetDataTransform<false> {
    const NAME: &'static str = "AsyncReadParquetDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let parts = block_part_meta.part_ptr.clone();
                if !parts.is_empty() {
                    let mut chunks = Vec::with_capacity(parts.len());
                    let mut filters = self.context.get_inlist_runtime_filter_with_id(self.scan_id);
                    filters.extend(
                        self.context
                            .get_min_max_runtime_filter_with_id(self.scan_id),
                    );
                    let mut fuse_part_infos = Vec::with_capacity(parts.len());

                    let runtime_filter = ExprRuntimePruner::new(filters.clone());
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

                        fuse_part_infos.push(part.clone());
                        let block_reader = self.block_reader.clone();
                        let settings = ReadSettings::from_ctx(&self.context)?;
                        let index_reader = self.index_reader.clone();
                        let virtual_reader = self.virtual_reader.clone();

                        chunks.push(async move {
                            databend_common_base::runtime::spawn(async move {
                                let part = FuseBlockPartInfo::from_part(&part)?;

                                if let Some(index_reader) = index_reader.as_ref() {
                                    let loc =
                                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                            &part.location,
                                            index_reader.index_id(),
                                        );
                                    if let Some(data) = index_reader
                                        .read_parquet_data_by_merge_io(&settings, &loc)
                                        .await
                                    {
                                        // Read from aggregating index.
                                        return Ok::<_, ErrorCode>(ParquetDataSource::AggIndex(data));
                                    }
                                }

                                // If virtual column file exists, read the data from the virtual columns directly.
                                let virtual_source = if let Some(virtual_reader) = virtual_reader.as_ref() {
                                    let virtual_block_meta = part.block_meta_index.as_ref().and_then(|b| b.virtual_block_meta.as_ref());
                                    virtual_reader
                                        .read_parquet_data_by_merge_io(&settings, &virtual_block_meta, part.nums_rows)
                                        .await
                                } else {
                                    None
                                };

                                let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                                    &virtual_source.ignore_column_ids
                                } else {
                                    &None
                                };

                                let source = block_reader
                                    .read_columns_data_by_merge_io(
                                        &settings,
                                        &part.location,
                                        &part.columns_meta,
                                        ignore_column_ids,
                                    )
                                    .await?;

                                Ok(ParquetDataSource::Normal((source, virtual_source)))
                            })
                                .await
                                .unwrap()
                        });
                    }

                    debug!("ReadParquetDataSource parts: {}", chunks.len());
                    return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                        fuse_part_infos,
                        futures::future::try_join_all(chunks).await?,
                    )));
                }
            }
        }

        Err(ErrorCode::Internal(
            "AsyncReadParquetDataSource get wrong meta data",
        ))
    }

    async fn on_finish(&mut self) -> Result<()> {
        let unfinished_processors_count = self
            .unfinished_processors_count
            .fetch_sub(1, Ordering::Relaxed);
        if unfinished_processors_count == 1 {
            let blocks_total = self.stats.blocks_total.load(Ordering::Relaxed);
            let blocks_pruned = self.stats.blocks_pruned.load(Ordering::Relaxed);
            info!("RUNTIME-FILTER: AsyncReadParquetDataTransform finished, scan_id: {}, blocks_total: {}, blocks_pruned: {}", self.scan_id, blocks_total, blocks_pruned);
        }
        Ok(())
    }
}
