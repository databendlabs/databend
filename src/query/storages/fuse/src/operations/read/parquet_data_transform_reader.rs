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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
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
use databend_common_sql::IndexType;
use databend_storages_common_io::ReadSettings;
use log::debug;

use super::parquet_data_source::ParquetDataSource;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;
use crate::pruning::RuntimeFilterExprKind;
use crate::pruning::SpatialRuntimePruner;

pub struct ReadStats {
    pub blocks_total: AtomicU64,
    pub blocks_pruned: AtomicU64,
}

pub struct ReadParquetDataTransform {
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
}

impl ReadParquetDataTransform {
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
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadParquetDataTransform {
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
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadParquetDataTransform {
    const NAME: &'static str = "AsyncReadParquetDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let parts = block_part_meta.part_ptr.clone();
                if !parts.is_empty() {
                    let mut chunks = Vec::with_capacity(parts.len());
                    let mut fuse_part_infos = Vec::with_capacity(parts.len());
                    let inlist_bloom_prune_threshold = self
                        .context
                        .get_settings()
                        .get_inlist_runtime_bloom_prune_threshold()?
                        as usize;

                    let runtime_filters = self.context.get_runtime_filters(self.scan_id);
                    let runtime_filter = ExprRuntimePruner::new(
                        self.func_ctx.clone(),
                        self.table_schema.clone(),
                        self.block_reader.operator(),
                        ReadSettings::from_ctx(&self.context)?,
                        inlist_bloom_prune_threshold,
                        runtime_filters
                            .iter()
                            .flat_map(|entry| {
                                let mut exprs = Vec::new();
                                if let Some(expr) = entry.inlist.clone() {
                                    exprs.push(RuntimeFilterExpr {
                                        filter_id: entry.id,
                                        kind: RuntimeFilterExprKind::Inlist,
                                        inlist_value_count: entry.inlist_value_count,
                                        expr,
                                        stats: entry.stats.clone(),
                                    });
                                }
                                if let Some(expr) = entry.min_max.clone() {
                                    exprs.push(RuntimeFilterExpr {
                                        filter_id: entry.id,
                                        kind: RuntimeFilterExprKind::MinMax,
                                        inlist_value_count: 0,
                                        expr,
                                        stats: entry.stats.clone(),
                                    });
                                }
                                exprs
                            })
                            .collect(),
                    );
                    let spatial_runtime_pruner = SpatialRuntimePruner::try_create(
                        self.table_schema.clone(),
                        self.block_reader.operator(),
                        self.read_settings,
                        &runtime_filters,
                    )?;
                    for part in parts.into_iter() {
                        self.stats.blocks_total.fetch_add(1, Ordering::Relaxed);
                        let prune_start = Instant::now();
                        let prune_result = runtime_filter.prune(&part).await?;
                        let prune_duration = prune_start.elapsed();
                        Profile::record_usize_profile(
                            ProfileStatisticsName::RuntimeFilterInlistMinMaxTime,
                            prune_duration.as_nanos() as usize,
                        );
                        if prune_result {
                            self.stats.blocks_pruned.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if let Some(spatial_runtime_pruner) = &spatial_runtime_pruner {
                            let spatial_prune_start = Instant::now();
                            let spatial_prune_result = spatial_runtime_pruner.prune(&part).await?;
                            let spatial_prune_duration = spatial_prune_start.elapsed();
                            Profile::record_usize_profile(
                                ProfileStatisticsName::RuntimeFilterSpatialTime,
                                spatial_prune_duration.as_nanos() as usize,
                            );
                            if spatial_prune_result {
                                self.stats.blocks_pruned.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        }

                        fuse_part_infos.push(part.clone());
                        let block_reader = self.block_reader.clone();
                        let settings = self.read_settings;
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
        self.unfinished_processors_count
            .fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}
