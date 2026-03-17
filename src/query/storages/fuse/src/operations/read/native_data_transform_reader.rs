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

use super::native_data_source::NativeDataSource;
use crate::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;
use crate::pruning::RuntimeFilterExprKind;
use crate::pruning::SpatialRuntimePruner;

pub struct ReadNativeDataTransform {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,

    index_reader: Arc<Option<AggIndexReader>>,

    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    read_settings: ReadSettings,
}

impl ReadNativeDataTransform {
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadNativeDataTransform {
                func_ctx,
                block_reader,
                index_reader,
                table_schema,
                scan_id,
                context: ctx,
                read_settings,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadNativeDataTransform {
    const NAME: &'static str = "AsyncReadNativeDataTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let parts = block_part_meta.part_ptr.clone();
                if !parts.is_empty() {
                    let mut chunks = Vec::with_capacity(parts.len());
                    let mut native_part_infos = Vec::with_capacity(parts.len());
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
                        if runtime_filter.prune(&part).await? {
                            continue;
                        }
                        if let Some(spatial_runtime_pruner) = &spatial_runtime_pruner {
                            if spatial_runtime_pruner.prune(&part).await? {
                                continue;
                            }
                        }

                        native_part_infos.push(part.clone());
                        let block_reader = self.block_reader.clone();
                        let index_reader = self.index_reader.clone();
                        let ctx = self.context.clone();
                        chunks.push(async move {
                            let handler = databend_common_base::runtime::spawn(async move {
                                let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                                if let Some(index_reader) = index_reader.as_ref() {
                                    let loc =
                                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                            &fuse_part.location,
                                            index_reader.index_id(),
                                        );
                                    if let Some(data) = index_reader.read_native_data(&loc).await {
                                        // Read from aggregating index.
                                        return Ok::<_, ErrorCode>(NativeDataSource::AggIndex(data));
                                    }
                                }

                                Ok(NativeDataSource::Normal(
                                    block_reader
                                        .async_read_native_columns_data(&part, &ctx, &None)
                                        .await?,
                                ))
                            });
                            handler.await.unwrap()
                        });
                    }

                    debug!("ReadNativeDataSource parts: {}", chunks.len());
                    return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                        native_part_infos,
                        futures::future::try_join_all(chunks).await?,
                    )));
                }
            }
        }

        Err(ErrorCode::Internal(
            "AsyncReadNativeDataTransform get wrong meta data",
        ))
    }
}
