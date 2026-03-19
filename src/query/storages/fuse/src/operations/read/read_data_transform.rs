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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
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

use super::block_format::FuseBlockFormat;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;
use crate::pruning::RuntimeFilterExprKind;
use crate::pruning::SpatialRuntimePruner;

pub struct ReadDataTransform {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    block_format: Arc<dyn FuseBlockFormat>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    read_settings: ReadSettings,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        block_format: Arc<dyn FuseBlockFormat>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadDataTransform {
                func_ctx,
                block_reader,
                block_format,
                table_schema,
                scan_id,
                context: ctx,
                read_settings,
            },
        )))
    }

    fn build_runtime_filter_exprs(entry: &RuntimeFilterEntry) -> Vec<RuntimeFilterExpr> {
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
    }

    fn create_runtime_pruners(&self) -> Result<(ExprRuntimePruner, Option<SpatialRuntimePruner>)> {
        let inlist_bloom_prune_threshold =
            self.context
                .get_settings()
                .get_inlist_runtime_bloom_prune_threshold()? as usize;
        let runtime_filters = self.context.get_runtime_filters(self.scan_id);

        let runtime_filter = ExprRuntimePruner::new(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            self.block_reader.operator(),
            self.read_settings,
            inlist_bloom_prune_threshold,
            runtime_filters
                .iter()
                .flat_map(Self::build_runtime_filter_exprs)
                .collect(),
        );
        let spatial_runtime_pruner = SpatialRuntimePruner::try_create(
            self.table_schema.clone(),
            self.block_reader.operator(),
            self.read_settings,
            &runtime_filters,
        )?;

        Ok((runtime_filter, spatial_runtime_pruner))
    }

    async fn read_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let mut read_tasks = Vec::with_capacity(parts.len());
        let mut parts_to_read = Vec::with_capacity(parts.len());
        let (expr_runtime_pruner, spatial_runtime_pruner) = self.create_runtime_pruners()?;

        for part in parts {
            if expr_runtime_pruner.prune(&part).await? {
                continue;
            }

            if let Some(spatial_runtime_pruner) = &spatial_runtime_pruner {
                if spatial_runtime_pruner.prune(&part).await? {
                    continue;
                }
            }

            parts_to_read.push(part.clone());
            let block_format = self.block_format.clone();
            let settings = self.read_settings;

            read_tasks.push(async move {
                databend_common_base::runtime::spawn(async move {
                    block_format.read_data(part, settings).await
                })
                .await
                .unwrap()
            });
        }

        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            parts_to_read,
            futures::future::try_join_all(read_tasks).await?,
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadDataTransform {
    const NAME: &'static str = "AsyncReadDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let parts = data
            .get_meta()
            .and_then(BlockPartitionMeta::downcast_ref_from)
            .and_then(|meta| (!meta.part_ptr.is_empty()).then(|| meta.part_ptr.clone()))
            .ok_or_else(|| ErrorCode::Internal("AsyncReadDataTransform got wrong meta data"))?;

        self.read_parts(parts).await
    }
}
