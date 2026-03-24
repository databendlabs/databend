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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
use log::info;

use super::read_block_context::ReadBlockContext;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning::ExprRuntimePruner;
use crate::pruning::RuntimeFilterExpr;
use crate::pruning::RuntimeFilterExprKind;
use crate::pruning::SpatialRuntimePruner;

pub struct ReadDataProgress {
    processed_parts: AtomicUsize,
    read_parts: AtomicUsize,
    active_transforms: AtomicUsize,
    last_logged_milestone: AtomicUsize,
}

impl ReadDataProgress {
    pub fn new(active_transforms: usize) -> Arc<Self> {
        Arc::new(Self {
            processed_parts: AtomicUsize::new(0),
            read_parts: AtomicUsize::new(0),
            active_transforms: AtomicUsize::new(active_transforms),
            last_logged_milestone: AtomicUsize::new(0),
        })
    }

    fn report(&self, scan_id: IndexType) {
        let processed = self.processed_parts.load(Ordering::Relaxed);
        let read = self.read_parts.load(Ordering::Relaxed);
        let milestone = processed / 1000;
        let prev = self.last_logged_milestone.fetch_max(milestone, Ordering::Relaxed);
        if milestone > prev {
            info!(
                "[ReadData] scan_id: {}, progress: processed {} parts, pruned by runtime filter {} parts, read {} parts",
                scan_id,
                processed,
                processed.saturating_sub(read),
                read,
            );
        }
    }

    fn finish(&self, scan_id: IndexType) {
        let remaining = self.active_transforms.fetch_sub(1, Ordering::AcqRel);
        if remaining == 1 {
            let processed = self.processed_parts.load(Ordering::Acquire);
            let read = self.read_parts.load(Ordering::Acquire);
            info!(
                "[ReadData] scan_id: {}, completed: processed {} parts, pruned by runtime filter {} parts, read {} parts",
                scan_id,
                processed,
                processed.saturating_sub(read),
                read,
            );
        }
    }
}

pub struct ReadDataTransform {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,
    table_schema: Arc<TableSchema>,
    scan_id: IndexType,
    context: Arc<dyn TableContext>,
    progress: Arc<ReadDataProgress>,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        scan_id: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        progress: Arc<ReadDataProgress>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadDataTransform {
                func_ctx,
                block_reader,
                read_block_context,
                table_schema,
                scan_id,
                context: ctx,
                progress,
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
        let read_settings = self.read_block_context.read_settings();
        let inlist_bloom_prune_threshold =
            self.context
                .get_settings()
                .get_inlist_runtime_bloom_prune_threshold()? as usize;
        let runtime_filters = self.context.get_runtime_filters(self.scan_id);

        let runtime_filter = ExprRuntimePruner::new(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            self.block_reader.operator(),
            read_settings,
            inlist_bloom_prune_threshold,
            runtime_filters
                .iter()
                .flat_map(Self::build_runtime_filter_exprs)
                .collect(),
        );
        let spatial_runtime_pruner = SpatialRuntimePruner::try_create(
            self.table_schema.clone(),
            self.block_reader.operator(),
            read_settings,
            &runtime_filters,
        )?;

        Ok((runtime_filter, spatial_runtime_pruner))
    }

    async fn read_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let num_parts = parts.len();
        let mut read_tasks = Vec::with_capacity(num_parts);
        let mut parts_to_read = Vec::with_capacity(num_parts);
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
            let read_block_context = self.read_block_context.clone();

            read_tasks.push(async move {
                databend_common_base::runtime::spawn(async move {
                    read_block_context.read_data(part).await
                })
                .await
                .unwrap()
            });
        }

        let num_read = parts_to_read.len();
        self.progress.processed_parts.fetch_add(num_parts, Ordering::Relaxed);
        self.progress.read_parts.fetch_add(num_read, Ordering::Relaxed);
        self.progress.report(self.scan_id);

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

    async fn on_finish(&mut self) -> Result<()> {
        self.progress.finish(self.scan_id);
        Ok(())
    }
}
