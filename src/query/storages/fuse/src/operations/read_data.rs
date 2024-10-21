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

use async_channel::Receiver;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_storages_common_index::BloomIndex;

use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::BloomIndexBuilder;
use crate::io::VirtualColumnReader;
use crate::operations::read::build_fuse_parquet_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_native_source_pipeline;
use crate::pruning::PruningContext;
use crate::pruning::SegmentLocation;
use crate::pruning_pipeline::AsyncBlockPruningTransform;
use crate::pruning_pipeline::BlockPruningTransform;
use crate::pruning_pipeline::CompactReadTransform;
use crate::pruning_pipeline::ExtractSegmentTransform;
use crate::pruning_pipeline::ReadSegmentSource;
use crate::pruning_pipeline::SendPartitionSink;
use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        projection: Projection,
        query_internal_columns: bool,
        update_stream_columns: bool,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.schema_with_stream();
        BlockReader::create(
            ctx,
            self.operator.clone(),
            table_schema,
            projection,
            query_internal_columns,
            update_stream_columns,
            put_cache,
        )
    }

    // Build the block reader.
    pub fn build_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            ctx,
            PushDownInfo::projection_of_push_downs(
                &self.schema_with_stream(),
                plan.push_downs.as_ref(),
            ),
            plan.query_internal_columns,
            plan.update_stream_columns,
            put_cache,
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        if !self.operator.info().native_capability().blocking {
            Ok(std::cmp::max(max_threads, max_io_requests))
        } else {
            // For blocking fs, we don't want this to be too large
            Ok(std::cmp::min(max_threads, max_io_requests).clamp(1, 48))
        }
    }

    fn apply_data_mask_policy_if_needed(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if let Some(data_mask_policy) = &plan.data_mask_policy {
            let num_input_columns = plan.schema().num_fields();
            let mut exprs = Vec::with_capacity(num_input_columns);
            let mut projection = Vec::with_capacity(num_input_columns);
            let mut mask_count = 0;
            for i in 0..num_input_columns {
                if let Some(raw_expr) = data_mask_policy.get(&i) {
                    let expr = raw_expr.as_expr(&BUILTIN_FUNCTIONS);
                    exprs.push(expr.project_column_ref(|_col_id| i));
                    projection.push(mask_count + num_input_columns);
                    mask_count += 1;
                } else {
                    projection.push(i);
                }
            }

            let ops = vec![
                BlockOperator::Map {
                    exprs,
                    projections: None,
                },
                BlockOperator::Project { projection },
            ];

            let query_ctx = ctx.clone();
            let func_ctx = query_ctx.get_function_context()?;

            pipeline.add_transformer(|| {
                CompoundBlockOperator::new(ops.clone(), func_ctx.clone(), num_input_columns)
            });
        }

        Ok(())
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let snapshot_loc = plan.statistics.snapshot.clone();
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(SegmentLocation {
                    segment_idx: lazy_part_info.segment_index,
                    location: lazy_part_info.segment_location.clone(),
                    snapshot_loc: snapshot_loc.clone(),
                });
            }
        }
        let enable_prune_pipeline = ctx.get_settings().get_enable_prune_pipeline()?;
        if !enable_prune_pipeline && !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_schema = self.schema_with_stream();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let table = table.clone();
                let table_schema = table_schema.clone();
                let ctx = query_ctx.clone();
                let push_downs = push_downs.clone();
                // let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(ctx, push_downs, table_schema, lazy_init_segments, 0)
                        .await?;

                    Result::<_>::Ok(partitions)
                })?;

                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        }

        let block_reader = self.build_block_reader(ctx.clone(), plan, put_cache)?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        let topk = plan
            .push_downs
            .as_ref()
            .filter(|_| self.is_native()) // Only native format supports topk push down.
            .and_then(|x| x.top_k(plan.schema().as_ref()));

        let index_reader = Arc::new(
            plan.push_downs
                .as_ref()
                .and_then(|p| p.agg_index.as_ref())
                .map(|agg| {
                    AggIndexReader::try_create(
                        ctx.clone(),
                        self.operator.clone(),
                        agg,
                        self.table_compression,
                        put_cache,
                    )
                })
                .transpose()?,
        );

        let virtual_reader = Arc::new(
            PushDownInfo::virtual_columns_of_push_downs(&plan.push_downs)
                .as_ref()
                .map(|virtual_columns| {
                    VirtualColumnReader::try_create(
                        ctx.clone(),
                        self.operator.clone(),
                        block_reader.schema(),
                        plan,
                        virtual_columns.clone(),
                        self.table_compression,
                        put_cache,
                    )
                })
                .transpose()?,
        );

        self.build_fuse_source_pipeline(
            ctx.clone(),
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
            index_reader,
            virtual_reader,
        )?;

        // replace the column which has data mask if needed
        self.apply_data_mask_policy_if_needed(ctx, plan, pipeline)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_fuse_source_pipeline(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        max_io_requests: usize,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<()> {
        let setting = ctx.get_settings();
        let max_threads = setting.get_max_threads()? as usize;
        let table_schema = self.schema_with_stream();
        let receiver = self.meta_receiver.lock().unwrap().clone();
        match storage_format {
            FuseStorageFormat::Native => build_fuse_native_source_pipeline(
                ctx,
                table_schema,
                pipeline,
                block_reader,
                max_threads,
                plan,
                top_k,
                max_io_requests,
                index_reader,
                virtual_reader,
                receiver,
            ),
            FuseStorageFormat::Parquet => build_fuse_parquet_source_pipeline(
                ctx,
                table_schema,
                pipeline,
                block_reader,
                plan,
                max_threads,
                max_io_requests,
                index_reader,
                virtual_reader,
            ),
        }
    }

    pub fn do_build_prune_pipeline(
        &self,
        table_ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<(Pipeline, Receiver<Partitions>)> {
        let mut pipeline = Pipeline::create();
        let table_schema = self.schema_with_stream();
        let dal = self.operator.clone();
        let push_downs = plan.push_downs.clone();
        let snapshot_loc = plan.statistics.snapshot.clone();
        let bloom_index_builder = if table_ctx
            .get_settings()
            .get_enable_auto_fix_missing_bloom_index()?
        {
            let storage_format = self.storage_format;

            let bloom_columns_map = self
                .bloom_index_cols()
                .bloom_index_fields(table_schema.clone(), BloomIndex::supported_type)?;

            Some(BloomIndexBuilder {
                table_ctx: table_ctx.clone(),
                table_schema: table_schema.clone(),
                table_dal: dal.clone(),
                storage_format,
                bloom_columns_map,
            })
        } else {
            None
        };

        let pruner_context = if !self.is_native() || self.cluster_key_meta.is_none() {
            PruningContext::try_create(
                &table_ctx,
                dal.clone(),
                table_schema.clone(),
                &push_downs,
                None,
                vec![],
                self.bloom_index_cols.clone(),
                8, // TODO
                bloom_index_builder,
            )
        } else {
            let cluster_keys = self.cluster_keys(table_ctx.clone());

            PruningContext::try_create(
                &table_ctx,
                dal.clone(),
                table_schema.clone(),
                &push_downs,
                self.cluster_key_meta.clone(),
                cluster_keys,
                self.bloom_index_cols.clone(),
                8, // TODO
                bloom_index_builder,
            )
        }?;

        pipeline.add_source(
            |output| {
                ReadSegmentSource::create(
                    table_ctx.clone(),
                    pruner_context.internal_column_pruner.clone(),
                    snapshot_loc.clone(),
                    output,
                )
            },
            1,
        )?;

        pipeline.add_transform(|input, output| {
            CompactReadTransform::create(
                dal.clone(),
                table_schema.clone(),
                pruner_context.range_pruner.clone(),
                input,
                output,
            )
        })?;

        pipeline.add_transform(ExtractSegmentTransform::create)?;

        if pruner_context.bloom_pruner.is_some() || pruner_context.inverted_index_pruner.is_some() {
            // Async block pruning
            pipeline.add_transform(|input, output| {
                AsyncBlockPruningTransform::create(pruner_context.clone(), input, output)
            })?;
        } else {
            // Sync block pruning
            pipeline.add_transform(|input, output| {
                BlockPruningTransform::create(pruner_context.clone(), input, output)
            })?;
        }

        let (sender, receiver) = async_channel::bounded(1);

        pipeline.add_sink(|input| {
            SendPartitionSink::create(
                table_ctx.clone(),
                table_schema.clone(),
                push_downs.clone(),
                self.is_native(),
                sender.clone(),
                input,
            )
        })?;

        Ok((pipeline, receiver))
    }
}
