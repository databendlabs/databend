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

use common_base::runtime::Runtime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use crate::bloom_fields;
use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::read::build_fuse_parquet_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_native_source_pipeline;
use crate::pruning::FusePruner;
use crate::pruning::SegmentLocation;
use crate::BlockRangeFilterTransform;
use crate::BloomFilterReadTransform;
use crate::BloomFilterTransform;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::PartInfoConvertTransform;
use crate::SegmentFilterTransform;
use crate::SegmentReadTransform;
use crate::SnapshotReadSource;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        projection: Projection,
        query_internal_columns: bool,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.table_info.schema();
        BlockReader::create(
            self.operator.clone(),
            table_schema,
            projection,
            ctx,
            query_internal_columns,
        )
    }

    // Build the block reader.
    fn build_block_reader(
        &self,
        plan: &DataSourcePlan,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs),
            plan.query_internal_columns,
            ctx,
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        if !self.operator.info().can_blocking() {
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

            let ops = vec![BlockOperator::Map { exprs }, BlockOperator::Project {
                projection,
            }];

            let query_ctx = ctx.clone();
            let func_ctx = query_ctx.get_function_context()?;

            pipeline.add_transform(|input, output| {
                let transform = CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx.clone(),
                    ops.clone(),
                );
                Ok(ProcessorPtr::create(transform))
            })?;
        }

        Ok(())
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let max_io_requests = self.adjust_io_request(&ctx)?;
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

        if !lazy_init_segments.is_empty() {
            self.index_analyzer(&ctx, &plan, pipeline)?;
        }

        let block_reader = self.build_block_reader(plan, ctx.clone())?;

        let topk = plan.push_downs.as_ref().and_then(|x| {
            x.top_k(
                plan.schema().as_ref(),
                self.cluster_key_str(),
                RangeIndex::supported_type,
            )
        });

        Self::build_fuse_source_pipeline(
            ctx.clone(),
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
        )?;

        // replace the column which has data mask if needed
        self.apply_data_mask_policy_if_needed(ctx, plan, pipeline)?;

        Ok(())
    }

    fn build_fuse_source_pipeline(
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        max_io_requests: usize,
    ) -> Result<()> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;

        match storage_format {
            FuseStorageFormat::Native => build_fuse_native_source_pipeline(
                ctx,
                pipeline,
                block_reader,
                max_threads,
                plan,
                top_k,
                max_io_requests,
            ),
            FuseStorageFormat::Parquet => build_fuse_parquet_source_pipeline(
                ctx,
                pipeline,
                block_reader,
                plan,
                max_threads,
                max_io_requests,
            ),
        }
    }

    fn index_analyzer(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &&DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_loc = plan.statistics.snapshot.clone();
        let operator = self.operator.clone();
        let table_schema = self.table_info.schema();
        let snapshot_loc = snapshot_loc.clone().unwrap();

        // TODO: remove
        let pruner = if !self.is_native() || self.cluster_key_meta.is_none() {
            FusePruner::create(
                &ctx,
                operator.clone(),
                self.table_info.schema(),
                &plan.push_downs,
            )?
        } else {
            let cluster_keys = self.cluster_keys(ctx.clone());

            FusePruner::create_with_pages(
                &ctx,
                operator.clone(),
                self.table_info.schema(),
                &plan.push_downs,
                self.cluster_key_meta.clone(),
                cluster_keys,
            )?
        };

        pipeline.add_source(
            |output| SnapshotReadSource::create(ctx.clone(), output, snapshot_loc.clone()),
            1,
        )?;

        pipeline.add_transform(|input, output| {
            SegmentReadTransform::create(input, output, operator.clone(), table_schema.clone())
        })?;

        pipeline.add_transform(|input, output| {
            SegmentFilterTransform::create(input, output, pruner.pruning_ctx.clone())
        })?;

        pipeline.add_transform(|input, output| {
            BlockRangeFilterTransform::create(pruner.pruning_ctx.clone(), input, output)
        })?;

        let filter_expr = plan
            .push_downs
            .as_ref()
            .and_then(|extra| extra.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS)));

        if let Some(filter_expr) = filter_expr {
            let index_fields = bloom_fields(&filter_expr, &table_schema)?;

            if !index_fields.is_empty() {
                pipeline.add_transform(|input, output| {
                    BloomFilterReadTransform::create(
                        input,
                        output,
                        operator.clone(),
                        index_fields.clone(),
                    )
                })?;

                let func_ctx = ctx.get_function_context()?;
                pipeline.add_transform(|input, output| {
                    BloomFilterTransform::create(
                        input,
                        output,
                        func_ctx.clone(),
                        table_schema.clone(),
                        filter_expr.clone(),
                    )
                })?;
            }
        }

        return pipeline.add_transform(|input, output| {
            Ok(PartInfoConvertTransform::create(
                input,
                output,
                self.table_info.schema(),
            ))
        });
    }
}
