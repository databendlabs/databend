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
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::fuse_source::build_fuse_source_pipeline;
use crate::pruning::FusePruner;
use crate::pruning::SegmentLocation;
use crate::BlockRangeFilterTransform;
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

        build_fuse_source_pipeline(
            ctx,
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
        )
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

        return pipeline.add_transform(|input, output| {
            Ok(PartInfoConvertTransform::create(
                input,
                output,
                self.table_info.schema(),
            ))
        });
    }
}
