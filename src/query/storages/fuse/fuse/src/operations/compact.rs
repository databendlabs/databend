//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::table::CompactTarget;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_planner::ReadDataSourcePlan;
use common_planner::SourceInfo;
use common_storages_table_meta::meta::TableSnapshot;

use super::FuseTableSink;
use crate::operations::mutation::SegmentCompactMutator;
use crate::operations::FullCompactMutator;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

pub struct CompactOptions {
    // the snapshot that compactor working on, it never changed during phases compaction.
    pub base_snapshot: Arc<TableSnapshot>,
    pub block_per_seg: usize,
    pub limit: Option<usize>,
}

impl FuseTable {
    pub(crate) async fn do_compact(
        &self,
        ctx: Arc<dyn TableContext>,
        target: CompactTarget,
        limit: Option<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<Option<Box<dyn TableMutator>>> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let base_snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(None);
        };

        if base_snapshot.summary.block_count <= 1 {
            return Ok(None);
        }

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let compact_params = CompactOptions {
            base_snapshot,
            block_per_seg,
            limit,
        };

        match target {
            CompactTarget::Blocks => self.compact_blocks(ctx, pipeline, compact_params).await,
            CompactTarget::Segments => self.compact_segments(ctx, pipeline, compact_params).await,
        }
    }

    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        options: CompactOptions,
    ) -> Result<Option<Box<dyn TableMutator>>> {
        let mut segment_mutator = SegmentCompactMutator::try_create(
            ctx.clone(),
            options,
            self.meta_location_generator().clone(),
            self.operator.clone(),
        )?;

        if segment_mutator.target_select().await? {
            Ok(Some(Box::new(segment_mutator)))
        } else {
            Ok(None)
        }
    }

    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        options: CompactOptions,
    ) -> Result<Option<Box<dyn TableMutator>>> {
        let block_compact_thresholds = self.get_block_compact_thresholds();

        let block_per_seg = options.block_per_seg;
        let mut mutator = FullCompactMutator::try_create(
            ctx.clone(),
            options,
            block_compact_thresholds.clone(),
            self.meta_location_generator().clone(),
            self.cluster_key_meta.is_some(),
            self.operator.clone(),
        )?;
        let need_compact = mutator.target_select().await?;
        if !need_compact {
            return Ok(None);
        }

        let partitions_total = mutator.partitions_total();
        let (statistics, parts) = self.read_partitions_with_metas(
            ctx.clone(),
            self.table_info.schema(),
            None,
            mutator.selected_blocks(),
            partitions_total,
        )?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(table_info);
        let plan = ReadDataSourcePlan {
            catalog: table_info.catalog().to_string(),
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        };

        ctx.try_set_partitions(plan.parts.clone())?;

        // It's easy to OOM if we set the max_io_request more than the max threads.
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        self.do_read_data(ctx.clone(), &plan, pipeline, max_threads)?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(block_compact_thresholds, false),
            )
        })?;

        pipeline.add_sink(|input| {
            FuseTableSink::try_create(
                input,
                ctx.clone(),
                block_per_seg,
                mutator.get_storage_operator(),
                self.meta_location_generator().clone(),
                ClusterStatsGenerator::default(),
                None,
            )
        })?;

        Ok(Some(Box::new(mutator)))
    }
}
