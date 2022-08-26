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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::SortColumnDescription;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SinkPipeBuilder;
use common_pipeline_transforms::processors::transforms::SortMergeCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortMerge;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;

use crate::operations::FuseTableSink;
use crate::operations::ReclusterMutator;
use crate::FuseTable;
use crate::TableMutator;
use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;

impl FuseTable {
    pub(crate) async fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        pipeline: &mut Pipeline,
    ) -> Result<Option<Arc<dyn TableMutator>>> {
        if self.cluster_key_meta.is_none() {
            return Ok(None);
        }

        let snapshot_opt = self.read_table_snapshot(ctx.clone()).await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        if snapshot.summary.block_count <= 1 {
            return Ok(None);
        }

        let block_compactor = self.get_block_compactor();
        let avg_depth_threshold = self.get_option(
            FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD,
            DEFAULT_AVG_DEPTH_THRESHOLD,
        );
        let block_count = snapshot.summary.block_count;
        let threshold = if block_count > 100 {
            block_count as f64 * avg_depth_threshold
        } else {
            1.0
        };
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let mut mutator = ReclusterMutator::try_create(
            ctx.clone(),
            self.meta_location_generator.clone(),
            snapshot,
            threshold,
            block_compactor.clone(),
        )?;

        let need_recluster = mutator.blocks_select().await?;
        if !need_recluster {
            return Ok(None);
        }

        let partitions_total = mutator.partitions_total();
        let (statistics, parts) = self.read_partitions_with_metas(
            ctx.clone(),
            None,
            mutator.selected_blocks(),
            partitions_total,
        )?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(table_info);
        let plan = ReadDataSourcePlan {
            catalog,
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        };

        ctx.try_set_partitions(plan.parts.clone())?;
        self.do_read2(ctx.clone(), &plan, pipeline)?;

        let cluster_stats_gen = self.get_cluster_stats_gen(
            ctx.clone(),
            pipeline,
            mutator.level() + 1,
            block_compactor.clone(),
        )?;

        // sort
        let sort_descs: Vec<SortColumnDescription> = self
            .cluster_keys
            .iter()
            .map(|expr| SortColumnDescription {
                column_name: expr.column_name(),
                asc: true,
                nulls_first: false,
            })
            .collect();
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortPartial::try_create(
                transform_input_port,
                transform_output_port,
                None,
                sort_descs.clone(),
            )
        })?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(None, sort_descs.clone()),
            )
        })?;
        pipeline.resize(1)?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(None, sort_descs.clone()),
            )
        })?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                block_compactor.to_compactor(),
            )
        })?;

        let da = ctx.get_storage_operator()?;
        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                FuseTableSink::try_create(
                    input_port,
                    ctx.clone(),
                    block_per_seg,
                    da.clone(),
                    self.meta_location_generator().clone(),
                    cluster_stats_gen.clone(),
                )?,
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(Some(Arc::new(mutator)))
    }
}
