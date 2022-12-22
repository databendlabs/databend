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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_pipeline_transforms::processors::transforms::try_add_multi_sort_merge;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::SortMergeCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortMerge;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_storages_table_meta::meta::BlockMeta;

use crate::operations::FuseTableSink;
use crate::operations::ReadDataKind;
use crate::operations::ReclusterMutator;
use crate::pipelines::Pipeline;
use crate::pruning::BlockPruner;
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
        pipeline: &mut Pipeline,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<Box<dyn TableMutator>>> {
        if self.cluster_key_meta.is_none() {
            return Ok(None);
        }

        let snapshot_opt = self.read_table_snapshot().await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let schema = self.table_info.schema();
        let segments_locations = snapshot.segments.clone();
        let block_metas = BlockPruner::prune(
            &ctx,
            self.operator.clone(),
            schema,
            &push_downs,
            segments_locations,
        )
        .await?;

        let default_cluster_key_id = self.cluster_key_meta.clone().unwrap().0;
        let mut blocks_map: BTreeMap<i32, Vec<(usize, Arc<BlockMeta>)>> = BTreeMap::new();
        block_metas.iter().for_each(|(idx, b)| {
            if let Some(stats) = &b.cluster_stats {
                if stats.cluster_key_id == default_cluster_key_id && stats.level >= 0 {
                    blocks_map
                        .entry(stats.level)
                        .or_default()
                        .push((idx.0, b.clone()));
                }
            }
        });

        let block_compact_thresholds = self.get_block_compact_thresholds();
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
            block_compact_thresholds,
            blocks_map,
            self.operator.clone(),
        )?;

        let need_recluster = mutator.target_select().await?;
        if !need_recluster {
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
        let description = statistics.get_description(&table_info.desc);
        let plan = DataSourcePlan {
            catalog: table_info.catalog().to_string(),
            source_info: DataSourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        };

        ctx.try_set_partitions(plan.parts.clone())?;

        // ReadDataKind to avoid OOM.
        self.do_read_data(
            ctx.clone(),
            &plan,
            pipeline,
            ReadDataKind::OptimizeDataLessIORequests,
        )?;

        let cluster_stats_gen = self.get_cluster_stats_gen(
            ctx.clone(),
            pipeline,
            mutator.level() + 1,
            block_compact_thresholds,
        )?;

        // sort
        let sort_descs: Vec<SortColumnDescription> = self
            .cluster_keys()
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
        let block_size = ctx.get_settings().get_max_block_size()? as usize;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(block_size, None, sort_descs.clone()),
            )
        })?;

        // construct output fields
        let mut output_fields = plan.schema().fields().clone();
        for expr in self.cluster_keys().iter() {
            let cname = expr.column_name();
            if !output_fields.iter().any(|x| x.name() == &cname) {
                let field = DataField::new(&cname, expr.data_type());
                output_fields.push(field);
            }
        }

        try_add_multi_sort_merge(
            pipeline,
            DataSchemaRefExt::create(output_fields),
            block_size,
            None,
            sort_descs,
        )?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(block_compact_thresholds, true),
            )
        })?;

        pipeline.add_sink(|input| {
            FuseTableSink::try_create(
                input,
                ctx.clone(),
                block_per_seg,
                self.operator.clone(),
                self.meta_location_generator().clone(),
                cluster_stats_gen.clone(),
                block_compact_thresholds,
                self.storage_format,
                None,
            )
        })?;
        Ok(Some(Box::new(mutator)))
    }
}
