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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::try_add_multi_sort_merge;
use common_pipeline_transforms::processors::transforms::try_create_transform_sort_merge;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_sql::evaluator::CompoundBlockOperator;
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::FuseTableSink;
use crate::operations::ReclusterMutator;
use crate::pipelines::Pipeline;
use crate::pruning::FusePruner;
use crate::FuseTable;
use crate::TableMutator;
use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;

impl FuseTable {
    #[async_backtrace::framed]
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
        let segment_locations = snapshot.segments.clone();
        let pruner = FusePruner::create(&ctx, self.operator.clone(), schema, &push_downs)?;
        let block_metas = pruner.pruning(segment_locations, None, None).await?;

        let default_cluster_key_id = self.cluster_key_meta.clone().unwrap().0;
        let mut blocks_map: BTreeMap<i32, Vec<(usize, Arc<BlockMeta>)>> = BTreeMap::new();
        block_metas.iter().for_each(|(idx, b)| {
            if let Some(stats) = &b.cluster_stats {
                if stats.cluster_key_id == default_cluster_key_id && stats.level >= 0 {
                    blocks_map
                        .entry(stats.level)
                        .or_default()
                        .push((idx.segment_idx, b.clone()));
                }
            }
        });

        let block_thresholds = self.get_block_thresholds();
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

        let mut mutator = ReclusterMutator::try_create(
            ctx.clone(),
            self.meta_location_generator.clone(),
            snapshot,
            threshold,
            block_thresholds,
            blocks_map,
            self.operator.clone(),
        )?;

        let need_recluster = mutator.target_select().await?;
        if !need_recluster {
            return Ok(None);
        }

        let partitions_total = mutator.partitions_total();

        let block_metas: Vec<_> = mutator
            .selected_blocks()
            .iter()
            .map(|meta| (None, meta.clone()))
            .collect();
        let (statistics, parts) = self.read_partitions_with_metas(
            self.table_info.schema(),
            None,
            &block_metas,
            partitions_total,
            PruningStatistics::default(),
        )?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(&table_info.desc);
        let plan = DataSourcePlan {
            catalog: table_info.catalog().to_string(),
            source_info: DataSourceInfo::TableSource(table_info.clone()),
            output_schema: table_info.schema(),
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
            query_internal_columns: false,
        };

        ctx.set_partitions(plan.parts.clone())?;

        // ReadDataKind to avoid OOM.
        self.do_read_data(ctx.clone(), &plan, pipeline)?;

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), mutator.level() + 1, block_thresholds)?;
        let operators = cluster_stats_gen.operators.clone();
        if !operators.is_empty() {
            let num_input_columns = self.table_info.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();
            pipeline.add_transform(move |input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx2.clone(),
                    operators.clone(),
                )))
            })?;
        }

        // sort
        let sort_descs: Vec<SortColumnDescription> = cluster_stats_gen
            .cluster_key_index
            .iter()
            .map(|offset| SortColumnDescription {
                offset: *offset,
                asc: true,
                nulls_first: false,
                is_nullable: false, // This information is not needed here.
            })
            .collect();

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            Ok(ProcessorPtr::create(TransformSortPartial::try_create(
                transform_input_port,
                transform_output_port,
                None,
                sort_descs.clone(),
            )?))
        })?;

        let block_size = ctx.get_settings().get_max_block_size()? as usize;
        // construct output fields
        let output_fields: Vec<DataField> = cluster_stats_gen.out_fields.clone();
        let schema = DataSchemaRefExt::create(output_fields);

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(try_create_transform_sort_merge(
                input,
                output,
                schema.clone(),
                block_size,
                None,
                sort_descs.clone(),
            )?))
        })?;

        try_add_multi_sort_merge(pipeline, schema, block_size, None, sort_descs)?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            Ok(ProcessorPtr::create(TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(block_thresholds, true),
            )?))
        })?;

        pipeline.add_sink(|input| {
            FuseTableSink::try_create(
                input,
                ctx.clone(),
                self.get_write_settings(),
                self.operator.clone(),
                self.meta_location_generator().clone(),
                cluster_stats_gen.clone(),
                block_thresholds,
                self.table_info.schema(),
                None,
            )
        })?;
        Ok(Some(Box::new(mutator)))
    }
}
