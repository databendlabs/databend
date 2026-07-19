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

use std::any::Any;

use databend_common_catalog::plan::BlockMetaOptions;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::LimitType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_ordered_compact_pipeline;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformVectorCluster;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub meta: PhysicalPlanMeta,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for Recluster {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(Recluster {
            meta: self.meta.clone(),
            tasks: self.tasks.clone(),
            table_info: self.table_info.clone(),
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    /// The flow of Pipeline is as follows:
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
    // └──────────┘     └───────────────┘     └─────────┘    │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
    // └──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
    // └──────────┘     └───────────────┘     └─────────┘                                             │
    // ┌──────────────────────────────────────────────────────────────────────────────────────────────┘
    // │         ┌──────────────┐
    // │    ┌───►│SerializeBlock├───┐
    // │    │    └──────────────┘   │
    // │    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────┐     ┌──────────┐
    // └───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│ReclusterAggr├────►│CommitSink│
    //      │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────┘     └──────────┘
    //      │    ┌──────────────┐   │
    //      └───►│SerializeBlock├───┘
    //           └──────────────┘
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        match self.tasks.len() {
            // Keep the pipeline constructible if an empty task list reaches
            // this layer; normal recluster planning filters no-op parts out.
            0 => builder.main_pipeline.add_source(EmptySource::create, 1),
            1 => {
                let table = builder
                    .ctx
                    .build_table_by_table_info(&self.table_info, None)?;
                let table = FuseTable::try_from_table(table.as_ref())?;

                let task = &self.tasks[0];
                let recluster_block_nums = task.parts.len();
                let block_thresholds = table.get_block_thresholds();
                let table_info = table.get_table_info();
                let schema = table.schema_with_stream();
                let description = task.stats.get_description(&table_info.desc);
                let plan = DataSourcePlan {
                    source_info: DataSourceInfo::TableSource(table_info.clone()),
                    output_schema: schema.clone(),
                    parts: task.parts.clone(),
                    statistics: task.stats.clone(),
                    description,
                    tbl_args: table.table_args(),
                    push_downs: None,
                    internal_columns: None,
                    base_block_ids: None,
                    block_meta_options: BlockMetaOptions::default()
                        .set_update_stream_columns(table.change_tracking_enabled()),
                    table_index: usize::MAX,
                    scan_id: usize::MAX,
                };

                {
                    metrics_inc_recluster_block_nums_to_read(recluster_block_nums as u64);
                    metrics_inc_recluster_block_bytes_to_read(task.total_bytes as u64);
                    metrics_inc_recluster_row_nums_to_read(task.total_rows as u64);

                    log::info!(
                        "recluster: scheduled blocks level={} block_count={} rows={} bytes={} compressed={}",
                        task.level,
                        recluster_block_nums,
                        task.total_rows,
                        task.total_bytes,
                        task.total_compressed,
                    );
                }

                builder.ctx.set_partitions(plan.parts.clone())?;

                table.read_data(
                    builder.ctx.clone(),
                    &plan,
                    &mut builder.main_pipeline,
                    false,
                )?;

                let num_input_columns = schema.fields().len();
                if table.change_tracking_enabled() {
                    let stream_ctx = StreamContext::try_create(
                        builder.ctx.get_function_context()?,
                        schema,
                        table_info.ident.seq,
                        false,
                        false,
                    )?;

                    builder
                        .main_pipeline
                        .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
                }

                let input_schema = DataSchema::from(table.schema_with_stream()).into();
                let cluster_stats_gen = table.get_cluster_stats_gen(
                    builder.ctx.clone(),
                    task.level + 1,
                    block_thresholds,
                    input_schema,
                )?;
                let operators = cluster_stats_gen.operators.clone();
                if !operators.is_empty() {
                    let func_ctx2 = cluster_stats_gen.func_ctx.clone();
                    builder.main_pipeline.add_transformer(move || {
                        CompoundBlockOperator::new(
                            operators.clone(),
                            func_ctx2.clone(),
                            num_input_columns,
                        )
                    });
                }

                let settings = builder.ctx.get_settings();
                let max_threads = settings.get_max_threads()? as usize;

                let (rows_per_block, bytes_per_block) = block_thresholds.calc_rows_for_recluster(
                    task.total_rows,
                    task.total_bytes,
                    task.total_compressed,
                );

                if let Some(vector_operator) = cluster_stats_gen.vector_operator.clone() {
                    builder.main_pipeline.try_resize(1)?;
                    builder.main_pipeline.add_accumulating_transformer(move || {
                        TransformVectorCluster::new(
                            vector_operator.vector_column_input_offset,
                            vector_operator.info.dimension,
                            vector_operator.info.distance_type,
                            rows_per_block,
                        )
                    });
                    builder.main_pipeline.try_resize(max_threads)?;
                }

                // construct output fields
                let output_fields = cluster_stats_gen.out_fields.clone();
                let schema = DataSchemaRefExt::create(output_fields);
                let sort_descs = cluster_stats_gen.sort_descs();
                let skip_partial_sort =
                    task.all_ordered && cluster_stats_gen.vector_operator.is_none();

                // merge sort
                let sort_pipeline_builder = SortPipelineBuilder::create(
                    builder.ctx.clone(),
                    schema,
                    sort_descs.into(),
                    None,
                    settings.get_enable_fixed_rows_sort()?,
                )?
                .with_block_size_hit(rows_per_block);
                if !skip_partial_sort {
                    let partial_sort_descs = sort_pipeline_builder.sort_column_desc();
                    builder.main_pipeline.add_transformer(move || {
                        TransformSortPartial::new(LimitType::None, partial_sort_descs.clone())
                    });
                }
                sort_pipeline_builder.build_merge_sort_pipeline(
                    &mut builder.main_pipeline,
                    false,
                    false,
                )?;

                // Compact after merge sort. This ordered compactor keeps block growth bounded
                // without requiring a hard post-sort size cap, since final serialized sizes are
                // not known yet and over-splitting here would create small fragmented blocks.
                let compact_thresholds = block_thresholds
                    .set_rows_per_block(rows_per_block)
                    .set_bytes_per_block(bytes_per_block);
                build_ordered_compact_pipeline(
                    &mut builder.main_pipeline,
                    compact_thresholds,
                    max_threads,
                    cluster_stats_gen.extra_key_num,
                )?;

                builder.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        let proc = TransformSerializeBlock::try_create(
                            builder.ctx.clone(),
                            transform_input_port,
                            transform_output_port,
                            table,
                            cluster_stats_gen.clone(),
                            MutationKind::Recluster,
                            self.table_meta_timestamps,
                        )?;
                        proc.into_processor()
                    },
                )
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}
