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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_catalog::plan::BlockMetaOptions;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_local_ordered_compact_pipeline;
use databend_common_pipeline_transforms::build_ordered_compact_pipeline;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::HilbertRangeExchange;
use databend_common_storages_fuse::operations::HilbertRangeState;
use databend_common_storages_fuse::operations::TransformHilbertCluster;
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
                let mut cluster_stats_gen = table.get_cluster_stats_gen(
                    builder.ctx.clone(),
                    task.level + 1,
                    block_thresholds,
                    input_schema,
                )?;
                if !cluster_stats_gen.eval_operators.is_empty() {
                    let eval_operators = cluster_stats_gen.eval_operators.clone();
                    let func_ctx2 = cluster_stats_gen.func_ctx.clone();
                    builder.main_pipeline.add_transformer(move || {
                        CompoundBlockOperator::new(
                            eval_operators.clone(),
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

                let compact_thresholds = block_thresholds
                    .set_rows_per_block(rows_per_block)
                    .set_bytes_per_block(bytes_per_block);

                if cluster_stats_gen.is_hilbert() {
                    let dimension_offsets = cluster_stats_gen.hilbert_dimension_offsets()?;
                    let worker_count = builder.main_pipeline.output_len().max(1);
                    let target_blocks = task.total_rows.div_ceil(rows_per_block).max(1);
                    let num_collectors = max_threads
                        .min(target_blocks)
                        .clamp(1, u8::MAX as usize + 1);
                    let state = HilbertRangeState::create(
                        dimension_offsets,
                        task.total_rows,
                        worker_count,
                        num_collectors,
                    );

                    // Every input stream samples locally, then all streams replay against one
                    // immutable task-local weighted range plan.
                    let worker_id = AtomicUsize::new(0);
                    builder.main_pipeline.add_transform(|input, output| {
                        let id = worker_id.fetch_add(1, Ordering::Relaxed);
                        Ok(ProcessorPtr::create(TransformHilbertCluster::create(
                            input,
                            output,
                            state.clone(),
                            id,
                        )))
                    })?;

                    builder.main_pipeline.try_resize(num_collectors)?;
                    builder
                        .main_pipeline
                        .exchange(num_collectors, HilbertRangeExchange::create(state))?;

                    // Each collector owns a disjoint Hilbert-key interval. Sort only inside that
                    // interval, then compact locally so every output block covers a continuous key
                    // range without an extra global merge.
                    let mut sort_fields = cluster_stats_gen.out_fields.clone();
                    let hilbert_value_offset = sort_fields.len();
                    sort_fields.push(DataField::new(
                        "_task_hilbert_value",
                        DataType::Number(NumberDataType::UInt32),
                    ));
                    let sort_schema = DataSchemaRefExt::create(sort_fields);
                    let sort_desc = vec![SortColumnDescription {
                        offset: hilbert_value_offset,
                        asc: true,
                        nulls_first: false,
                    }];
                    SortPipelineBuilder::create(
                        builder.ctx.clone(),
                        sort_schema,
                        sort_desc.into(),
                        None,
                        settings.get_enable_fixed_rows_sort()?,
                    )?
                    .with_block_size_hit(rows_per_block)
                    .build_local_full_sort_pipeline(&mut builder.main_pipeline, false)?;

                    // The ordinary cluster statistics generator removes all trailing temporary
                    // columns before serialization; include the task-local Hilbert sort key.
                    cluster_stats_gen.extra_key_num += 1;
                    build_local_ordered_compact_pipeline(
                        &mut builder.main_pipeline,
                        compact_thresholds,
                        cluster_stats_gen.extra_key_num,
                    )?;
                } else {
                    if let Some(vector_operator) = cluster_stats_gen.vector_operator() {
                        let vector_column_input_offset = vector_operator.vector_column_input_offset;
                        let dimension = vector_operator.info.dimension;
                        let distance_type = vector_operator.info.distance_type;
                        builder.main_pipeline.try_resize(1)?;
                        builder.main_pipeline.add_accumulating_transformer(move || {
                            TransformVectorCluster::new(
                                vector_column_input_offset,
                                dimension,
                                distance_type,
                                rows_per_block,
                            )
                        });
                        builder.main_pipeline.try_resize(max_threads)?;
                    }

                    // Linear and vector clustering use their regular global row-sort pipeline.
                    let schema = DataSchemaRefExt::create(cluster_stats_gen.out_fields.clone());
                    let sort_descs = cluster_stats_gen.sort_descs();
                    let skip_partial_sort = task.all_ordered && cluster_stats_gen.is_linear();
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

                    build_ordered_compact_pipeline(
                        &mut builder.main_pipeline,
                        compact_thresholds,
                        max_threads,
                        cluster_stats_gen.extra_key_num,
                    )?;
                }

                // All layouts share the ordinary block statistics and serialization path after
                // they have formed output blocks and removed layout-only temporary columns.
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
                )?;

                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}
