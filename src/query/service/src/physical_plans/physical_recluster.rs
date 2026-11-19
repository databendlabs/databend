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

databend_common_tracing::register_module_tag!("[FUSE-RECLUSTER]");

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::plan::BlockMetaOptions;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::plan::ReclusterTaskKind;
use databend_common_catalog::plan::VerticalReclusterKind;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::TableDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::PlanScope;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::always_callback;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline::sources::SyncSourcer;
use databend_common_pipeline_transforms::OrderedBlockCompactBuilder;
use databend_common_pipeline_transforms::TransformCompactBlock;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::HilbertRangeExchange;
use databend_common_storages_fuse::operations::ReclusterIndexMergeInputs;
use databend_common_storages_fuse::operations::TransformHilbertCluster;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformVectorCluster;
use databend_common_storages_fuse::operations::VerticalReclusterSource;
use databend_common_storages_fuse::operations::add_aggregate_state_reaggregate_transform;
use databend_common_storages_fuse::operations::build_recluster_index_merge_pipeline;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use parking_lot::Mutex;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::processors::horizontal_recluster::HorizontalReclusterSource;
use crate::pipelines::processors::horizontal_recluster::TransformPrepareReclusterIndex;
use crate::pipelines::processors::horizontal_recluster::TransformReclusterOutputOrder;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextQueryProfile;
use crate::sessions::TableContextSettings;
use crate::spillers::ReclusterSpiller;

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
                if task.parts.is_empty() {
                    return Err(ErrorCode::Internal(
                        "recluster task must have source blocks",
                    ));
                }
                let settings = builder.ctx.get_settings();
                // Execution-time guard: planning-time admission runs on the
                // coordinator and cannot see this node's live memory pressure.
                // Oversized tasks are only tolerable when sort spill can absorb
                // the pressure; otherwise fail fast instead of risking an OOM kill.
                let multiway_merge = task.vertical_kind.is_none()
                    && task.kind == ReclusterTaskKind::MergeBlocks
                    && settings.get_enable_recluster_multiway_merge()?
                    && matches!(table.get_storage_format(), FuseStorageFormat::Parquet)
                    && !table.schema().fields().iter().any(|field| {
                        matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_)))
                    });
                if multiway_merge && task.merge_groups.len() > 1 {
                    return self.build_merge_groups_pipeline(builder, task);
                }
                if !multiway_merge && !builder.ctx.get_enable_sort_spill() {
                    let max_memory_usage = settings.get_max_memory_usage()? as usize;
                    // `max_memory_usage == 0` means memory usage is unlimited.
                    if max_memory_usage != 0 {
                        let global_used = GLOBAL_MEM_STAT.get_memory_usage();
                        let memory_budget = max_memory_usage.saturating_sub(global_used) * 30 / 100;
                        if task.total_bytes > memory_budget {
                            return Err(ErrorCode::MemoryExceedsLimit(format!(
                                "Not enough memory to execute recluster task on this node: task_bytes = {}, global_used = {}, max_memory_usage = {}.",
                                task.total_bytes, global_used, max_memory_usage
                            )));
                        }
                    }
                }
                let recluster_block_nums = task.parts.len();
                metrics_inc_recluster_block_nums_to_read(recluster_block_nums as u64);
                metrics_inc_recluster_block_bytes_to_read(task.total_bytes as u64);
                metrics_inc_recluster_row_nums_to_read(task.total_rows as u64);
                log::debug!(
                    event = "recluster.input_planned",
                    table_id = table.get_id(),
                    task_kind :? = task.kind,
                    input_levels :serde = task.input_level_stats;
                    "Recluster input planned"
                );
                if let Some(kind) = task.vertical_kind {
                    let expected = match kind {
                        VerticalReclusterKind::SortBlocks => "SortBlocks",
                        VerticalReclusterKind::MergeBlocks => "MergeBlocks",
                    };
                    log::debug!(
                        "recluster: scheduled vertical kind={} block_count={} rows={} budget={}",
                        expected,
                        recluster_block_nums,
                        task.total_rows,
                        task.memory_budget,
                    );
                    return builder.main_pipeline.add_source(
                        |output| {
                            SyncSourcer::create(
                                builder.ctx.get_scan_progress(),
                                output,
                                VerticalReclusterSource::create(
                                    builder.ctx.clone(),
                                    table.clone(),
                                    task.clone(),
                                    self.table_meta_timestamps,
                                ),
                            )
                        },
                        1,
                    );
                }

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

                let input_schema = DataSchema::from(table.schema_with_stream()).into();
                let mut cluster_stats_gen = table.get_cluster_stats_gen(
                    builder.ctx.clone(),
                    task.level + 1,
                    block_thresholds,
                    input_schema,
                )?;
                let index_merge = match multiway_merge {
                    true if settings.get_enable_recluster_inverted_index_merge()?
                        && !table.schema().fields().iter().any(|field| {
                            matches!(field.data_type(), TableDataType::AggregateState { .. })
                        }) =>
                    {
                        ReclusterIndexMergeInputs::try_create(table, task)?
                    }
                    _ => None,
                };
                if multiway_merge {
                    if !cluster_stats_gen.is_linear() {
                        return Err(ErrorCode::Internal(
                            "MergeBlocks requires linear cluster keys",
                        ));
                    }
                    HorizontalReclusterSource::build_pipeline(
                        builder.ctx.clone(),
                        &mut builder.main_pipeline,
                        table.clone(),
                        task,
                        &cluster_stats_gen,
                        index_merge.is_some(),
                    )?;
                } else {
                    builder.ctx.set_partitions(plan.parts.clone())?;

                    table.read_data(
                        builder.ctx.clone(),
                        &plan,
                        &mut builder.main_pipeline,
                        CachePolicy::READ_ONLY,
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
                }
                if index_merge.is_some() {
                    builder
                        .main_pipeline
                        .add_transformer(|| TransformReclusterOutputOrder { next_row: 0 });
                }
                let max_threads = settings.get_max_threads()? as usize;

                let (rows_per_block, bytes_per_block) = block_thresholds.calc_rows_for_recluster(
                    task.total_rows,
                    task.total_bytes,
                    task.total_compressed,
                );

                // Cut lines never drop below the table thresholds, so every output block,
                // including a constant-key one, is large enough to be judged complete.
                // The layout pipelines keep the task-scaled `rows_per_block`: Hilbert sizes
                // its partitions and vector clustering sizes its KMeans batches with it.
                let compact_thresholds =
                    block_thresholds.recluster_output_thresholds(rows_per_block, bytes_per_block);

                if task.kind == ReclusterTaskKind::MergeBlocks && !cluster_stats_gen.is_linear() {
                    return Err(ErrorCode::Internal(
                        "MergeBlocks requires linear cluster keys",
                    ));
                }

                if multiway_merge {
                    let extra_key_num = cluster_stats_gen.extra_key_num;
                    builder.main_pipeline.add_accumulating_transformer(move || {
                        OrderedBlockCompactBuilder::new(compact_thresholds, extra_key_num)
                    });
                    builder.main_pipeline.try_resize(max_threads)?;
                    builder
                        .main_pipeline
                        .add_block_meta_transformer(|| TransformCompactBlock);
                } else if cluster_stats_gen.is_hilbert() {
                    Self::build_hilbert_layout_pipeline(
                        builder,
                        task,
                        &mut cluster_stats_gen,
                        rows_per_block,
                        compact_thresholds,
                        max_threads,
                    )?;
                } else {
                    Self::build_regular_layout_pipeline(
                        builder,
                        task,
                        &cluster_stats_gen,
                        rows_per_block,
                        compact_thresholds,
                        max_threads,
                    )?;
                }

                let reaggregated = add_aggregate_state_reaggregate_transform(
                    &mut builder.main_pipeline,
                    table.engine(),
                    table.schema().as_ref(),
                )?;
                // Re-aggregation flushes rows in hash-table order, which
                // breaks the ordering the merge sort just established.
                // Re-sort each block before serialization: serialize-time
                // cluster statistics read the first and last rows of a
                // sorted block. Compact relies on the partial sort inside
                // `cluster_gen_for_append` for the same guarantee.
                if reaggregated {
                    let resort_descs: Arc<[_]> = cluster_stats_gen.sort_descs().into();
                    if !resort_descs.is_empty() {
                        builder.main_pipeline.add_transformer(move || {
                            TransformSortPartial::new(LimitType::None, resort_descs.clone())
                        });
                    }
                }

                if let Some(index_merge) = &index_merge {
                    let merged_names = index_merge.merged_names();
                    builder
                        .main_pipeline
                        .add_transformer(move || TransformPrepareReclusterIndex {
                            merged_names: merged_names.clone(),
                        });
                }
                let virtual_column_layout = task.virtual_column_layout.clone();
                let query_ctx = builder.ctx.clone();
                // All layouts share the ordinary block statistics and serialization path after
                // they have formed output blocks and removed layout-only temporary columns.
                let index_merge_table = table.clone();
                builder.main_pipeline.add_transform(
                    move |transform_input_port, transform_output_port| {
                        let proc = match &virtual_column_layout {
                            Some(layout) => {
                                TransformSerializeBlock::try_create_with_virtual_layout(
                                    query_ctx.clone(),
                                    transform_input_port,
                                    transform_output_port,
                                    table,
                                    cluster_stats_gen.clone(),
                                    MutationKind::Recluster,
                                    Arc::new(layout.clone()),
                                    self.table_meta_timestamps,
                                )?
                            }
                            None => TransformSerializeBlock::try_create(
                                query_ctx.clone(),
                                transform_input_port,
                                transform_output_port,
                                table,
                                cluster_stats_gen.clone(),
                                MutationKind::Recluster,
                                self.table_meta_timestamps,
                            )?,
                        };
                        proc.into_processor()
                    },
                )?;
                if let Some(index_merge) = index_merge {
                    // The written blocks end in a sink; the index merge, which the commit
                    // sink follows, becomes the main pipeline.
                    let mut data_pipeline =
                        std::mem::replace(&mut builder.main_pipeline, Pipeline::create());
                    build_recluster_index_merge_pipeline(
                        &mut data_pipeline,
                        &mut builder.main_pipeline,
                        builder.ctx.clone(),
                        &index_merge_table,
                        index_merge,
                        max_threads,
                    )?;
                    builder.pipelines.push(data_pipeline);
                }
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}

impl Recluster {
    /// Runs the merge groups of the task one after another, from a single source that builds
    /// the pipeline of a group only after the previous group has finished.
    fn build_merge_groups_pipeline(
        &self,
        builder: &mut PipelineBuilder,
        task: &ReclusterTask,
    ) -> Result<()> {
        let groups = task.split_merge_groups()?;
        log::info!(
            event = "recluster.merge_groups",
            groups = groups.len(),
            group_blocks :? = task.merge_groups.iter().map(|group| group.blocks).collect::<Vec<_>>(),
            group_bytes :? = task.merge_groups.iter().map(|group| group.total_bytes).collect::<Vec<_>>(),
            input_levels :serde = task.input_level_stats;
            "Recluster merge groups planned"
        );
        let running_group = Arc::new(Mutex::new(None));
        let source = ReclusterMergeGroupsSource {
            ctx: builder.ctx.clone(),
            plan: Recluster {
                meta: self.meta.clone(),
                tasks: vec![],
                table_info: self.table_info.clone(),
                table_meta_timestamps: self.table_meta_timestamps,
            },
            scope: PlanScope::get_plan_scope(),
            total_groups: groups.len(),
            groups: groups.into(),
            running: None,
            running_group: running_group.clone(),
        };
        let output = OutputPort::create();
        let processor =
            AsyncSourcer::create(builder.ctx.get_scan_progress(), output.clone(), source)?;
        builder
            .main_pipeline
            .add_pipe(Pipe::create(0, 1, vec![PipeItem::create(
                processor,
                vec![],
                vec![output],
            )]));
        // A group runs in its own executor, which a stop of this pipeline, such as a timeout or
        // an error in another processor, does not reach by itself.
        builder
            .main_pipeline
            .set_on_finished(always_callback(move |info: &ExecutionInfo| {
                if let Some(group) = running_group.lock().take() {
                    let cause = match &info.res {
                        Ok(()) => ErrorCode::AbortedQuery("recluster merge groups stopped"),
                        Err(cause) => cause.clone(),
                    };
                    log::info!("recluster merge group stopped with the pipeline: {cause}");
                    group.finish(Some(cause));
                }
                Ok(())
            }));
        Ok(())
    }

    fn build_hilbert_layout_pipeline(
        builder: &mut PipelineBuilder,
        task: &ReclusterTask,
        cluster_stats_gen: &mut ClusterStatsGenerator,
        rows_per_block: usize,
        compact_thresholds: BlockThresholds,
        max_threads: usize,
    ) -> Result<()> {
        let dimension_offsets = cluster_stats_gen.hilbert_dimension_offsets()?;
        let worker_count = builder.main_pipeline.output_len().max(1);
        let target_blocks = task.total_rows.div_ceil(rows_per_block).max(1);
        let num_collectors = max_threads.min(target_blocks);
        let num_collectors = num_collectors.clamp(1, u8::MAX as usize + 1);
        let exchange = HilbertRangeExchange::create(
            dimension_offsets,
            task.total_rows,
            worker_count,
            num_collectors,
        );

        // The barrier uses the complete sort spill policy, including the sort-spill enable switch;
        // the configured sort batch size controls each oldest-first spill unit.
        let spiller = ReclusterSpiller::create(builder.ctx.clone())?;

        // Every input stream samples locally, then all streams replay against one immutable task-local
        // weighted range plan.
        let worker_id = AtomicUsize::new(0);
        let range_exchange = exchange.clone();
        let pipeline = &mut builder.main_pipeline;
        pipeline.add_transform(move |input, output| {
            let id = worker_id.fetch_add(1, Ordering::Relaxed);
            let memory_settings = spiller.memory_settings().clone();
            Ok(ProcessorPtr::create(TransformHilbertCluster::create(
                input,
                output,
                exchange.clone(),
                id,
                spiller.clone(),
                memory_settings,
            )))
        })?;

        pipeline.try_resize(num_collectors)?;
        pipeline.exchange(num_collectors, range_exchange)?;

        // Each collector owns a disjoint Hilbert-key interval. Sort only inside that interval, then
        // compact locally so every output block covers a continuous key range without a global merge.
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
            builder.ctx.get_settings().get_enable_fixed_rows_sort()?,
        )?
        .with_block_size_hit(rows_per_block)
        .build_local_full_sort_pipeline(pipeline, false)?;

        // The ordinary cluster statistics generator removes all trailing temporary columns before
        // serialization; include the task-local Hilbert sort key.
        cluster_stats_gen.extra_key_num += 1;
        let extra_key_num = cluster_stats_gen.extra_key_num;
        pipeline.add_accumulating_transformer(move || {
            OrderedBlockCompactBuilder::new(compact_thresholds, extra_key_num)
        });
        pipeline.add_block_meta_transformer(|| TransformCompactBlock);
        Ok(())
    }

    fn build_recluster_sort_pipeline(
        kind: ReclusterTaskKind,
        sort: SortPipelineBuilder,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match kind {
            ReclusterTaskKind::SortBlocks => {
                // Establish order in the input fragments, then merge them using the
                // existing full-sort builder, including its spill support.
                sort.build_full_sort_pipeline(pipeline, false)
            }
            ReclusterTaskKind::MergeBlocks => {
                // Each source block is already ordered. A stream may contain several
                // overlapping blocks, so retain local and cross-stream run merges,
                // without sorting rows inside an input block.
                sort.build_merge_sort_pipeline(pipeline, false, false)
            }
        }
    }

    fn build_regular_layout_pipeline(
        builder: &mut PipelineBuilder,
        task: &ReclusterTask,
        cluster_stats_gen: &ClusterStatsGenerator,
        rows_per_block: usize,
        compact_thresholds: BlockThresholds,
        max_threads: usize,
    ) -> Result<()> {
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
        let sort_pipeline_builder = SortPipelineBuilder::create(
            builder.ctx.clone(),
            schema,
            sort_descs.into(),
            None,
            builder.ctx.get_settings().get_enable_fixed_rows_sort()?,
        )?
        .with_block_size_hit(rows_per_block);
        Self::build_recluster_sort_pipeline(
            task.kind,
            sort_pipeline_builder,
            &mut builder.main_pipeline,
        )?;

        builder.main_pipeline.try_resize(1)?;
        let extra_key_num = cluster_stats_gen.extra_key_num;
        builder.main_pipeline.add_accumulating_transformer(move || {
            OrderedBlockCompactBuilder::new(compact_thresholds, extra_key_num)
        });
        builder.main_pipeline.try_resize(max_threads)?;
        builder
            .main_pipeline
            .add_block_meta_transformer(|| TransformCompactBlock);
        Ok(())
    }
}

/// Runs the merge groups of one task one after another. The pipeline of a group is built and
/// started only when the previous group has finished, so the groups never open their inputs at
/// the same time and only one group's processors exist at once. It yields the mutation logs of
/// every group, which keeps the task a single commit.
struct ReclusterMergeGroupsSource {
    ctx: Arc<QueryContext>,
    /// The recluster plan without tasks; each group runs it with the group as its only task.
    plan: Recluster,
    /// Profiles the processors of the groups under the recluster plan.
    scope: Option<Arc<PlanScope>>,
    groups: VecDeque<ReclusterTask>,
    total_groups: usize,
    running: Option<PipelinePullingExecutor>,
    /// The executor of the running group, for the outer pipeline to stop when it finishes.
    running_group: Arc<Mutex<Option<Arc<PipelineExecutor>>>>,
}

impl ReclusterMergeGroupsSource {
    fn start_group(&self, task: ReclusterTask) -> Result<PipelinePullingExecutor> {
        let ctx = self.ctx.clone();
        let mut builder =
            PipelineBuilder::create(ctx.get_function_context()?, ctx.get_settings(), ctx.clone());
        let plan = Recluster {
            tasks: vec![task],
            ..self.plan.clone()
        };
        {
            let _guard = self.scope.clone().map(|scope| scope.enter_scope_guard());
            plan.build_pipeline2(&mut builder)?;
        }
        let mut build_res = PipelineBuildResult {
            main_pipeline: builder.main_pipeline,
            sources_pipelines: builder.pipelines,
            exchange_injector: builder.exchange_injector,
            r_cte_scan_interpreters: vec![],
        };
        build_res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(ctx.clone())?;
        let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        *self.running_group.lock() = Some(executor.get_inner());
        executor.start();
        Ok(executor)
    }
}

#[async_trait::async_trait]
impl AsyncSource for ReclusterMergeGroupsSource {
    const NAME: &'static str = "ReclusterMergeGroupsSource";
    // Mutation logs are blocks without rows.
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(executor) = self.running.as_mut() {
                if let Some(block) = executor.pull_data().await? {
                    return Ok(Some(block));
                }
                // The group has finished: its pipeline is complete and its readers are closed.
                self.ctx
                    .add_query_profiles(&executor.get_inner().fetch_profiling(false));
                self.running_group.lock().take();
                self.running = None;
            }
            let Some(task) = self.groups.pop_front() else {
                return Ok(None);
            };
            log::info!(
                "recluster merge group {}/{}: blocks={}, rows={}, bytes={}",
                self.total_groups - self.groups.len(),
                self.total_groups,
                task.parts.len(),
                task.total_rows,
                task.total_bytes
            );
            self.running = Some(self.start_group(task)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recluster_sort_pipeline_boundary() -> anyhow::Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        for width in [1, 2] {
            for kind in [
                ReclusterTaskKind::SortBlocks,
                ReclusterTaskKind::MergeBlocks,
            ] {
                let kind: ReclusterTaskKind = serde_json::from_str(&serde_json::to_string(&kind)?)?;
                let schema = DataSchemaRefExt::create(vec![DataField::new(
                    "k",
                    DataType::Number(NumberDataType::Int32),
                )]);
                let sort = SortPipelineBuilder::create(
                    ctx.clone(),
                    schema,
                    vec![SortColumnDescription {
                        offset: 0,
                        asc: true,
                        nulls_first: false,
                    }]
                    .into(),
                    None,
                    false,
                )?;
                let mut pipeline = Pipeline::create();
                pipeline.add_source(EmptySource::create, width)?;
                Recluster::build_recluster_sort_pipeline(kind, sort, &mut pipeline)?;
                let partial_sorts = pipeline
                    .graph
                    .node_weights()
                    .filter(|node| unsafe { node.proc.name() }.contains("SortPartialTransform"))
                    .count();
                assert_eq!(
                    partial_sorts,
                    if kind == ReclusterTaskKind::SortBlocks {
                        width
                    } else {
                        0
                    }
                );
            }
        }
        Ok(())
    }

    /// Merges four overlapping inputs as two groups: two blocks with `g = 1`, then two with
    /// `g = 2`, all spread over the same key range.
    async fn check_merge_groups(index_merge: bool) -> anyhow::Result<()> {
        use databend_common_catalog::plan::ReclusterMergeGroup;
        use databend_common_catalog::table_context::TableContextTableAccess;
        use databend_common_catalog::table_context::TableContextTableManagement;
        use databend_common_expression::BlockMetaInfoDowncast;
        use databend_common_expression::Scalar;
        use databend_common_storage::ColumnNodes;
        use databend_common_storages_fuse::io::SegmentsIO;
        use databend_common_storages_fuse::operations::MutationLogEntry;
        use databend_common_storages_fuse::operations::MutationLogs;
        use databend_storages_common_table_meta::meta::BlockMeta;
        use databend_storages_common_table_meta::meta::SegmentInfo;

        use crate::test_kits::execute_command;

        let fixture = TestFixture::setup().await?;
        let index = match index_merge {
            true => ", inverted index idx(content)",
            false => "",
        };
        for sql in [
            format!(
                "create table default.merge_groups(k int, g int, content string{index}) cluster by(k) row_per_block=3"
            ),
            "insert into default.merge_groups values (1,1,'a'),(5,1,'b'),(9,1,'c')".into(),
            "insert into default.merge_groups values (3,1,'d'),(7,1,'e'),(11,1,'f')".into(),
            "insert into default.merge_groups values (2,2,'g'),(6,2,'h'),(10,2,'i')".into(),
            "insert into default.merge_groups values (4,2,'j'),(8,2,'k'),(12,2,'l')".into(),
        ] {
            execute_command(fixture.new_query_ctx().await?, &sql).await?;
        }
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        // One-row merge batches, so the compaction cuts each group into several blocks.
        settings.set_setting("max_block_size".into(), "1".into())?;
        settings.set_setting(
            "enable_recluster_inverted_index_merge".into(),
            u8::from(index_merge).to_string(),
        )?;
        let table = ctx.get_table("default", "default", "merge_groups").await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let schema = fuse.schema();
        let (k, g) = (schema.column_id_of("k")?, schema.column_id_of("g")?);
        let snapshot = fuse.read_table_snapshot().await?.unwrap();
        let mut blocks = Vec::new();
        for segment in SegmentsIO::create(ctx.clone(), fuse.get_operator(), schema.clone())
            .read_segments::<SegmentInfo>(&snapshot.segments, CachePolicy::DEFAULT)
            .await?
        {
            for block in segment?.blocks {
                blocks.push((None, block));
            }
        }
        let group_of = |block: &BlockMeta| block.col_stats[&g].min().clone();
        blocks.sort_by_key(|(_, block)| (group_of(block), block.col_stats[&k].min().clone()));
        assert_eq!(blocks.len(), 4);
        let merge_groups = blocks
            .chunks(2)
            .map(|group| ReclusterMergeGroup {
                blocks: 2,
                total_rows: group.iter().map(|(_, b)| b.row_count as usize).sum(),
                total_bytes: group.iter().map(|(_, b)| b.block_size as usize).sum(),
                total_compressed: group.iter().map(|(_, b)| b.file_size as usize).sum(),
            })
            .collect::<Vec<_>>();
        let arrow_schema = arrow_schema::Schema::from(schema.as_ref());
        let nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));
        let (stats, parts) = FuseTable::to_partitions(Some(&schema), &blocks, &nodes, None, None);
        let task = ReclusterTask {
            parts,
            stats,
            total_rows: 12,
            total_bytes: merge_groups.iter().map(|group| group.total_bytes).sum(),
            total_compressed: merge_groups
                .iter()
                .map(|group| group.total_compressed)
                .sum(),
            level: 0,
            input_level_stats: vec![],
            kind: ReclusterTaskKind::MergeBlocks,
            vertical_kind: None,
            memory_budget: 0,
            virtual_column_layout: None,
            inverted_index_sources: match index_merge {
                true => blocks
                    .iter()
                    .map(|(_, block)| block.inverted_index_metas.clone().unwrap_or_default())
                    .collect(),
                false => vec![],
            },
            merge_groups,
        };
        let plan = PhysicalPlan::new(Recluster {
            meta: PhysicalPlanMeta::new("Recluster"),
            tasks: vec![task],
            table_info: fuse.get_table_info().clone(),
            table_meta_timestamps: ctx.get_table_meta_timestamps(table.as_ref(), Some(snapshot))?,
        });
        #[derive(Clone, Copy, PartialEq)]
        enum Stop {
            Never,
            /// Kill the query before any group starts.
            KillFirst,
            /// Kill the query while the first group runs.
            KillAfterFirstBlock,
            /// Stop the outer executor while the first group runs, as a timeout or an error in
            /// another processor would, without touching the query.
            FinishAfterFirstBlock,
        }
        let run = async |ctx: Arc<QueryContext>, stop: Stop| -> Result<Vec<DataBlock>> {
            let builder = PipelineBuilder::create(
                ctx.get_function_context()?,
                ctx.get_settings(),
                ctx.clone(),
            );
            let build_res = builder.finalize(&plan)?;
            // Before execution, the task is a single source: no group has a processor yet.
            assert_eq!(build_res.main_pipeline.graph.node_count(), 1);
            assert!(build_res.sources_pipelines.is_empty());
            let settings = ExecutorSettings::try_create(ctx.clone())?;
            let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
            if stop == Stop::KillFirst {
                ctx.kill(ErrorCode::aborting());
            }
            executor.start();
            let mut outputs = Vec::new();
            while let Some(block) = executor.pull_data().await? {
                outputs.push(block);
                match stop {
                    Stop::KillAfterFirstBlock => ctx.kill(ErrorCode::aborting()),
                    Stop::FinishAfterFirstBlock => {
                        executor.finish(Some(ErrorCode::AbortedQuery("stopped by the test")))
                    }
                    Stop::Never | Stop::KillFirst => {}
                }
            }
            Ok(outputs)
        };

        let mut outputs = Vec::new();
        for mut block in run(ctx.clone(), Stop::Never).await? {
            let logs = MutationLogs::downcast_from(block.take_meta().unwrap()).unwrap();
            for entry in logs.entries {
                let MutationLogEntry::AppendBlock { block_meta, .. } = entry else {
                    panic!("expected appended blocks");
                };
                outputs.push(block_meta.block_meta.clone());
            }
        }
        let mut group_rows = [0; 2];
        let mut group_blocks = [0; 2];
        let mut previous_group = 0;
        for output in &outputs {
            // An output block holds rows of one group only, and the groups run in order.
            let group = group_of(output);
            assert_eq!(output.col_stats[&g].max(), &group);
            let group = match group == Scalar::Number(1i32.into()) {
                true => 0,
                false => 1,
            };
            assert!(group >= previous_group, "a group ran after the next one");
            previous_group = group;
            group_rows[group] += output.row_count;
            group_blocks[group] += 1;
            // Ordered output: the cluster statistics, taken from the first and last rows,
            // are the key range of the block.
            let cluster_stats = output.cluster_stats.as_ref().unwrap();
            assert_eq!(cluster_stats.level, 1);
            assert_eq!(cluster_stats.min(), &vec![
                output.col_stats[&k].min().clone()
            ]);
            assert_eq!(cluster_stats.max(), &vec![
                output.col_stats[&k].max().clone()
            ]);
            if index_merge {
                assert!(output.inverted_index_meta("idx").is_some());
            }
        }
        assert_eq!(group_rows, [6, 6]);
        // Each group is cut into blocks of its own.
        assert!(
            group_blocks.iter().all(|blocks| *blocks >= 2),
            "{group_blocks:?}"
        );

        // A stopped query ends with an error instead of waiting on its groups, whether it is
        // killed before or during a group, or its executor stops without the query being killed.
        for stop in [
            Stop::KillFirst,
            Stop::KillAfterFirstBlock,
            Stop::FinishAfterFirstBlock,
        ] {
            let stopped = fixture.new_query_ctx().await?;
            stopped
                .get_settings()
                .set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(60), run(stopped, stop))
                    .await
                    .expect("a stopped recluster must not hang");
            assert!(result.is_err());
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recluster_merge_groups_run_in_order() -> anyhow::Result<()> {
        check_merge_groups(false).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recluster_merge_groups_with_inverted_index_merge() -> anyhow::Result<()> {
        check_merge_groups(true).await
    }
}
