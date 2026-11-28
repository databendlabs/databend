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
use std::collections::HashSet;
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
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::TableDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::OrderedBlockCompactBuilder;
use databend_common_pipeline_transforms::TransformCompactBlock;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_ordered_compact_pipeline;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::HilbertRangeExchange;
use databend_common_storages_fuse::operations::ReclusterIndexMergeInputs;
use databend_common_storages_fuse::operations::ReclusterTaskBudget;
use databend_common_storages_fuse::operations::TransformHilbertCluster;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformVectorCluster;
use databend_common_storages_fuse::operations::add_aggregate_state_reaggregate_transform;
use databend_common_storages_fuse::operations::build_recluster_index_merge_pipeline;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::processors::horizontal_recluster::HorizontalReclusterSource;
use crate::pipelines::processors::horizontal_recluster::TransformPrepareReclusterIndex;
use crate::pipelines::processors::horizontal_recluster::TransformReclusterOutputOrder;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;
use crate::spillers::ReclusterSpiller;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub meta: PhysicalPlanMeta,
    pub tasks: Vec<ReclusterTask>,
    /// Shared-budget executor groups before distribution; each worker receives
    /// one group with task indices remapped to its local `tasks`. None denotes
    /// an unbatched strategy or a singleton maintenance task.
    #[serde(default)]
    pub task_batches: Option<Vec<Vec<usize>>>,
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
            task_batches: self.task_batches.clone(),
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
        if self.tasks.is_empty() {
            return builder.main_pipeline.add_source(EmptySource::create, 1);
        }
        let settings = builder.ctx.get_settings();
        let threads = settings.get_max_threads()? as usize;
        let io_requests = threads.max(settings.get_max_storage_io_requests()? as usize);
        if self.tasks.len() == 1 && self.task_batches.is_none() {
            return self.build_task_pipeline(
                builder,
                &self.tasks[0],
                threads,
                io_requests,
                0,
                true,
            );
        }
        let bytes = self.validate_node_batch(&builder.ctx)?;
        log::info!(
            event = "recluster.node_batch_planned",
            tasks = self.tasks.len(), input_bytes = bytes,
            threads, io_requests;
            "Building isolated recluster branches with node-local concurrency limits"
        );
        if self.tasks.len() == 1 {
            return self.build_task_pipeline(
                builder,
                &self.tasks[0],
                threads,
                io_requests,
                0,
                true,
            );
        }
        let sequential = self.tasks.len() > threads;
        let active_tasks = if sequential { 1 } else { self.tasks.len() };
        let mut groups = Vec::new();
        let mut outputs = Vec::new();
        for (idx, task) in self.tasks.iter().enumerate() {
            let task_threads = threads / active_tasks + usize::from(idx < threads % active_tasks);
            let task_io =
                io_requests / active_tasks + usize::from(idx < io_requests % active_tasks);
            let child_ctx = QueryContext::create_from(&builder.ctx);
            // Copy already-validated effective settings as well as shared state:
            // create_from alone does not inherit context-local overrides.
            unsafe {
                child_ctx
                    .get_settings()
                    .unchecked_apply_changes(settings.changes());
            }
            let parent_ctx = std::mem::replace(&mut builder.ctx, child_ctx);
            // Restore the parent even if constructing this branch fails.
            let result =
                self.build_task_pipeline(builder, task, task_threads, task_io, idx, !sequential);
            builder.ctx = parent_ctx;
            result?;
            let sinks = builder.main_pipeline.take_sinks();
            groups.push((sinks.len(), false));
            outputs.extend(sinks);
        }
        // Merge only serialized mutation output, never the task-local sorted rows.
        builder.main_pipeline.extend_sinks(outputs);
        if sequential {
            // Pull only the current task's serialized output. Later branches stay idle.
            builder.main_pipeline.sequence_group(groups, 1)
        } else {
            builder.main_pipeline.try_resize(1)
        }
    }
}

impl Recluster {
    fn validate_node_batch(&self, ctx: &QueryContext) -> Result<usize> {
        let settings = ctx.get_settings();
        let assigned_batch = self.task_batches.as_ref().is_some_and(|batches| {
            let mut seen = HashSet::new();
            batches.len() == 1
                && batches[0].len() == self.tasks.len()
                && batches[0]
                    .iter()
                    .all(|&idx| idx < self.tasks.len() && seen.insert(idx))
        });
        // Threads bound parallelism, not batch size. Oversubscribed batches run sequentially.
        if self.tasks.len() > ReclusterTaskBudget::MAX_TASKS_PER_NODE
            || !assigned_batch
            || self.tasks.len() > 1
                && self
                    .tasks
                    .iter()
                    .any(|task| task.kind != ReclusterTaskKind::MergeBlocks)
        {
            return Err(ErrorCode::Internal(
                "Unsupported recluster executor task batch",
            ));
        }
        let mut bytes = 0usize;
        let mut inputs = HashSet::new();
        for task in &self.tasks {
            bytes = bytes
                .checked_add(task.total_bytes)
                .ok_or_else(|| ErrorCode::Internal("Recluster task batch byte count overflow"))?;
            for part in &task.parts.partitions {
                let part = FuseBlockPartInfo::from_part(part)?;
                if !inputs.insert(&part.location) {
                    return Err(ErrorCode::Internal(
                        "Shared input in recluster executor task batch",
                    ));
                }
            }
        }
        let max_memory = settings.get_max_memory_usage()? as usize;
        if max_memory != 0
            && self.tasks.len() > 1
            && self.tasks.len() <= settings.get_max_threads()? as usize
            && !ctx.get_enable_sort_spill()
        {
            let table = ctx.build_table_by_table_info(&self.table_info, None)?;
            let table = FuseTable::try_from_table(table.as_ref())?;
            let multiway_merge =
                settings.get_enable_recluster_multiway_merge()?
                    && matches!(table.get_storage_format(), FuseStorageFormat::Parquet)
                    && !table.schema().fields().iter().any(|field| {
                        matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_)))
                    });
            if !multiway_merge {
                // Only concurrent, non-streaming tasks without spill need a joint
                // live-memory guard. Sequential tasks retain the per-task guard below.
                let used = GLOBAL_MEM_STAT.get_memory_usage();
                let budget = max_memory.saturating_sub(used) * 30 / 100;
                if bytes > budget {
                    return Err(ErrorCode::MemoryExceedsLimit(format!(
                        "Not enough memory for recluster executor batch: input_bytes = {bytes}, budget = {budget}."
                    )));
                }
            }
        }
        Ok(bytes)
    }

    fn build_task_pipeline(
        &self,
        builder: &mut PipelineBuilder,
        task: &ReclusterTask,
        max_threads: usize,
        max_io_requests: usize,
        task_index: usize,
        index_merge_allowed: bool,
    ) -> Result<()> {
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if task.parts.is_empty() {
            return Err(ErrorCode::Internal(
                "recluster task must have source blocks",
            ));
        }
        let settings = builder.ctx.get_settings();
        let multiway_merge = task.kind == ReclusterTaskKind::MergeBlocks
            && settings.get_enable_recluster_multiway_merge()?
            && matches!(table.get_storage_format(), FuseStorageFormat::Parquet)
            && !table
                .schema()
                .fields()
                .iter()
                .any(|field| matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))));
        let block_thresholds = table.get_block_thresholds();
        let schema = table.schema_with_stream();
        let mut cluster_stats_gen = table.get_cluster_stats_gen(
            builder.ctx.clone(),
            task.level + 1,
            block_thresholds,
            DataSchema::from(schema.clone()).into(),
        )?;
        let blockwise_sort = task.kind == ReclusterTaskKind::SortBlocks
            && cluster_stats_gen.is_linear()
            && !is_materialized_view_engine(table.engine());

        // Blockwise sort cannot spill, so recheck the worker's input budget
        // even when sort spilling is enabled. Multiway merge manages its own budget.
        if !multiway_merge && (blockwise_sort || !builder.ctx.get_enable_sort_spill()) {
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
        let table_info = table.get_table_info();
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

            // Keep all effective input levels in one event per task pipeline build.
            log::info!(
                event = "recluster.input_planned",
                table_id = table.get_id(),
                task_index,
                node_tasks = self.tasks.len(),
                task_bytes = task.total_bytes,
                max_threads,
                max_io_requests,
                task_kind :? = task.kind,
                cluster_layout = cluster_stats_gen.layout_name(),
                input_blocks = recluster_block_nums,
                input_levels :serde = task.input_level_stats;
                "Recluster input planned"
            );
        }
        // The index merge pipeline ends the data pipeline with a sink, which a sequential
        // node batch cannot hold back, so such a batch rebuilds the indexes instead.
        let index_merge = match multiway_merge && index_merge_allowed {
            true if settings.get_enable_recluster_inverted_index_merge()?
                && !table.schema().fields().iter().any(|field| {
                    matches!(field.data_type(), TableDataType::AggregateState { .. })
                }) =>
            {
                ReclusterIndexMergeInputs::try_create(table, task)?
            }
            _ => None,
        };
        // With an index merge, the written blocks end in a sink of their own pipeline, and the
        // index merge continues the main pipeline, which may already hold the other tasks of
        // the node batch.
        let main_pipeline = index_merge
            .as_ref()
            .map(|_| std::mem::replace(&mut builder.main_pipeline, Pipeline::create()));
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
                max_threads,
            )?;
        } else {
            builder.ctx.set_partitions(plan.parts.clone())?;

            table.read_data_with_parallelism(
                builder.ctx.clone(),
                &plan,
                &mut builder.main_pipeline,
                CachePolicy::READ_ONLY,
                max_threads,
                max_io_requests,
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

            if blockwise_sort {
                // Compact across read streams before sorting each output block.
                let output_len = builder.main_pipeline.output_len();
                build_ordered_compact_pipeline(
                    &mut builder.main_pipeline,
                    block_thresholds,
                    output_len,
                    0,
                )?;
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

        if blockwise_sort {
            let sort_descs: Arc<[_]> = cluster_stats_gen.sort_descs().into();
            builder.main_pipeline.add_transformer(move || {
                TransformSortPartial::new(LimitType::None, sort_descs.clone())
            });
        } else if multiway_merge {
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
                    Some(layout) => TransformSerializeBlock::try_create_with_virtual_layout(
                        query_ctx.clone(),
                        transform_input_port,
                        transform_output_port,
                        table,
                        cluster_stats_gen.clone(),
                        MutationKind::Recluster,
                        Arc::new(layout.clone()),
                        self.table_meta_timestamps,
                    )?,
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
        if let (Some(index_merge), Some(main_pipeline)) = (index_merge, main_pipeline) {
            let mut data_pipeline = std::mem::replace(&mut builder.main_pipeline, main_pipeline);
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
}

impl Recluster {
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
        .with_block_size_hit(rows_per_block)
        .with_max_threads(max_threads);
        Self::build_recluster_sort_pipeline(
            task.kind,
            sort_pipeline_builder,
            &mut builder.main_pipeline,
        )?;

        build_ordered_compact_pipeline(
            &mut builder.main_pipeline,
            compact_thresholds,
            max_threads,
            cluster_stats_gen.extra_key_num,
        )
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::ReclusterInfoSideCar;
    use databend_common_expression::ScalarRef;
    use databend_common_sql::executor::physical_plans::FragmentKind;
    use databend_common_storage::ColumnNodes;
    use databend_common_storages_fuse::io::SegmentInfoReader;
    use databend_storages_common_cache::Reader;
    use futures::TryStreamExt;

    use super::*;
    use crate::physical_plans::CommitSink;
    use crate::physical_plans::CommitType;
    use crate::physical_plans::Exchange;
    use crate::physical_plans::ExchangeSink;
    use crate::physical_plans::PhysicalPlanCast;
    use crate::schedulers::Fragmenter;
    use crate::schedulers::QueryFragmentsActions;
    use crate::test_kits::ClusterDescriptor;
    use crate::test_kits::TestFixture;
    use crate::test_kits::execute_pipeline;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recluster_batch_pipeline() -> anyhow::Result<()> {
        let fixture = TestFixture::setup().await?;
        fixture.create_default_database().await?;
        let name = format!(
            "{}.{}",
            fixture.default_db_name(),
            fixture.default_table_name()
        );
        fixture.execute_command(&format!(
            "CREATE TABLE {name}(id INT NOT NULL) CLUSTER BY(id) ROW_PER_BLOCK=4 BLOCK_PER_SEGMENT=2"
        )).await?;
        for (multiway, local_id, threads, blocks_per_task) in [
            (false, "node-b", 2, 4),
            (true, "node-c", 2, 4),
            (false, "node-b", 2, 2),
            (true, "node-c", 2, 2),
            (false, "node-b", 1, 2),
            (true, "node-c", 1, 2),
            (false, "node-b", 3, 1),
            (true, "node-c", 3, 1),
        ] {
            fixture
                .default_session()
                .get_settings()
                .set_max_threads(threads)?;
            fixture
                .execute_command(&format!("TRUNCATE TABLE {name}"))
                .await?;
            for start in [0, 2, 4, 6] {
                fixture
                    .execute_command(&format!(
                        "INSERT INTO {name} VALUES ({start}),({})",
                        start + 1
                    ))
                    .await?;
            }
            let ctx = fixture.new_query_ctx().await?;
            ctx.get_settings().set_setting(
                "enable_recluster_multiway_merge".to_string(),
                u64::from(multiway).to_string(),
            )?;
            let table = fixture.latest_default_table().await?;
            let fuse = FuseTable::try_from_table(table.as_ref())?;
            let snapshot = fuse.read_table_snapshot().await?.unwrap();
            let mut blocks = Vec::new();
            for location in &snapshot.segments {
                let segment = SegmentInfoReader::new(fuse.get_operator(), fuse.schema())
                    .read(location, &CachePolicy::BYPASS)
                    .await?;
                blocks.extend(segment.block_metas()?.iter().cloned());
            }
            assert_eq!(blocks.len(), 4);
            let schema = fuse.schema();
            let column_nodes = ColumnNodes::new_from_schema(&schema.as_ref().into(), Some(&schema));
            let tasks = blocks
                .chunks(blocks_per_task)
                .map(|blocks| {
                    let metas = blocks
                        .iter()
                        .map(|block| (None, block.clone()))
                        .collect::<Vec<_>>();
                    let (stats, parts) =
                        FuseTable::to_partitions(Some(&schema), &metas, &column_nodes, None, None);
                    ReclusterTask {
                        parts,
                        stats,
                        total_rows: blocks.iter().map(|block| block.row_count as usize).sum(),
                        total_bytes: blocks.iter().map(|block| block.block_size as usize).sum(),
                        total_compressed: blocks.iter().map(|block| block.file_size as usize).sum(),
                        level: 0,
                        input_level_stats: vec![],
                        kind: ReclusterTaskKind::MergeBlocks,
                        virtual_column_layout: None,
                        inverted_index_sources: vec![],
                    }
                })
                .collect::<Vec<_>>();
            let task_count = tasks.len();
            let table_meta_timestamps = TestFixture::default_table_meta_timestamps();
            let source = Recluster {
                meta: PhysicalPlanMeta::new("Recluster"),
                tasks,
                task_batches: Some(vec![(0..task_count).rev().collect()]),
                table_info: table.get_table_info().clone(),
                table_meta_timestamps,
            };

            // Placement already checked the coordinator's input bound. A smaller
            // worker-local setting must not reject that plan or change its parallelism.
            ctx.get_settings()
                .set_setting("recluster_block_size".to_string(), "1".to_string())?;

            // Streaming/spilling batches must not acquire a new live-memory rejection.
            // A one-byte limit makes the local 30% budget zero without global mutation.
            let max_memory = ctx.get_settings().get_max_memory_usage()?;
            ctx.get_settings().set_max_memory_usage(1)?;
            ctx.set_enable_sort_spill(true);
            source.validate_node_batch(&ctx)?;
            ctx.set_enable_sort_spill(false);
            if !multiway && task_count > 1 && task_count <= threads as usize {
                assert_eq!(
                    source.validate_node_batch(&ctx).unwrap_err().code(),
                    ErrorCode::MemoryExceedsLimit("").code(),
                );
            } else {
                source.validate_node_batch(&ctx)?;
            }
            if !multiway {
                // Sequential batches still retain the existing per-task no-spill guard.
                let mut builder = PipelineBuilder::create(
                    ctx.get_function_context()?,
                    ctx.get_settings(),
                    ctx.clone(),
                );
                assert_eq!(
                    source.build_pipeline2(&mut builder).unwrap_err().code(),
                    ErrorCode::MemoryExceedsLimit("").code(),
                );
                if task_count == 1 {
                    ctx.set_enable_sort_spill(true);
                    let mut task = source.tasks[0].clone();
                    task.kind = ReclusterTaskKind::SortBlocks;
                    let err = source
                        .build_task_pipeline(&mut builder, &task, 1, 1, 0, true)
                        .unwrap_err();
                    assert_eq!(err.code(), ErrorCode::MEMORY_EXCEEDS_LIMIT);
                }
            }
            ctx.set_enable_sort_spill(true);
            ctx.get_settings().set_max_memory_usage(max_memory)?;
            let input_bytes: usize = source.tasks.iter().map(|task| task.total_bytes).sum();
            assert!(input_bytes > 1);
            assert_eq!(source.validate_node_batch(&ctx)?, input_bytes);

            // A whole batch follows either coordinator; worker indices are local
            // even when planning-time task indices arrive in reverse order.
            let cluster_ctx = fixture
                .new_query_ctx_with_cluster(
                    ClusterDescriptor::new()
                        .with_node("node-c", "")
                        .with_node("node-a", "")
                        .with_node("node-b", "")
                        .with_local_id(local_id),
                )
                .await?;
            let mut plan = PhysicalPlan::new(Exchange {
                meta: PhysicalPlanMeta::new("Exchange"),
                input: PhysicalPlan::new(source.clone()),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
            plan.adjust_plan_id(&mut 0);
            let mut actions = QueryFragmentsActions::create(cluster_ctx.clone());
            for fragment in Fragmenter::try_create(cluster_ctx.clone())?.build_fragment(&plan)? {
                fragment.get_actions(cluster_ctx.clone(), &mut actions)?;
            }
            let assigned = actions
                .fragments_actions
                .iter()
                .flat_map(|fragment| fragment.get_actions())
                .filter_map(|action| {
                    let sink = ExchangeSink::from_physical_plan(&action.physical_plan)?;
                    let recluster = Recluster::from_physical_plan(&sink.input)?;
                    (!recluster.tasks.is_empty()).then_some((&action.executor, recluster))
                })
                .collect::<Vec<_>>();
            assert_eq!(assigned.len(), 1);
            let (executor, worker) = assigned[0];
            assert_eq!(executor, local_id);
            assert_eq!(worker.tasks.len(), task_count);
            assert_eq!(worker.task_batches, Some(vec![(0..task_count).collect()]));
            for (task, original) in worker.tasks.iter().zip(source.tasks.iter().rev()) {
                assert_eq!(
                    FuseBlockPartInfo::from_part(&task.parts.partitions[0])?.location,
                    FuseBlockPartInfo::from_part(&original.parts.partitions[0])?.location,
                );
            }

            let mut plan = PhysicalPlan::new(CommitSink {
                meta: PhysicalPlanMeta::new("CommitSink"),
                input: PhysicalPlan::new(worker.clone()),
                snapshot: Some(snapshot.clone()),
                table_info: table.get_table_info().clone(),
                commit_type: CommitType::Mutation {
                    kind: MutationKind::Recluster,
                    merge_meta: false,
                },
                update_stream_meta: vec![],
                table_meta_timestamps,
                recluster_info: Some(ReclusterInfoSideCar {
                    removed_segment_indexes: (0..snapshot.segments.len()).collect(),
                    removed_statistics: snapshot.summary.clone(),
                    ..Default::default()
                }),
                expected_ttl: None,
            });
            plan.adjust_plan_id(&mut 0);
            let pipeline = PipelineBuilder::create(
                ctx.get_function_context()?,
                ctx.get_settings(),
                ctx.clone(),
            )
            .finalize(&plan)?;
            assert!(
                !pipeline
                    .main_pipeline
                    .graph
                    .node_weights()
                    .any(|node| unsafe { node.proc.name().contains("SortPartialTransform") })
            );
            assert_eq!(
                pipeline
                    .main_pipeline
                    .graph
                    .node_weights()
                    .filter(|node| unsafe { node.proc.name() == "SequenceGroup" })
                    .count(),
                usize::from(task_count > threads as usize),
            );
            assert_eq!(
                pipeline
                    .main_pipeline
                    .graph
                    .node_weights()
                    .filter(|node| unsafe {
                        node.proc.name() == "HorizontalMultiwayReclusterSource"
                    })
                    .count(),
                if multiway {
                    task_count.max(threads as usize).min(blocks.len())
                } else {
                    0
                },
            );
            execute_pipeline(ctx, pipeline).await?;
            let table = fixture.latest_default_table().await?;
            let next = FuseTable::try_from_table(table.as_ref())?
                .read_table_snapshot()
                .await?
                .unwrap();
            assert_eq!(next.prev_snapshot_id.unwrap().0, snapshot.snapshot_id);
            assert_eq!(next.summary.row_count, 8);
            if task_count == 1 {
                // Parallel compaction can leave one or two blocks for the singleton.
                assert!((1..=threads).contains(&next.summary.block_count));
            } else {
                assert_eq!(next.summary.block_count, task_count as u64);
            }
            let result: Vec<_> = fixture.execute_query(&format!(
                "SELECT count()=8 AND count(distinct id)=8 AND min(id)=0 AND max(id)=7 FROM {name}"
            )).await?.try_collect().await?;
            assert_eq!(
                result[0].get_by_offset(0).index(0),
                Some(ScalarRef::Boolean(true))
            );
        }
        Ok(())
    }
}
