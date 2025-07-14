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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_core::always_callback;
use databend_common_pipeline_core::processors::PlanScope;
use databend_common_pipeline_core::processors::PlanScopeGuard;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::Settings;

use super::PipelineBuilderData;
use crate::interpreters::CreateTableInterpreter;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanDynExt;
use crate::pipelines::processors::HashJoinBuildState;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::sessions::QueryContext;

pub struct PipelineBuilder {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    pub(crate) main_pipeline: Pipeline,
    pub(crate) settings: Arc<Settings>,

    pub pipelines: Vec<Pipeline>,

    // probe data_fields for distributed merge into when source build
    pub merge_into_probe_data_fields: Option<Vec<DataField>>,
    pub join_state: Option<Arc<HashJoinBuildState>>,

    pub(crate) exchange_injector: Arc<dyn ExchangeInjector>,

    pub hash_join_states: HashMap<usize, Arc<HashJoinState>>,

    pub r_cte_scan_interpreters: Vec<CreateTableInterpreter>,
    pub(crate) is_exchange_stack: Vec<bool>,

    pub contain_sink_processor: bool,
}

impl PipelineBuilder {
    pub fn create(
        func_ctx: FunctionContext,
        settings: Arc<Settings>,
        ctx: Arc<QueryContext>,
    ) -> PipelineBuilder {
        PipelineBuilder {
            ctx,
            func_ctx,
            settings,
            pipelines: vec![],
            main_pipeline: Pipeline::create(),
            exchange_injector: DefaultExchangeInjector::create(),
            merge_into_probe_data_fields: None,
            join_state: None,
            hash_join_states: HashMap::new(),
            r_cte_scan_interpreters: vec![],
            contain_sink_processor: false,
            is_exchange_stack: vec![],
        }
    }

    pub fn finalize(mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<PipelineBuildResult> {
        // self.build_pipeline(plan)?;

        for source_pipeline in &self.pipelines {
            if !source_pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::Internal(
                    "Source pipeline must be complete pipeline.",
                ));
            }
        }

        // unload spill metas
        if !self.ctx.mark_unload_callbacked() {
            self.main_pipeline
                .set_on_finished(always_callback(move |_info: &ExecutionInfo| {
                    self.ctx.unload_spill_meta();
                    Ok(())
                }));
        }

        Ok(PipelineBuildResult {
            main_pipeline: self.main_pipeline,
            sources_pipelines: self.pipelines,
            exchange_injector: self.exchange_injector,
            builder_data: PipelineBuilderData {
                input_join_state: self.join_state,
                input_probe_schema: self.merge_into_probe_data_fields,
            },
            r_cte_scan_interpreters: self.r_cte_scan_interpreters,
        })
    }

    pub(crate) fn add_plan_scope(
        &mut self,
        plan: &Box<dyn IPhysicalPlan>,
    ) -> Result<Option<PlanScopeGuard>> {
        if !plan.display_in_profile() {
            return Ok(None);
        }

        let desc = plan.get_desc()?;
        let plan_labels = plan.get_labels()?;
        let mut profile_labels = Vec::with_capacity(plan_labels.len());
        for (name, value) in plan_labels {
            profile_labels.push(ProfileLabel::create(name, value));
        }

        let scope = PlanScope::create(
            plan.get_id(),
            plan.get_name(),
            Arc::new(desc),
            Arc::new(profile_labels),
        );

        Ok(Some(scope.enter_scope_guard()))
    }

    pub(crate) fn is_exchange_parent(&self) -> bool {
        if self.is_exchange_stack.len() >= 2 {
            return self.is_exchange_stack[self.is_exchange_stack.len() - 2];
        }

        false
    }

    #[recursive::recursive]
    pub(crate) fn build_pipeline(&mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<()> {
        let _guard = self.add_plan_scope(plan)?;
        self.is_exchange_stack
            .push(plan.downcast_ref::<ExchangeSink>().is_some());

        match plan {
            // ==============================
            // 1. Data Source Plans
            // ==============================
            // Basic table scans - retrieve data from tables
            PhysicalPlan::TableScan(scan) => self.build_table_scan(scan),
            PhysicalPlan::ConstantTableScan(scan) => self.build_constant_table_scan(scan),
            PhysicalPlan::CacheScan(cache_scan) => self.build_cache_scan(cache_scan),
            PhysicalPlan::ExpressionScan(expression_scan) => {
                self.build_expression_scan(expression_scan)
            }
            PhysicalPlan::RecursiveCteScan(scan) => self.build_recursive_cte_scan(scan),

            // Special source operations
            PhysicalPlan::MutationSource(mutation_source) => {
                self.build_mutation_source(mutation_source)
            }

            // ==============================
            // 2. Relational Operators
            // ==============================
            // Filtering and projection
            PhysicalPlan::Filter(filter) => self.build_filter(filter),
            PhysicalPlan::EvalScalar(eval_scalar) => self.build_eval_scalar(eval_scalar),
            PhysicalPlan::ProjectSet(project_set) => self.build_project_set(project_set),

            // Sorting and limiting
            PhysicalPlan::Sort(sort) => self.build_sort(sort),
            PhysicalPlan::Limit(limit) => self.build_limit(limit),
            PhysicalPlan::RowFetch(row_fetch) => self.build_row_fetch(row_fetch),

            // Join operations
            PhysicalPlan::HashJoin(join) => self.build_hash_join(join),
            PhysicalPlan::RangeJoin(range_join) => self.build_range_join(range_join),

            // Aggregation operations
            PhysicalPlan::AggregateExpand(aggregate) => self.build_aggregate_expand(aggregate),
            PhysicalPlan::AggregatePartial(aggregate) => self.build_aggregate_partial(aggregate),
            PhysicalPlan::AggregateFinal(aggregate) => self.build_aggregate_final(aggregate),

            // Window functions
            PhysicalPlan::Window(window) => self.build_window(window),
            PhysicalPlan::WindowPartition(window_partition) => {
                self.build_window_partition(window_partition)
            }

            PhysicalPlan::UnionAll(union_all) => self.build_union_all(union_all),

            // ==============================
            // 3. Data Distribution
            // ==============================
            PhysicalPlan::ExchangeSink(sink) => self.build_exchange_sink(sink),
            PhysicalPlan::ExchangeSource(source) => self.build_exchange_source(source),
            PhysicalPlan::DistributedInsertSelect(insert_select) => {
                self.build_distributed_insert_select(insert_select)
            }
            PhysicalPlan::Shuffle(shuffle) => self.build_shuffle(shuffle),
            PhysicalPlan::Duplicate(duplicate) => self.build_duplicate(duplicate),
            PhysicalPlan::BroadcastSource(source) => self.build_broadcast_source(source),
            PhysicalPlan::BroadcastSink(sink) => self.build_broadcast_sink(sink),

            // ==============================
            // 4. Data Modification Operations
            // ==============================
            // Copy operations
            PhysicalPlan::CopyIntoTable(copy) => self.build_copy_into_table(copy),
            PhysicalPlan::CopyIntoLocation(copy) => self.build_copy_into_location(copy),

            // Replace operations
            PhysicalPlan::ReplaceAsyncSourcer(async_sourcer) => {
                self.build_async_sourcer(async_sourcer)
            }
            PhysicalPlan::ReplaceDeduplicate(deduplicate) => self.build_deduplicate(deduplicate),
            PhysicalPlan::ReplaceInto(replace) => self.build_replace_into(replace),

            // Mutation operations (DELETE/UPDATE)
            PhysicalPlan::Mutation(mutation) => self.build_mutation(mutation),
            PhysicalPlan::MutationSplit(mutation_split) => {
                self.build_mutation_split(mutation_split)
            }
            PhysicalPlan::MutationManipulate(mutation_manipulate) => {
                self.build_mutation_manipulate(mutation_manipulate)
            }
            PhysicalPlan::MutationOrganize(mutation_organize) => {
                self.build_mutation_organize(mutation_organize)
            }
            PhysicalPlan::AddStreamColumn(add_stream_column) => {
                self.build_add_stream_column(add_stream_column)
            }
            PhysicalPlan::ColumnMutation(column_mutation) => {
                self.build_column_mutation(column_mutation)
            }

            // Commit operations
            PhysicalPlan::CommitSink(plan) => self.build_commit_sink(plan),

            // MERGE INTO chunk processing operations
            PhysicalPlan::ChunkFilter(chunk_filter) => self.build_chunk_filter(chunk_filter),
            PhysicalPlan::ChunkEvalScalar(chunk_project) => {
                self.build_chunk_eval_scalar(chunk_project)
            }
            PhysicalPlan::ChunkCastSchema(chunk_cast_schema) => {
                self.build_chunk_cast_schema(chunk_cast_schema)
            }
            PhysicalPlan::ChunkFillAndReorder(chunk_fill_and_reorder) => {
                self.build_chunk_fill_and_reorder(chunk_fill_and_reorder)
            }
            PhysicalPlan::ChunkAppendData(chunk_append_data) => {
                self.build_chunk_append_data(chunk_append_data)
            }
            PhysicalPlan::ChunkMerge(chunk_merge) => self.build_chunk_merge(chunk_merge),
            PhysicalPlan::ChunkCommitInsert(chunk_commit_insert) => {
                self.build_chunk_commit_insert(chunk_commit_insert)
            }

            // ==============================
            // 5. Data Maintenance Operations
            // ==============================
            PhysicalPlan::CompactSource(compact) => self.build_compact_source(compact),
            PhysicalPlan::Recluster(recluster) => self.build_recluster(recluster),
            PhysicalPlan::HilbertPartition(partition) => self.build_hilbert_partition(partition),

            // ==============================
            // 6. Special Processing Operations
            // ==============================
            // User-defined functions and async operations
            PhysicalPlan::Udf(udf) => self.build_udf(udf),
            PhysicalPlan::AsyncFunction(async_func) => self.build_async_function(async_func),

            // ==============================
            // 7. Invalid Plans
            // ==============================
            PhysicalPlan::Exchange(_) => Err(ErrorCode::Internal(
                "Invalid physical plan with PhysicalPlan::Exchange",
            )),
        }?;

        self.is_exchange_stack.pop();

        Ok(())
    }
}
