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
use std::convert::TryFrom;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use async_channel::Receiver;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_catalog::table::AppendMode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::with_number_mapped_type;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::SortColumnDescription;
use common_formats::FastFieldDecoderValues;
use common_formats::FastValuesDecodeFallback;
use common_formats::FastValuesDecoder;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::query_spill_prefix;
use common_pipeline_sinks::EmptySink;
use common_pipeline_sinks::Sinker;
use common_pipeline_sinks::UnionReceiveSink;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::OneBlockSource;
use common_pipeline_transforms::processors::profile_wrapper::ProcessorProfileWrapper;
use common_pipeline_transforms::processors::profile_wrapper::ProfileStub;
use common_pipeline_transforms::processors::profile_wrapper::TransformProfileWrapper;
use common_pipeline_transforms::processors::transforms::build_full_sort_pipeline;
use common_pipeline_transforms::processors::transforms::Transformer;
use common_profile::SharedProcessorProfiles;
use common_settings::Settings;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::CommitSink;
use common_sql::executor::ConstantTableScan;
use common_sql::executor::CteScan;
use common_sql::executor::DistributedInsertSelect;
use common_sql::executor::EvalScalar;
use common_sql::executor::ExchangeSink;
use common_sql::executor::ExchangeSource;
use common_sql::executor::Filter;
use common_sql::executor::Lambda;
use common_sql::executor::Limit;
use common_sql::executor::MaterializedCte;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::Project;
use common_sql::executor::ProjectSet;
use common_sql::executor::ReclusterSource;
use common_sql::executor::RowFetch;
use common_sql::executor::RuntimeFilterSource;
use common_sql::executor::Sort;
use common_sql::executor::TableScan;
use common_sql::executor::UnionAll;
use common_sql::executor::Window;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::IndexType;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_storages_factory::Table;
use common_storages_fuse::operations::build_row_fetcher_pipeline;
use common_storages_fuse::operations::FillInternalColumnProcessor;
use common_storages_fuse::FuseTable;
use parking_lot::RwLock;

use super::processors::transforms::FrameBound;
use super::processors::transforms::WindowFunctionInfo;
use super::PipelineBuilderData;
use crate::api::DefaultExchangeInjector;
use crate::api::ExchangeInjector;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::pipelines::processors::transforms::hash_join::TransformHashJoinBuild;
use crate::pipelines::processors::transforms::MaterializedCteSink;
use crate::pipelines::processors::transforms::MaterializedCteSource;
use crate::pipelines::processors::transforms::MaterializedCteState;
use crate::pipelines::processors::transforms::RuntimeFilterState;
use crate::pipelines::processors::transforms::TransformMergeBlock;
use crate::pipelines::processors::transforms::TransformWindow;
use crate::pipelines::processors::SinkRuntimeFilterSource;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformLimit;
use crate::pipelines::processors::TransformRuntimeFilter;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct PipelineBuilder {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    pub(crate) main_pipeline: Pipeline,
    pub(crate) settings: Arc<Settings>,

    pub pipelines: Vec<Pipeline>,

    // probe data_fields for merge into
    pub probe_data_fields: Option<Vec<DataField>>,
    // Used in runtime filter source
    pub join_state: Option<Arc<HashJoinBuildState>>,
    // record the index of join build side pipeline in `pipelines`
    pub index: Option<usize>,

    // Cte -> state, each cte has it's own state
    pub cte_state: HashMap<IndexType, Arc<MaterializedCteState>>,

    pub(crate) enable_profiling: bool,
    pub(crate) proc_profs: SharedProcessorProfiles,
    pub(crate) exchange_injector: Arc<dyn ExchangeInjector>,
}

impl PipelineBuilder {
    pub fn create(
        func_ctx: FunctionContext,
        settings: Arc<Settings>,
        ctx: Arc<QueryContext>,
        enable_profiling: bool,
        prof_span_set: SharedProcessorProfiles,
    ) -> PipelineBuilder {
        PipelineBuilder {
            enable_profiling,
            ctx,
            func_ctx,
            settings,
            pipelines: vec![],
            join_state: None,
            main_pipeline: Pipeline::create(),
            proc_profs: prof_span_set,
            exchange_injector: DefaultExchangeInjector::create(),
            index: None,
            cte_state: HashMap::new(),
            probe_data_fields: None,
        }
    }

    pub fn finalize(mut self, plan: &PhysicalPlan) -> Result<PipelineBuildResult> {
        self.build_pipeline(plan)?;

        for source_pipeline in &self.pipelines {
            if !source_pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::Internal(
                    "Source pipeline must be complete pipeline.",
                ));
            }
        }

        Ok(PipelineBuildResult {
            main_pipeline: self.main_pipeline,
            sources_pipelines: self.pipelines,
            prof_span_set: self.proc_profs,
            exchange_injector: self.exchange_injector,
            builder_data: PipelineBuilderData {
                input_join_state: self.join_state,
                input_probe_schema: self.probe_data_fields,
            },
        })
    }

    pub(crate) fn build_pipeline(&mut self, plan: &PhysicalPlan) -> Result<()> {
        match plan {
            PhysicalPlan::TableScan(scan) => self.build_table_scan(scan),
            PhysicalPlan::CteScan(scan) => self.build_cte_scan(scan),
            PhysicalPlan::ConstantTableScan(scan) => self.build_constant_table_scan(scan),
            PhysicalPlan::Filter(filter) => self.build_filter(filter),
            PhysicalPlan::Project(project) => self.build_project(project),
            PhysicalPlan::EvalScalar(eval_scalar) => self.build_eval_scalar(eval_scalar),
            PhysicalPlan::AggregateExpand(aggregate) => self.build_aggregate_expand(aggregate),
            PhysicalPlan::AggregatePartial(aggregate) => self.build_aggregate_partial(aggregate),
            PhysicalPlan::AggregateFinal(aggregate) => self.build_aggregate_final(aggregate),
            PhysicalPlan::Window(window) => self.build_window(window),
            PhysicalPlan::Sort(sort) => self.build_sort(sort),
            PhysicalPlan::Limit(limit) => self.build_limit(limit),
            PhysicalPlan::RowFetch(row_fetch) => self.build_row_fetch(row_fetch),
            PhysicalPlan::HashJoin(join) => self.build_join(join),
            PhysicalPlan::ExchangeSink(sink) => self.build_exchange_sink(sink),
            PhysicalPlan::ExchangeSource(source) => self.build_exchange_source(source),
            PhysicalPlan::UnionAll(union_all) => self.build_union_all(union_all),
            PhysicalPlan::DistributedInsertSelect(insert_select) => {
                self.build_distributed_insert_select(insert_select)
            }
            PhysicalPlan::ProjectSet(project_set) => self.build_project_set(project_set),
            PhysicalPlan::Lambda(lambda) => self.build_lambda(lambda),
            PhysicalPlan::Exchange(_) => Err(ErrorCode::Internal(
                "Invalid physical plan with PhysicalPlan::Exchange",
            )),
            PhysicalPlan::RuntimeFilterSource(runtime_filter_source) => {
                self.build_runtime_filter_source(runtime_filter_source)
            }
            PhysicalPlan::RangeJoin(range_join) => self.build_range_join(range_join),
            PhysicalPlan::MaterializedCte(materialized_cte) => {
                self.build_materialized_cte(materialized_cte)
            }

            // Copy into.
            PhysicalPlan::CopyIntoTable(copy) => self.build_copy_into_table(copy),

            // Delete.
            PhysicalPlan::DeleteSource(delete) => self.build_delete_source(delete),

            // Replace.
            PhysicalPlan::ReplaceAsyncSourcer(async_sourcer) => {
                self.build_async_sourcer(async_sourcer)
            }
            PhysicalPlan::ReplaceDeduplicate(deduplicate) => self.build_deduplicate(deduplicate),
            PhysicalPlan::ReplaceInto(replace) => self.build_replace_into(replace),

            // Merge into.
            PhysicalPlan::MergeInto(merge_into) => self.build_merge_into(merge_into),
            PhysicalPlan::MergeIntoSource(merge_into_source) => {
                self.build_merge_into_source(merge_into_source)
            }
            PhysicalPlan::MergeIntoAppendNotMatched(merge_into_append_not_matched) => {
                self.build_merge_into_append_not_matched(merge_into_append_not_matched)
            }
            PhysicalPlan::MergeIntoAddRowNumber(add_row_number) => {
                self.build_add_row_number(add_row_number)
            }

            // Commit.
            PhysicalPlan::CommitSink(plan) => self.build_commit_sink(plan),

            // Compact.
            PhysicalPlan::CompactSource(compact) => self.build_compact_source(compact),

            // Recluster.
            PhysicalPlan::ReclusterSource(recluster_source) => {
                self.build_recluster_source(recluster_source)
            }
            PhysicalPlan::ReclusterSink(recluster_sink) => {
                self.build_recluster_sink(recluster_sink)
            }
        }
    }

    fn build_commit_sink(&mut self, plan: &CommitSink) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let table =
            self.ctx
                .build_table_by_table_info(&plan.catalog_info, &plan.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let ctx: Arc<dyn TableContext> = self.ctx.clone();

        table.chain_mutation_pipes(
            &ctx,
            &mut self.main_pipeline,
            plan.snapshot.clone(),
            plan.mutation_kind,
            plan.merge_meta,
            plan.need_lock,
        )
    }

    pub fn build_result_projection(
        func_ctx: &FunctionContext,
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        pipeline: &mut Pipeline,
        ignore_result: bool,
    ) -> Result<()> {
        if ignore_result {
            return pipeline.add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))));
        }

        let mut projections = Vec::with_capacity(result_columns.len());

        for column_binding in result_columns {
            let index = column_binding.index;
            projections.push(input_schema.index_of(index.to_string().as_str())?);
        }
        let num_input_columns = input_schema.num_fields();
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                func_ctx.clone(),
                vec![BlockOperator::Project {
                    projection: projections.clone(),
                }],
            )))
        })?;

        Ok(())
    }

    fn build_table_scan(&mut self, scan: &TableScan) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(&scan.source)?;
        self.ctx.set_partitions(scan.source.parts.clone())?;
        table.read_data(
            self.ctx.clone(),
            &scan.source,
            &mut self.main_pipeline,
            true,
        )?;

        if self.enable_profiling {
            self.main_pipeline.add_transform(|input, output| {
                // shared timer between `on_start` and `on_finish`
                let start_timer = Arc::new(Mutex::new(Instant::now()));
                let finish_timer = Arc::new(Mutex::new(Instant::now()));
                Ok(ProcessorPtr::create(Transformer::create(
                    input,
                    output,
                    ProfileStub::new(scan.plan_id, self.proc_profs.clone())
                        .on_start(move |v| {
                            *start_timer.lock().unwrap() = Instant::now();
                            *v
                        })
                        .on_finish(move |prof| {
                            let elapsed = finish_timer.lock().unwrap().elapsed();
                            let mut prof = *prof;
                            prof.wait_time = elapsed;
                            prof
                        })
                        .accumulate_output_bytes()
                        .accumulate_output_rows(),
                )))
            })?;
        }

        // Fill internal columns if needed.
        if let Some(internal_columns) = &scan.internal_column {
            if table.support_row_id_column() {
                self.main_pipeline.add_transform(|input, output| {
                    Ok(ProcessorPtr::create(Box::new(
                        FillInternalColumnProcessor::create(
                            internal_columns.clone(),
                            input,
                            output,
                        ),
                    )))
                })?;
            } else {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "Table engine `{}` does not support virtual column _row_id",
                    table.engine()
                )));
            }
        }

        let schema = scan.source.schema();
        let mut projection = scan
            .name_mapping
            .keys()
            .map(|name| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;
        projection.sort();

        // if projection is sequential, no need to add projection
        if projection != (0..schema.fields().len()).collect::<Vec<usize>>() {
            let ops = vec![BlockOperator::Project { projection }];
            let num_input_columns = schema.num_fields();
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    self.func_ctx.clone(),
                    ops.clone(),
                )))
            })?;
        }

        Ok(())
    }

    fn build_cte_scan(&mut self, cte_scan: &CteScan) -> Result<()> {
        let max_threads = self.settings.get_max_threads()?;
        self.main_pipeline.add_source(
            |output| {
                MaterializedCteSource::create(
                    self.ctx.clone(),
                    output,
                    cte_scan.cte_idx,
                    self.cte_state.get(&cte_scan.cte_idx.0).unwrap().clone(),
                    cte_scan.offsets.clone(),
                )
            },
            max_threads as usize,
        )
    }

    fn build_constant_table_scan(&mut self, scan: &ConstantTableScan) -> Result<()> {
        self.main_pipeline.add_source(
            |output| {
                let block = if !scan.values.is_empty() {
                    DataBlock::new_from_columns(scan.values.clone())
                } else {
                    DataBlock::new(vec![], scan.num_rows)
                };
                OneBlockSource::create(output, block)
            },
            1,
        )
    }

    fn build_filter(&mut self, filter: &Filter) -> Result<()> {
        self.build_pipeline(&filter.input)?;

        let predicate = filter
            .predicates
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })
            .transpose()
            .unwrap_or_else(|| {
                Err(ErrorCode::Internal(
                    "Invalid empty predicate list".to_string(),
                ))
            })?;

        let num_input_columns = filter.input.output_schema()?.num_fields();
        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![BlockOperator::Filter {
                    projections: filter.projections.clone(),
                    expr: predicate.clone(),
                }],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    filter.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })?;

        Ok(())
    }

    fn build_project(&mut self, project: &Project) -> Result<()> {
        self.build_pipeline(&project.input)?;
        let num_input_columns = project.input.output_schema()?.num_fields();
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                self.func_ctx.clone(),
                vec![BlockOperator::Project {
                    projection: project.projections.clone(),
                }],
            )))
        })
    }

    fn build_eval_scalar(&mut self, eval_scalar: &EvalScalar) -> Result<()> {
        self.build_pipeline(&eval_scalar.input)?;

        let input_schema = eval_scalar.input.output_schema()?;
        let exprs = eval_scalar
            .exprs
            .iter()
            .map(|(scalar, _)| scalar.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        if exprs.is_empty() {
            return Ok(());
        }

        let op = BlockOperator::Map {
            exprs,
            projections: Some(eval_scalar.projections.clone()),
        };

        let num_input_columns = input_schema.num_fields();

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![op.clone()],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    eval_scalar.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })?;

        Ok(())
    }

    fn build_project_set(&mut self, project_set: &ProjectSet) -> Result<()> {
        self.build_pipeline(&project_set.input)?;

        let op = BlockOperator::FlatMap {
            projections: project_set.projections.clone(),
            srf_exprs: project_set
                .srf_exprs
                .iter()
                .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS))
                .collect(),
        };

        let num_input_columns = project_set.input.output_schema()?.num_fields();

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![op.clone()],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    project_set.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })
    }

    fn build_lambda(&mut self, lambda: &Lambda) -> Result<()> {
        self.build_pipeline(&lambda.input)?;

        let funcs = lambda.lambda_funcs.clone();
        let op = BlockOperator::LambdaMap { funcs };

        let input_schema = lambda.input.output_schema()?;
        let num_input_columns = input_schema.num_fields();

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![op.clone()],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    lambda.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })?;

        Ok(())
    }

    pub fn build_exchange_source(&mut self, exchange_source: &ExchangeSource) -> Result<()> {
        let exchange_manager = self.ctx.get_exchange_manager();
        let build_res = exchange_manager.get_fragment_source(
            &exchange_source.query_id,
            exchange_source.source_fragment_id,
            self.enable_profiling,
            self.exchange_injector.clone(),
        )?;
        // add sharing data
        self.join_state = build_res.builder_data.input_join_state;
        self.probe_data_fields = build_res.builder_data.input_probe_schema;

        self.main_pipeline = build_res.main_pipeline;
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    pub fn build_exchange_sink(&mut self, exchange_sink: &ExchangeSink) -> Result<()> {
        // ExchangeSink will be appended by `ExchangeManager::execute_pipeline`
        self.build_pipeline(&exchange_sink.input)
    }

    fn expand_union_all(
        &mut self,
        input: &PhysicalPlan,
        union_plan: &UnionAll,
    ) -> Result<Receiver<DataBlock>> {
        let union_ctx = QueryContext::create_from(self.ctx.clone());
        let mut pipeline_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            union_ctx,
            self.enable_profiling,
            self.proc_profs.clone(),
        );
        pipeline_builder.cte_state = self.cte_state.clone();
        let mut build_res = pipeline_builder.finalize(input)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);

        let (tx, rx) = async_channel::unbounded();

        build_res.main_pipeline.add_sink(|input_port| {
            let transform =
                UnionReceiveSink::create(Some(tx.clone()), input_port, self.ctx.clone());

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    union_plan.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        self.pipelines.push(build_res.main_pipeline);
        self.pipelines
            .extend(build_res.sources_pipelines.into_iter());
        Ok(rx)
    }

    pub fn build_union_all(&mut self, union_all: &UnionAll) -> Result<()> {
        self.build_pipeline(&union_all.left)?;
        let union_all_receiver = self.expand_union_all(&union_all.right, union_all)?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                let transform = TransformMergeBlock::try_create(
                    transform_input_port,
                    transform_output_port,
                    union_all.left.output_schema()?,
                    union_all.right.output_schema()?,
                    union_all.pairs.clone(),
                    union_all_receiver.clone(),
                )?;

                if self.enable_profiling {
                    Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                        transform,
                        union_all.plan_id,
                        self.proc_profs.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        Ok(())
    }

    pub fn build_distributed_insert_select(
        &mut self,
        insert_select: &DistributedInsertSelect,
    ) -> Result<()> {
        let select_schema = &insert_select.select_schema;
        let insert_schema = &insert_select.insert_schema;

        self.build_pipeline(&insert_select.input)?;

        // should render result for select
        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            insert_select.input.output_schema()?,
            &insert_select.select_column_bindings,
            &mut self.main_pipeline,
            false,
        )?;

        if insert_select.cast_needed {
            self.main_pipeline
                .add_transform(|transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        select_schema.clone(),
                        insert_schema.clone(),
                        self.func_ctx.clone(),
                    )
                })?;
        }

        let table = self.ctx.build_table_by_table_info(
            &insert_select.catalog_info,
            &insert_select.table_info,
            None,
        )?;

        let source_schema = insert_schema;
        Self::build_fill_missing_columns_pipeline(
            self.ctx.clone(),
            &mut self.main_pipeline,
            table.clone(),
            source_schema.clone(),
        )?;

        table.append_data(
            self.ctx.clone(),
            &mut self.main_pipeline,
            AppendMode::Normal,
        )?;

        Ok(())
    }

    pub fn build_runtime_filter_source(
        &mut self,
        runtime_filter_source: &RuntimeFilterSource,
    ) -> Result<()> {
        let state = self.build_runtime_filter_state(self.ctx.clone(), runtime_filter_source)?;
        self.expand_runtime_filter_source(&runtime_filter_source.right_side, state.clone())?;
        self.build_runtime_filter(&runtime_filter_source.left_side, state)?;
        Ok(())
    }

    fn expand_runtime_filter_source(
        &mut self,
        _right_side: &PhysicalPlan,
        state: Arc<RuntimeFilterState>,
    ) -> Result<()> {
        let pipeline = &mut self.pipelines[self.index.unwrap()];
        let output_size = pipeline.output_len();
        debug_assert!(output_size % 2 == 0);

        let mut items = Vec::with_capacity(output_size);
        //           Join
        //          /   \
        //        /      \
        //   RFSource     \
        //      /    \     \
        //     /      \     \
        // scan t1     scan t2
        for _ in 0..output_size / 2 {
            let input = InputPort::create();
            items.push(PipeItem::create(
                ProcessorPtr::create(TransformHashJoinBuild::try_create(
                    input.clone(),
                    self.join_state.as_ref().unwrap().clone(),
                    None,
                )?),
                vec![input],
                vec![],
            ));
            let input = InputPort::create();
            items.push(PipeItem::create(
                ProcessorPtr::create(Sinker::<SinkRuntimeFilterSource>::create(
                    input.clone(),
                    SinkRuntimeFilterSource::new(state.clone()),
                )),
                vec![input],
                vec![],
            ));
        }
        pipeline.add_pipe(Pipe::create(output_size, 0, items));
        Ok(())
    }

    fn build_runtime_filter(
        &mut self,
        left_side: &PhysicalPlan,
        state: Arc<RuntimeFilterState>,
    ) -> Result<()> {
        self.build_pipeline(left_side)?;
        self.main_pipeline.add_transform(|input, output| {
            let processor = TransformRuntimeFilter::create(input, output, state.clone());
            Ok(ProcessorPtr::create(processor))
        })?;
        Ok(())
    }

    fn build_runtime_filter_state(
        &self,
        ctx: Arc<QueryContext>,
        runtime_filter_source: &RuntimeFilterSource,
    ) -> Result<Arc<RuntimeFilterState>> {
        Ok(Arc::new(RuntimeFilterState::new(
            ctx,
            runtime_filter_source.left_runtime_filters.clone(),
            runtime_filter_source.right_runtime_filters.clone(),
        )))
    }

    fn build_materialized_cte(&mut self, materialized_cte: &MaterializedCte) -> Result<()> {
        self.expand_left_side_pipeline(
            &materialized_cte.left,
            materialized_cte.cte_idx,
            &materialized_cte.left_output_columns,
        )?;
        self.build_pipeline(&materialized_cte.right)
    }

    fn expand_left_side_pipeline(
        &mut self,
        left_side: &PhysicalPlan,
        cte_idx: IndexType,
        left_output_columns: &[ColumnBinding],
    ) -> Result<()> {
        let left_side_ctx = QueryContext::create_from(self.ctx.clone());
        let state = Arc::new(MaterializedCteState::new(self.ctx.clone()));
        self.cte_state.insert(cte_idx, state.clone());
        let mut left_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            left_side_ctx,
            self.enable_profiling,
            self.proc_profs.clone(),
        );
        left_side_builder.cte_state = self.cte_state.clone();
        let mut left_side_pipeline = left_side_builder.finalize(left_side)?;
        assert!(left_side_pipeline.main_pipeline.is_pulling_pipeline()?);

        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            left_side.output_schema()?,
            left_output_columns,
            &mut left_side_pipeline.main_pipeline,
            false,
        )?;

        left_side_pipeline.main_pipeline.add_sink(|input| {
            let transform = Sinker::<MaterializedCteSink>::create(
                input,
                MaterializedCteSink::create(self.ctx.clone(), cte_idx, state.clone())?,
            );
            Ok(ProcessorPtr::create(transform))
        })?;
        self.pipelines.push(left_side_pipeline.main_pipeline);
        self.pipelines
            .extend(left_side_pipeline.sources_pipelines.into_iter());
        Ok(())
    }
}

pub struct ValueSource {
    data: String,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: NameResolutionContext,
    bind_context: BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
    start: usize,
    is_finished: bool,
}

#[async_trait::async_trait]
impl AsyncSource for ValueSource {
    const NAME: &'static str = "ValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let format = self.ctx.get_format_settings()?;
        let field_decoder = FastFieldDecoderValues::create_for_insert(format);

        let mut values_decoder = FastValuesDecoder::new(&self.data, &field_decoder);
        let estimated_rows = values_decoder.estimated_rows();

        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), estimated_rows))
            .collect::<Vec<_>>();

        values_decoder.parse(&mut columns, self).await?;

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(columns);
        self.is_finished = true;
        Ok(Some(block))
    }
}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for ValueSource {
    async fn parse_fallback(&self, sql: &str) -> Result<Vec<Scalar>> {
        let res: Result<Vec<Scalar>> = try {
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let tokens = tokenize_sql(sql)?;
            let mut bind_context = self.bind_context.clone();
            let metadata = self.metadata.clone();

            let exprs = parse_comma_separated_exprs(&tokens[1..tokens.len()], sql_dialect)?;
            bind_context
                .exprs_to_scalar(
                    exprs,
                    &self.schema,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    metadata,
                )
                .await?
        };
        res.map_err(|mut err| {
            // The input for ValueSource is a sub-section of the original SQL. This causes
            // the error span to have an offset, so we adjust the span accordingly.
            if let Some(span) = err.span() {
                err = err.set_span(Some(
                    (span.start() + self.start..span.end() + self.start).into(),
                ));
            }
            err
        })
    }
}

impl ValueSource {
    pub fn new(
        data: String,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: NameResolutionContext,
        schema: DataSchemaRef,
        start: usize,
    ) -> Self {
        let bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));

        Self {
            data,
            ctx,
            name_resolution_ctx,
            schema,
            bind_context,
            metadata,
            start,
            is_finished: false,
        }
    }
}
