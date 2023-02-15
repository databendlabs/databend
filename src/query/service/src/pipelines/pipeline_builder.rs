// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use async_channel::Receiver;
use common_catalog::table::AppendMode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::FunctionContext;
use common_expression::SortColumnDescription;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sinks::EmptySink;
use common_pipeline_sinks::Sinker;
use common_pipeline_sinks::UnionReceiveSink;
use common_pipeline_transforms::processors::transforms::try_add_multi_sort_merge;
use common_pipeline_transforms::processors::transforms::try_create_transform_sort_merge;
use common_profile::ProfSpanSetRef;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::AggregateFinal;
use common_sql::executor::AggregateFunctionDesc;
use common_sql::executor::AggregatePartial;
use common_sql::executor::DistributedInsertSelect;
use common_sql::executor::EvalScalar;
use common_sql::executor::ExchangeSink;
use common_sql::executor::ExchangeSource;
use common_sql::executor::Filter;
use common_sql::executor::HashJoin;
use common_sql::executor::Limit;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::Project;
use common_sql::executor::Sort;
use common_sql::executor::TableScan;
use common_sql::executor::UnionAll;
use common_sql::plans::JoinType;
use common_sql::ColumnBinding;
use common_sql::IndexType;

use super::processors::ProfileWrapper;
use crate::pipelines::processors::transforms::efficiently_memory_final_aggregator;
use crate::pipelines::processors::transforms::HashJoinDesc;
use crate::pipelines::processors::transforms::RightSemiAntiJoinCompactor;
use crate::pipelines::processors::transforms::TransformLeftJoin;
use crate::pipelines::processors::transforms::TransformMarkJoin;
use crate::pipelines::processors::transforms::TransformMergeBlock;
use crate::pipelines::processors::transforms::TransformRightJoin;
use crate::pipelines::processors::transforms::TransformRightSemiAntiJoin;
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::JoinHashTable;
use crate::pipelines::processors::LeftJoinCompactor;
use crate::pipelines::processors::MarkJoinCompactor;
use crate::pipelines::processors::RightJoinCompactor;
use crate::pipelines::processors::SinkBuildHashTable;
use crate::pipelines::processors::TransformAggregator;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformHashJoinProbe;
use crate::pipelines::processors::TransformLimit;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::processors::TransformSortPartial;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,

    main_pipeline: Pipeline,
    pub pipelines: Vec<Pipeline>,

    enable_profiling: bool,
    prof_span_set: ProfSpanSetRef,
}

impl PipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        enable_profiling: bool,
        prof_span_set: ProfSpanSetRef,
    ) -> PipelineBuilder {
        PipelineBuilder {
            enable_profiling,
            ctx,
            pipelines: vec![],
            main_pipeline: Pipeline::create(),
            prof_span_set,
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
            prof_span_set: self.prof_span_set,
        })
    }

    fn build_pipeline(&mut self, plan: &PhysicalPlan) -> Result<()> {
        match plan {
            PhysicalPlan::TableScan(scan) => self.build_table_scan(scan),
            PhysicalPlan::Filter(filter) => self.build_filter(filter),
            PhysicalPlan::Project(project) => self.build_project(project),
            PhysicalPlan::EvalScalar(eval_scalar) => self.build_eval_scalar(eval_scalar),
            PhysicalPlan::AggregatePartial(aggregate) => self.build_aggregate_partial(aggregate),
            PhysicalPlan::AggregateFinal(aggregate) => self.build_aggregate_final(aggregate),
            PhysicalPlan::Sort(sort) => self.build_sort(sort),
            PhysicalPlan::Limit(limit) => self.build_limit(limit),
            PhysicalPlan::HashJoin(join) => self.build_join(join),
            PhysicalPlan::ExchangeSink(sink) => self.build_exchange_sink(sink),
            PhysicalPlan::ExchangeSource(source) => self.build_exchange_source(source),
            PhysicalPlan::UnionAll(union_all) => self.build_union_all(union_all),
            PhysicalPlan::DistributedInsertSelect(insert_select) => {
                self.build_distributed_insert_select(insert_select)
            }
            PhysicalPlan::Exchange(_) => Err(ErrorCode::Internal(
                "Invalid physical plan with PhysicalPlan::Exchange",
            )),
        }
    }

    fn build_join(&mut self, join: &HashJoin) -> Result<()> {
        let state = self.build_join_state(join)?;
        self.expand_build_side_pipeline(&join.build, join, state.clone())?;
        self.build_join_probe(join, state)
    }

    fn build_join_state(&mut self, join: &HashJoin) -> Result<Arc<JoinHashTable>> {
        JoinHashTable::create_join_state(
            self.ctx.clone(),
            &join.build_keys,
            join.build.output_schema()?,
            join.probe.output_schema()?,
            HashJoinDesc::create(join)?,
        )
    }

    fn expand_build_side_pipeline(
        &mut self,
        build: &PhysicalPlan,
        hash_join_plan: &HashJoin,
        join_state: Arc<JoinHashTable>,
    ) -> Result<()> {
        let build_side_context = QueryContext::create_from(self.ctx.clone());
        let build_side_builder = PipelineBuilder::create(
            build_side_context,
            self.enable_profiling,
            self.prof_span_set.clone(),
        );
        let mut build_res = build_side_builder.finalize(build)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);
        build_res.main_pipeline.add_sink(|input| {
            let transform = Sinker::<SinkBuildHashTable>::create(
                input,
                SinkBuildHashTable::try_create(join_state.clone())?,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    hash_join_plan.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        self.pipelines.push(build_res.main_pipeline);
        self.pipelines
            .extend(build_res.sources_pipelines.into_iter());
        Ok(())
    }

    pub fn render_result_set(
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
        let mut result_fields = Vec::with_capacity(result_columns.len());

        for column_binding in result_columns {
            let index = column_binding.index;
            let name = column_binding.column_name.clone();
            let data_type = input_schema
                .field_with_name(index.to_string().as_str())?
                .data_type()
                .clone();
            projections.push(input_schema.index_of(index.to_string().as_str())?);
            result_fields.push(DataField::new(name.as_str(), data_type.clone()));
        }
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                *func_ctx,
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
        table.read_data(self.ctx.clone(), &scan.source, &mut self.main_pipeline)?;

        let schema = scan.source.schema();
        let projection = scan
            .name_mapping
            .keys()
            .map(|name| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;

        // if projection is sequential, no need to add projection
        if projection != (0..schema.fields().len()).collect::<Vec<usize>>() {
            let ops = vec![BlockOperator::Project { projection }];
            let func_ctx = self.ctx.get_function_context()?;
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    func_ctx,
                    ops.clone(),
                )))
            })?;
        }
        Ok(())
    }

    fn build_filter(&mut self, filter: &Filter) -> Result<()> {
        self.build_pipeline(&filter.input)?;

        let predicate = filter
            .predicates
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })
            .transpose()
            .unwrap_or_else(|| {
                Err(ErrorCode::Internal(
                    "Invalid empty predicate list".to_string(),
                ))
            })?;

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::create(
                input,
                output,
                self.ctx.get_function_context()?,
                vec![BlockOperator::Filter {
                    expr: predicate.clone(),
                }],
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    filter.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        Ok(())
    }

    fn build_project(&mut self, project: &Project) -> Result<()> {
        self.build_pipeline(&project.input)?;
        let func_ctx = self.ctx.get_function_context()?;

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                func_ctx,
                vec![BlockOperator::Project {
                    projection: project.projections.clone(),
                }],
            )))
        })
    }

    fn build_eval_scalar(&mut self, eval_scalar: &EvalScalar) -> Result<()> {
        self.build_pipeline(&eval_scalar.input)?;

        let operators = eval_scalar
            .exprs
            .iter()
            .map(|(scalar, _)| {
                Ok(BlockOperator::Map {
                    expr: scalar.as_expr(&BUILTIN_FUNCTIONS),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let func_ctx = self.ctx.get_function_context()?;

        self.main_pipeline.add_transform(|input, output| {
            let transform =
                CompoundBlockOperator::create(input, output, func_ctx, operators.clone());

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    eval_scalar.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        Ok(())
    }

    fn build_aggregate_partial(&mut self, aggregate: &AggregatePartial) -> Result<()> {
        self.build_pipeline(&aggregate.input)?;
        let params = Self::build_aggregator_params(
            aggregate.input.output_schema()?,
            // aggregate.output_schema()?,
            &aggregate.group_by,
            &aggregate.agg_funcs,
            None,
        )?;

        let pass_state_to_final = self.enable_memory_efficient_aggregator(&params);

        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformAggregator::try_create_partial(
                AggregatorTransformParams::try_create(input, output, &params)?,
                self.ctx.clone(),
                pass_state_to_final,
            )?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    aggregate.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        Ok(())
    }

    fn enable_memory_efficient_aggregator(&self, params: &Arc<AggregatorParams>) -> bool {
        self.ctx.get_cluster().is_empty()
            && !params.group_columns.is_empty()
            && self.main_pipeline.output_len() > 1
    }

    fn build_aggregate_final(&mut self, aggregate: &AggregateFinal) -> Result<()> {
        self.build_pipeline(&aggregate.input)?;

        let params = Self::build_aggregator_params(
            aggregate.before_group_by_schema.clone(),
            // aggregate.output_schema()?,
            &aggregate.group_by,
            &aggregate.agg_funcs,
            aggregate.limit,
        )?;

        if self.enable_memory_efficient_aggregator(&params) {
            return efficiently_memory_final_aggregator(params, &mut self.main_pipeline);
        }

        self.main_pipeline.resize(1)?;
        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformAggregator::try_create_final(
                self.ctx.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
            )?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    aggregate.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        Ok(())
    }

    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        group_by: &[IndexType],
        agg_funcs: &[AggregateFunctionDesc],
        limit: Option<usize>,
    ) -> Result<Arc<AggregatorParams>> {
        let mut agg_args = Vec::with_capacity(agg_funcs.len());
        let (group_by, group_data_types) = group_by
            .iter()
            .map(|i| {
                let index = input_schema.index_of(&i.to_string())?;
                Ok((index, input_schema.field(index).data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let aggs: Vec<AggregateFunctionRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                agg_args.push(agg_func.args.clone());
                let params = agg_func
                    .sig
                    .params
                    .iter()
                    .map(|p| p.clone().into_scalar())
                    .collect();
                AggregateFunctionFactory::instance().get(
                    agg_func.sig.name.as_str(),
                    params,
                    agg_func.sig.args.clone(),
                )
            })
            .collect::<Result<_>>()?;

        let params = AggregatorParams::try_create(
            input_schema,
            group_data_types,
            &group_by,
            &aggs,
            &agg_args,
            limit,
        )?;

        Ok(params)
    }

    fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        self.build_pipeline(&sort.input)?;

        let input_schema = sort.input.output_schema()?;

        let sort_desc = sort
            .order_by
            .iter()
            .map(|desc| {
                let offset = input_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let block_size = self.ctx.get_settings().get_max_block_size()? as usize;

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.resize(max_threads)?;
        }

        // Sort
        self.main_pipeline.add_transform(|input, output| {
            let transform =
                TransformSortPartial::try_create(input, output, sort.limit, sort_desc.clone())?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    sort.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        // Merge
        self.main_pipeline.add_transform(|input, output| {
            let transform = try_create_transform_sort_merge(
                input,
                output,
                input_schema.clone(),
                block_size,
                sort.limit,
                sort_desc.clone(),
            )?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    sort.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        // Concat merge in single thread
        try_add_multi_sort_merge(
            &mut self.main_pipeline,
            input_schema,
            block_size,
            sort.limit,
            sort_desc,
        )
    }

    fn build_limit(&mut self, limit: &Limit) -> Result<()> {
        self.build_pipeline(&limit.input)?;

        self.main_pipeline.resize(1)?;
        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformLimit::try_create(limit.limit, limit.offset, input, output)?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    limit.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })
    }

    fn build_join_probe(&mut self, join: &HashJoin, state: Arc<JoinHashTable>) -> Result<()> {
        self.build_pipeline(&join.probe)?;

        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformHashJoinProbe::create(
                self.ctx.clone(),
                input,
                output,
                state.clone(),
                join.output_schema()?,
            )?;

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    join.plan_id,
                    self.prof_span_set.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        if (join.join_type == JoinType::Left
            || join.join_type == JoinType::Full
            || join.join_type == JoinType::Single)
            && join.non_equi_conditions.is_empty()
        {
            self.main_pipeline.resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                let transform = TransformLeftJoin::try_create(
                    input,
                    output,
                    LeftJoinCompactor::create(state.clone()),
                )?;

                if self.enable_profiling {
                    Ok(ProcessorPtr::create(ProfileWrapper::create(
                        transform,
                        join.plan_id,
                        self.prof_span_set.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        }

        if join.join_type == JoinType::LeftMark {
            self.main_pipeline.resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                let transform = TransformMarkJoin::try_create(
                    input,
                    output,
                    MarkJoinCompactor::create(state.clone()),
                )?;

                if self.enable_profiling {
                    Ok(ProcessorPtr::create(ProfileWrapper::create(
                        transform,
                        join.plan_id,
                        self.prof_span_set.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        }

        if join.join_type == JoinType::Right || join.join_type == JoinType::Full {
            self.main_pipeline.resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                let transform = TransformRightJoin::try_create(
                    input,
                    output,
                    RightJoinCompactor::create(state.clone()),
                )?;

                if self.enable_profiling {
                    Ok(ProcessorPtr::create(ProfileWrapper::create(
                        transform,
                        join.plan_id,
                        self.prof_span_set.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        }

        if join.join_type == JoinType::RightAnti || join.join_type == JoinType::RightSemi {
            self.main_pipeline.resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                let transform = TransformRightSemiAntiJoin::try_create(
                    input,
                    output,
                    RightSemiAntiJoinCompactor::create(state.clone()),
                )?;

                if self.enable_profiling {
                    Ok(ProcessorPtr::create(ProfileWrapper::create(
                        transform,
                        join.plan_id,
                        self.prof_span_set.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        }

        Ok(())
    }

    pub fn build_exchange_source(&mut self, exchange_source: &ExchangeSource) -> Result<()> {
        let exchange_manager = self.ctx.get_exchange_manager();
        let build_res = exchange_manager.get_fragment_source(
            &exchange_source.query_id,
            exchange_source.source_fragment_id,
        )?;

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
        let pipeline_builder =
            PipelineBuilder::create(union_ctx, self.enable_profiling, self.prof_span_set.clone());
        let mut build_res = pipeline_builder.finalize(input)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);

        let (tx, rx) = async_channel::unbounded();

        build_res.main_pipeline.add_sink(|input_port| {
            let transform = UnionReceiveSink::create(Some(tx.clone()), input_port);

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    union_plan.plan_id,
                    self.prof_span_set.clone(),
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
                    Ok(ProcessorPtr::create(ProfileWrapper::create(
                        transform,
                        union_all.plan_id,
                        self.prof_span_set.clone(),
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
        PipelineBuilder::render_result_set(
            &self.ctx.get_function_context()?,
            insert_select.input.output_schema()?,
            &insert_select.select_column_bindings,
            &mut self.main_pipeline,
            false,
        )?;

        if insert_select.cast_needed {
            let func_ctx = self.ctx.get_function_context()?;
            self.main_pipeline
                .add_transform(|transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        select_schema.clone(),
                        insert_schema.clone(),
                        func_ctx,
                    )
                })?;
        }

        let table = self
            .ctx
            .get_catalog(&insert_select.catalog)?
            .get_table_by_info(&insert_select.table_info)?;

        // Fill missing columns.
        {
            let source_schema = insert_schema;
            if source_schema.fields().len() < table.schema().fields().len() {
                self.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        TransformResortAddOn::try_create(
                            self.ctx.clone(),
                            transform_input_port,
                            transform_output_port,
                            source_schema.clone(),
                            table.clone(),
                        )
                    },
                )?;
            }
        }
        table.append_data(
            self.ctx.clone(),
            &mut self.main_pipeline,
            AppendMode::Normal,
            true,
        )?;

        Ok(())
    }
}
