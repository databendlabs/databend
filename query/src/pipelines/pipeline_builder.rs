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

use common_base::base::tokio::sync::broadcast::channel;
use common_base::base::tokio::sync::broadcast::Receiver;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::LimitByPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;
use common_planners::PlanVisitor;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::SortPlan;
use common_planners::SubQueriesSetPlan;
use common_planners::WindowFuncPlan;

use super::processors::transforms::TransformWindowFunc;
use super::processors::transforms::WindowFuncCompact;
use super::processors::SortMergeCompactor;
use crate::pipelines::pipeline::Pipeline;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::transforms::get_sort_descriptions;
use crate::pipelines::processors::transforms::SubqueryReceiver;
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::ExpressionTransform;
use crate::pipelines::processors::ProjectionTransform;
use crate::pipelines::processors::SubqueryReceiveSink;
use crate::pipelines::processors::TransformAggregator;
use crate::pipelines::processors::TransformCreateSets;
use crate::pipelines::processors::TransformFilter;
use crate::pipelines::processors::TransformHaving;
use crate::pipelines::processors::TransformLimit;
use crate::pipelines::processors::TransformLimitBy;
use crate::pipelines::processors::TransformSortMerge;
use crate::pipelines::processors::TransformSortPartial;
use crate::pipelines::Pipe;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

/// Builder for query pipeline
/// ```
/// # let builder = QueryPipelineBuilder::create(ctx);
/// # let pipeline = builder.finalize(plan)?;
/// ```
pub struct QueryPipelineBuilder {
    ctx: Arc<QueryContext>,
    main_pipeline: Pipeline,
    sources_pipeline: Vec<Pipeline>,
    limit: Option<usize>,
    offset: usize,
}

impl QueryPipelineBuilder {
    /// Create a Builder from QueryContext, others params are default
    pub fn create(ctx: Arc<QueryContext>) -> QueryPipelineBuilder {
        QueryPipelineBuilder {
            ctx,
            main_pipeline: Pipeline::create(),
            sources_pipeline: vec![],
            limit: None,
            offset: 0,
        }
    }

    /// The core of generating the pipeline
    /// It will recursively visit the entire plan tree, and create a `SimplePipe` for each node,
    /// adding it to the pipeline
    pub fn finalize(mut self, plan: &PlanNode) -> Result<PipelineBuildResult> {
        self.visit_plan_node(plan)?;

        for source_pipeline in &self.sources_pipeline {
            if !source_pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::IllegalPipelineState(
                    "Source pipeline must be complete pipeline.",
                ));
            }
        }

        Ok(PipelineBuildResult {
            main_pipeline: self.main_pipeline,
            sources_pipelines: self.sources_pipeline,
        })
    }

    fn expand_subquery(&mut self, query_plan: &Arc<PlanNode>) -> Result<Receiver<DataValue>> {
        let subquery_ctx = QueryContext::create_from(self.ctx.clone());
        let pipeline_builder = QueryPipelineBuilder::create(subquery_ctx);
        let mut build_res = pipeline_builder.finalize(query_plan)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);

        build_res.main_pipeline.resize(1)?;
        let (tx, rx) = channel(1);
        let input = InputPort::create();
        build_res.main_pipeline.add_pipe(Pipe::SimplePipe {
            outputs_port: vec![],
            inputs_port: vec![input.clone()],
            processors: vec![SubqueryReceiveSink::try_create(
                input,
                query_plan.schema(),
                tx,
            )?],
        });

        self.sources_pipeline.push(build_res.main_pipeline);
        self.sources_pipeline
            .extend(build_res.sources_pipelines.into_iter());
        Ok(rx)
    }
}

impl PlanVisitor for QueryPipelineBuilder {
    fn visit_plan_node(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::Projection(n) => self.visit_projection(n),
            PlanNode::Expression(n) => self.visit_expression(n),
            PlanNode::AggregatorPartial(n) => self.visit_aggregate_partial(n),
            PlanNode::AggregatorFinal(n) => self.visit_aggregate_final(n),
            PlanNode::WindowFunc(n) => self.visit_window_func(n),
            PlanNode::Filter(n) => self.visit_filter(n),
            PlanNode::Having(n) => self.visit_having(n),
            PlanNode::Sort(n) => self.visit_sort(n),
            PlanNode::Limit(n) => self.visit_limit(n),
            PlanNode::LimitBy(n) => self.visit_limit_by(n),
            PlanNode::ReadSource(n) => self.visit_read_data_source(n),
            PlanNode::Select(n) => self.visit_select(n),
            PlanNode::Remote(n) => self.visit_remote(n),
            PlanNode::SubQueryExpression(n) => self.visit_sub_queries_sets(n),
            node => Err(ErrorCode::UnImplement(format!(
                "Unknown plan type, {:?}",
                node
            ))),
        }
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        let aggregator_params = AggregatorParams::try_create(
            &plan.aggr_expr,
            &plan.group_expr,
            &plan.input.schema(),
            &plan.schema(),
        )?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_partial(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &aggregator_params,
                    )?,
                    self.ctx.clone(),
                )
            })
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline.resize(1)?;
        let aggregator_params = AggregatorParams::try_create(
            &plan.aggr_expr,
            &plan.group_expr,
            &plan.schema_before_group_by,
            &plan.schema,
        )?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_final(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &aggregator_params,
                    )?,
                    self.ctx.clone(),
                )
            })
    }

    fn visit_window_func(&mut self, plan: &WindowFuncPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;
        self.main_pipeline.resize(1)?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                let compactor = WindowFuncCompact::create(
                    plan.window_func.clone(),
                    plan.schema.clone(),
                    plan.input.schema(),
                );
                TransformWindowFunc::try_create(
                    transform_input_port,
                    transform_output_port,
                    compactor,
                )
            })
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                ProjectionTransform::try_create(
                    transform_input_port,
                    transform_output_port,
                    plan.input.schema(),
                    plan.schema(),
                    plan.expr.to_owned(),
                    self.ctx.clone(),
                )
            })
    }

    fn visit_remote(&mut self, plan: &RemotePlan) -> Result<()> {
        let schema = plan.schema();
        match plan {
            RemotePlan::V1(_) => Err(ErrorCode::LogicalError(
                "Use version 1 remote plan in version 2 framework.",
            )),
            RemotePlan::V2(plan) => {
                let fragment_id = plan.receive_fragment_id;
                let query_id = plan.receive_query_id.to_owned();
                let build_res = self.ctx.get_exchange_manager().get_fragment_source(
                    query_id,
                    fragment_id,
                    schema,
                )?;

                self.main_pipeline = build_res.main_pipeline;
                self.sources_pipeline
                    .extend(build_res.sources_pipelines.into_iter());
                Ok(())
            }
        }
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                ExpressionTransform::try_create(
                    transform_input_port,
                    transform_output_port,
                    plan.input.schema(),
                    plan.schema(),
                    plan.exprs.to_owned(),
                    self.ctx.clone(),
                )
            })
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformFilter::try_create(
                    plan.schema(),
                    plan.predicate.clone(),
                    transform_input_port,
                    transform_output_port,
                    self.ctx.clone(),
                )
            })
    }

    fn visit_having(&mut self, plan: &HavingPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformHaving::try_create(
                    plan.schema(),
                    plan.predicate.clone(),
                    transform_input_port,
                    transform_output_port,
                    self.ctx.clone(),
                )
            })
    }

    fn visit_limit(&mut self, plan: &LimitPlan) -> Result<()> {
        self.limit = plan.n;
        self.offset = plan.offset;
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline.resize(1)?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformLimit::try_create(
                    plan.n,
                    plan.offset,
                    transform_input_port,
                    transform_output_port,
                )
            })
    }

    fn visit_sub_queries_sets(&mut self, plan: &SubQueriesSetPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        let schema = plan.schema();
        let mut sub_queries_receiver = Vec::with_capacity(plan.expressions.len());
        for expression in &plan.expressions {
            sub_queries_receiver.push(match expression {
                Expression::Subquery { query_plan, .. } => Ok(SubqueryReceiver::Subquery(
                    self.expand_subquery(query_plan)?,
                )),
                Expression::ScalarSubquery { query_plan, .. } => Ok(
                    SubqueryReceiver::ScalarSubquery(self.expand_subquery(query_plan)?),
                ),
                _ => Err(ErrorCode::IllegalPipelineState(
                    "Must be subquery plan or scalar subquery plan.",
                )),
            }?);
        }

        let mut inputs_port = Vec::with_capacity(self.main_pipeline.output_len());
        let mut outputs_port = Vec::with_capacity(self.main_pipeline.output_len());
        let mut processors = Vec::with_capacity(self.main_pipeline.output_len());

        for _index in 0..self.main_pipeline.output_len() {
            let transform_input_port = InputPort::create();
            let transform_output_port = OutputPort::create();

            let mut receivers = Vec::with_capacity(sub_queries_receiver.len());

            for subquery_receiver in &mut sub_queries_receiver {
                receivers.push(subquery_receiver.subscribe());
            }

            inputs_port.push(transform_input_port.clone());
            outputs_port.push(transform_output_port.clone());
            processors.push(TransformCreateSets::try_create(
                transform_input_port,
                transform_output_port,
                schema.clone(),
                receivers,
            )?);
        }

        self.main_pipeline.add_pipe(Pipe::SimplePipe {
            processors,
            inputs_port,
            outputs_port,
        });
        Ok(())
    }

    fn visit_sort(&mut self, plan: &SortPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        // The number of rows should be limit + offset. For example, for the query
        // 'select * from numbers(100) order by number desc limit 10 offset 5', the
        // sort pipeline should return at least 15 rows.
        let rows_limit = self.limit.map(|limit| limit + self.offset);

        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformSortPartial::try_create(
                    transform_input_port,
                    transform_output_port,
                    rows_limit,
                    get_sort_descriptions(&plan.schema, &plan.order_by)?,
                )
            })?;

        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformSortMerge::try_create(
                    transform_input_port,
                    transform_output_port,
                    SortMergeCompactor::new(
                        rows_limit,
                        get_sort_descriptions(&plan.schema, &plan.order_by)?,
                    ),
                )
            })?;

        // processor1 sorted block --
        //                             \
        // processor2 sorted block ----> processor  --> merge to one sorted block
        //                             /
        // processor3 sorted block --
        self.main_pipeline.resize(1)?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformSortMerge::try_create(
                    transform_input_port,
                    transform_output_port,
                    SortMergeCompactor::new(
                        rows_limit,
                        get_sort_descriptions(&plan.schema, &plan.order_by)?,
                    ),
                )
            })
    }

    fn visit_limit_by(&mut self, plan: &LimitByPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.main_pipeline.resize(1)?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformLimitBy::try_create(
                    transform_input_port,
                    transform_output_port,
                    plan.limit,
                    &plan.limit_by,
                )
            })
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;
        let table = self.ctx.build_table_from_source_plan(plan)?;
        table.read2(self.ctx.clone(), plan, &mut self.main_pipeline)
    }
}
