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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::LimitByPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;
use common_planners::PlanVisitor;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::SelectPlan;
use common_planners::SortPlan;
use common_planners::SubQueriesSetPlan;

use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ExpressionTransform;
use crate::pipelines::new::processors::ProjectionTransform;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformFilter;
use crate::pipelines::new::processors::TransformHaving;
use crate::pipelines::new::processors::TransformLimit;
use crate::pipelines::new::processors::TransformLimitBy;
use crate::pipelines::new::processors::TransformSortMerge;
use crate::pipelines::new::processors::TransformSortPartial;
use crate::pipelines::transforms::get_sort_descriptions;
use crate::sessions::QueryContext;

pub struct QueryPipelineBuilder {
    ctx: Arc<QueryContext>,
    pipeline: NewPipeline,
    limit: Option<usize>,
    offset: usize,
}

impl QueryPipelineBuilder {
    pub fn create(ctx: Arc<QueryContext>) -> QueryPipelineBuilder {
        QueryPipelineBuilder {
            ctx,
            pipeline: NewPipeline::create(),
            limit: None,
            offset: 0,
        }
    }

    pub fn finalize(mut self, plan: &SelectPlan) -> Result<NewPipeline> {
        self.visit_select(plan)?;
        Ok(self.pipeline)
    }
}

impl PlanVisitor for QueryPipelineBuilder {
    fn visit_plan_node(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::Projection(n) => self.visit_projection(n),
            PlanNode::Expression(n) => self.visit_expression(n),
            PlanNode::AggregatorPartial(n) => self.visit_aggregate_partial(n),
            PlanNode::AggregatorFinal(n) => self.visit_aggregate_final(n),
            PlanNode::Filter(n) => self.visit_filter(n),
            PlanNode::Having(n) => self.visit_having(n),
            PlanNode::Sort(n) => self.visit_sort(n),
            PlanNode::Limit(n) => self.visit_limit(n),
            PlanNode::LimitBy(n) => self.visit_limit_by(n),
            PlanNode::ReadSource(n) => self.visit_read_data_source(n),
            PlanNode::Select(n) => self.visit_select(n),
            _ => Err(ErrorCode::UnImplement("")),
        }
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        let aggregator_params = AggregatorParams::try_create_partial(plan)?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_partial(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &aggregator_params,
                    )?,
                )
            })
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline.resize(1)?;
        let aggregator_params = AggregatorParams::try_create_final(plan)?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_final(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &aggregator_params,
                    )?,
                )
            })
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                ProjectionTransform::try_create(
                    transform_input_port,
                    transform_output_port,
                    plan.input.schema(),
                    plan.schema(),
                    plan.expr.to_owned(),
                )
            })
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                ExpressionTransform::try_create(
                    transform_input_port,
                    transform_output_port,
                    plan.input.schema(),
                    plan.schema(),
                    plan.exprs.to_owned(),
                )
            })
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformFilter::try_create(
                    plan.schema(),
                    plan.predicate.clone(),
                    transform_input_port,
                    transform_output_port,
                )
            })
    }

    fn visit_having(&mut self, plan: &HavingPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformHaving::try_create(
                    plan.schema(),
                    plan.predicate.clone(),
                    transform_input_port,
                    transform_output_port,
                )
            })
    }

    fn visit_limit(&mut self, plan: &LimitPlan) -> Result<()> {
        self.limit = plan.n;
        self.offset = plan.offset;
        self.visit_plan_node(&plan.input)?;

        self.pipeline.resize(1)?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformLimit::try_create(
                    plan.n,
                    plan.offset,
                    transform_input_port,
                    transform_output_port,
                )
            })
    }

    fn visit_sub_queries_sets(&mut self, _: &SubQueriesSetPlan) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "New processor framework unsupported subquery.",
        ))
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
        self.pipeline
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
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformSortMerge::try_create(
                    transform_input_port,
                    transform_output_port,
                    rows_limit,
                    get_sort_descriptions(&plan.schema, &plan.order_by)?,
                )
            })?;

        // processor1 sorted block --
        //                             \
        // processor2 sorted block ----> processor  --> merge to one sorted block
        //                             /
        // processor3 sorted block --
        self.pipeline.resize(1)?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformSortMerge::try_create(
                    transform_input_port,
                    transform_output_port,
                    rows_limit,
                    get_sort_descriptions(&plan.schema, &plan.order_by)?,
                )
            })
    }

    fn visit_limit_by(&mut self, plan: &LimitByPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline.resize(1)?;
        self.pipeline
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
        table.read2(self.ctx.clone(), plan, &mut self.pipeline)
    }
}
