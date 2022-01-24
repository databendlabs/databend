// Copyright 2021 Datafuse Labs.
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
use common_planners::BroadcastPlan;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::LimitByPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::SelectPlan;
use common_planners::SinkPlan;
use common_planners::SortPlan;
use common_planners::StagePlan;
use common_planners::SubQueriesSetPlan;
use common_tracing::tracing;

use crate::api::FlightTicket;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::transforms::AggregatorFinalTransform;
use crate::pipelines::transforms::AggregatorPartialTransform;
use crate::pipelines::transforms::CreateSetsTransform;
use crate::pipelines::transforms::ExpressionTransform;
use crate::pipelines::transforms::GroupByFinalTransform;
use crate::pipelines::transforms::GroupByPartialTransform;
use crate::pipelines::transforms::HavingTransform;
use crate::pipelines::transforms::LimitByTransform;
use crate::pipelines::transforms::LimitTransform;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::RemoteTransform;
use crate::pipelines::transforms::SinkTransform;
use crate::pipelines::transforms::SortMergeTransform;
use crate::pipelines::transforms::SortPartialTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::pipelines::transforms::SubQueriesPuller;
use crate::pipelines::transforms::WhereTransform;
use crate::sessions::QueryContext;

pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,

    limit: Option<usize>,
    offset: usize,
}

impl PipelineBuilder {
    pub fn create(ctx: Arc<QueryContext>) -> PipelineBuilder {
        PipelineBuilder {
            ctx,
            limit: None,
            offset: 0,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn build(mut self, node: &PlanNode) -> Result<Pipeline> {
        tracing::debug!("Received plan:\n{:?}", node);
        let pipeline = self.visit(node)?;
        tracing::debug!("Pipeline:\n{:?}", pipeline);
        Ok(pipeline)
    }

    fn visit(&mut self, node: &PlanNode) -> Result<Pipeline> {
        match node {
            PlanNode::Select(node) => self.visit_select(node),
            PlanNode::Stage(node) => self.visit_stage(node),
            PlanNode::Broadcast(node) => self.visit_broadcast(node),
            PlanNode::Remote(node) => self.visit_remote(node),
            PlanNode::Expression(node) => self.visit_expression(node),
            PlanNode::Projection(node) => self.visit_projection(node),
            PlanNode::AggregatorPartial(node) => self.visit_aggregator_partial(node),
            PlanNode::AggregatorFinal(node) => self.visit_aggregator_final(node),
            PlanNode::Filter(node) => self.visit_filter(node),
            PlanNode::Having(node) => self.visit_having(node),
            PlanNode::Sort(node) => self.visit_sort(node),
            PlanNode::Limit(node) => self.visit_limit(node),
            PlanNode::LimitBy(node) => self.visit_limit_by(node),
            PlanNode::ReadSource(node) => self.visit_read_data_source(node),
            PlanNode::SubQueryExpression(node) => self.visit_create_sets(node),
            PlanNode::Sink(node) => self.visit_sink(node),
            other => Result::Err(ErrorCode::UnknownPlan(format!(
                "Build pipeline from the plan node unsupported:{:?}",
                other.name()
            ))),
        }
    }

    fn visit_select(&mut self, node: &SelectPlan) -> Result<Pipeline> {
        self.visit(&*node.input)
    }

    fn visit_stage(&self, _: &StagePlan) -> Result<Pipeline> {
        Result::Err(ErrorCode::LogicalError(
            "Logical Error: visit_stage_plan in pipeline_builder",
        ))
    }

    fn visit_broadcast(&self, _: &BroadcastPlan) -> Result<Pipeline> {
        Result::Err(ErrorCode::LogicalError(
            "Logical Error: visit_broadcast in pipeline_builder",
        ))
    }

    fn visit_remote(&self, plan: &RemotePlan) -> Result<Pipeline> {
        let mut pipeline = Pipeline::create(self.ctx.clone());

        for fetch_node in &plan.fetch_nodes {
            let flight_ticket =
                FlightTicket::stream(&plan.query_id, &plan.stage_id, &plan.stream_id);

            pipeline.add_source(Arc::new(RemoteTransform::try_create(
                flight_ticket,
                self.ctx.clone(),
                /* fetch_node_name */ fetch_node.clone(),
                /* fetch_stream_schema */ plan.schema.clone(),
            )?))?;
        }

        Ok(pipeline)
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone(),
            )?))
        })?;
        Ok(pipeline)
    }

    fn visit_projection(&mut self, node: &ProjectionPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                node.input.schema(),
                node.schema(),
                node.expr.clone(),
            )?))
        })?;
        Ok(pipeline)
    }

    fn visit_aggregator_partial(&mut self, node: &AggregatorPartialPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;

        if node.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorPartialTransform::try_create(
                    node.schema(),
                    node.input.schema(),
                    node.aggr_expr.clone(),
                )?))
            })?;
        } else {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByPartialTransform::create(
                    node.schema(),
                    node.input.schema(),
                    node.aggr_expr.clone(),
                    node.group_expr.clone(),
                )))
            })?;
        }
        Ok(pipeline)
    }

    fn visit_aggregator_final(&mut self, node: &AggregatorFinalPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;
        pipeline.merge_processor()?;

        if node.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorFinalTransform::try_create(
                    node.schema(),
                    node.schema_before_group_by.clone(),
                    node.aggr_expr.clone(),
                )?))
            })?;
        } else {
            let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByFinalTransform::create(
                    node.schema(),
                    max_block_size,
                    node.schema_before_group_by.clone(),
                    node.aggr_expr.clone(),
                    node.group_expr.clone(),
                )))
            })?;
            pipeline.mixed_processor(self.ctx.get_settings().get_max_threads()? as usize)?;
        }
        Ok(pipeline)
    }

    fn visit_filter(&mut self, node: &FilterPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(WhereTransform::try_create(
                node.schema(),
                node.predicate.clone(),
            )?))
        })?;
        Ok(pipeline)
    }

    fn visit_having(&mut self, node: &HavingPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(HavingTransform::try_create(
                node.schema(),
                node.predicate.clone(),
            )?))
        })?;
        Ok(pipeline)
    }

    fn visit_sort(&mut self, plan: &SortPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*plan.input)?;

        // The number of rows should be limit + offset. For example, for the query
        // 'select * from numbers(100) order by number desc limit 10 offset 5', the
        // sort pipeline should return at least 15 rows.
        let rows_limit = self.limit.map(|limit| limit + self.offset);

        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortPartialTransform::try_create(
                plan.schema(),
                plan.order_by.clone(),
                rows_limit,
            )?))
        })?;

        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortMergeTransform::try_create(
                plan.schema(),
                plan.order_by.clone(),
                rows_limit,
            )?))
        })?;

        // processor1 sorted block --
        //                             \
        // processor2 sorted block ----> processor  --> merge to one sorted block
        //                             /
        // processor3 sorted block --
        if pipeline.last_pipe()?.nums() > 1 {
            pipeline.merge_processor()?;
            pipeline.add_simple_transform(|| {
                Ok(Box::new(SortMergeTransform::try_create(
                    plan.schema(),
                    plan.order_by.clone(),
                    rows_limit,
                )?))
            })?;
        }
        Ok(pipeline)
    }

    fn visit_limit(&mut self, node: &LimitPlan) -> Result<Pipeline> {
        self.limit = node.n;
        self.offset = node.offset;

        let mut pipeline = self.visit(&*node.input)?;
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitTransform::try_create(node.n, node.offset)?))
        })?;
        Ok(pipeline)
    }

    fn visit_limit_by(&mut self, node: &LimitByPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*node.input)?;
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitByTransform::create(
                node.limit,
                node.limit_by.clone(),
            )))
        })?;
        Ok(pipeline)
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<Pipeline> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;

        let mut pipeline = Pipeline::create(self.ctx.clone());
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(max_threads, plan.parts.len());
        let workers = std::cmp::max(max_threads, 1);

        for _i in 0..workers {
            let source = SourceTransform::try_create(self.ctx.clone(), plan.clone())?;
            pipeline.add_source(Arc::new(source))?;
        }
        Ok(pipeline)
    }

    fn visit_sink(&mut self, plan: &SinkPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SinkTransform::create(
                self.ctx.clone(),
                plan.table_info.clone(),
                plan.cast_schema.clone(),
                plan.input.schema(),
            )))
        })?;
        Ok(pipeline)
    }

    fn visit_create_sets(&mut self, plan: &SubQueriesSetPlan) -> Result<Pipeline> {
        let mut pipeline = self.visit(&*plan.input)?;
        let schema = plan.schema();
        let context = self.ctx.clone();
        let expressions = plan.expressions.clone();
        let sub_queries_puller = SubQueriesPuller::create(context.clone(), expressions);
        pipeline.add_simple_transform(move || {
            Ok(Box::new(CreateSetsTransform::try_create(
                context.clone(),
                schema.clone(),
                sub_queries_puller.clone(),
            )?))
        })?;

        Ok(pipeline)
    }
}
