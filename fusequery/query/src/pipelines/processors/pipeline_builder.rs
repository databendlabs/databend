// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::JoinPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::SortPlan;
use common_planners::StagePlan;
use log::info;

use crate::pipelines::processors::Pipeline;
use crate::pipelines::transforms::AggregatorFinalTransform;
use crate::pipelines::transforms::AggregatorPartialTransform;
use crate::pipelines::transforms::ExpressionTransform;
use crate::pipelines::transforms::FilterTransform;
use crate::pipelines::transforms::GroupByFinalTransform;
use crate::pipelines::transforms::GroupByPartialTransform;
use crate::pipelines::transforms::LimitTransform;
use crate::pipelines::transforms::NestedLoopJoinTransform;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::RemoteTransform;
use crate::pipelines::transforms::SortMergeTransform;
use crate::pipelines::transforms::SortPartialTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::planners::PlanScheduler;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub struct PipelineBuilder {
    ctx: FuseQueryContextRef,
    plan: PlanNode
}

impl PipelineBuilder {
    pub fn create(ctx: FuseQueryContextRef, plan: PlanNode) -> Self {
        PipelineBuilder {
            ctx: ctx.clone(),
            plan
        }
    }

    pub fn build(&self) -> Result<Pipeline> {
        info!("Received for plan:\n{:?}", self.plan);

        let mut pipeline = Pipeline::create(self.ctx.clone());
        self.build_impl(&mut pipeline, &self.plan.clone())?;
        info!("Pipeline:\n{:?}", pipeline);

        Ok(pipeline)
    }

    fn build_impl(&self, pipeline: &mut Pipeline, plan: &PlanNode) -> Result<()> {
        match plan {
            PlanNode::Select(plan) => self.build_impl(pipeline, &plan.input),
            PlanNode::Stage(plan) => self.build_stage_plan(pipeline, plan),
            PlanNode::Expression(plan) => self.build_expression_plan(pipeline, plan),
            PlanNode::Projection(plan) => self.build_projection_plan(pipeline, plan),
            PlanNode::AggregatorPartial(plan) => self.build_aggregator_partial_plan(pipeline, plan),
            PlanNode::AggregatorFinal(plan) => self.build_aggregator_final_plan(pipeline, plan),
            PlanNode::Filter(plan) => self.build_filter_plan(pipeline, plan),
            PlanNode::Having(plan) => self.build_having_plan(pipeline, plan),
            PlanNode::Sort(plan) => self.build_sort_plan(pipeline, plan),
            PlanNode::Limit(plan) => self.build_limit_plan(pipeline, plan),
            PlanNode::ReadSource(plan) => self.build_read_data_source_plan(pipeline, plan),
            PlanNode::Join(plan) => self.build_join_plan(pipeline, plan),
            other => Result::Err(ErrorCodes::UnknownPlan(format!(
                "Build pipeline from the plan node unsupported:{:?}",
                other.name()
            )))
        }
    }

    fn build_stage_plan(&self, pipeline: &mut Pipeline, plan: &StagePlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        let executors = PlanScheduler::reschedule(self.ctx.clone(), plan.input.as_ref())?;

        // If the executors is not empty.
        if !executors.is_empty() {
            // Reset.
            pipeline.reset();
            self.ctx.reset()?;

            // Add remote transform as the new source.
            for (address, remote_plan) in executors.iter() {
                let remote_transform = RemoteTransform::try_create(
                    self.ctx.clone(),
                    self.ctx.get_id()?,
                    address.clone(),
                    remote_plan.clone()
                )?;
                pipeline.add_source(Arc::new(remote_transform))?;
            }
        }
        Ok(())
    }

    fn build_expression_plan(&self, pipeline: &mut Pipeline, plan: &ExpressionPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone()
            )?))
        })?;
        Ok(())
    }

    fn build_projection_plan(&self, pipeline: &mut Pipeline, plan: &ProjectionPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.input.schema(),
                plan.schema(),
                plan.expr.clone()
            )?))
        })?;
        Ok(())
    }

    fn build_aggregator_partial_plan(
        &self,
        pipeline: &mut Pipeline,
        plan: &AggregatorPartialPlan
    ) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        if plan.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorPartialTransform::try_create(
                    plan.schema(),
                    plan.aggr_expr.clone()
                )?))
            })?;
        } else {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByPartialTransform::create(
                    plan.schema(),
                    plan.aggr_expr.clone(),
                    plan.group_expr.clone()
                )))
            })?;
        }
        Ok(())
    }

    fn build_aggregator_final_plan(
        &self,
        pipeline: &mut Pipeline,
        plan: &AggregatorFinalPlan
    ) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.merge_processor()?;
        if plan.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorFinalTransform::try_create(
                    plan.schema(),
                    plan.aggr_expr.clone()
                )?))
            })?;
        } else {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByFinalTransform::create(
                    plan.schema(),
                    plan.aggr_expr.clone(),
                    plan.group_expr.clone()
                )))
            })?;
        }
        Ok(())
    }

    fn build_filter_plan(&self, pipeline: &mut Pipeline, plan: &FilterPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                plan.input.schema(),
                plan.predicate.clone(),
                false
            )?))
        })?;
        Ok(())
    }

    fn build_having_plan(&self, pipeline: &mut Pipeline, plan: &HavingPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                plan.input.schema(),
                plan.predicate.clone(),
                true
            )?))
        })?;
        Ok(())
    }

    fn build_sort_plan(&self, pipeline: &mut Pipeline, plan: &SortPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortPartialTransform::try_create(
                plan.schema(),
                plan.order_by.clone()
            )?))
        })?;

        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortMergeTransform::try_create(
                plan.schema(),
                plan.order_by.clone()
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
                    plan.order_by.clone()
                )?))
            })?;
        }
        Ok(())
    }

    fn build_limit_plan(&self, pipeline: &mut Pipeline, plan: &LimitPlan) -> Result<()> {
        self.build_impl(pipeline, &plan.input)?;
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| Ok(Box::new(LimitTransform::try_create(plan.n)?)))?;
        Ok(())
    }

    fn build_read_data_source_plan(
        &self,
        pipeline: &mut Pipeline,
        plan: &ReadDataSourcePlan
    ) -> Result<()> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.partitions.clone())?;

        let max_threads = self.ctx.get_max_threads()? as usize;
        let workers = if max_threads == 0 {
            1
        } else if max_threads > plan.partitions.len() {
            plan.partitions.len()
        } else {
            max_threads
        };

        for _i in 0..workers {
            let source = SourceTransform::try_create(
                self.ctx.clone(),
                plan.db.as_str(),
                plan.table.as_str()
            )?;
            pipeline.add_source(Arc::new(source))?;
        }
        Ok(())
    }

    fn build_join_plan(&self, pipeline: &mut Pipeline, plan: &JoinPlan) -> Result<()> {
        // Build left pipeline
        let left_pipeline_builder =
            PipelineBuilder::create(FuseQueryContext::try_create()?, (*plan.left_input).clone());
        let mut left_pipeline = left_pipeline_builder.build()?;
        left_pipeline.merge_processor()?;
        // Build right pipeline
        let right_pipeline_builder =
            PipelineBuilder::create(FuseQueryContext::try_create()?, (*plan.right_input).clone());
        let mut right_pipeline = right_pipeline_builder.build()?;
        right_pipeline.merge_processor()?;

        let join = NestedLoopJoinTransform::try_create(
            self.ctx.clone(),
            plan.schema().clone(),
            Arc::from(left_pipeline.last_pipe()?.processor_by_index(0)),
            Arc::from(right_pipeline.last_pipe()?.processor_by_index(0))
        )?;

        pipeline.add_source(Arc::new(join))?;
        Ok(())
    }
}
