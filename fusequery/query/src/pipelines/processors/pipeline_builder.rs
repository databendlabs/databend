// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
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
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::SortPlan;
use common_planners::StagePlan;
use common_tracing::tracing;

use crate::pipelines::processors::Pipeline;
use crate::pipelines::transforms::AggregatorFinalTransform;
use crate::pipelines::transforms::AggregatorPartialTransform;
use crate::pipelines::transforms::ExpressionTransform;
use crate::pipelines::transforms::FilterTransform;
use crate::pipelines::transforms::GroupByFinalTransform;
use crate::pipelines::transforms::GroupByPartialTransform;
use crate::pipelines::transforms::LimitByTransform;
use crate::pipelines::transforms::LimitTransform;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::RemoteTransform;
use crate::pipelines::transforms::SortMergeTransform;
use crate::pipelines::transforms::SortPartialTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::sessions::FuseQueryContextRef;

pub struct PipelineBuilder {
    ctx: FuseQueryContextRef,
    exists_res_map: HashMap<String, bool>,
    plan: PlanNode,
}

impl PipelineBuilder {
    pub fn create(
        ctx: FuseQueryContextRef,
        exists_res_map: HashMap<String, bool>,
        plan: PlanNode,
    ) -> Self {
        PipelineBuilder {
            ctx,
            exists_res_map,
            plan,
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub fn build(&self) -> Result<Pipeline> {
        tracing::debug!("Received plan:\n{:?}", self.plan);

        let mut limit = None;
        self.plan.walk_preorder(|node| -> Result<bool> {
            match node {
                PlanNode::Limit(ref limit_plan) => {
                    limit = limit_plan.n;
                    Ok(true)
                }
                _ => Ok(true),
            }
        })?;

        let mut pipeline = Pipeline::create(self.ctx.clone());
        self.plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::Select(_) => Ok(true),
                PlanNode::Stage(plan) => self.visit_stage_plan(&mut pipeline, &plan),
                PlanNode::Remote(plan) => self.visit_remote_plan(&mut pipeline, &plan),
                PlanNode::Expression(plan) => {
                    PipelineBuilder::visit_expression_plan(&mut pipeline, plan)
                }
                PlanNode::Projection(plan) => {
                    PipelineBuilder::visit_projection_plan(&mut pipeline, plan)
                }
                PlanNode::AggregatorPartial(plan) => {
                    PipelineBuilder::visit_aggregator_partial_plan(&mut pipeline, plan)
                }
                PlanNode::AggregatorFinal(plan) => {
                    PipelineBuilder::visit_aggregator_final_plan(&mut pipeline, plan)
                }
                PlanNode::Filter(plan) => self.visit_filter_plan(&mut pipeline, plan),
                PlanNode::Having(plan) => self.visit_having_plan(&mut pipeline, plan),
                PlanNode::Sort(plan) => {
                    PipelineBuilder::visit_sort_plan(limit, &mut pipeline, plan)
                }
                PlanNode::Limit(plan) => PipelineBuilder::visit_limit_plan(&mut pipeline, plan),
                PlanNode::LimitBy(plan) => {
                    PipelineBuilder::visit_limit_by_plan(&mut pipeline, plan)
                }
                PlanNode::ReadSource(plan) => self.visit_read_data_source_plan(&mut pipeline, plan),
                other => Result::Err(ErrorCode::UnknownPlan(format!(
                    "Build pipeline from the plan node unsupported:{:?}",
                    other.name()
                ))),
            }
        })?;
        tracing::debug!("Pipeline:\n{:?}", pipeline);

        Ok(pipeline)
    }

    fn visit_stage_plan(&self, _: &mut Pipeline, _: &&StagePlan) -> Result<bool> {
        Result::Err(ErrorCode::LogicalError(
            "Logical Error: visit_stage_plan in pipeline_builder",
        ))
    }

    fn visit_remote_plan(&self, pipeline: &mut Pipeline, plan: &&RemotePlan) -> Result<bool> {
        for fetch_node in &plan.fetch_nodes {
            pipeline.add_source(Arc::new(RemoteTransform::try_create(
                self.ctx.clone(),
                plan.fetch_name.clone(),
                fetch_node.clone(),
                plan.schema.clone(),
            )?))?;
        }

        Ok(true)
    }

    fn visit_expression_plan(pipeline: &mut Pipeline, plan: &ExpressionPlan) -> Result<bool> {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone(),
            )?))
        })?;
        Ok(true)
    }

    fn visit_projection_plan(pipeline: &mut Pipeline, plan: &ProjectionPlan) -> Result<bool> {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.input.schema(),
                plan.schema(),
                plan.expr.clone(),
            )?))
        })?;
        Ok(true)
    }

    fn visit_aggregator_partial_plan(
        pipeline: &mut Pipeline,
        plan: &AggregatorPartialPlan,
    ) -> Result<bool> {
        if plan.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorPartialTransform::try_create(
                    plan.schema(),
                    plan.input.schema(),
                    plan.aggr_expr.clone(),
                )?))
            })?;
        } else {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByPartialTransform::create(
                    plan.schema(),
                    plan.input.schema(),
                    plan.aggr_expr.clone(),
                    plan.group_expr.clone(),
                )))
            })?;
        }
        Ok(true)
    }

    fn visit_aggregator_final_plan(
        pipeline: &mut Pipeline,
        plan: &AggregatorFinalPlan,
    ) -> Result<bool> {
        pipeline.merge_processor()?;
        if plan.group_expr.is_empty() {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(AggregatorFinalTransform::try_create(
                    plan.schema(),
                    plan.schema_before_group_by.clone(),
                    plan.aggr_expr.clone(),
                )?))
            })?;
        } else {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(GroupByFinalTransform::create(
                    plan.schema(),
                    plan.schema_before_group_by.clone(),
                    plan.aggr_expr.clone(),
                    plan.group_expr.clone(),
                )))
            })?;
        }
        Ok(true)
    }

    fn visit_filter_plan(&self, pipeline: &mut Pipeline, plan: &FilterPlan) -> Result<bool> {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                self.exists_res_map.clone(),
                plan.input.schema(),
                plan.predicate.clone(),
                false,
            )?))
        })?;
        Ok(true)
    }

    fn visit_having_plan(&self, pipeline: &mut Pipeline, plan: &HavingPlan) -> Result<bool> {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                HashMap::<String, bool>::new(),
                plan.input.schema(),
                plan.predicate.clone(),
                true,
            )?))
        })?;
        Ok(true)
    }

    fn visit_sort_plan(
        limit: Option<usize>,
        pipeline: &mut Pipeline,
        plan: &SortPlan,
    ) -> Result<bool> {
        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortPartialTransform::try_create(
                plan.schema(),
                plan.order_by.clone(),
                limit,
            )?))
        })?;

        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortMergeTransform::try_create(
                plan.schema(),
                plan.order_by.clone(),
                limit,
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
                    limit,
                )?))
            })?;
        }
        Ok(true)
    }

    fn visit_limit_plan(pipeline: &mut Pipeline, plan: &LimitPlan) -> Result<bool> {
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitTransform::try_create(plan.n, plan.offset)?))
        })?;
        Ok(false)
    }

    fn visit_limit_by_plan(pipeline: &mut Pipeline, plan: &LimitByPlan) -> Result<bool> {
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitByTransform::create(
                plan.limit,
                plan.limit_by.clone(),
            )))
        })?;
        Ok(false)
    }

    fn visit_read_data_source_plan(
        &self,
        pipeline: &mut Pipeline,
        plan: &ReadDataSourcePlan,
    ) -> Result<bool> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.partitions.clone())?;

        let max_threads = self.ctx.get_max_threads()? as usize;
        let max_threads = std::cmp::min(max_threads, plan.partitions.len());
        let workers = std::cmp::max(max_threads, 1);

        for _i in 0..workers {
            let source = SourceTransform::try_create(
                self.ctx.clone(),
                plan.db.as_str(),
                plan.table.as_str(),
                plan.remote,
            )?;
            pipeline.add_source(Arc::new(source))?;
        }
        Ok(true)
    }
}
