// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use common_planners::PlanNode;
use log::info;

use crate::pipelines::processors::Pipeline;
use crate::pipelines::transforms::AggregatorFinalTransform;
use crate::pipelines::transforms::AggregatorPartialTransform;
use crate::pipelines::transforms::FilterTransform;
use crate::pipelines::transforms::GroupByFinalTransform;
use crate::pipelines::transforms::GroupByPartialTransform;
use crate::pipelines::transforms::LimitTransform;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::RemoteTransform;
use crate::pipelines::transforms::SortMergeTransform;
use crate::pipelines::transforms::SortPartialTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::planners::PlanScheduler;
use crate::sessions::FuseQueryContextRef;

pub struct PipelineBuilder {
    ctx: FuseQueryContextRef,
    plan: PlanNode
}

impl PipelineBuilder {
    pub fn create(ctx: FuseQueryContextRef, plan: PlanNode) -> Self {
        PipelineBuilder { ctx, plan }
    }

    pub fn build(&self) -> Result<Pipeline> {
        info!("Received for plan:\n{:?}", self.plan);

        let mut limit = None;
        self.plan.walk_preorder(|node| match node {
            PlanNode::Limit(ref limit_plan) => {
                limit = Some(limit_plan.n);
                Ok(true)
            }
            _ => Ok(true)
        })?;

        let mut pipeline = Pipeline::create();
        self.plan.walk_postorder(|node| match node {
            PlanNode::Stage(plan) => {
                let executors = self.ctx.try_get_cluster()?.get_nodes()?;
                if !executors.is_empty() {
                    // Reset the pipes already in the pipeline.
                    pipeline.reset();

                    // Reset the context partition.
                    self.ctx.reset()?;

                    // Build the distributed plan for the executors.
                    let children = plan.input().as_ref().clone();
                    let remote_plan_nodes = PlanScheduler::schedule(self.ctx.clone(), &children)?;

                    // Add remote transform as the new source.
                    for (i, remote_plan_node) in remote_plan_nodes.iter().enumerate() {
                        let executor = executors[i % executors.len()].clone();
                        let remote_transform = RemoteTransform::try_create(
                            self.ctx.clone(),
                            self.ctx.get_id()?,
                            executor.address,
                            remote_plan_node.clone()
                        )?;
                        pipeline.add_source(Arc::new(remote_transform))?;
                    }
                }
                Ok(true)
            }
            PlanNode::Projection(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(ProjectionTransform::try_create(
                        plan.schema.clone(),
                        plan.expr.clone()
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::AggregatorPartial(plan) => {
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
                Ok(true)
            }
            PlanNode::AggregatorFinal(plan) => {
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
                Ok(true)
            }
            PlanNode::Filter(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(FilterTransform::try_create(
                        plan.predicate.clone()
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::Sort(plan) => {
                // processor 1: block ---> sort_stream
                // processor 2: block ---> sort_stream
                // processor 3: block ---> sort_stream
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(SortPartialTransform::try_create(
                        plan.schema(),
                        plan.order_by.clone(),
                        limit
                    )?))
                })?;

                // processor 1: [sorted blocks ...] ---> merge to one sorted block
                // processor 2: [sorted blocks ...] ---> merge to one sorted block
                // processor 3: [sorted blocks ...] ---> merge to one sorted block
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(SortMergeTransform::try_create(
                        plan.schema(),
                        plan.order_by.clone(),
                        limit
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
                            limit
                        )?))
                    })?;
                }
                Ok(true)
            }

            PlanNode::Limit(plan) => {
                pipeline.merge_processor()?;
                pipeline
                    .add_simple_transform(|| Ok(Box::new(LimitTransform::try_create(plan.n)?)))?;
                Ok(false)
            }
            PlanNode::ReadSource(plan) => {
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
                Ok(true)
            }
            PlanNode::Select(_) => Ok(true),
            other => {
                bail!(
                    "Build pipeline from the plan node unsupported:{:?}",
                    other.name()
                );
            }
        })?;
        info!("Pipeline:\n{:?}", pipeline);

        Ok(pipeline)
    }
}
