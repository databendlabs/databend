// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::PlanNode;
use crate::processors::Pipeline;
use crate::sessions::FuseQueryContextRef;
use crate::transforms::{
    AggregatorFinalTransform, AggregatorPartialTransform, FilterTransform, LimitTransform,
    ProjectionTransform, SourceTransform,
};

pub struct PipelineBuilder {
    ctx: FuseQueryContextRef,
    plan: PlanNode,
}

impl PipelineBuilder {
    pub fn create(ctx: FuseQueryContextRef, plan: PlanNode) -> Self {
        PipelineBuilder { ctx, plan }
    }

    pub fn build(&self) -> FuseQueryResult<Pipeline> {
        let mut pipeline = Pipeline::create();
        let plans = self.plan.get_children_nodes()?;
        for plan in &plans {
            match plan {
                PlanNode::Fragment(_) => {
                    pipeline.merge_processor()?;
                }
                PlanNode::Limit(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(LimitTransform::try_create(plan.n)?))
                    })?;
                    if pipeline.pipe_num() > 1 {
                        pipeline.merge_processor()?;
                        pipeline.add_simple_transform(|| {
                            Ok(Box::new(LimitTransform::try_create(plan.n)?))
                        })?;
                    }
                }
                PlanNode::Projection(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(ProjectionTransform::try_create(
                            self.ctx.clone(),
                            plan.schema.clone(),
                            plan.expr.clone(),
                        )?))
                    })?;
                }
                PlanNode::AggregatorPartial(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorPartialTransform::try_create(
                            self.ctx.clone(),
                            plan.schema(),
                            plan.aggr_expr.clone(),
                        )?))
                    })?;
                }
                PlanNode::AggregatorFinal(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorFinalTransform::try_create(
                            self.ctx.clone(),
                            plan.schema(),
                            plan.aggr_expr.clone(),
                        )?))
                    })?;
                }
                PlanNode::Filter(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(FilterTransform::try_create(
                            self.ctx.clone(),
                            plan.predicate.clone(),
                        )?))
                    })?;
                }
                PlanNode::ReadSource(plan) => {
                    // Bind plan partitions to context.
                    self.ctx.try_update_partitions(plan.partitions.clone())?;

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
                            plan.table.as_str(),
                        )?;
                        pipeline.add_source(Arc::new(source))?;
                    }
                }
                other => {
                    return Err(FuseQueryError::Internal(format!(
                        "Build pipeline from the plan node unsupported:{:?}",
                        other.name()
                    )))
                }
            }
        }
        pipeline.merge_processor()?;
        Ok(pipeline)
    }
}
