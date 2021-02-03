// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::PlanNode;
use crate::processors::Pipeline;
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
        let plans = self.plan.subplan_to_list()?;
        for plan in &plans {
            match plan {
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
                            plan.schema.clone(),
                            plan.expr.clone(),
                        )?))
                    })?;
                }
                PlanNode::Aggregate(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorPartialTransform::try_create(
                            plan.schema.clone(),
                            plan.aggr_expr.clone(),
                        )?))
                    })?;

                    pipeline.merge_processor()?;
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorFinalTransform::try_create(
                            plan.schema.clone(),
                            plan.aggr_expr.clone(),
                        )?))
                    })?;
                }
                PlanNode::Filter(plan) => {
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(FilterTransform::try_create(
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
