// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::PlanNode;
use crate::processors::Pipeline;
use crate::transforms::{FilterTransform, SourceTransform};

pub struct PipelineBuilder {
    ctx: Arc<FuseQueryContext>,
    plan: PlanNode,
}

impl PipelineBuilder {
    pub fn create(ctx: Arc<FuseQueryContext>, plan: PlanNode) -> Self {
        PipelineBuilder { ctx, plan }
    }

    pub fn build(&self) -> FuseQueryResult<Pipeline> {
        let mut pipeline = Pipeline::create();
        match &self.plan {
            PlanNode::Select(select) => {
                let mut clone = select.nodes.clone();
                clone.reverse();
                for child in &clone {
                    match child {
                        PlanNode::Filter(plan) => {
                            pipeline.add_simple_transform(|| {
                                Ok(Box::new(FilterTransform::try_create(
                                    plan.predicate.clone(),
                                )?))
                            })?;
                        }
                        PlanNode::ReadSource(plan) => {
                            for partition in &plan.partitions {
                                let source = SourceTransform::try_create(
                                    self.ctx.clone(),
                                    self.ctx.get_current_database()?.as_str(),
                                    plan.table.as_str(),
                                    vec![partition.clone()],
                                )?;
                                pipeline.add_source(Arc::new(source))?;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                return Err(FuseQueryError::Internal(
                    "Cannot build pipeline from pipe".to_string(),
                ))
            }
        }
        pipeline.merge_processor()?;
        Ok(pipeline)
    }
}
