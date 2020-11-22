// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::FuseQueryError;
use crate::error::FuseQueryResult;
use crate::executors::{ExplainExecutor, IExecutor, SelectExecutor};
use crate::planners::PlanNode;

pub struct ExecutorFactory;

impl ExecutorFactory {
    pub fn get(ctx: Arc<FuseQueryContext>, plan: PlanNode) -> FuseQueryResult<Arc<dyn IExecutor>> {
        match plan {
            PlanNode::Select(v) => SelectExecutor::try_create(ctx, v),
            PlanNode::Explain(v) => ExplainExecutor::try_create(ctx, v.as_ref().clone()),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Can't get the executor by plan:{}",
                plan.name()
            ))),
        }
    }
}
