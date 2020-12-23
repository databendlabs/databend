// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::{ExplainExecutor, IExecutor, SelectExecutor, SettingExecutor};
use crate::planners::PlanNode;

pub struct ExecutorFactory;

impl ExecutorFactory {
    pub fn get(ctx: FuseQueryContextRef, plan: PlanNode) -> FuseQueryResult<Arc<dyn IExecutor>> {
        match plan {
            PlanNode::Select(v) => SelectExecutor::try_create(ctx, v),
            PlanNode::Explain(v) => ExplainExecutor::try_create(ctx, v),
            PlanNode::SetVariable(v) => SettingExecutor::try_create(ctx, v),
            _ => Err(FuseQueryError::Internal(format!(
                "Can't get the executor by plan:{}",
                plan.name()
            ))),
        }
    }
}
