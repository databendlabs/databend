// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_planners::PlanNode;

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::interpreters::interpreter_create::CreateInterpreter;
use crate::interpreters::{
    ExplainInterpreter, IInterpreter, SelectInterpreter, SettingInterpreter,
};
use crate::sessions::FuseQueryContextRef;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: FuseQueryContextRef, plan: PlanNode) -> FuseQueryResult<Arc<dyn IInterpreter>> {
        match plan {
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx, v),
            PlanNode::Create(v) => CreateInterpreter::try_create(ctx, v),
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx, v),
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx, v),
            _ => Err(FuseQueryError::build_internal_error(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }
    }
}
