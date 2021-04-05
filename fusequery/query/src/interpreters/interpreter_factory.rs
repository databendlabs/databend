// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::{bail, Result};
use common_planners::PlanNode;

use crate::interpreters::interpreter_create_table::CreateTableInterpreter;
use crate::interpreters::{
    ExplainInterpreter, IInterpreter, SelectInterpreter, SettingInterpreter,
};
use crate::sessions::FuseQueryContextRef;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: FuseQueryContextRef, plan: PlanNode) -> Result<Arc<dyn IInterpreter>> {
        match plan {
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx, v),
            PlanNode::CreateTable(v) => CreateTableInterpreter::try_create(ctx, v),
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx, v),
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx, v),
            _ => bail!("Can't get the interpreter by plan:{}", plan.name()),
        }
    }
}
