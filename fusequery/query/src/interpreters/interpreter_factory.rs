// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;

use crate::interpreters::CreateDatabaseInterpreter;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::DescribeTableInterpreter;
use crate::interpreters::DropDatabaseInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::InsertIntoInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::SettingInterpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::interpreters::UseDatabaseInterpreter;
use crate::sessions::FuseQueryContextRef;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: FuseQueryContextRef, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        match plan {
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx, v),
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx, v),
            PlanNode::CreateDatabase(v) => CreateDatabaseInterpreter::try_create(ctx, v),
            PlanNode::DropDatabase(v) => DropDatabaseInterpreter::try_create(ctx, v),
            PlanNode::CreateTable(v) => CreateTableInterpreter::try_create(ctx, v),
            PlanNode::DropTable(v) => DropTableInterpreter::try_create(ctx, v),
            PlanNode::DescribeTable(v) => DescribeTableInterpreter::try_create(ctx, v),
            PlanNode::TruncateTable(v) => TruncateTableInterpreter::try_create(ctx, v),
            PlanNode::UseDatabase(v) => UseDatabaseInterpreter::try_create(ctx, v),
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx, v),
            PlanNode::InsertInto(v) => InsertIntoInterpreter::try_create(ctx, v),
            PlanNode::ShowCreateTable(v) => ShowCreateTableInterpreter::try_create(ctx, v),
            _ => Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }
    }
}
