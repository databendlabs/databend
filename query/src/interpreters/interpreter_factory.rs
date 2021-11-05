// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;

use crate::interpreters::interpreter_kill::KillInterpreter;
use crate::interpreters::CreatUserInterpreter;
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
use crate::sessions::DatabendQueryContextRef;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: DatabendQueryContextRef, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
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
            PlanNode::Kill(v) => KillInterpreter::try_create(ctx, v),
            PlanNode::CreateUser(v) => CreatUserInterpreter::try_create(ctx, v),
            _ => Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }
    }
}
