// Copyright 2021 Datafuse Labs.
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

use super::DescribeStageInterpreter;
use crate::interpreters::interpreter_stage_drop::DropStageInterpreter;
use crate::interpreters::interpreter_table_optimize::OptimizeTableInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::CopyInterpreter;
use crate::interpreters::CreatStageInterpreter;
use crate::interpreters::CreateDatabaseInterpreter;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::CreateUserInterpreter;
use crate::interpreters::DescribeTableInterpreter;
use crate::interpreters::DropDatabaseInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::GrantPrivilegeInterpreter;
use crate::interpreters::InsertInterpreter;
use crate::interpreters::InterceptorInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::interpreters::RevokePrivilegeInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::SettingInterpreter;
use crate::interpreters::ShowCreateDatabaseInterpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::interpreters::ShowGrantsInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::interpreters::UseDatabaseInterpreter;
use crate::sessions::QueryContext;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: Arc<QueryContext>, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        let ctx_clone = ctx.clone();
        let inner = match plan.clone() {
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx_clone, v),
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx_clone, v),
            PlanNode::CreateDatabase(v) => CreateDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::DropDatabase(v) => DropDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::CreateTable(v) => CreateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DropTable(v) => DropTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeTable(v) => DescribeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::TruncateTable(v) => TruncateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::OptimizeTable(v) => OptimizeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::UseDatabase(v) => UseDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx_clone, v),
            PlanNode::Insert(v) => InsertInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateTable(v) => ShowCreateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::Kill(v) => KillInterpreter::try_create(ctx_clone, v),
            PlanNode::CreateUser(v) => CreateUserInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterUser(v) => AlterUserInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUser(v) => DropUserInterpreter::try_create(ctx_clone, v),
            PlanNode::GrantPrivilege(v) => GrantPrivilegeInterpreter::try_create(ctx_clone, v),
            PlanNode::RevokePrivilege(v) => RevokePrivilegeInterpreter::try_create(ctx_clone, v),
            PlanNode::Copy(v) => CopyInterpreter::try_create(ctx_clone, v),
            PlanNode::CreateUserStage(v) => CreatStageInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUserStage(v) => DropStageInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowGrants(v) => ShowGrantsInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeStage(v) => DescribeStageInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateDatabase(v) => {
                ShowCreateDatabaseInterpreter::try_create(ctx_clone, v)
            }
            _ => Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }?;
        Ok(Arc::new(InterceptorInterpreter::create(ctx, inner, plan)))
    }
}
