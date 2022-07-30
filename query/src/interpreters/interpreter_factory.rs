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
use common_planners::ShowPlan;

use super::AlterViewInterpreter;
use super::ShowDatabasesInterpreter;
use crate::interpreters::interpreter_show_engines::ShowEnginesInterpreter;
use crate::interpreters::interpreter_table_rename::RenameTableInterpreter;
use crate::interpreters::AlterTableClusterKeyInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::AlterUserUDFInterpreter;
use crate::interpreters::CallInterpreter;
use crate::interpreters::CreateDatabaseInterpreter;
use crate::interpreters::CreateRoleInterpreter;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::CreateUserInterpreter;
use crate::interpreters::CreateUserUDFInterpreter;
use crate::interpreters::CreateViewInterpreter;
use crate::interpreters::DeleteInterpreter;
use crate::interpreters::DescribeTableInterpreter;
use crate::interpreters::DropDatabaseInterpreter;
use crate::interpreters::DropRoleInterpreter;
use crate::interpreters::DropTableClusterKeyInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::interpreters::DropUserUDFInterpreter;
use crate::interpreters::DropViewInterpreter;
use crate::interpreters::EmptyInterpreter;
use crate::interpreters::ExistsTableInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::GrantPrivilegeInterpreter;
use crate::interpreters::GrantRoleInterpreter;
use crate::interpreters::InsertInterpreter;
use crate::interpreters::InterceptorInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::KillInterpreter;
use crate::interpreters::OptimizeTableInterpreter;
use crate::interpreters::RenameDatabaseInterpreter;
use crate::interpreters::RevokePrivilegeInterpreter;
use crate::interpreters::RevokeRoleInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::SettingInterpreter;
use crate::interpreters::ShowCreateDatabaseInterpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::interpreters::ShowFunctionsInterpreter;
use crate::interpreters::ShowGrantsInterpreter;
use crate::interpreters::ShowMetricsInterpreter;
use crate::interpreters::ShowProcessListInterpreter;
use crate::interpreters::ShowRolesInterpreter;
use crate::interpreters::ShowSettingsInterpreter;
use crate::interpreters::ShowTablesInterpreter;
use crate::interpreters::ShowTablesStatusInterpreter;
use crate::interpreters::ShowUsersInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::interpreters::UndropDatabaseInterpreter;
use crate::interpreters::UndropTableInterpreter;
use crate::interpreters::UseDatabaseInterpreter;
use crate::sessions::QueryContext;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactory;

/// InterpreterFactory provides `get` method which transforms the PlanNode into the corresponding interpreter.
/// Such as: SelectPlan -> SelectInterpreter, ExplainPlan -> ExplainInterpreter, ...
impl InterpreterFactory {
    pub fn get(ctx: Arc<QueryContext>, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        let inner = Self::create_interpreter(ctx.clone(), &plan)?;
        let query_kind = plan.name().to_string();
        Ok(Arc::new(InterceptorInterpreter::create(
            ctx, inner, plan, query_kind,
        )))
    }

    fn create_interpreter(ctx: Arc<QueryContext>, plan: &PlanNode) -> Result<InterpreterPtr> {
        match plan.clone() {
            PlanNode::Select(v) => Ok(Arc::new(SelectInterpreter::try_create(ctx, v)?)),
            PlanNode::Explain(v) => Ok(Arc::new(ExplainInterpreter::try_create(ctx, v)?)),
            PlanNode::Insert(v) => Ok(Arc::new(InsertInterpreter::try_create(ctx, v, false)?)),
            PlanNode::Delete(v) => Ok(Arc::new(DeleteInterpreter::try_create(ctx, v)?)),

            _ => Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan: {}",
                plan.name()
            ))),
        }
    }
}
