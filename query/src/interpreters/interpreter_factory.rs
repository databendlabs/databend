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

use super::interpreter_user_stage_describe::DescribeUserStageInterpreter;
use super::interpreter_user_stage_drop::DropUserStageInterpreter;
use super::AlterViewInterpreter;
use super::CreateUserStageInterpreter;
use super::ListInterpreter;
use crate::interpreters::interpreter_show_engines::ShowEnginesInterpreter;
use crate::interpreters::interpreter_table_rename::RenameTableInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::AlterUserUDFInterpreter;
use crate::interpreters::CallInterpreter;
use crate::interpreters::CopyInterpreter;
use crate::interpreters::CreateDatabaseInterpreter;
use crate::interpreters::CreateRoleInterpreter;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::CreateUserInterpreter;
use crate::interpreters::CreateUserUDFInterpreter;
use crate::interpreters::CreateViewInterpreter;
use crate::interpreters::DescribeTableInterpreter;
use crate::interpreters::DropDatabaseInterpreter;
use crate::interpreters::DropRoleInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::interpreters::DropUserUDFInterpreter;
use crate::interpreters::DropViewInterpreter;
use crate::interpreters::EmptyInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::GrantPrivilegeInterpreter;
use crate::interpreters::GrantRoleInterpreter;
use crate::interpreters::InsertInterpreter;
use crate::interpreters::InterceptorInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::interpreters::OptimizeTableInterpreter;
use crate::interpreters::RenameDatabaseInterpreter;
use crate::interpreters::RevokePrivilegeInterpreter;
use crate::interpreters::RevokeRoleInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::SettingInterpreter;
use crate::interpreters::ShowCreateDatabaseInterpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::interpreters::ShowDatabasesInterpreter;
use crate::interpreters::ShowFunctionsInterpreter;
use crate::interpreters::ShowGrantsInterpreter;
use crate::interpreters::ShowMetricsInterpreter;
use crate::interpreters::ShowProcessListInterpreter;
use crate::interpreters::ShowRolesInterpreter;
use crate::interpreters::ShowSettingsInterpreter;
use crate::interpreters::ShowTabStatInterpreter;
use crate::interpreters::ShowTablesInterpreter;
use crate::interpreters::ShowUsersInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::interpreters::UnDropTableInterpreter;
use crate::interpreters::UseDatabaseInterpreter;
use crate::sessions::QueryContext;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactory;

/// InterpreterFactory provides `get` method which transforms the PlanNode into the corresponding interpreter.
/// Such as: SelectPlan -> SelectInterpreter, ExplainPlan -> ExplainInterpreter, ...
impl InterpreterFactory {
    pub fn get(ctx: Arc<QueryContext>, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        let ctx_clone = ctx.clone();
        let inner = match plan.clone() {
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx_clone, v),
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx_clone, v),
            PlanNode::Insert(v) => InsertInterpreter::try_create(ctx_clone, v),
            PlanNode::Copy(v) => CopyInterpreter::try_create(ctx_clone, v),
            PlanNode::Call(v) => CallInterpreter::try_create(ctx_clone, v),
            PlanNode::Show(ShowPlan::ShowDatabases(v)) => {
                ShowDatabasesInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowTables(v)) => {
                ShowTablesInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowTabStat(v)) => {
                ShowTabStatInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowEngines(v)) => {
                ShowEnginesInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowFunctions(v)) => {
                ShowFunctionsInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowGrants(v)) => {
                ShowGrantsInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowMetrics(v)) => {
                ShowMetricsInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowProcessList(v)) => {
                ShowProcessListInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowSettings(v)) => {
                ShowSettingsInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowUsers(v)) => {
                ShowUsersInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowRoles(v)) => {
                ShowRolesInterpreter::try_create(ctx_clone, v)
            }

            // Database related transforms.
            PlanNode::CreateDatabase(v) => CreateDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::DropDatabase(v) => DropDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateDatabase(v) => {
                ShowCreateDatabaseInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::RenameDatabase(v) => RenameDatabaseInterpreter::try_create(ctx_clone, v),

            // Table related transforms
            PlanNode::CreateTable(v) => CreateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DropTable(v) => DropTableInterpreter::try_create(ctx_clone, v),
            PlanNode::UnDropTable(v) => UnDropTableInterpreter::try_create(ctx_clone, v),
            PlanNode::RenameTable(v) => RenameTableInterpreter::try_create(ctx_clone, v),
            PlanNode::TruncateTable(v) => TruncateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::OptimizeTable(v) => OptimizeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeTable(v) => DescribeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateTable(v) => ShowCreateTableInterpreter::try_create(ctx_clone, v),

            // View related transforms
            PlanNode::CreateView(v) => CreateViewInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterView(v) => AlterViewInterpreter::try_create(ctx_clone, v),
            PlanNode::DropView(v) => DropViewInterpreter::try_create(ctx_clone, v),

            // User related transforms
            PlanNode::CreateUser(v) => CreateUserInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterUser(v) => AlterUserInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUser(v) => DropUserInterpreter::try_create(ctx_clone, v),

            // Privilege related transforms
            PlanNode::GrantPrivilege(v) => GrantPrivilegeInterpreter::try_create(ctx_clone, v),
            PlanNode::GrantRole(v) => GrantRoleInterpreter::try_create(ctx_clone, v),

            PlanNode::RevokePrivilege(v) => RevokePrivilegeInterpreter::try_create(ctx_clone, v),
            PlanNode::RevokeRole(v) => RevokeRoleInterpreter::try_create(ctx_clone, v),

            PlanNode::CreateRole(v) => CreateRoleInterpreter::try_create(ctx_clone, v),
            PlanNode::DropRole(v) => DropRoleInterpreter::try_create(ctx_clone, v),

            // UDF related transforms
            PlanNode::CreateUserUDF(v) => CreateUserUDFInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUserUDF(v) => DropUserUDFInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterUserUDF(v) => AlterUserUDFInterpreter::try_create(ctx_clone, v),

            // Stage related transforms
            PlanNode::CreateUserStage(v) => CreateUserStageInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUserStage(v) => DropUserStageInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeUserStage(v) => {
                DescribeUserStageInterpreter::try_create(ctx_clone, v)
            }

            // others
            PlanNode::List(v) => ListInterpreter::try_create(ctx_clone, v),
            PlanNode::UseDatabase(v) => UseDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::Kill(v) => KillInterpreter::try_create(ctx_clone, v),
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx_clone, v),
            PlanNode::Empty(v) => EmptyInterpreter::try_create(ctx_clone, v),

            _ => Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }?;
        Ok(Arc::new(InterceptorInterpreter::create(ctx, inner, plan)))
    }
}
