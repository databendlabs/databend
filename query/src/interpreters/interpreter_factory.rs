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
use super::ShowStagesInterpreter;
use crate::interpreters::interpreter_show_engines::ShowEnginesInterpreter;
use crate::interpreters::interpreter_table_rename::RenameTableInterpreter;
use crate::interpreters::AlterTableClusterKeyInterpreter;
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
use crate::interpreters::RemoveUserStageInterpreter;
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
            PlanNode::Copy(v) => Ok(Arc::new(CopyInterpreter::try_create(ctx, v)?)),
            PlanNode::Call(v) => Ok(Arc::new(CallInterpreter::try_create(ctx, v)?)),
          
            PlanNode::Show(ShowPlan::ShowTables(v)) => {
                Ok(Arc::new(ShowTablesInterpreter::try_create(ctx, v)?))
            }
            PlanNode::Show(ShowPlan::ShowTablesStatus(v)) => {
                Ok(Arc::new(ShowTablesStatusInterpreter::try_create(ctx, v)?))
            }
            PlanNode::Show(ShowPlan::ShowEngines(v)) => {
                Ok(Arc::new(ShowEnginesInterpreter::try_create(ctx, v)?))
            }
            PlanNode::Show(ShowPlan::ShowFunctions(v)) => {
                Ok(Arc::new(ShowFunctionsInterpreter::try_create(ctx, v)?))
            }
            PlanNode::Show(ShowPlan::ShowGrants(v)) => {
                Ok(Arc::new(ShowGrantsInterpreter::try_create(ctx, v)?))
            }
            PlanNode::Show(ShowPlan::ShowMetrics(_)) => {
                Ok(Arc::new(ShowMetricsInterpreter::try_create(ctx)?))
            }
            PlanNode::Show(ShowPlan::ShowProcessList(_)) => {
                Ok(Arc::new(ShowProcessListInterpreter::try_create(ctx)?))
            }
            PlanNode::Show(ShowPlan::ShowSettings(_)) => {
                Ok(Arc::new(ShowSettingsInterpreter::try_create(ctx)?))
            }
            PlanNode::Show(ShowPlan::ShowUsers(_)) => {
                Ok(Arc::new(ShowUsersInterpreter::try_create(ctx)?))
            }
            PlanNode::Show(ShowPlan::ShowRoles(_)) => {
                Ok(Arc::new(ShowRolesInterpreter::try_create(ctx)?))
            }
            PlanNode::Show(ShowPlan::ShowStages) => {
                Ok(Arc::new(ShowStagesInterpreter::try_create(ctx)?))
            }

            // Database related transforms.
            PlanNode::CreateDatabase(v) => {
                Ok(Arc::new(CreateDatabaseInterpreter::try_create(ctx, v)?))
            }
            PlanNode::DropDatabase(v) => Ok(Arc::new(DropDatabaseInterpreter::try_create(ctx, v)?)),
            PlanNode::ShowCreateDatabase(v) => {
                Ok(Arc::new(ShowCreateDatabaseInterpreter::try_create(ctx, v)?))
            }
            PlanNode::RenameDatabase(v) => {
                Ok(Arc::new(RenameDatabaseInterpreter::try_create(ctx, v)?))
            }
            PlanNode::UndropDatabase(v) => {
                Ok(Arc::new(UndropDatabaseInterpreter::try_create(ctx, v)?))
            }

            // Table related transforms
            PlanNode::CreateTable(v) => Ok(Arc::new(CreateTableInterpreter::try_create(ctx, v)?)),
            PlanNode::DropTable(v) => Ok(Arc::new(DropTableInterpreter::try_create(ctx, v)?)),
            PlanNode::UndropTable(v) => Ok(Arc::new(UndropTableInterpreter::try_create(ctx, v)?)),
            PlanNode::RenameTable(v) => Ok(Arc::new(RenameTableInterpreter::try_create(ctx, v)?)),
            PlanNode::TruncateTable(v) => {
                Ok(Arc::new(TruncateTableInterpreter::try_create(ctx, v)?))
            }
            PlanNode::OptimizeTable(v) => {
                Ok(Arc::new(OptimizeTableInterpreter::try_create(ctx, v)?))
            }
            PlanNode::ExistsTable(v) => Ok(Arc::new(ExistsTableInterpreter::try_create(ctx, v)?)),
            PlanNode::DescribeTable(v) => {
                Ok(Arc::new(DescribeTableInterpreter::try_create(ctx, v)?))
            }
            PlanNode::ShowCreateTable(v) => {
                Ok(Arc::new(ShowCreateTableInterpreter::try_create(ctx, v)?))
            }

            // View related transforms
            PlanNode::CreateView(v) => Ok(Arc::new(CreateViewInterpreter::try_create(ctx, v)?)),
            PlanNode::AlterView(v) => Ok(Arc::new(AlterViewInterpreter::try_create(ctx, v)?)),
            PlanNode::DropView(v) => Ok(Arc::new(DropViewInterpreter::try_create(ctx, v)?)),

            // User related transforms
            PlanNode::CreateUser(v) => Ok(Arc::new(CreateUserInterpreter::try_create(ctx, v)?)),
            PlanNode::AlterUser(v) => Ok(Arc::new(AlterUserInterpreter::try_create(ctx, v)?)),
            PlanNode::DropUser(v) => Ok(Arc::new(DropUserInterpreter::try_create(ctx, v)?)),

            // Privilege related transforms
            PlanNode::GrantPrivilege(v) => {
                Ok(Arc::new(GrantPrivilegeInterpreter::try_create(ctx, v)?))
            }
            PlanNode::GrantRole(v) => Ok(Arc::new(GrantRoleInterpreter::try_create(ctx, v)?)),

            PlanNode::RevokePrivilege(v) => {
                Ok(Arc::new(RevokePrivilegeInterpreter::try_create(ctx, v)?))
            }
            PlanNode::RevokeRole(v) => Ok(Arc::new(RevokeRoleInterpreter::try_create(ctx, v)?)),

            PlanNode::CreateRole(v) => Ok(Arc::new(CreateRoleInterpreter::try_create(ctx, v)?)),
            PlanNode::DropRole(v) => Ok(Arc::new(DropRoleInterpreter::try_create(ctx, v)?)),

            // UDF related transforms
            PlanNode::CreateUserUDF(v) => {
                Ok(Arc::new(CreateUserUDFInterpreter::try_create(ctx, v)?))
            }
            PlanNode::DropUserUDF(v) => Ok(Arc::new(DropUserUDFInterpreter::try_create(ctx, v)?)),
            PlanNode::AlterUserUDF(v) => Ok(Arc::new(AlterUserUDFInterpreter::try_create(ctx, v)?)),

            // Stage related transforms
            PlanNode::CreateUserStage(v) => {
                Ok(Arc::new(CreateUserStageInterpreter::try_create(ctx, v)?))
            }
            PlanNode::DropUserStage(v) => {
                Ok(Arc::new(DropUserStageInterpreter::try_create(ctx, v)?))
            }
            PlanNode::DescribeUserStage(v) => {
                Ok(Arc::new(DescribeUserStageInterpreter::try_create(ctx, v)?))
            }
            PlanNode::RemoveUserStage(v) => {
                Ok(Arc::new(RemoveUserStageInterpreter::try_create(ctx, v)?))
            }

            // cluster key.
            PlanNode::AlterTableClusterKey(v) => Ok(Arc::new(
                AlterTableClusterKeyInterpreter::try_create(ctx, v)?,
            )),
            PlanNode::DropTableClusterKey(v) => Ok(Arc::new(
                DropTableClusterKeyInterpreter::try_create(ctx, v)?,
            )),

            // others
            PlanNode::List(v) => Ok(Arc::new(ListInterpreter::try_create(ctx, v)?)),
            PlanNode::UseDatabase(v) => Ok(Arc::new(UseDatabaseInterpreter::try_create(ctx, v)?)),
            PlanNode::Kill(v) => Ok(Arc::new(KillInterpreter::try_create(ctx, v)?)),
            PlanNode::SetVariable(v) => Ok(Arc::new(SettingInterpreter::try_create(ctx, v)?)),
            PlanNode::Empty(v) => Ok(Arc::new(EmptyInterpreter::try_create(ctx, v)?)),

            _ => Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }
    }
}
