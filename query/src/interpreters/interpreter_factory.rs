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

use super::DescribeUserStageInterpreter;
use crate::interpreters::interpreter_show_engines::ShowEnginesInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::AlterUserUDFInterpreter;
use crate::interpreters::CopyInterpreter;
use crate::interpreters::CreateDatabaseInterpreter;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::CreateUserInterpreter;
use crate::interpreters::CreateUserStageInterpreter;
use crate::interpreters::CreateUserUDFInterpreter;
use crate::interpreters::DescribeTableInterpreter;
use crate::interpreters::DropDatabaseInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::interpreters::DropUserStageInterpreter;
use crate::interpreters::DropUserUDFInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::GrantPrivilegeInterpreter;
use crate::interpreters::InsertInterpreter;
use crate::interpreters::InterceptorInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::interpreters::OptimizeTableInterpreter;
use crate::interpreters::RevokePrivilegeInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::SettingInterpreter;
use crate::interpreters::ShowCreateDatabaseInterpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::interpreters::ShowDatabasesInterpreter;
use crate::interpreters::ShowFunctionsInterpreter;
use crate::interpreters::ShowGrantsInterpreter;
use crate::interpreters::ShowMetricsInterpreter;
use crate::interpreters::ShowProcessListInterpreter;
use crate::interpreters::ShowSettingsInterpreter;
use crate::interpreters::ShowTablesInterpreter;
use crate::interpreters::ShowUsersInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::interpreters::UseDatabaseInterpreter;
use crate::interpreters::UseTenantInterpreter;
use crate::sessions::QueryContext;

pub struct InterpreterFactory;

impl InterpreterFactory {
    pub fn get(ctx: Arc<QueryContext>, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        let ctx_clone = ctx.clone();
        let inner = match plan.clone() {
            // Query.
            PlanNode::Select(v) => SelectInterpreter::try_create(ctx_clone, v),

            // Select.
            PlanNode::Explain(v) => ExplainInterpreter::try_create(ctx_clone, v),

            // Insert.
            PlanNode::Insert(v) => InsertInterpreter::try_create(ctx_clone, v),

            // Copy.
            PlanNode::Copy(v) => CopyInterpreter::try_create(ctx_clone, v),

            // Show.
            PlanNode::Show(ShowPlan::ShowDatabases(v)) => {
                ShowDatabasesInterpreter::try_create(ctx_clone, v)
            }
            PlanNode::Show(ShowPlan::ShowTables(v)) => {
                ShowTablesInterpreter::try_create(ctx_clone, v)
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

            // Database.
            PlanNode::CreateDatabase(v) => CreateDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::DropDatabase(v) => DropDatabaseInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateDatabase(v) => {
                ShowCreateDatabaseInterpreter::try_create(ctx_clone, v)
            }

            // Table.
            PlanNode::CreateTable(v) => CreateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DropTable(v) => DropTableInterpreter::try_create(ctx_clone, v),
            PlanNode::TruncateTable(v) => TruncateTableInterpreter::try_create(ctx_clone, v),
            PlanNode::OptimizeTable(v) => OptimizeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeTable(v) => DescribeTableInterpreter::try_create(ctx_clone, v),
            PlanNode::ShowCreateTable(v) => ShowCreateTableInterpreter::try_create(ctx_clone, v),

            // User.
            PlanNode::CreateUser(v) => CreateUserInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterUser(v) => AlterUserInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUser(v) => DropUserInterpreter::try_create(ctx_clone, v),
            PlanNode::GrantPrivilege(v) => GrantPrivilegeInterpreter::try_create(ctx_clone, v),
            PlanNode::RevokePrivilege(v) => RevokePrivilegeInterpreter::try_create(ctx_clone, v),

            // Stage.
            PlanNode::CreateUserStage(v) => CreateUserStageInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUserStage(v) => DropUserStageInterpreter::try_create(ctx_clone, v),
            PlanNode::DescribeUserStage(v) => {
                DescribeUserStageInterpreter::try_create(ctx_clone, v)
            }

            // UDF.
            PlanNode::CreateUserUDF(v) => CreateUserUDFInterpreter::try_create(ctx_clone, v),
            PlanNode::DropUserUDF(v) => DropUserUDFInterpreter::try_create(ctx_clone, v),
            PlanNode::AlterUserUDF(v) => AlterUserUDFInterpreter::try_create(ctx_clone, v),

            // Use.
            PlanNode::UseDatabase(v) => UseDatabaseInterpreter::try_create(ctx_clone, v),

            // Kill.
            PlanNode::Kill(v) => KillInterpreter::try_create(ctx_clone, v),

            // Set.
            PlanNode::SetVariable(v) => SettingInterpreter::try_create(ctx_clone, v),

            // Admin.
            PlanNode::AdminUseTenant(v) => UseTenantInterpreter::try_create(ctx_clone, v),

            _ => Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan:{}",
                plan.name()
            ))),
        }?;
        Ok(Arc::new(InterceptorInterpreter::create(ctx, inner, plan)))
    }
}
