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

use common_exception::Result;
use common_planners::EmptyPlan;
use common_planners::PlanNode;

use super::*;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;
use crate::sql::DfStatement;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactoryV2;

/// InterpreterFactoryV2 provides `get` method which transforms `Plan` into the corresponding interpreter.
/// Such as: Plan::Query -> InterpreterSelectV2
impl InterpreterFactoryV2 {
    /// Check if statement is supported by InterpreterFactoryV2
    pub fn check(stmt: &DfStatement) -> bool {
        matches!(
            stmt,
            DfStatement::Query(_)
                | DfStatement::Explain(_)
                | DfStatement::CreateStage(_)
                | DfStatement::ShowStages(_)
                | DfStatement::DescribeStage(_)
                | DfStatement::List(_)
                | DfStatement::DropStage(_)
                | DfStatement::RemoveStage(_)
                | DfStatement::ShowDatabases(_)
                | DfStatement::ShowCreateDatabase(_)
                | DfStatement::ShowTables(_)
                | DfStatement::ShowCreateTable(_)
                | DfStatement::DescribeTable(_)
                | DfStatement::ShowTablesStatus(_)
                | DfStatement::CreateTable(_)
                | DfStatement::CreateView(_)
                | DfStatement::AlterView(_)
                | DfStatement::DropTable(_)
                | DfStatement::UndropTable(_)
                | DfStatement::AlterTable(_)
                | DfStatement::RenameTable(_)
                | DfStatement::TruncateTable(_)
                | DfStatement::OptimizeTable(_)
                | DfStatement::DropView(_)
                | DfStatement::ShowFunctions(_)
                | DfStatement::ShowMetrics(_)
                | DfStatement::ShowProcessList(_)
                | DfStatement::ShowSettings(_)
                | DfStatement::CreateDatabase(_)
                | DfStatement::DropDatabase(_)
                | DfStatement::InsertQuery(_)
                | DfStatement::ShowUsers(_)
                | DfStatement::CreateUser(_)
                | DfStatement::ShowRoles(_)
                | DfStatement::AlterDatabase(_)
                | DfStatement::DropUser(_)
                | DfStatement::AlterUser(_)
                | DfStatement::CreateRole(_)
                | DfStatement::DropRole(_)
                | DfStatement::GrantPrivilege(_)
                | DfStatement::GrantRole(_)
                | DfStatement::ShowGrants(_)
                | DfStatement::RevokeRole(_)
                | DfStatement::RevokePrivilege(_)
        )
    }

    pub fn get(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        let inner = match plan {
            Plan::Query {
                s_expr,
                bind_context,
                metadata,
            } => SelectInterpreterV2::try_create(
                ctx.clone(),
                *bind_context.clone(),
                s_expr.clone(),
                metadata.clone(),
            ),
            Plan::Explain { kind, plan } => {
                ExplainInterpreterV2::try_create(ctx.clone(), *plan.clone(), kind.clone())
            }

            // Shows
            Plan::ShowMetrics => ShowMetricsInterpreter::try_create(ctx.clone()),
            Plan::ShowProcessList => ShowProcessListInterpreter::try_create(ctx.clone()),
            Plan::ShowSettings => ShowSettingsInterpreter::try_create(ctx.clone()),

            // Databases
            Plan::ShowDatabases(show_databases) => {
                ShowDatabasesInterpreter::try_create(ctx.clone(), *show_databases.clone())
            }
            Plan::ShowCreateDatabase(show_create_database) => {
                ShowCreateDatabaseInterpreter::try_create(
                    ctx.clone(),
                    *show_create_database.clone(),
                )
            }
            Plan::CreateDatabase(create_database) => {
                CreateDatabaseInterpreter::try_create(ctx.clone(), *create_database.clone())
            }
            Plan::DropDatabase(drop_database) => {
                DropDatabaseInterpreter::try_create(ctx.clone(), *drop_database.clone())
            }
            Plan::RenameDatabase(rename_database) => {
                RenameDatabaseInterpreter::try_create(ctx.clone(), *rename_database.clone())
            }

            // Tables
            Plan::ShowTables(show_tables) => {
                ShowTablesInterpreter::try_create(ctx.clone(), *show_tables.clone())
            }
            Plan::ShowCreateTable(show_create_table) => {
                ShowCreateTableInterpreter::try_create(ctx.clone(), *show_create_table.clone())
            }
            Plan::DescribeTable(describe_table) => {
                DescribeTableInterpreter::try_create(ctx.clone(), *describe_table.clone())
            }
            Plan::ShowTablesStatus(show_tables_status) => {
                ShowTablesStatusInterpreter::try_create(ctx.clone(), *show_tables_status.clone())
            }
            Plan::CreateTable(create_table) => {
                CreateTableInterpreter::try_create(ctx.clone(), *create_table.clone())
            }
            Plan::DropTable(drop_table) => {
                DropTableInterpreter::try_create(ctx.clone(), *drop_table.clone())
            }
            Plan::UndropTable(undrop_table) => {
                UndropTableInterpreter::try_create(ctx.clone(), *undrop_table.clone())
            }
            Plan::RenameTable(rename_table) => {
                RenameTableInterpreter::try_create(ctx.clone(), *rename_table.clone())
            }
            Plan::AlterTableClusterKey(alter_table_cluster_key) => {
                AlterTableClusterKeyInterpreter::try_create(
                    ctx.clone(),
                    *alter_table_cluster_key.clone(),
                )
            }
            Plan::DropTableClusterKey(drop_table_cluster_key) => {
                DropTableClusterKeyInterpreter::try_create(
                    ctx.clone(),
                    *drop_table_cluster_key.clone(),
                )
            }
            Plan::TruncateTable(truncate_table) => {
                TruncateTableInterpreter::try_create(ctx.clone(), *truncate_table.clone())
            }
            Plan::OptimizeTable(optimize_table) => {
                OptimizeTableInterpreter::try_create(ctx.clone(), *optimize_table.clone())
            }

            // Views
            Plan::CreateView(create_view) => {
                CreateViewInterpreter::try_create(ctx.clone(), *create_view.clone())
            }
            Plan::AlterView(alter_view) => {
                AlterViewInterpreter::try_create(ctx.clone(), *alter_view.clone())
            }
            Plan::DropView(drop_view) => {
                DropViewInterpreter::try_create(ctx.clone(), *drop_view.clone())
            }

            // Users
            Plan::ShowUsers => ShowUsersInterpreter::try_create(ctx.clone()),
            Plan::CreateUser(create_user) => {
                CreateUserInterpreter::try_create(ctx.clone(), *create_user.clone())
            }
            Plan::DropUser(drop_user) => {
                DropUserInterpreter::try_create(ctx.clone(), *drop_user.clone())
            }
            Plan::AlterUser(alter_user) => {
                AlterUserInterpreter::try_create(ctx.clone(), *alter_user.clone())
            }

            Plan::Insert(insert) => {
                InsertInterpreterV2::try_create(ctx.clone(), *insert.clone(), false)
            }

            // Roles
            Plan::ShowRoles => ShowRolesInterpreter::try_create(ctx.clone()),
            Plan::CreateRole(create_role) => {
                CreateRoleInterpreter::try_create(ctx.clone(), *create_role.clone())
            }
            Plan::DropRole(drop_role) => {
                DropRoleInterpreter::try_create(ctx.clone(), *drop_role.clone())
            }

            // Stages
            Plan::ShowStages => ShowStagesInterpreter::try_create(ctx.clone()),
            Plan::ListStage(s) => ListInterpreter::try_create(ctx.clone(), *s.clone()),
            Plan::DescribeStage(s) => {
                DescribeUserStageInterpreter::try_create(ctx.clone(), *s.clone())
            }
            Plan::CreateStage(create_stage) => {
                CreateUserStageInterpreter::try_create(ctx.clone(), *create_stage.clone())
            }
            Plan::DropStage(s) => DropUserStageInterpreter::try_create(ctx.clone(), *s.clone()),
            Plan::RemoveStage(s) => RemoveUserStageInterpreter::try_create(ctx.clone(), *s.clone()),

            // Grant
            Plan::GrantPriv(grant_priv) => {
                GrantPrivilegeInterpreter::try_create(ctx.clone(), *grant_priv.clone())
            }
            Plan::GrantRole(grant_role) => {
                GrantRoleInterpreter::try_create(ctx.clone(), *grant_role.clone())
            }
            Plan::ShowGrants(show_grants) => {
                ShowGrantsInterpreter::try_create(ctx.clone(), *show_grants.clone())
            }
            Plan::RevokePriv(revoke_priv) => {
                RevokePrivilegeInterpreter::try_create(ctx.clone(), *revoke_priv.clone())
            }
            Plan::RevokeRole(revoke_role) => {
                RevokeRoleInterpreter::try_create(ctx.clone(), *revoke_role.clone())
            }
        }?;

        Ok(Arc::new(InterceptorInterpreter::create(
            ctx,
            inner,
            PlanNode::Empty(EmptyPlan::create()),
            plan.to_string(),
        )))
    }
}
