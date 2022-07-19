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

use super::interpreter_user_stage_describe::DescribeUserStageInterpreter;
use super::interpreter_user_stage_drop::DropUserStageInterpreter;
use super::*;
use crate::interpreters::interpreter_copy_v2::CopyInterpreterV2;
use crate::interpreters::interpreter_presign::PresignInterpreter;
use crate::interpreters::interpreter_table_create_v2::CreateTableInterpreterV2;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::DropUserInterpreter;
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
                | DfStatement::Copy(_)
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
                | DfStatement::ExistsTable(_)
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
                | DfStatement::CreateUDF(_)
                | DfStatement::DropUser(_)
                | DfStatement::AlterUser(_)
                | DfStatement::CreateRole(_)
                | DfStatement::DropRole(_)
                | DfStatement::GrantPrivilege(_)
                | DfStatement::GrantRole(_)
                | DfStatement::ShowGrants(_)
                | DfStatement::RevokeRole(_)
                | DfStatement::RevokePrivilege(_)
                | DfStatement::Call(_)
                | DfStatement::SetVariable(_)
        )
    }

    pub fn enable_default(stmt: &DfStatement) -> bool {
        matches!(
            stmt,
            // DfStatement::Query(_)
            DfStatement::Copy(_)
            //     | DfStatement::Explain(_)
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
                | DfStatement::ExistsTable(_)
                | DfStatement::DropView(_)
                | DfStatement::ShowFunctions(_)
                | DfStatement::ShowMetrics(_)
                | DfStatement::ShowProcessList(_)
                | DfStatement::ShowSettings(_)
                | DfStatement::CreateDatabase(_)
                | DfStatement::DropDatabase(_) /* | DfStatement::InsertQuery(_)
                                                * | DfStatement::ShowUsers(_)
                                                * | DfStatement::CreateUser(_)
                                                * | DfStatement::ShowRoles(_)
                                                * | DfStatement::AlterDatabase(_)
                                                * | DfStatement::CreateUDF(_)
                                                * | DfStatement::DropUser(_)
                                                * | DfStatement::AlterUser(_)
                                                * | DfStatement::CreateRole(_)
                                                * | DfStatement::DropRole(_)
                                                * | DfStatement::GrantPrivilege(_)
                                                * | DfStatement::GrantRole(_)
                                                * | DfStatement::ShowGrants(_)
                                                * | DfStatement::RevokeRole(_)
                                                * | DfStatement::RevokePrivilege(_)
                                                * | DfStatement::Call(_)
                                                * | DfStatement::SetVariable(_) */
        )
    }

    pub fn get(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        let inner = InterpreterFactoryV2::create_interpreter(ctx.clone(), plan)?;

        Ok(Arc::new(InterceptorInterpreter::create(
            ctx,
            inner,
            PlanNode::Empty(EmptyPlan::create()),
            plan.to_string(),
        )))
    }

    fn create_interpreter(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        match plan {
            Plan::Query {
                s_expr,
                bind_context,
                metadata,
            } => Ok(Arc::new(SelectInterpreterV2::try_create(
                ctx,
                *bind_context.clone(),
                s_expr.clone(),
                metadata.clone(),
            )?)),
            Plan::Explain { kind, plan } => Ok(Arc::new(ExplainInterpreterV2::try_create(
                ctx,
                *plan.clone(),
                kind.clone(),
            )?)),

            Plan::Call(plan) => Ok(Arc::new(CallInterpreter::try_create(ctx, *plan.clone())?)),

            Plan::Copy(copy_plan) => Ok(Arc::new(CopyInterpreterV2::try_create(
                ctx,
                *copy_plan.clone(),
            )?)),

            // Shows
            Plan::ShowMetrics => Ok(Arc::new(ShowMetricsInterpreter::try_create(ctx)?)),
            Plan::ShowProcessList => Ok(Arc::new(ShowProcessListInterpreter::try_create(ctx)?)),
            Plan::ShowSettings => Ok(Arc::new(ShowSettingsInterpreter::try_create(ctx)?)),

            // Databases
         
            Plan::ShowCreateDatabase(show_create_database) => Ok(Arc::new(
                ShowCreateDatabaseInterpreter::try_create(ctx, *show_create_database.clone())?,
            )),
            Plan::CreateDatabase(create_database) => Ok(Arc::new(
                CreateDatabaseInterpreter::try_create(ctx, *create_database.clone())?,
            )),
            Plan::DropDatabase(drop_database) => Ok(Arc::new(DropDatabaseInterpreter::try_create(
                ctx,
                *drop_database.clone(),
            )?)),
            Plan::RenameDatabase(rename_database) => Ok(Arc::new(
                RenameDatabaseInterpreter::try_create(ctx, *rename_database.clone())?,
            )),

            // Tables
            Plan::ShowCreateTable(show_create_table) => Ok(Arc::new(
                ShowCreateTableInterpreter::try_create(ctx, *show_create_table.clone())?,
            )),
            Plan::DescribeTable(describe_table) => Ok(Arc::new(
                DescribeTableInterpreter::try_create(ctx, *describe_table.clone())?,
            )),
            Plan::CreateTable(create_table) => Ok(Arc::new(CreateTableInterpreterV2::try_create(
                ctx,
                *create_table.clone(),
            )?)),
            Plan::DropTable(drop_table) => Ok(Arc::new(DropTableInterpreter::try_create(
                ctx,
                *drop_table.clone(),
            )?)),
            Plan::UndropTable(undrop_table) => Ok(Arc::new(UndropTableInterpreter::try_create(
                ctx,
                *undrop_table.clone(),
            )?)),
            Plan::RenameTable(rename_table) => Ok(Arc::new(RenameTableInterpreter::try_create(
                ctx,
                *rename_table.clone(),
            )?)),
            Plan::AlterTableClusterKey(alter_table_cluster_key) => Ok(Arc::new(
                AlterTableClusterKeyInterpreter::try_create(ctx, *alter_table_cluster_key.clone())?,
            )),
            Plan::DropTableClusterKey(drop_table_cluster_key) => Ok(Arc::new(
                DropTableClusterKeyInterpreter::try_create(ctx, *drop_table_cluster_key.clone())?,
            )),
            Plan::TruncateTable(truncate_table) => Ok(Arc::new(
                TruncateTableInterpreter::try_create(ctx, *truncate_table.clone())?,
            )),
            Plan::OptimizeTable(optimize_table) => Ok(Arc::new(
                OptimizeTableInterpreter::try_create(ctx, *optimize_table.clone())?,
            )),
            Plan::ExistsTable(exists_table) => Ok(Arc::new(ExistsTableInterpreter::try_create(
                ctx,
                *exists_table.clone(),
            )?)),

            // Views
            Plan::CreateView(create_view) => Ok(Arc::new(CreateViewInterpreter::try_create(
                ctx,
                *create_view.clone(),
            )?)),
            Plan::AlterView(alter_view) => Ok(Arc::new(AlterViewInterpreter::try_create(
                ctx,
                *alter_view.clone(),
            )?)),
            Plan::DropView(drop_view) => Ok(Arc::new(DropViewInterpreter::try_create(
                ctx,
                *drop_view.clone(),
            )?)),

            // Users
            Plan::ShowUsers => Ok(Arc::new(ShowUsersInterpreter::try_create(ctx)?)),
            Plan::CreateUser(create_user) => Ok(Arc::new(CreateUserInterpreter::try_create(
                ctx,
                *create_user.clone(),
            )?)),
            Plan::DropUser(drop_user) => Ok(Arc::new(DropUserInterpreter::try_create(
                ctx,
                *drop_user.clone(),
            )?)),
            Plan::AlterUser(alter_user) => Ok(Arc::new(AlterUserInterpreter::try_create(
                ctx,
                *alter_user.clone(),
            )?)),

            Plan::Insert(insert) => InsertInterpreterV2::try_create(ctx, *insert.clone(), false),

            Plan::Delete(delete) => Ok(Arc::new(DeleteInterpreter::try_create(
                ctx,
                *delete.clone(),
            )?)),

            // Roles
            Plan::ShowRoles => Ok(Arc::new(ShowRolesInterpreter::try_create(ctx)?)),
            Plan::CreateRole(create_role) => Ok(Arc::new(CreateRoleInterpreter::try_create(
                ctx,
                *create_role.clone(),
            )?)),
            Plan::DropRole(drop_role) => Ok(Arc::new(DropRoleInterpreter::try_create(
                ctx,
                *drop_role.clone(),
            )?)),

            // Stages
            Plan::ShowStages => Ok(Arc::new(ShowStagesInterpreter::try_create(ctx)?)),
            Plan::ListStage(s) => Ok(Arc::new(ListInterpreter::try_create(ctx, *s.clone())?)),
            Plan::DescribeStage(s) => Ok(Arc::new(DescribeUserStageInterpreter::try_create(
                ctx,
                *s.clone(),
            )?)),
            Plan::CreateStage(create_stage) => Ok(Arc::new(
                CreateUserStageInterpreter::try_create(ctx, *create_stage.clone())?,
            )),
            Plan::DropStage(s) => Ok(Arc::new(DropUserStageInterpreter::try_create(
                ctx,
                *s.clone(),
            )?)),
            Plan::RemoveStage(s) => Ok(Arc::new(RemoveUserStageInterpreter::try_create(
                ctx,
                *s.clone(),
            )?)),

            // Grant
            Plan::GrantPriv(grant_priv) => Ok(Arc::new(GrantPrivilegeInterpreter::try_create(
                ctx,
                *grant_priv.clone(),
            )?)),
            Plan::GrantRole(grant_role) => Ok(Arc::new(GrantRoleInterpreter::try_create(
                ctx,
                *grant_role.clone(),
            )?)),
            Plan::ShowGrants(show_grants) => Ok(Arc::new(ShowGrantsInterpreter::try_create(
                ctx,
                *show_grants.clone(),
            )?)),
            Plan::RevokePriv(revoke_priv) => Ok(Arc::new(RevokePrivilegeInterpreter::try_create(
                ctx,
                *revoke_priv.clone(),
            )?)),
            Plan::RevokeRole(revoke_role) => Ok(Arc::new(RevokeRoleInterpreter::try_create(
                ctx,
                *revoke_role.clone(),
            )?)),
            Plan::CreateUDF(create_user_udf) => Ok(Arc::new(CreateUserUDFInterpreter::try_create(
                ctx,
                *create_user_udf.clone(),
            )?)),
            Plan::AlterUDF(alter_udf) => Ok(Arc::new(AlterUserUDFInterpreter::try_create(
                ctx,
                *alter_udf.clone(),
            )?)),
            Plan::DropUDF(drop_udf) => Ok(Arc::new(DropUserUDFInterpreter::try_create(
                ctx,
                *drop_udf.clone(),
            )?)),

            Plan::Presign(presign) => Ok(Arc::new(PresignInterpreter::try_create(
                ctx,
                *presign.clone(),
            )?)),

            Plan::SetVariable(set_variable) => Ok(Arc::new(SettingInterpreter::try_create(
                ctx,
                *set_variable.clone(),
            )?)),
        }
    }
}
