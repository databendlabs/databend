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

use super::interpreter_share_desc::DescShareInterpreter;
use super::interpreter_user_stage_drop::DropUserStageInterpreter;
use super::*;
use crate::interpreters::interpreter_copy_v2::CopyInterpreterV2;
use crate::interpreters::interpreter_presign::PresignInterpreter;
use crate::interpreters::interpreter_table_create_v2::CreateTableInterpreterV2;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::CreateShareInterpreter;
use crate::interpreters::DropShareInterpreter;
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
        matches!(stmt, DfStatement::SeeYouAgain)
    }

    pub fn get(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        let inner = InterpreterFactoryV2::create_interpreter(ctx.clone(), plan)?;

        Ok(Arc::new(InterceptorInterpreter::create(
            ctx,
            inner,
            PlanNode::Empty(EmptyPlan::create()),
            Some(plan.clone()),
            plan.to_string(),
        )))
    }

    fn create_interpreter(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        match plan {
            Plan::Query {
                s_expr,
                bind_context,
                metadata,
                ..
            } => Ok(Arc::new(SelectInterpreterV2::try_create(
                ctx,
                *bind_context.clone(),
                *s_expr.clone(),
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

            Plan::UndropDatabase(undrop_database) => Ok(Arc::new(
                UndropDatabaseInterpreter::try_create(ctx, *undrop_database.clone())?,
            )),

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
            Plan::CreateRole(create_role) => Ok(Arc::new(CreateRoleInterpreter::try_create(
                ctx,
                *create_role.clone(),
            )?)),
            Plan::DropRole(drop_role) => Ok(Arc::new(DropRoleInterpreter::try_create(
                ctx,
                *drop_role.clone(),
            )?)),

            // Stages
            Plan::ListStage(s) => Ok(Arc::new(ListInterpreter::try_create(ctx, *s.clone())?)),
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
            Plan::UseDatabase(p) => Ok(Arc::new(UseDatabaseInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::Kill(p) => Ok(Arc::new(KillInterpreter::try_create(ctx, *p.clone())?)),

            // share plans
            Plan::CreateShare(p) => Ok(Arc::new(CreateShareInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DropShare(p) => Ok(Arc::new(DropShareInterpreter::try_create(ctx, *p.clone())?)),
            Plan::GrantShareObject(p) => Ok(Arc::new(GrantShareObjectInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::RevokeShareObject(p) => Ok(Arc::new(RevokeShareObjectInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::AlterShareTenants(p) => Ok(Arc::new(AlterShareTenantsInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DescShare(p) => Ok(Arc::new(DescShareInterpreter::try_create(ctx, *p.clone())?)),
            Plan::ShowShares(p) => Ok(Arc::new(ShowSharesInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::ShowObjectGrantPrivileges(p) => Ok(Arc::new(
                ShowObjectGrantPrivilegesInterpreter::try_create(ctx, *p.clone())?,
            )),
            Plan::ShowGrantTenantsOfShare(p) => Ok(Arc::new(
                ShowGrantTenantsOfShareInterpreter::try_create(ctx, *p.clone())?,
            )),
        }
    }
}
