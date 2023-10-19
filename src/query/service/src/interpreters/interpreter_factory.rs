// Copyright 2021 Datafuse Labs
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

use common_ast::ast::ExplainKind;
use common_exception::Result;
use log::error;

use super::interpreter_catalog_create::CreateCatalogInterpreter;
use super::interpreter_catalog_show_create::ShowCreateCatalogInterpreter;
use super::interpreter_index_create::CreateIndexInterpreter;
use super::interpreter_index_drop::DropIndexInterpreter;
use super::interpreter_merge_into::MergeIntoInterpreter;
use super::interpreter_share_desc::DescShareInterpreter;
use super::interpreter_table_set_options::SetOptionsInterpreter;
use super::interpreter_user_stage_drop::DropUserStageInterpreter;
use super::*;
use crate::interpreters::access::Accessor;
use crate::interpreters::interpreter_catalog_drop::DropCatalogInterpreter;
use crate::interpreters::interpreter_copy_into_location::CopyIntoLocationInterpreter;
use crate::interpreters::interpreter_copy_into_table::CopyIntoTableInterpreter;
use crate::interpreters::interpreter_file_format_create::CreateFileFormatInterpreter;
use crate::interpreters::interpreter_file_format_drop::DropFileFormatInterpreter;
use crate::interpreters::interpreter_file_format_show::ShowFileFormatsInterpreter;
use crate::interpreters::interpreter_presign::PresignInterpreter;
use crate::interpreters::interpreter_role_show::ShowRolesInterpreter;
use crate::interpreters::interpreter_table_create::CreateTableInterpreter;
use crate::interpreters::interpreter_table_revert::RevertTableInterpreter;
use crate::interpreters::AlterUserInterpreter;
use crate::interpreters::CreateShareEndpointInterpreter;
use crate::interpreters::CreateShareInterpreter;
use crate::interpreters::DropShareInterpreter;
use crate::interpreters::DropUserInterpreter;
use crate::interpreters::SetRoleInterpreter;
use crate::interpreters::UpdateInterpreter;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactory;

/// InterpreterFactory provides `get` method which transforms `Plan` into the corresponding interpreter.
/// Such as: Plan::Query -> InterpreterSelectV2
impl InterpreterFactory {
    #[async_backtrace::framed]
    pub async fn get(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        // Check the access permission.
        let access_checker = Accessor::create(ctx.clone());
        access_checker.check(plan).await.map_err(|e| {
            error!("Access.denied(v2): {:?}", e);
            e
        })?;
        Self::get_inner(ctx, plan)
    }

    pub fn get_inner(ctx: Arc<QueryContext>, plan: &Plan) -> Result<InterpreterPtr> {
        match plan {
            Plan::Query {
                s_expr,
                bind_context,
                metadata,
                ignore_result,
                formatted_ast,
                ..
            } => Ok(Arc::new(SelectInterpreter::try_create(
                ctx,
                *bind_context.clone(),
                *s_expr.clone(),
                metadata.clone(),
                formatted_ast.clone(),
                *ignore_result,
            )?)),
            Plan::Explain { kind, plan } => Ok(Arc::new(ExplainInterpreter::try_create(
                ctx,
                *plan.clone(),
                kind.clone(),
            )?)),
            Plan::ExplainAst { formatted_string } => Ok(Arc::new(ExplainInterpreter::try_create(
                ctx,
                plan.clone(),
                ExplainKind::Ast(formatted_string.clone()),
            )?)),
            Plan::ExplainSyntax { formatted_sql } => Ok(Arc::new(ExplainInterpreter::try_create(
                ctx,
                plan.clone(),
                ExplainKind::Syntax(formatted_sql.clone()),
            )?)),
            Plan::ExplainAnalyze { plan } => Ok(Arc::new(ExplainInterpreter::try_create(
                ctx,
                *plan.clone(),
                ExplainKind::AnalyzePlan,
            )?)),

            Plan::CopyIntoTable(copy_plan) => Ok(Arc::new(CopyIntoTableInterpreter::try_create(
                ctx,
                *copy_plan.clone(),
            )?)),
            Plan::CopyIntoLocation(copy_plan) => Ok(Arc::new(
                CopyIntoLocationInterpreter::try_create(ctx, copy_plan.clone())?,
            )),
            // catalogs
            Plan::ShowCreateCatalog(plan) => Ok(Arc::new(
                ShowCreateCatalogInterpreter::try_create(ctx, *plan.clone())?,
            )),
            Plan::CreateCatalog(plan) => Ok(Arc::new(CreateCatalogInterpreter::try_create(
                ctx,
                *plan.clone(),
            )?)),
            Plan::DropCatalog(plan) => {
                Ok(Arc::new(DropCatalogInterpreter::create(ctx, *plan.clone())))
            }

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
            Plan::CreateTable(create_table) => Ok(Arc::new(CreateTableInterpreter::try_create(
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
            Plan::SetOptions(set_options) => Ok(Arc::new(SetOptionsInterpreter::try_create(
                ctx,
                *set_options.clone(),
            )?)),
            Plan::RenameTableColumn(rename_table_column) => Ok(Arc::new(
                RenameTableColumnInterpreter::try_create(ctx, *rename_table_column.clone())?,
            )),
            Plan::AddTableColumn(add_table_column) => Ok(Arc::new(
                AddTableColumnInterpreter::try_create(ctx, *add_table_column.clone())?,
            )),
            Plan::ModifyTableColumn(modify_table_column) => Ok(Arc::new(
                ModifyTableColumnInterpreter::try_create(ctx, *modify_table_column.clone())?,
            )),
            Plan::DropTableColumn(drop_table_column) => Ok(Arc::new(
                DropTableColumnInterpreter::try_create(ctx, *drop_table_column.clone())?,
            )),
            Plan::AlterTableClusterKey(alter_table_cluster_key) => Ok(Arc::new(
                AlterTableClusterKeyInterpreter::try_create(ctx, *alter_table_cluster_key.clone())?,
            )),
            Plan::DropTableClusterKey(drop_table_cluster_key) => Ok(Arc::new(
                DropTableClusterKeyInterpreter::try_create(ctx, *drop_table_cluster_key.clone())?,
            )),
            Plan::ReclusterTable(recluster_table) => Ok(Arc::new(
                ReclusterTableInterpreter::try_create(ctx, *recluster_table.clone())?,
            )),
            Plan::TruncateTable(truncate_table) => Ok(Arc::new(
                TruncateTableInterpreter::try_create(ctx, *truncate_table.clone())?,
            )),
            Plan::OptimizeTable(optimize_table) => Ok(Arc::new(
                OptimizeTableInterpreter::try_create(ctx, *optimize_table.clone())?,
            )),
            Plan::VacuumTable(vacuum_table) => Ok(Arc::new(VacuumTableInterpreter::try_create(
                ctx,
                *vacuum_table.clone(),
            )?)),
            Plan::VacuumDropTable(vacuum_drop_table) => Ok(Arc::new(
                VacuumDropTablesInterpreter::try_create(ctx, *vacuum_drop_table.clone())?,
            )),
            Plan::AnalyzeTable(analyze_table) => Ok(Arc::new(AnalyzeTableInterpreter::try_create(
                ctx,
                *analyze_table.clone(),
            )?)),
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

            // Indexes
            Plan::CreateIndex(index) => Ok(Arc::new(CreateIndexInterpreter::try_create(
                ctx,
                *index.clone(),
            )?)),

            Plan::DropIndex(index) => Ok(Arc::new(DropIndexInterpreter::try_create(
                ctx,
                *index.clone(),
            )?)),
            Plan::RefreshIndex(index) => Ok(Arc::new(RefreshIndexInterpreter::try_create(
                ctx,
                *index.clone(),
            )?)),
            // Virtual columns
            Plan::CreateVirtualColumn(create_virtual_column) => Ok(Arc::new(
                CreateVirtualColumnInterpreter::try_create(ctx, *create_virtual_column.clone())?,
            )),
            Plan::AlterVirtualColumn(alter_virtual_column) => Ok(Arc::new(
                AlterVirtualColumnInterpreter::try_create(ctx, *alter_virtual_column.clone())?,
            )),
            Plan::DropVirtualColumn(drop_virtual_column) => Ok(Arc::new(
                DropVirtualColumnInterpreter::try_create(ctx, *drop_virtual_column.clone())?,
            )),
            Plan::RefreshVirtualColumn(refresh_virtual_column) => Ok(Arc::new(
                RefreshVirtualColumnInterpreter::try_create(ctx, *refresh_virtual_column.clone())?,
            )),
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

            Plan::Insert(insert) => InsertInterpreter::try_create(ctx, *insert.clone()),

            Plan::Replace(replace) => ReplaceInterpreter::try_create(ctx, *replace.clone()),
            Plan::MergeInto(merge_into) => {
                MergeIntoInterpreter::try_create(ctx, *merge_into.clone())
            }
            Plan::Delete(delete) => Ok(Arc::new(DeleteInterpreter::try_create(
                ctx,
                *delete.clone(),
            )?)),

            Plan::Update(update) => Ok(Arc::new(UpdateInterpreter::try_create(
                ctx,
                *update.clone(),
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
            Plan::SetRole(set_role) => Ok(Arc::new(SetRoleInterpreter::try_create(
                ctx,
                *set_role.clone(),
            )?)),
            Plan::ShowRoles(_show_roles) => Ok(Arc::new(ShowRolesInterpreter::try_create(ctx)?)),

            // Stages
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

            // FileFormats
            Plan::CreateFileFormat(create_file_format) => Ok(Arc::new(
                CreateFileFormatInterpreter::try_create(ctx, *create_file_format.clone())?,
            )),
            Plan::DropFileFormat(drop_file_format) => Ok(Arc::new(
                DropFileFormatInterpreter::try_create(ctx, *drop_file_format.clone())?,
            )),
            Plan::ShowFileFormats(_) => Ok(Arc::new(ShowFileFormatsInterpreter::try_create(ctx)?)),

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
            Plan::UnSetVariable(unset_variable) => Ok(Arc::new(UnSettingInterpreter::try_create(
                ctx,
                *unset_variable.clone(),
            )?)),
            Plan::UseDatabase(p) => Ok(Arc::new(UseDatabaseInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::Kill(p) => Ok(Arc::new(KillInterpreter::try_create(ctx, *p.clone())?)),

            // share plans
            Plan::CreateShareEndpoint(p) => Ok(Arc::new(
                CreateShareEndpointInterpreter::try_create(ctx, *p.clone())?,
            )),
            Plan::ShowShareEndpoint(p) => Ok(Arc::new(ShowShareEndpointInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DropShareEndpoint(p) => Ok(Arc::new(DropShareEndpointInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
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
            Plan::ShowShares(_) => Ok(Arc::new(ShowSharesInterpreter::try_create(ctx)?)),
            Plan::ShowObjectGrantPrivileges(p) => Ok(Arc::new(
                ShowObjectGrantPrivilegesInterpreter::try_create(ctx, *p.clone())?,
            )),
            Plan::ShowGrantTenantsOfShare(p) => Ok(Arc::new(
                ShowGrantTenantsOfShareInterpreter::try_create(ctx, *p.clone())?,
            )),
            Plan::RevertTable(p) => Ok(Arc::new(RevertTableInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::CreateDatamaskPolicy(p) => Ok(Arc::new(CreateDataMaskInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DropDatamaskPolicy(p) => Ok(Arc::new(DropDataMaskInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DescDatamaskPolicy(p) => Ok(Arc::new(DescDataMaskInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),

            Plan::CreateNetworkPolicy(p) => Ok(Arc::new(
                CreateNetworkPolicyInterpreter::try_create(ctx, *p.clone())?,
            )),
            Plan::AlterNetworkPolicy(p) => Ok(Arc::new(AlterNetworkPolicyInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DropNetworkPolicy(p) => Ok(Arc::new(DropNetworkPolicyInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::DescNetworkPolicy(p) => Ok(Arc::new(DescNetworkPolicyInterpreter::try_create(
                ctx,
                *p.clone(),
            )?)),
            Plan::ShowNetworkPolicies(_) => {
                Ok(Arc::new(ShowNetworkPoliciesInterpreter::try_create(ctx)?))
            }

            Plan::CreateTask(_) => todo!(),
        }
    }
}
