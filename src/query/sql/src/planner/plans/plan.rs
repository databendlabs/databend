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

use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use common_ast::ast::ExplainKind;
use common_catalog::query_kind::QueryKind;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;

use crate::optimizer::SExpr;
use crate::plans::AddTableColumnPlan;
use crate::plans::AlterNetworkPolicyPlan;
use crate::plans::AlterShareTenantsPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AlterUDFPlan;
use crate::plans::AlterUserPlan;
use crate::plans::AlterViewPlan;
use crate::plans::AlterVirtualColumnPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyPlan;
use crate::plans::CreateCatalogPlan;
use crate::plans::CreateDatabasePlan;
use crate::plans::CreateDatamaskPolicyPlan;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateIndexPlan;
use crate::plans::CreateNetworkPolicyPlan;
use crate::plans::CreateRolePlan;
use crate::plans::CreateShareEndpointPlan;
use crate::plans::CreateSharePlan;
use crate::plans::CreateStagePlan;
use crate::plans::CreateTablePlan;
use crate::plans::CreateUDFPlan;
use crate::plans::CreateUserPlan;
use crate::plans::CreateViewPlan;
use crate::plans::CreateVirtualColumnPlan;
use crate::plans::DeletePlan;
use crate::plans::DescDatamaskPolicyPlan;
use crate::plans::DescNetworkPolicyPlan;
use crate::plans::DescSharePlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DropCatalogPlan;
use crate::plans::DropDatabasePlan;
use crate::plans::DropDatamaskPolicyPlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropIndexPlan;
use crate::plans::DropNetworkPolicyPlan;
use crate::plans::DropRolePlan;
use crate::plans::DropShareEndpointPlan;
use crate::plans::DropSharePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTablePlan;
use crate::plans::DropUDFPlan;
use crate::plans::DropUserPlan;
use crate::plans::DropViewPlan;
use crate::plans::DropVirtualColumnPlan;
use crate::plans::ExistsTablePlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::GrantShareObjectPlan;
use crate::plans::Insert;
use crate::plans::KillPlan;
use crate::plans::MergeInto;
use crate::plans::ModifyTableColumnPlan;
use crate::plans::OptimizeTablePlan;
use crate::plans::PresignPlan;
use crate::plans::ReclusterTablePlan;
use crate::plans::RefreshIndexPlan;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RemoveStagePlan;
use crate::plans::RenameDatabasePlan;
use crate::plans::RenameTableColumnPlan;
use crate::plans::RenameTablePlan;
use crate::plans::Replace;
use crate::plans::RevertTablePlan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
use crate::plans::RevokeShareObjectPlan;
use crate::plans::SetOptionsPlan;
use crate::plans::SetRolePlan;
use crate::plans::SettingPlan;
use crate::plans::ShowCreateCatalogPlan;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowGrantTenantsOfSharePlan;
use crate::plans::ShowGrantsPlan;
use crate::plans::ShowNetworkPoliciesPlan;
use crate::plans::ShowObjectGrantPrivilegesPlan;
use crate::plans::ShowRolesPlan;
use crate::plans::ShowShareEndpointPlan;
use crate::plans::ShowSharesPlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UnSettingPlan;
use crate::plans::UndropDatabasePlan;
use crate::plans::UndropTablePlan;
use crate::plans::UpdatePlan;
use crate::plans::UseDatabasePlan;
use crate::plans::VacuumDropTablePlan;
use crate::plans::VacuumTablePlan;
use crate::BindContext;
use crate::MetadataRef;

#[derive(Clone, Debug)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: Box<SExpr>,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
        rewrite_kind: Option<RewriteKind>,
        // Use for generate query result cache key.
        formatted_ast: Option<String>,
        ignore_result: bool,
    },

    Explain {
        kind: ExplainKind,
        plan: Box<Plan>,
    },
    ExplainAst {
        formatted_string: String,
    },
    ExplainSyntax {
        formatted_sql: String,
    },
    ExplainAnalyze {
        plan: Box<Plan>,
    },

    // Copy
    Copy(Box<CopyPlan>),

    // Call is rewrite into Query
    // Call(Box<CallPlan>),

    // Catalogs
    ShowCreateCatalog(Box<ShowCreateCatalogPlan>),
    CreateCatalog(Box<CreateCatalogPlan>),
    DropCatalog(Box<DropCatalogPlan>),

    // Databases
    ShowCreateDatabase(Box<ShowCreateDatabasePlan>),
    CreateDatabase(Box<CreateDatabasePlan>),
    DropDatabase(Box<DropDatabasePlan>),
    UndropDatabase(Box<UndropDatabasePlan>),
    RenameDatabase(Box<RenameDatabasePlan>),
    UseDatabase(Box<UseDatabasePlan>),

    // Tables
    ShowCreateTable(Box<ShowCreateTablePlan>),
    DescribeTable(Box<DescribeTablePlan>),
    CreateTable(Box<CreateTablePlan>),
    DropTable(Box<DropTablePlan>),
    UndropTable(Box<UndropTablePlan>),
    RenameTable(Box<RenameTablePlan>),
    RenameTableColumn(Box<RenameTableColumnPlan>),
    AddTableColumn(Box<AddTableColumnPlan>),
    DropTableColumn(Box<DropTableColumnPlan>),
    ModifyTableColumn(Box<ModifyTableColumnPlan>),
    AlterTableClusterKey(Box<AlterTableClusterKeyPlan>),
    DropTableClusterKey(Box<DropTableClusterKeyPlan>),
    ReclusterTable(Box<ReclusterTablePlan>),
    RevertTable(Box<RevertTablePlan>),
    TruncateTable(Box<TruncateTablePlan>),
    OptimizeTable(Box<OptimizeTablePlan>),
    VacuumTable(Box<VacuumTablePlan>),
    VacuumDropTable(Box<VacuumDropTablePlan>),
    AnalyzeTable(Box<AnalyzeTablePlan>),
    ExistsTable(Box<ExistsTablePlan>),
    SetOptions(Box<SetOptionsPlan>),

    // Insert
    Insert(Box<Insert>),
    Replace(Box<Replace>),
    Delete(Box<DeletePlan>),
    Update(Box<UpdatePlan>),
    MergeInto(Box<MergeInto>),
    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),

    // Indexes
    CreateIndex(Box<CreateIndexPlan>),
    DropIndex(Box<DropIndexPlan>),
    RefreshIndex(Box<RefreshIndexPlan>),

    // Virtual Columns
    CreateVirtualColumn(Box<CreateVirtualColumnPlan>),
    AlterVirtualColumn(Box<AlterVirtualColumnPlan>),
    DropVirtualColumn(Box<DropVirtualColumnPlan>),
    RefreshVirtualColumn(Box<RefreshVirtualColumnPlan>),

    // Account
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),

    // UDF
    CreateUDF(Box<CreateUDFPlan>),
    AlterUDF(Box<AlterUDFPlan>),
    DropUDF(Box<DropUDFPlan>),

    // Role
    ShowRoles(Box<ShowRolesPlan>),
    CreateRole(Box<CreateRolePlan>),
    DropRole(Box<DropRolePlan>),
    GrantRole(Box<GrantRolePlan>),
    GrantPriv(Box<GrantPrivilegePlan>),
    ShowGrants(Box<ShowGrantsPlan>),
    RevokePriv(Box<RevokePrivilegePlan>),
    RevokeRole(Box<RevokeRolePlan>),
    SetRole(Box<SetRolePlan>),

    // FileFormat
    CreateFileFormat(Box<CreateFileFormatPlan>),
    DropFileFormat(Box<DropFileFormatPlan>),
    ShowFileFormats(Box<ShowFileFormatsPlan>),

    // Stages
    CreateStage(Box<CreateStagePlan>),
    DropStage(Box<DropStagePlan>),
    RemoveStage(Box<RemoveStagePlan>),

    // Presign
    Presign(Box<PresignPlan>),

    // Set
    SetVariable(Box<SettingPlan>),
    UnSetVariable(Box<UnSettingPlan>),
    Kill(Box<KillPlan>),

    // Share
    CreateShareEndpoint(Box<CreateShareEndpointPlan>),
    ShowShareEndpoint(Box<ShowShareEndpointPlan>),
    DropShareEndpoint(Box<DropShareEndpointPlan>),
    CreateShare(Box<CreateSharePlan>),
    DropShare(Box<DropSharePlan>),
    GrantShareObject(Box<GrantShareObjectPlan>),
    RevokeShareObject(Box<RevokeShareObjectPlan>),
    AlterShareTenants(Box<AlterShareTenantsPlan>),
    DescShare(Box<DescSharePlan>),
    ShowShares(Box<ShowSharesPlan>),
    ShowObjectGrantPrivileges(Box<ShowObjectGrantPrivilegesPlan>),
    ShowGrantTenantsOfShare(Box<ShowGrantTenantsOfSharePlan>),

    // Data mask
    CreateDatamaskPolicy(Box<CreateDatamaskPolicyPlan>),
    DropDatamaskPolicy(Box<DropDatamaskPolicyPlan>),
    DescDatamaskPolicy(Box<DescDatamaskPolicyPlan>),

    // Network policy
    CreateNetworkPolicy(Box<CreateNetworkPolicyPlan>),
    AlterNetworkPolicy(Box<AlterNetworkPolicyPlan>),
    DropNetworkPolicy(Box<DropNetworkPolicyPlan>),
    DescNetworkPolicy(Box<DescNetworkPolicyPlan>),
    ShowNetworkPolicies(Box<ShowNetworkPoliciesPlan>),
}

#[derive(Clone, Debug)]
pub enum RewriteKind {
    ShowSettings,
    ShowMetrics,
    ShowProcessList,
    ShowEngines,
    ShowIndexes,

    ShowCatalogs,
    ShowDatabases,
    ShowTables(String),
    ShowColumns(String, String),
    ShowTablesStatus,

    ShowFunctions,
    ShowTableFunctions,

    ShowUsers,
    ShowStages,
    DescribeStage,
    ListStage,
    ShowRoles,

    Call,
}

impl Plan {
    pub fn kind(&self) -> QueryKind {
        match self {
            Plan::Query { .. } => QueryKind::Query,
            Plan::Copy(plan) => match plan.deref() {
                CopyPlan::IntoTable(copy_plan) => match copy_plan.write_mode {
                    CopyIntoTableMode::Insert { .. } => QueryKind::Insert,
                    _ => QueryKind::Copy,
                },
                _ => QueryKind::Copy,
            },
            Plan::Explain { .. } => QueryKind::Explain,
            Plan::ExplainAnalyze { .. } => QueryKind::ExplainAnalyze,
            Plan::ShowCreateCatalog(_) => QueryKind::ShowCreateCatalog,
            Plan::CreateCatalog(_) => QueryKind::CreateCatalog,
            Plan::DropCatalog(_) => QueryKind::DropCatalog,
            Plan::ShowCreateDatabase(_) => QueryKind::ShowCreateDatabase,
            Plan::CreateDatabase(_) => QueryKind::CreateDatabase,
            Plan::DropDatabase(_) => QueryKind::DropDatabase,
            Plan::UndropDatabase(_) => QueryKind::UndropDatabase,
            Plan::UseDatabase(_) => QueryKind::UseDatabase,
            Plan::RenameDatabase(_) => QueryKind::RenameDatabase,
            Plan::ShowCreateTable(_) => QueryKind::ShowCreateTable,
            Plan::DescribeTable(_) => QueryKind::DescribeTable,
            Plan::CreateTable(_) => QueryKind::CreateTable,
            Plan::DropTable(_) => QueryKind::DropTable,
            Plan::UndropTable(_) => QueryKind::UndropTable,
            Plan::RenameTable(_) => QueryKind::RenameTable,
            Plan::RenameTableColumn(_) => QueryKind::RenameTableColumn,
            Plan::AddTableColumn(_) => QueryKind::AddTableColumn,
            Plan::ModifyTableColumn(_) => QueryKind::ModifyTableColumn,
            Plan::DropTableColumn(_) => QueryKind::DropTableColumn,
            Plan::AlterTableClusterKey(_) => QueryKind::AlterTableClusterKey,
            Plan::DropTableClusterKey(_) => QueryKind::DropTableClusterKey,
            Plan::ReclusterTable(_) => QueryKind::ReclusterTable,
            Plan::TruncateTable(_) => QueryKind::TruncateTable,
            Plan::OptimizeTable(_) => QueryKind::OptimizeTable,
            Plan::VacuumTable(_) => QueryKind::VacuumTable,
            Plan::VacuumDropTable(_) => QueryKind::VacuumDropTable,
            Plan::AnalyzeTable(_) => QueryKind::AnalyzeTable,
            Plan::ExistsTable(_) => QueryKind::ExistsTable,
            Plan::CreateView(_) => QueryKind::CreateView,
            Plan::AlterView(_) => QueryKind::AlterView,
            Plan::DropView(_) => QueryKind::DropView,
            Plan::CreateIndex(_) => QueryKind::CreateIndex,
            Plan::DropIndex(_) => QueryKind::DropIndex,
            Plan::RefreshIndex(_) => QueryKind::RefreshIndex,
            Plan::CreateVirtualColumn(_) => QueryKind::CreateVirtualColumn,
            Plan::AlterVirtualColumn(_) => QueryKind::AlterVirtualColumn,
            Plan::DropVirtualColumn(_) => QueryKind::DropVirtualColumn,
            Plan::RefreshVirtualColumn(_) => QueryKind::RefreshVirtualColumn,
            Plan::AlterUser(_) => QueryKind::AlterUser,
            Plan::CreateUser(_) => QueryKind::CreateUser,
            Plan::DropUser(_) => QueryKind::DropUser,
            Plan::CreateRole(_) => QueryKind::CreateRole,
            Plan::DropRole(_) => QueryKind::DropRole,
            Plan::CreateStage(_) => QueryKind::CreateStage,
            Plan::DropStage(_) => QueryKind::DropStage,
            Plan::CreateFileFormat(_) => QueryKind::CreateFileFormat,
            Plan::DropFileFormat(_) => QueryKind::DropFileFormat,
            Plan::ShowFileFormats(_) => QueryKind::ShowFileFormats,
            Plan::RemoveStage(_) => QueryKind::RemoveStage,
            Plan::GrantRole(_) => QueryKind::GrantRole,
            Plan::GrantPriv(_) => QueryKind::GrantPriv,
            Plan::ShowGrants(_) => QueryKind::ShowGrants,
            Plan::ShowRoles(_) => QueryKind::ShowRoles,
            Plan::RevokePriv(_) => QueryKind::RevokePriv,
            Plan::RevokeRole(_) => QueryKind::RevokeRole,
            Plan::CreateUDF(_) => QueryKind::CreateUDF,
            Plan::AlterUDF(_) => QueryKind::AlterUDF,
            Plan::DropUDF(_) => QueryKind::DropUDF,
            Plan::Insert(_) => QueryKind::Insert,
            Plan::Replace(_) => QueryKind::Replace,
            Plan::Delete(_) => QueryKind::Delete,
            Plan::Update(_) => QueryKind::Update,
            Plan::Presign(_) => QueryKind::Presign,
            Plan::SetVariable(_) => QueryKind::SetVariable,
            Plan::UnSetVariable(_) => QueryKind::UnSetVariable,
            Plan::SetRole(_) => QueryKind::SetRole,
            Plan::Kill(_) => QueryKind::Kill,
            Plan::CreateShareEndpoint(_) => QueryKind::CreateShareEndpoint,
            Plan::ShowShareEndpoint(_) => QueryKind::ShowShareEndpoint,
            Plan::DropShareEndpoint(_) => QueryKind::DropShareEndpoint,
            Plan::CreateShare(_) => QueryKind::CreateShare,
            Plan::DropShare(_) => QueryKind::DropShare,
            Plan::GrantShareObject(_) => QueryKind::GrantShareObject,
            Plan::RevokeShareObject(_) => QueryKind::RevokeShareObject,
            Plan::AlterShareTenants(_) => QueryKind::AlterShareTenants,
            Plan::DescShare(_) => QueryKind::DescShare,
            Plan::ShowShares(_) => QueryKind::ShowShares,
            Plan::ShowObjectGrantPrivileges(_) => QueryKind::ShowObjectGrantPrivileges,
            Plan::ShowGrantTenantsOfShare(_) => QueryKind::ShowGrantTenantsOfShare,
            Plan::ExplainAst { .. } => QueryKind::ExplainAst,
            Plan::ExplainSyntax { .. } => QueryKind::ExplainSyntax,
            Plan::RevertTable(..) => QueryKind::RevertTable,
            Plan::CreateDatamaskPolicy(..) => QueryKind::CreateDatamaskPolicy,
            Plan::DropDatamaskPolicy(..) => QueryKind::DropDatamaskPolicy,
            Plan::DescDatamaskPolicy(..) => QueryKind::DescDatamaskPolicy,
            Plan::SetOptions(..) => QueryKind::SetOptions,
            Plan::CreateNetworkPolicy(_) => QueryKind::CreateNetworkPolicy,
            Plan::AlterNetworkPolicy(_) => QueryKind::AlterNetworkPolicy,
            Plan::DropNetworkPolicy(_) => QueryKind::DropNetworkPolicy,
            Plan::DescNetworkPolicy(_) => QueryKind::DescNetworkPolicy,
            Plan::ShowNetworkPolicies(_) => QueryKind::ShowNetworkPolicies,
            Plan::MergeInto(_) => QueryKind::MergeInto,
        }
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind())
    }
}

impl Plan {
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
                ..
            } => bind_context.output_schema(),
            Plan::Explain { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. }
            | Plan::ExplainAnalyze { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", DataType::String)])
            }
            Plan::ShowCreateCatalog(plan) => plan.schema(),
            Plan::ShowCreateDatabase(plan) => plan.schema(),
            Plan::ShowCreateTable(plan) => plan.schema(),
            Plan::DescribeTable(plan) => plan.schema(),
            Plan::VacuumTable(plan) => plan.schema(),
            Plan::VacuumDropTable(plan) => plan.schema(),
            Plan::ExistsTable(plan) => plan.schema(),
            Plan::ShowRoles(plan) => plan.schema(),
            Plan::ShowGrants(plan) => plan.schema(),
            Plan::ShowFileFormats(plan) => plan.schema(),

            Plan::Insert(plan) => plan.schema(),
            Plan::Replace(plan) => plan.schema(),

            Plan::Presign(plan) => plan.schema(),
            Plan::ShowShareEndpoint(plan) => plan.schema(),
            Plan::DescShare(plan) => plan.schema(),
            Plan::ShowShares(plan) => plan.schema(),
            Plan::ShowGrantTenantsOfShare(plan) => plan.schema(),
            Plan::CreateDatamaskPolicy(plan) => plan.schema(),
            Plan::DropDatamaskPolicy(plan) => plan.schema(),
            Plan::DescDatamaskPolicy(plan) => plan.schema(),
            Plan::CreateNetworkPolicy(plan) => plan.schema(),
            Plan::AlterNetworkPolicy(plan) => plan.schema(),
            Plan::DropNetworkPolicy(plan) => plan.schema(),
            Plan::DescNetworkPolicy(plan) => plan.schema(),
            Plan::ShowNetworkPolicies(plan) => plan.schema(),
            Plan::Copy(plan) => plan.schema(),
            other => {
                debug_assert!(!other.has_result_set());
                Arc::new(DataSchema::empty())
            }
        }
    }

    pub fn has_result_set(&self) -> bool {
        matches!(
            self,
            Plan::Query { .. }
                | Plan::Explain { .. }
                | Plan::ExplainAst { .. }
                | Plan::ExplainSyntax { .. }
                | Plan::ExplainAnalyze { .. }
                | Plan::ShowCreateDatabase(_)
                | Plan::ShowCreateTable(_)
                | Plan::ShowCreateCatalog(_)
                | Plan::ShowFileFormats(_)
                | Plan::ShowRoles(_)
                | Plan::DescShare(_)
                | Plan::ShowShares(_)
                | Plan::ShowShareEndpoint(_)
                | Plan::ShowObjectGrantPrivileges(_)
                | Plan::ShowGrantTenantsOfShare(_)
                | Plan::DescribeTable(_)
                | Plan::ShowGrants(_)
                | Plan::Presign(_)
                | Plan::VacuumTable(_)
                | Plan::VacuumDropTable(_)
                | Plan::DescDatamaskPolicy(_)
                | Plan::DescNetworkPolicy(_)
                | Plan::ShowNetworkPolicies(_)
                | Plan::Copy(_)
        )
    }
}
