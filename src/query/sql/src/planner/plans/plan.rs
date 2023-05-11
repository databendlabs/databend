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
use std::sync::Arc;

use common_ast::ast::ExplainKind;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;

use super::CreateShareEndpointPlan;
use super::DropShareEndpointPlan;
use super::VacuumTablePlan;
use crate::optimizer::SExpr;
use crate::plans::copy::CopyPlan;
use crate::plans::insert::Insert;
use crate::plans::presign::PresignPlan;
use crate::plans::recluster_table::ReclusterTablePlan;
use crate::plans::share::AlterShareTenantsPlan;
use crate::plans::share::CreateSharePlan;
use crate::plans::share::DescSharePlan;
use crate::plans::share::DropSharePlan;
use crate::plans::share::GrantShareObjectPlan;
use crate::plans::share::RevokeShareObjectPlan;
use crate::plans::share::ShowGrantTenantsOfSharePlan;
use crate::plans::share::ShowObjectGrantPrivilegesPlan;
use crate::plans::share::ShowSharesPlan;
use crate::plans::AddTableColumnPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AlterUDFPlan;
use crate::plans::AlterUserPlan;
use crate::plans::AlterViewPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CallPlan;
use crate::plans::CreateCatalogPlan;
use crate::plans::CreateDatabasePlan;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateRolePlan;
use crate::plans::CreateStagePlan;
use crate::plans::CreateTablePlan;
use crate::plans::CreateUDFPlan;
use crate::plans::CreateUserPlan;
use crate::plans::CreateViewPlan;
use crate::plans::DeletePlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DropCatalogPlan;
use crate::plans::DropDatabasePlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropRolePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTablePlan;
use crate::plans::DropUDFPlan;
use crate::plans::DropUserPlan;
use crate::plans::DropViewPlan;
use crate::plans::ExistsTablePlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::KillPlan;
use crate::plans::OptimizeTablePlan;
use crate::plans::RemoveStagePlan;
use crate::plans::RenameDatabasePlan;
use crate::plans::RenameTablePlan;
use crate::plans::Replace;
use crate::plans::RevertTablePlan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
use crate::plans::SetRolePlan;
use crate::plans::SettingPlan;
use crate::plans::ShowCreateCatalogPlan;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowGrantsPlan;
use crate::plans::ShowRolesPlan;
use crate::plans::ShowShareEndpointPlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UnSettingPlan;
use crate::plans::UndropDatabasePlan;
use crate::plans::UndropTablePlan;
use crate::plans::UpdatePlan;
use crate::plans::UseDatabasePlan;
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

    // Call
    Call(Box<CallPlan>),

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
    AddTableColumn(Box<AddTableColumnPlan>),
    DropTableColumn(Box<DropTableColumnPlan>),
    AlterTableClusterKey(Box<AlterTableClusterKeyPlan>),
    DropTableClusterKey(Box<DropTableClusterKeyPlan>),
    ReclusterTable(Box<ReclusterTablePlan>),
    RevertTable(Box<RevertTablePlan>),
    TruncateTable(Box<TruncateTablePlan>),
    OptimizeTable(Box<OptimizeTablePlan>),
    VacuumTable(Box<VacuumTablePlan>),
    AnalyzeTable(Box<AnalyzeTablePlan>),
    ExistsTable(Box<ExistsTablePlan>),

    // Insert
    Insert(Box<Insert>),
    Replace(Box<Replace>),
    Delete(Box<DeletePlan>),
    Update(Box<UpdatePlan>),

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),

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
}

#[derive(Clone, Debug)]
pub enum RewriteKind {
    ShowSettings,
    ShowMetrics,
    ShowProcessList,
    ShowEngines,

    ShowCatalogs,
    ShowDatabases,
    ShowTables,
    ShowColumns,
    ShowTablesStatus,

    ShowFunctions,
    ShowTableFunctions,

    ShowUsers,
    ShowStages,
    DescribeStage,
    ListStage,
    ShowRoles,
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Plan::Query { .. } => write!(f, "Query"),
            Plan::Copy(_) => write!(f, "Copy"),
            Plan::Explain { .. } => write!(f, "Explain"),
            Plan::ExplainAnalyze { .. } => write!(f, "ExplainAnalyze"),
            Plan::ShowCreateCatalog(_) => write!(f, "ShowCreateCatalog"),
            Plan::CreateCatalog(_) => write!(f, "CreateCatalog"),
            Plan::DropCatalog(_) => write!(f, "DropCatalog"),
            Plan::ShowCreateDatabase(_) => write!(f, "ShowCreateDatabase"),
            Plan::CreateDatabase(_) => write!(f, "CreateDatabase"),
            Plan::DropDatabase(_) => write!(f, "DropDatabase"),
            Plan::UndropDatabase(_) => write!(f, "UndropDatabase"),
            Plan::UseDatabase(_) => write!(f, "UseDatabase"),
            Plan::RenameDatabase(_) => write!(f, "RenameDatabase"),
            Plan::ShowCreateTable(_) => write!(f, "ShowCreateTable"),
            Plan::DescribeTable(_) => write!(f, "DescribeTable"),
            Plan::CreateTable(_) => write!(f, "CreateTable"),
            Plan::DropTable(_) => write!(f, "DropTable"),
            Plan::UndropTable(_) => write!(f, "UndropTable"),
            Plan::RenameTable(_) => write!(f, "RenameTable"),
            Plan::AddTableColumn(_) => write!(f, "AddTableColumn"),
            Plan::DropTableColumn(_) => write!(f, "DropTableColumn"),
            Plan::AlterTableClusterKey(_) => write!(f, "AlterTableClusterKey"),
            Plan::DropTableClusterKey(_) => write!(f, "DropTableClusterKey"),
            Plan::ReclusterTable(_) => write!(f, "ReclusterTable"),
            Plan::TruncateTable(_) => write!(f, "TruncateTable"),
            Plan::OptimizeTable(_) => write!(f, "OptimizeTable"),
            Plan::VacuumTable(_) => write!(f, "VacuumTable"),
            Plan::AnalyzeTable(_) => write!(f, "AnalyzeTable"),
            Plan::ExistsTable(_) => write!(f, "ExistsTable"),
            Plan::CreateView(_) => write!(f, "CreateView"),
            Plan::AlterView(_) => write!(f, "AlterView"),
            Plan::DropView(_) => write!(f, "DropView"),
            Plan::AlterUser(_) => write!(f, "AlterUser"),
            Plan::CreateUser(_) => write!(f, "CreateUser"),
            Plan::DropUser(_) => write!(f, "DropUser"),
            Plan::CreateRole(_) => write!(f, "CreateRole"),
            Plan::DropRole(_) => write!(f, "DropRole"),
            Plan::CreateStage(_) => write!(f, "CreateStage"),
            Plan::DropStage(_) => write!(f, "DropStage"),
            Plan::CreateFileFormat(_) => write!(f, "CreateFileFormat"),
            Plan::DropFileFormat(_) => write!(f, "DropFileFormat"),
            Plan::ShowFileFormats(_) => write!(f, "ShowFileFormats"),
            Plan::RemoveStage(_) => write!(f, "RemoveStage"),
            Plan::GrantRole(_) => write!(f, "GrantRole"),
            Plan::GrantPriv(_) => write!(f, "GrantPriv"),
            Plan::ShowGrants(_) => write!(f, "ShowGrants"),
            Plan::ShowRoles(_) => write!(f, "ShowRoles"),
            Plan::RevokePriv(_) => write!(f, "RevokePriv"),
            Plan::RevokeRole(_) => write!(f, "RevokeRole"),
            Plan::CreateUDF(_) => write!(f, "CreateUDF"),
            Plan::AlterUDF(_) => write!(f, "AlterUDF"),
            Plan::DropUDF(_) => write!(f, "DropUDF"),
            Plan::Insert(_) => write!(f, "Insert"),
            Plan::Replace(_) => write!(f, "Replace"),
            Plan::Delete(_) => write!(f, "Delete"),
            Plan::Update(_) => write!(f, "Update"),
            Plan::Call(_) => write!(f, "Call"),
            Plan::Presign(_) => write!(f, "Presign"),
            Plan::SetVariable(_) => write!(f, "SetVariable"),
            Plan::UnSetVariable(_) => write!(f, "UnSetVariable"),
            Plan::SetRole(_) => write!(f, "SetRole"),
            Plan::Kill(_) => write!(f, "Kill"),
            Plan::CreateShareEndpoint(_) => write!(f, "CreateShareEndpoint"),
            Plan::ShowShareEndpoint(_) => write!(f, "ShowShareEndpoint"),
            Plan::DropShareEndpoint(_) => write!(f, "DropShareEndpoint"),
            Plan::CreateShare(_) => write!(f, "CreateShare"),
            Plan::DropShare(_) => write!(f, "DropShare"),
            Plan::GrantShareObject(_) => write!(f, "GrantShareObject"),
            Plan::RevokeShareObject(_) => write!(f, "RevokeShareObject"),
            Plan::AlterShareTenants(_) => write!(f, "AlterShareTenants"),
            Plan::DescShare(_) => write!(f, "DescShare"),
            Plan::ShowShares(_) => write!(f, "ShowShares"),
            Plan::ShowObjectGrantPrivileges(_) => write!(f, "ShowObjectGrantPrivileges"),
            Plan::ShowGrantTenantsOfShare(_) => write!(f, "ShowGrantTenantsOfShare"),
            Plan::ExplainAst { .. } => write!(f, "ExplainAst"),
            Plan::ExplainSyntax { .. } => write!(f, "ExplainSyntax"),
            Plan::RevertTable(..) => write!(f, "RevertTable"),
        }
    }
}

impl Plan {
    /// Notice: This is incomplete and should be only used when you know it must has schema (Plan::Query | Plan::Insert ...).
    /// If you want to get the real schema from plan use `InterpreterFactory::get_schema()` instead
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
                ..
            } => bind_context.output_schema(),
            Plan::Explain { .. } | Plan::ExplainAst { .. } | Plan::ExplainSyntax { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", DataType::String)])
            }
            Plan::ExplainAnalyze { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", DataType::String)])
            }
            Plan::Copy(_) => Arc::new(DataSchema::empty()),
            Plan::ShowCreateCatalog(plan) => plan.schema(),
            Plan::CreateCatalog(plan) => plan.schema(),
            Plan::DropCatalog(plan) => plan.schema(),
            Plan::ShowCreateDatabase(plan) => plan.schema(),
            Plan::CreateDatabase(plan) => plan.schema(),
            Plan::UseDatabase(_) => Arc::new(DataSchema::empty()),
            Plan::DropDatabase(plan) => plan.schema(),
            Plan::UndropDatabase(plan) => plan.schema(),
            Plan::RenameDatabase(plan) => plan.schema(),
            Plan::ShowCreateTable(plan) => plan.schema(),
            Plan::DescribeTable(plan) => plan.schema(),
            Plan::CreateTable(plan) => plan.schema(),
            Plan::DropTable(plan) => plan.schema(),
            Plan::UndropTable(plan) => plan.schema(),
            Plan::RenameTable(plan) => plan.schema(),
            Plan::AddTableColumn(plan) => plan.schema(),
            Plan::DropTableColumn(plan) => plan.schema(),
            Plan::AlterTableClusterKey(plan) => plan.schema(),
            Plan::DropTableClusterKey(plan) => plan.schema(),
            Plan::ReclusterTable(plan) => plan.schema(),
            Plan::TruncateTable(plan) => plan.schema(),
            Plan::OptimizeTable(plan) => plan.schema(),
            Plan::VacuumTable(plan) => plan.schema(),
            Plan::AnalyzeTable(plan) => plan.schema(),
            Plan::ExistsTable(plan) => plan.schema(),
            Plan::CreateView(plan) => plan.schema(),
            Plan::AlterView(plan) => plan.schema(),
            Plan::DropView(plan) => plan.schema(),
            Plan::AlterUser(plan) => plan.schema(),
            Plan::CreateUser(plan) => plan.schema(),
            Plan::DropUser(plan) => plan.schema(),
            Plan::CreateRole(plan) => plan.schema(),
            Plan::DropRole(plan) => plan.schema(),
            Plan::ShowRoles(plan) => plan.schema(),
            Plan::GrantRole(plan) => plan.schema(),
            Plan::GrantPriv(plan) => plan.schema(),
            Plan::ShowGrants(plan) => plan.schema(),
            Plan::CreateStage(plan) => plan.schema(),
            Plan::DropStage(plan) => plan.schema(),
            Plan::RemoveStage(plan) => plan.schema(),
            Plan::CreateFileFormat(plan) => plan.schema(),
            Plan::DropFileFormat(plan) => plan.schema(),
            Plan::ShowFileFormats(plan) => plan.schema(),
            Plan::RevokePriv(_) => Arc::new(DataSchema::empty()),
            Plan::RevokeRole(_) => Arc::new(DataSchema::empty()),
            Plan::CreateUDF(_) => Arc::new(DataSchema::empty()),
            Plan::AlterUDF(_) => Arc::new(DataSchema::empty()),
            Plan::DropUDF(_) => Arc::new(DataSchema::empty()),
            Plan::Insert(plan) => plan.schema(),
            Plan::Replace(plan) => plan.schema(),
            Plan::Delete(_) => Arc::new(DataSchema::empty()),
            Plan::Update(_) => Arc::new(DataSchema::empty()),
            Plan::Call(_) => Arc::new(DataSchema::empty()),
            Plan::Presign(plan) => plan.schema(),
            Plan::SetVariable(plan) => plan.schema(),
            Plan::UnSetVariable(plan) => plan.schema(),
            Plan::SetRole(plan) => plan.schema(),
            Plan::Kill(_) => Arc::new(DataSchema::empty()),
            Plan::CreateShareEndpoint(plan) => plan.schema(),
            Plan::ShowShareEndpoint(plan) => plan.schema(),
            Plan::DropShareEndpoint(plan) => plan.schema(),
            Plan::CreateShare(plan) => plan.schema(),
            Plan::DropShare(plan) => plan.schema(),
            Plan::GrantShareObject(plan) => plan.schema(),
            Plan::RevokeShareObject(plan) => plan.schema(),
            Plan::AlterShareTenants(plan) => plan.schema(),
            Plan::DescShare(plan) => plan.schema(),
            Plan::ShowShares(plan) => plan.schema(),
            Plan::ShowObjectGrantPrivileges(plan) => plan.schema(),
            Plan::ShowGrantTenantsOfShare(plan) => plan.schema(),
            Plan::RevertTable(plan) => plan.schema(),
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
                | Plan::Call(_)
                | Plan::ShowCreateDatabase(_)
                | Plan::ShowCreateTable(_)
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
        )
    }
}
