// Copyright 2022 Datafuse Labs.
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

mod aggregate;
mod copy_v2;
pub mod create_table_v2;
mod eval_scalar;
mod exchange;
mod filter;
mod hash_join;
pub mod insert;
mod limit;
mod logical_get;
mod logical_join;
mod operator;
mod pattern;
mod physical_scan;
mod presign;
mod project;
mod scalar;
pub mod share;
mod sort;
mod union_all;

use std::fmt::Display;
use std::sync::Arc;

pub use aggregate::*;
use common_ast::ast::ExplainKind;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_planners::AlterTableClusterKeyPlan;
use common_planners::AlterUserPlan;
use common_planners::AlterUserUDFPlan;
use common_planners::AlterViewPlan;
use common_planners::CallPlan;
use common_planners::CreateDatabasePlan;
use common_planners::CreateRolePlan;
use common_planners::CreateUserPlan;
use common_planners::CreateUserStagePlan;
use common_planners::CreateUserUDFPlan;
use common_planners::CreateViewPlan;
use common_planners::DeletePlan;
use common_planners::DescribeTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropRolePlan;
use common_planners::DropTableClusterKeyPlan;
use common_planners::DropTablePlan;
use common_planners::DropUserPlan;
use common_planners::DropUserStagePlan;
use common_planners::DropUserUDFPlan;
use common_planners::DropViewPlan;
use common_planners::ExistsTablePlan;
use common_planners::GrantPrivilegePlan;
use common_planners::GrantRolePlan;
use common_planners::KillPlan;
use common_planners::ListPlan;
use common_planners::OptimizeTablePlan;
use common_planners::RemoveUserStagePlan;
use common_planners::RenameDatabasePlan;
use common_planners::RenameTablePlan;
use common_planners::RevokePrivilegePlan;
use common_planners::RevokeRolePlan;
use common_planners::SettingPlan;
use common_planners::ShowCreateDatabasePlan;
use common_planners::ShowCreateTablePlan;
use common_planners::ShowGrantsPlan;
use common_planners::TruncateTablePlan;
use common_planners::UndropDatabasePlan;
use common_planners::UndropTablePlan;
use common_planners::UseDatabasePlan;
pub use copy_v2::CopyPlanV2;
pub use copy_v2::ValidationMode;
pub use create_table_v2::CreateTablePlanV2;
pub use eval_scalar::EvalScalar;
pub use eval_scalar::ScalarItem;
pub use exchange::Exchange;
pub use filter::Filter;
pub use hash_join::PhysicalHashJoin;
pub use insert::Insert;
pub use insert::InsertInputSource;
pub use insert::InsertValueBlock;
pub use limit::Limit;
pub use logical_get::LogicalGet;
pub use logical_get::Prewhere;
pub use logical_join::JoinType;
pub use logical_join::LogicalInnerJoin;
pub use operator::*;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use presign::PresignAction;
pub use presign::PresignPlan;
pub use project::Project;
pub use scalar::*;
pub use share::*;
pub use sort::Sort;
pub use sort::SortItem;
pub use union_all::UnionAll;

use super::BindContext;
use super::MetadataRef;
use crate::sql::optimizer::SExpr;

#[derive(Clone, Debug)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: Box<SExpr>,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
        rewrite_kind: Option<RewriteKind>,
    },

    Explain {
        kind: ExplainKind,
        plan: Box<Plan>,
    },

    // Copy
    Copy(Box<CopyPlanV2>),

    // Call
    Call(Box<CallPlan>),

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
    CreateTable(Box<CreateTablePlanV2>),
    DropTable(Box<DropTablePlan>),
    UndropTable(Box<UndropTablePlan>),
    RenameTable(Box<RenameTablePlan>),
    AlterTableClusterKey(Box<AlterTableClusterKeyPlan>),
    DropTableClusterKey(Box<DropTableClusterKeyPlan>),
    TruncateTable(Box<TruncateTablePlan>),
    OptimizeTable(Box<OptimizeTablePlan>),
    ExistsTable(Box<ExistsTablePlan>),

    // Insert
    Insert(Box<Insert>),
    Delete(Box<DeletePlan>),

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),

    // Account
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),

    // UDF
    CreateUDF(Box<CreateUserUDFPlan>),
    AlterUDF(Box<AlterUserUDFPlan>),
    DropUDF(Box<DropUserUDFPlan>),

    // Role
    CreateRole(Box<CreateRolePlan>),
    DropRole(Box<DropRolePlan>),
    GrantRole(Box<GrantRolePlan>),
    GrantPriv(Box<GrantPrivilegePlan>),
    ShowGrants(Box<ShowGrantsPlan>),
    RevokePriv(Box<RevokePrivilegePlan>),
    RevokeRole(Box<RevokeRolePlan>),

    // Stages
    ListStage(Box<ListPlan>),
    CreateStage(Box<CreateUserStagePlan>),
    DropStage(Box<DropUserStagePlan>),
    RemoveStage(Box<RemoveUserStagePlan>),

    // Presign
    Presign(Box<PresignPlan>),

    // Set
    SetVariable(Box<SettingPlan>),
    Kill(Box<KillPlan>),

    // Share
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

    ShowDatabases,
    ShowTables,
    ShowTablesStatus,

    ShowFunctions,

    ShowUsers,
    ShowStages,
    DescribeStage,
    ShowRoles,
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Plan::Query { .. } => write!(f, "Query"),
            Plan::Copy(_) => write!(f, "Copy"),
            Plan::Explain { .. } => write!(f, "Explain"),
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
            Plan::AlterTableClusterKey(_) => write!(f, "AlterTableClusterKey"),
            Plan::DropTableClusterKey(_) => write!(f, "DropTableClusterKey"),
            Plan::TruncateTable(_) => write!(f, "TruncateTable"),
            Plan::OptimizeTable(_) => write!(f, "OptimizeTable"),
            Plan::ExistsTable(_) => write!(f, "ExistsTable"),
            Plan::CreateView(_) => write!(f, "CreateView"),
            Plan::AlterView(_) => write!(f, "AlterView"),
            Plan::DropView(_) => write!(f, "DropView"),
            Plan::AlterUser(_) => write!(f, "AlterUser"),
            Plan::CreateUser(_) => write!(f, "CreateUser"),
            Plan::DropUser(_) => write!(f, "DropUser"),
            Plan::CreateRole(_) => write!(f, "CreateRole"),
            Plan::DropRole(_) => write!(f, "DropRole"),
            Plan::ListStage(_) => write!(f, "ListStage"),
            Plan::CreateStage(_) => write!(f, "CreateStage"),
            Plan::DropStage(_) => write!(f, "DropStage"),
            Plan::RemoveStage(_) => write!(f, "RemoveStage"),
            Plan::GrantRole(_) => write!(f, "GrantRole"),
            Plan::GrantPriv(_) => write!(f, "GrantPriv"),
            Plan::ShowGrants(_) => write!(f, "ShowGrants"),
            Plan::RevokePriv(_) => write!(f, "RevokePriv"),
            Plan::RevokeRole(_) => write!(f, "RevokeRole"),
            Plan::CreateUDF(_) => write!(f, "CreateUDF"),
            Plan::AlterUDF(_) => write!(f, "AlterUDF"),
            Plan::DropUDF(_) => write!(f, "DropUDF"),
            Plan::Insert(_) => write!(f, "Insert"),
            Plan::Delete(_) => write!(f, "Delete"),
            Plan::Call(_) => write!(f, "Call"),
            Plan::Presign(_) => write!(f, "Presign"),
            Plan::SetVariable(_) => write!(f, "SetVariable"),
            Plan::Kill(_) => write!(f, "Kill"),
            Plan::CreateShare(_) => write!(f, "CreateShare"),
            Plan::DropShare(_) => write!(f, "DropShare"),
            Plan::GrantShareObject(_) => write!(f, "GrantShareObject"),
            Plan::RevokeShareObject(_) => write!(f, "RevokeShareObject"),
            Plan::AlterShareTenants(_) => write!(f, "AlterShareTenants"),
            Plan::DescShare(_) => write!(f, "DescShare"),
            Plan::ShowShares(_) => write!(f, "ShowShares"),
            Plan::ShowObjectGrantPrivileges(_) => write!(f, "ShowObjectGrantPrivileges"),
            Plan::ShowGrantTenantsOfShare(_) => write!(f, "ShowGrantTenantsOfShare"),
        }
    }
}

// TODO the schema is not completed
impl Plan {
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
                ..
            } => bind_context.output_schema(),
            Plan::Explain { kind: _, plan: _ } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", Vu8::to_data_type())])
            }
            Plan::Copy(_) => Arc::new(DataSchema::empty()),
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
            Plan::AlterTableClusterKey(plan) => plan.schema(),
            Plan::DropTableClusterKey(plan) => plan.schema(),
            Plan::TruncateTable(plan) => plan.schema(),
            Plan::OptimizeTable(plan) => plan.schema(),
            Plan::ExistsTable(plan) => plan.schema(),
            Plan::CreateView(plan) => plan.schema(),
            Plan::AlterView(plan) => plan.schema(),
            Plan::DropView(plan) => plan.schema(),
            Plan::AlterUser(plan) => plan.schema(),
            Plan::CreateUser(plan) => plan.schema(),
            Plan::DropUser(plan) => plan.schema(),
            Plan::CreateRole(plan) => plan.schema(),
            Plan::DropRole(plan) => plan.schema(),
            Plan::GrantRole(plan) => plan.schema(),
            Plan::GrantPriv(plan) => plan.schema(),
            Plan::ShowGrants(plan) => plan.schema(),
            Plan::ListStage(plan) => plan.schema(),
            Plan::CreateStage(plan) => plan.schema(),
            Plan::DropStage(plan) => plan.schema(),
            Plan::RemoveStage(plan) => plan.schema(),
            Plan::RevokePriv(_) => Arc::new(DataSchema::empty()),
            Plan::RevokeRole(_) => Arc::new(DataSchema::empty()),
            Plan::CreateUDF(_) => Arc::new(DataSchema::empty()),
            Plan::AlterUDF(_) => Arc::new(DataSchema::empty()),
            Plan::DropUDF(_) => Arc::new(DataSchema::empty()),
            Plan::Insert(plan) => plan.schema(),
            Plan::Delete(_) => Arc::new(DataSchema::empty()),
            Plan::Call(_) => Arc::new(DataSchema::empty()),
            Plan::Presign(plan) => plan.schema(),
            Plan::SetVariable(plan) => plan.schema(),
            Plan::Kill(_) => Arc::new(DataSchema::empty()),
            Plan::CreateShare(plan) => plan.schema(),
            Plan::DropShare(plan) => plan.schema(),
            Plan::GrantShareObject(plan) => plan.schema(),
            Plan::RevokeShareObject(plan) => plan.schema(),
            Plan::AlterShareTenants(plan) => plan.schema(),
            Plan::DescShare(plan) => plan.schema(),
            Plan::ShowShares(plan) => plan.schema(),
            Plan::ShowObjectGrantPrivileges(plan) => plan.schema(),
            Plan::ShowGrantTenantsOfShare(plan) => plan.schema(),
        }
    }
}
