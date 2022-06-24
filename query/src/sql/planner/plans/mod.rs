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
mod apply;
mod copy_v2;
mod eval_scalar;
mod filter;
mod hash_join;
mod insert;
mod limit;
mod logical_get;
mod logical_join;
mod max_one_row;
mod operator;
mod pattern;
mod physical_scan;
mod project;
mod scalar;
mod sort;

use std::fmt::Display;
use std::sync::Arc;

pub use aggregate::Aggregate;
pub use apply::CrossApply;
use common_ast::ast::ExplainKind;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_planners::AlterTableClusterKeyPlan;
use common_planners::AlterUserPlan;
use common_planners::AlterViewPlan;
use common_planners::CreateDatabasePlan;
use common_planners::CreateRolePlan;
use common_planners::CreateTablePlan;
use common_planners::CreateUserPlan;
use common_planners::CreateUserStagePlan;
use common_planners::CreateViewPlan;
use common_planners::DescribeTablePlan;
use common_planners::DescribeUserStagePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropRolePlan;
use common_planners::DropTableClusterKeyPlan;
use common_planners::DropTablePlan;
use common_planners::DropUserPlan;
use common_planners::DropUserStagePlan;
use common_planners::DropViewPlan;
use common_planners::ExistsTablePlan;
use common_planners::GrantPrivilegePlan;
use common_planners::GrantRolePlan;
use common_planners::ListPlan;
use common_planners::OptimizeTablePlan;
use common_planners::RemoveUserStagePlan;
use common_planners::RenameDatabasePlan;
use common_planners::RenameTablePlan;
use common_planners::RevokePrivilegePlan;
use common_planners::RevokeRolePlan;
use common_planners::ShowCreateDatabasePlan;
use common_planners::ShowCreateTablePlan;
use common_planners::ShowDatabasesPlan;
use common_planners::ShowGrantsPlan;
use common_planners::ShowTablesPlan;
use common_planners::ShowTablesStatusPlan;
use common_planners::TruncateTablePlan;
use common_planners::UndropTablePlan;
// use common_planners::*;
pub use copy_v2::CopyPlanV2;
pub use copy_v2::ValidationMode;
pub use eval_scalar::EvalScalar;
pub use eval_scalar::ScalarItem;
pub use filter::Filter;
pub use hash_join::PhysicalHashJoin;
pub use insert::Insert;
pub use insert::InsertInputSource;
pub use insert::InsertValueBlock;
pub use limit::Limit;
pub use logical_get::LogicalGet;
pub use logical_join::JoinType;
pub use logical_join::LogicalInnerJoin;
pub use max_one_row::Max1Row;
pub use operator::*;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use project::Project;
pub use scalar::*;
pub use sort::Sort;
pub use sort::SortItem;

use super::BindContext;
use super::MetadataRef;
use crate::sql::optimizer::SExpr;

#[derive(Clone)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: SExpr,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
    },

    Explain {
        kind: ExplainKind,
        plan: Box<Plan>,
    },

    // Copy
    Copy(Box<CopyPlanV2>),

    // System
    ShowMetrics,
    ShowProcessList,
    ShowSettings,

    // Databases
    ShowDatabases(Box<ShowDatabasesPlan>),
    ShowCreateDatabase(Box<ShowCreateDatabasePlan>),
    CreateDatabase(Box<CreateDatabasePlan>),
    DropDatabase(Box<DropDatabasePlan>),
    RenameDatabase(Box<RenameDatabasePlan>),

    // Tables
    ShowTables(Box<ShowTablesPlan>),
    ShowCreateTable(Box<ShowCreateTablePlan>),
    DescribeTable(Box<DescribeTablePlan>),
    ShowTablesStatus(Box<ShowTablesStatusPlan>),
    CreateTable(Box<CreateTablePlan>),
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

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),

    // Account
    ShowUsers,
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),
    ShowRoles,
    CreateRole(Box<CreateRolePlan>),
    DropRole(Box<DropRolePlan>),
    GrantRole(Box<GrantRolePlan>),
    GrantPriv(Box<GrantPrivilegePlan>),
    ShowGrants(Box<ShowGrantsPlan>),
    RevokePriv(Box<RevokePrivilegePlan>),
    RevokeRole(Box<RevokeRolePlan>),

    // Stages
    ShowStages,
    ListStage(Box<ListPlan>),
    DescribeStage(Box<DescribeUserStagePlan>),
    CreateStage(Box<CreateUserStagePlan>),
    DropStage(Box<DropUserStagePlan>),
    RemoveStage(Box<RemoveUserStagePlan>),
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Plan::Query { .. } => write!(f, "Query"),
            Plan::Copy(_) => write!(f, "Copy"),
            Plan::Explain { .. } => write!(f, "Explain"),
            Plan::ShowMetrics => write!(f, "ShowMetrics"),
            Plan::ShowProcessList => write!(f, "ShowProcessList"),
            Plan::ShowSettings => write!(f, "ShowSettings"),
            Plan::ShowDatabases(_) => write!(f, "ShowDatabases"),
            Plan::ShowCreateDatabase(_) => write!(f, "ShowCreateDatabase"),
            Plan::CreateDatabase(_) => write!(f, "CreateDatabase"),
            Plan::DropDatabase(_) => write!(f, "DropDatabase"),
            Plan::RenameDatabase(_) => write!(f, "RenameDatabase"),
            Plan::ShowTables(_) => write!(f, "ShowTables"),
            Plan::ShowCreateTable(_) => write!(f, "ShowCreateTable"),
            Plan::DescribeTable(_) => write!(f, "DescribeTable"),
            Plan::ShowTablesStatus(_) => write!(f, "ShowTablesStatus"),
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
            Plan::ShowUsers => write!(f, "ShowUsers"),
            Plan::AlterUser(_) => write!(f, "AlterUser"),
            Plan::CreateUser(_) => write!(f, "CreateUser"),
            Plan::DropUser(_) => write!(f, "DropUser"),
            Plan::ShowRoles => write!(f, "ShowRoles"),
            Plan::CreateRole(_) => write!(f, "CreateRole"),
            Plan::DropRole(_) => write!(f, "DropRole"),
            Plan::ShowStages => write!(f, "ShowStages"),
            Plan::ListStage(_) => write!(f, "ListStage"),
            Plan::DescribeStage(_) => write!(f, "DescribeStage"),
            Plan::CreateStage(_) => write!(f, "CreateStage"),
            Plan::DropStage(_) => write!(f, "DropStage"),
            Plan::RemoveStage(_) => write!(f, "RemoveStage"),
            Plan::GrantRole(_) => write!(f, "GrantRole"),
            Plan::GrantPriv(_) => write!(f, "GrantPriv"),
            Plan::ShowGrants(_) => write!(f, "ShowGrants"),
            Plan::RevokePriv(_) => write!(f, "RevokePriv"),
            Plan::RevokeRole(_) => write!(f, "RevokeRole"),
            Plan::Insert(_) => write!(f, "Insert"),
        }
    }
}

impl Plan {
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
            } => bind_context.output_schema(),
            Plan::Explain { kind: _, plan: _ } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", Vu8::to_data_type())])
            }
            Plan::Copy(_) => Arc::new(DataSchema::empty()),
            Plan::ShowMetrics => Arc::new(DataSchema::empty()),
            Plan::ShowProcessList => Arc::new(DataSchema::empty()),
            Plan::ShowSettings => Arc::new(DataSchema::empty()),
            Plan::ShowDatabases(_) => Arc::new(DataSchema::empty()),
            Plan::ShowCreateDatabase(_) => Arc::new(DataSchema::empty()),
            Plan::CreateDatabase(plan) => plan.schema(),
            Plan::DropDatabase(plan) => plan.schema(),
            Plan::RenameDatabase(plan) => plan.schema(),
            Plan::ShowTables(_) => Arc::new(DataSchema::empty()),
            Plan::ShowCreateTable(_) => Arc::new(DataSchema::empty()),
            Plan::DescribeTable(plan) => plan.schema(),
            Plan::ShowTablesStatus(_) => Arc::new(DataSchema::empty()),
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
            Plan::ShowUsers => Arc::new(DataSchema::empty()),
            Plan::AlterUser(plan) => plan.schema(),
            Plan::CreateUser(plan) => plan.schema(),
            Plan::DropUser(plan) => plan.schema(),
            Plan::ShowRoles => Arc::new(DataSchema::empty()),
            Plan::CreateRole(plan) => plan.schema(),
            Plan::DropRole(plan) => plan.schema(),
            Plan::GrantRole(plan) => plan.schema(),
            Plan::GrantPriv(plan) => plan.schema(),
            Plan::ShowGrants(_) => Arc::new(DataSchema::empty()),
            Plan::ShowStages => Arc::new(DataSchema::empty()),
            Plan::ListStage(plan) => plan.schema(),
            Plan::DescribeStage(plan) => plan.schema(),
            Plan::CreateStage(plan) => plan.schema(),
            Plan::DropStage(plan) => plan.schema(),
            Plan::RemoveStage(plan) => plan.schema(),
            Plan::RevokePriv(_) => Arc::new(DataSchema::empty()),
            Plan::RevokeRole(_) => Arc::new(DataSchema::empty()),
            Plan::Insert(plan) => plan.schema(),
        }
    }
}
