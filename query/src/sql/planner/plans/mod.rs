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

pub use aggregate::Aggregate;
pub use apply::CrossApply;
use common_ast::ast::ExplainKind;
use common_planners::*;
pub use copy_v2::CopyPlanV2;
pub use copy_v2::ValidationMode;
pub use eval_scalar::EvalScalar;
pub use eval_scalar::ScalarItem;
pub use filter::Filter;
pub use hash_join::PhysicalHashJoin;
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
        }
    }
}
