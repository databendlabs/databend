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

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum QueryKind {
    Unknown,
    Query,
    Explain,
    ExplainAst,
    ExplainSyntax,
    ExplainAnalyze,
    Copy,
    // Catalogs
    ShowCreateCatalog,
    CreateCatalog,
    DropCatalog,
    // Databases
    ShowCreateDatabase,
    CreateDatabase,
    DropDatabase,
    UndropDatabase,
    RenameDatabase,
    UseDatabase,
    // Tables
    ShowCreateTable,
    DescribeTable,
    CreateTable,
    DropTable,
    UndropTable,
    RenameTable,
    RenameTableColumn,
    AddTableColumn,
    DropTableColumn,
    ModifyTableColumn,
    AlterTableClusterKey,
    DropTableClusterKey,
    ReclusterTable,
    RevertTable,
    TruncateTable,
    OptimizeTable,
    VacuumTable,
    VacuumDropTable,
    AnalyzeTable,
    ExistsTable,
    SetOptions,

    // Insert
    Insert,
    Replace,
    Delete,
    Update,
    MergeInto,
    // Views
    CreateView,
    AlterView,
    DropView,

    // Indexes
    CreateIndex,
    DropIndex,
    RefreshIndex,

    // Virtual Columns
    CreateVirtualColumn,
    AlterVirtualColumn,
    DropVirtualColumn,
    RefreshVirtualColumn,

    // Account
    AlterUser,
    CreateUser,
    DropUser,

    // UDF
    CreateUDF,
    AlterUDF,
    DropUDF,

    // Role
    ShowRoles,
    CreateRole,
    DropRole,
    GrantRole,
    GrantPriv,
    ShowGrants,
    RevokePriv,
    RevokeRole,
    SetRole,

    // FileFormat
    CreateFileFormat,
    DropFileFormat,
    ShowFileFormats,

    // Stages
    CreateStage,
    DropStage,
    RemoveStage,

    // Presign
    Presign,

    // Set
    SetVariable,
    UnSetVariable,
    Kill,

    // Share
    CreateShareEndpoint,
    ShowShareEndpoint,
    DropShareEndpoint,
    CreateShare,
    DropShare,
    GrantShareObject,
    RevokeShareObject,
    AlterShareTenants,
    DescShare,
    ShowShares,
    ShowObjectGrantPrivileges,
    ShowGrantTenantsOfShare,

    // Data mask
    CreateDatamaskPolicy,
    DropDatamaskPolicy,
    DescDatamaskPolicy,

    // Network policy
    CreateNetworkPolicy,
    AlterNetworkPolicy,
    DropNetworkPolicy,
    DescNetworkPolicy,
    ShowNetworkPolicies,
}

impl Display for QueryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
