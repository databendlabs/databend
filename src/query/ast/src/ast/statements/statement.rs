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

use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::PrincipalIdentity;
use common_meta_app::principal::UserIdentity;

use super::merge_into::MergeIntoStmt;
use super::*;
use crate::ast::statements::task::CreateTaskStmt;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableReference;

// SQL statement
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Box<Query>),
    Explain {
        kind: ExplainKind,
        query: Box<Statement>,
    },
    ExplainAnalyze {
        query: Box<Statement>,
    },

    CopyIntoTable(CopyIntoTableStmt),
    CopyIntoLocation(CopyIntoLocationStmt),

    Call(CallStmt),

    ShowSettings {
        like: Option<String>,
    },
    ShowProcessList,
    ShowMetrics,
    ShowEngines,
    ShowFunctions {
        limit: Option<ShowLimit>,
    },
    ShowTableFunctions {
        limit: Option<ShowLimit>,
    },
    ShowIndexes,

    KillStmt {
        kill_target: KillTarget,
        object_id: String,
    },

    SetVariable {
        is_global: bool,
        variable: Identifier,
        value: Box<Expr>,
    },

    UnSetVariable(UnSetStmt),

    SetRole {
        is_default: bool,
        role_name: String,
    },

    Insert(InsertStmt),
    Replace(ReplaceStmt),
    MergeInto(MergeIntoStmt),
    Delete {
        hints: Option<Hint>,
        table_reference: TableReference,
        selection: Option<Expr>,
    },

    Update(UpdateStmt),

    // Catalogs
    ShowCatalogs(ShowCatalogsStmt),
    ShowCreateCatalog(ShowCreateCatalogStmt),
    CreateCatalog(CreateCatalogStmt),
    DropCatalog(DropCatalogStmt),

    // Databases
    ShowDatabases(ShowDatabasesStmt),
    ShowCreateDatabase(ShowCreateDatabaseStmt),
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
    UndropDatabase(UndropDatabaseStmt),
    AlterDatabase(AlterDatabaseStmt),
    UseDatabase {
        database: Identifier,
    },

    // Tables
    ShowTables(ShowTablesStmt),
    ShowCreateTable(ShowCreateTableStmt),
    DescribeTable(DescribeTableStmt),
    ShowTablesStatus(ShowTablesStatusStmt),
    ShowDropTables(ShowDropTablesStmt),
    AttachTable(AttachTableStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    UndropTable(UndropTableStmt),
    AlterTable(AlterTableStmt),
    RenameTable(RenameTableStmt),
    TruncateTable(TruncateTableStmt),
    OptimizeTable(OptimizeTableStmt),
    VacuumTable(VacuumTableStmt),
    VacuumDropTable(VacuumDropTableStmt),
    AnalyzeTable(AnalyzeTableStmt),
    ExistsTable(ExistsTableStmt),
    // Columns
    ShowColumns(ShowColumnsStmt),

    // Views
    CreateView(CreateViewStmt),
    AlterView(AlterViewStmt),
    DropView(DropViewStmt),

    // Indexes
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    RefreshIndex(RefreshIndexStmt),

    // VirtualColumns
    CreateVirtualColumn(CreateVirtualColumnStmt),
    AlterVirtualColumn(AlterVirtualColumnStmt),
    DropVirtualColumn(DropVirtualColumnStmt),
    RefreshVirtualColumn(RefreshVirtualColumnStmt),

    // User
    ShowUsers,
    CreateUser(CreateUserStmt),
    AlterUser(AlterUserStmt),
    DropUser {
        if_exists: bool,
        user: UserIdentity,
    },
    ShowRoles,
    CreateRole {
        if_not_exists: bool,
        role_name: String,
    },
    DropRole {
        if_exists: bool,
        role_name: String,
    },
    Grant(GrantStmt),
    ShowGrants {
        principal: Option<PrincipalIdentity>,
    },
    Revoke(RevokeStmt),

    // UDF
    CreateUDF(CreateUDFStmt),
    DropUDF {
        if_exists: bool,
        udf_name: Identifier,
    },
    AlterUDF(AlterUDFStmt),

    // Stages
    CreateStage(CreateStageStmt),
    ShowStages,
    DropStage {
        if_exists: bool,
        stage_name: String,
    },
    DescribeStage {
        stage_name: String,
    },
    RemoveStage {
        location: String,
        pattern: String,
    },
    ListStage {
        location: String,
        pattern: Option<String>,
    },

    // UserDefinedFileFormat
    CreateFileFormat {
        if_not_exists: bool,
        name: String,
        file_format_options: FileFormatOptionsAst,
    },
    DropFileFormat {
        if_exists: bool,
        name: String,
    },
    ShowFileFormats,
    Presign(PresignStmt),

    // share
    CreateShareEndpoint(CreateShareEndpointStmt),
    ShowShareEndpoint(ShowShareEndpointStmt),
    DropShareEndpoint(DropShareEndpointStmt),
    CreateShare(CreateShareStmt),
    DropShare(DropShareStmt),
    GrantShareObject(GrantShareObjectStmt),
    RevokeShareObject(RevokeShareObjectStmt),
    AlterShareTenants(AlterShareTenantsStmt),
    DescShare(DescShareStmt),
    ShowShares(ShowSharesStmt),
    ShowObjectGrantPrivileges(ShowObjectGrantPrivilegesStmt),
    ShowGrantsOfShare(ShowGrantsOfShareStmt),

    // data mask
    CreateDatamaskPolicy(CreateDatamaskPolicyStmt),
    DropDatamaskPolicy(DropDatamaskPolicyStmt),
    DescDatamaskPolicy(DescDatamaskPolicyStmt),

    // network policy
    CreateNetworkPolicy(CreateNetworkPolicyStmt),
    AlterNetworkPolicy(AlterNetworkPolicyStmt),
    DropNetworkPolicy(DropNetworkPolicyStmt),
    DescNetworkPolicy(DescNetworkPolicyStmt),
    ShowNetworkPolicies,

    // tasks
    CreateTask(CreateTaskStmt),
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementMsg {
    pub(crate) stmt: Statement,
    pub(crate) format: Option<String>,
}

impl Statement {
    pub fn to_mask_sql(&self) -> String {
        match self {
            Statement::CopyIntoTable(copy) => {
                let mut copy_clone = copy.clone();

                if let CopyIntoTableSource::Location(FileLocation::Uri(location)) =
                    &mut copy_clone.src
                {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CopyIntoTable(copy_clone))
            }
            Statement::CopyIntoLocation(copy) => {
                let mut copy_clone = copy.clone();

                if let FileLocation::Uri(location) = &mut copy_clone.dst {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CopyIntoLocation(copy_clone))
            }
            Statement::CreateStage(stage) => {
                let mut stage_clone = stage.clone();
                if let Some(location) = &mut stage_clone.location {
                    location.connection = location.connection.mask()
                }
                format!("{}", Statement::CreateStage(stage_clone))
            }
            _ => format!("{}", self),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain { kind, query } => {
                write!(f, "EXPLAIN")?;
                match *kind {
                    ExplainKind::Ast(_) => write!(f, " AST")?,
                    ExplainKind::Syntax(_) => write!(f, " SYNTAX")?,
                    ExplainKind::Graph => write!(f, " GRAPH")?,
                    ExplainKind::Pipeline => write!(f, " PIPELINE")?,
                    ExplainKind::Fragments => write!(f, " FRAGMENTS")?,
                    ExplainKind::Raw => write!(f, " RAW")?,
                    ExplainKind::Plan => (),
                    ExplainKind::AnalyzePlan => write!(f, " ANALYZE")?,
                    ExplainKind::JOIN => write!(f, " JOIN")?,
                    ExplainKind::Memo(_) => write!(f, " MEMO")?,
                }
                write!(f, " {query}")?;
            }
            Statement::ExplainAnalyze { query } => {
                write!(f, "EXPLAIN ANALYZE {query}")?;
            }
            Statement::Query(query) => write!(f, "{query}")?,
            Statement::Insert(insert) => write!(f, "{insert}")?,
            Statement::Replace(replace) => write!(f, "{replace}")?,
            Statement::MergeInto(merge_into) => write!(f, "{merge_into}")?,
            Statement::Delete {
                table_reference,
                selection,
                hints,
            } => {
                write!(f, "DELETE FROM {table_reference} ")?;
                if let Some(hints) = hints {
                    write!(f, "{} ", hints)?;
                }
                if let Some(conditions) = selection {
                    write!(f, "WHERE {conditions} ")?;
                }
            }
            Statement::Update(update) => write!(f, "{update}")?,
            Statement::CopyIntoTable(stmt) => write!(f, "{stmt}")?,
            Statement::CopyIntoLocation(stmt) => write!(f, "{stmt}")?,
            Statement::ShowSettings { like } => {
                write!(f, "SHOW SETTINGS")?;
                if like.is_some() {
                    write!(f, " LIKE '{}'", like.as_ref().unwrap())?;
                }
            }
            Statement::ShowProcessList => write!(f, "SHOW PROCESSLIST")?,
            Statement::ShowMetrics => write!(f, "SHOW METRICS")?,
            Statement::ShowEngines => write!(f, "SHOW ENGINES")?,
            Statement::ShowIndexes => write!(f, "SHOW INDEXES")?,
            Statement::ShowFunctions { limit } => {
                write!(f, "SHOW FUNCTIONS")?;
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::ShowTableFunctions { limit } => {
                write!(f, "SHOW TABLE_FUNCTIONS")?;
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::KillStmt {
                kill_target,
                object_id,
            } => {
                write!(f, "KILL")?;
                match *kill_target {
                    KillTarget::Query => write!(f, " QUERY")?,
                    KillTarget::Connection => write!(f, " CONNECTION")?,
                }
                write!(f, " '{object_id}'")?;
            }
            Statement::SetVariable {
                is_global,
                variable,
                value,
            } => {
                write!(f, "SET ")?;
                if *is_global {
                    write!(f, "GLOBAL ")?;
                }
                write!(f, "{variable} = {value}")?;
            }
            Statement::UnSetVariable(unset) => write!(f, "{unset}")?,
            Statement::SetRole {
                is_default,
                role_name,
            } => {
                write!(f, "SET ROLE ")?;
                if *is_default {
                    write!(f, "DEFAULT")?;
                } else {
                    write!(f, "{role_name}")?;
                }
            }
            Statement::ShowCatalogs(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::CreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::DropCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDatabases(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UndropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::AlterDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UseDatabase { database } => write!(f, "USE {database}")?,
            Statement::ShowTables(stmt) => write!(f, "{stmt}")?,
            Statement::ShowColumns(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTablesStatus(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDropTables(stmt) => write!(f, "{stmt}")?,
            Statement::AttachTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DropTable(stmt) => write!(f, "{stmt}")?,
            Statement::UndropTable(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTable(stmt) => write!(f, "{stmt}")?,
            Statement::RenameTable(stmt) => write!(f, "{stmt}")?,
            Statement::TruncateTable(stmt) => write!(f, "{stmt}")?,
            Statement::OptimizeTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumDropTable(stmt) => write!(f, "{stmt}")?,
            Statement::AnalyzeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ExistsTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateView(stmt) => write!(f, "{stmt}")?,
            Statement::AlterView(stmt) => write!(f, "{stmt}")?,
            Statement::DropView(stmt) => write!(f, "{stmt}")?,
            Statement::CreateIndex(stmt) => write!(f, "{stmt}")?,
            Statement::DropIndex(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshIndex(stmt) => write!(f, "{stmt}")?,
            Statement::CreateVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::AlterVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::DropVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::ShowUsers => write!(f, "SHOW USERS")?,
            Statement::ShowRoles => write!(f, "SHOW ROLES")?,
            Statement::CreateUser(stmt) => write!(f, "{stmt}")?,
            Statement::AlterUser(stmt) => write!(f, "{stmt}")?,
            Statement::DropUser { if_exists, user } => {
                write!(f, "DROP USER")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {user}")?;
            }
            Statement::CreateRole {
                if_not_exists,
                role_name: role,
            } => {
                write!(f, "CREATE ROLE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " '{role}'")?;
            }
            Statement::DropRole {
                if_exists,
                role_name: role,
            } => {
                write!(f, "DROP ROLE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " '{role}'")?;
            }
            Statement::Grant(stmt) => write!(f, "{stmt}")?,
            Statement::ShowGrants { principal } => {
                write!(f, "SHOW GRANTS")?;
                if let Some(principal) = principal {
                    write!(f, " FOR")?;
                    write!(f, "{principal}")?;
                }
            }
            Statement::Revoke(stmt) => write!(f, "{stmt}")?,
            Statement::CreateUDF(stmt) => write!(f, "{stmt}")?,
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => {
                write!(f, "DROP FUNCTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {udf_name}")?;
            }
            Statement::AlterUDF(stmt) => write!(f, "{stmt}")?,
            Statement::ListStage { location, pattern } => {
                write!(f, "LIST @{location}")?;
                if let Some(pattern) = pattern {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::ShowStages => write!(f, "SHOW STAGES")?,
            Statement::DropStage {
                if_exists,
                stage_name,
            } => {
                write!(f, "DROP STAGES")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {stage_name}")?;
            }
            Statement::CreateStage(stmt) => write!(f, "{stmt}")?,
            Statement::RemoveStage { location, pattern } => {
                write!(f, "REMOVE STAGE @{location}")?;
                if !pattern.is_empty() {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::DescribeStage { stage_name } => write!(f, "DESC STAGE {stage_name}")?,
            Statement::CreateFileFormat {
                if_not_exists,
                name,
                file_format_options,
            } => {
                write!(f, "CREATE FILE_FORMAT")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {name}")?;
                write!(f, " {file_format_options}")?;
            }
            Statement::DropFileFormat { if_exists, name } => {
                write!(f, "DROP FILE_FORMAT")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")?;
            }
            Statement::ShowFileFormats => write!(f, "SHOW FILE FORMATS")?,
            Statement::Call(stmt) => write!(f, "{stmt}")?,
            Statement::Presign(stmt) => write!(f, "{stmt}")?,
            Statement::CreateShareEndpoint(stmt) => write!(f, "{stmt}")?,
            Statement::ShowShareEndpoint(stmt) => write!(f, "{stmt}")?,
            Statement::DropShareEndpoint(stmt) => write!(f, "{stmt}")?,
            Statement::CreateShare(stmt) => write!(f, "{stmt}")?,
            Statement::DropShare(stmt) => write!(f, "{stmt}")?,
            Statement::GrantShareObject(stmt) => write!(f, "{stmt}")?,
            Statement::RevokeShareObject(stmt) => write!(f, "{stmt}")?,
            Statement::AlterShareTenants(stmt) => write!(f, "{stmt}")?,
            Statement::DescShare(stmt) => write!(f, "{stmt}")?,
            Statement::ShowShares(stmt) => write!(f, "{stmt}")?,
            Statement::ShowObjectGrantPrivileges(stmt) => write!(f, "{stmt}")?,
            Statement::ShowGrantsOfShare(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::CreateNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::AlterNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowNetworkPolicies => write!(f, "SHOW NETWORK POLICIES")?,
            Statement::CreateTask(stmt) => {
                write!(f, "{stmt}", stmt = stmt)?;
            }
        }
        Ok(())
    }
}
