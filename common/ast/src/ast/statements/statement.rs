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

use std::fmt::Display;
use std::fmt::Formatter;

use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;

use super::*;
use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Literal;
use crate::ast::Query;

// SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    Query(Box<Query<'a>>),
    Explain {
        kind: ExplainKind,
        query: Box<Statement<'a>>,
    },

    Copy(CopyStmt<'a>),

    ShowSettings,
    ShowProcessList,
    ShowMetrics,
    ShowFunctions {
        limit: Option<ShowLimit<'a>>,
    },

    KillStmt {
        kill_target: KillTarget,
        object_id: Identifier<'a>,
    },

    SetVariable {
        variable: Identifier<'a>,
        value: Literal,
    },

    Insert(InsertStmt<'a>),

    // Databases
    ShowDatabases(ShowDatabasesStmt<'a>),
    ShowCreateDatabase(ShowCreateDatabaseStmt<'a>),
    CreateDatabase(CreateDatabaseStmt<'a>),
    DropDatabase(DropDatabaseStmt<'a>),
    AlterDatabase(AlterDatabaseStmt<'a>),
    UseDatabase {
        database: Identifier<'a>,
    },

    // Tables
    ShowTables(ShowTablesStmt<'a>),
    ShowCreateTable(ShowCreateTableStmt<'a>),
    DescribeTable(DescribeTableStmt<'a>),
    ShowTablesStatus(ShowTablesStatusStmt<'a>),
    CreateTable(CreateTableStmt<'a>),
    DropTable(DropTableStmt<'a>),
    UndropTable(UndropTableStmt<'a>),
    AlterTable(AlterTableStmt<'a>),
    RenameTable(RenameTableStmt<'a>),
    TruncateTable(TruncateTableStmt<'a>),
    OptimizeTable(OptimizeTableStmt<'a>),
    ExistsTable(ExistsTableStmt<'a>),

    // Views
    CreateView(CreateViewStmt<'a>),
    AlterView(AlterViewStmt<'a>),
    DropView(DropViewStmt<'a>),

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
    CreateUDF {
        if_not_exists: bool,
        udf_name: Identifier<'a>,
        parameters: Vec<Identifier<'a>>,
        definition: Box<Expr<'a>>,
        description: Option<String>,
    },
    DropUDF {
        if_exists: bool,
        udf_name: Identifier<'a>,
    },
    AlterUDF {
        udf_name: Identifier<'a>,
        parameters: Vec<Identifier<'a>>,
        definition: Box<Expr<'a>>,
        description: Option<String>,
    },

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
        pattern: String,
    },
}

impl<'a> Display for Statement<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Explain { kind, query } => {
                write!(f, "EXPLAIN")?;
                match *kind {
                    ExplainKind::Syntax => (),
                    ExplainKind::Graph => write!(f, " GRAPH")?,
                    ExplainKind::Pipeline => write!(f, " PIPELINE")?,
                }
                write!(f, " {query}")?;
            }
            Statement::Query(query) => {
                write!(f, "{query}")?;
            }
            Statement::Insert(insert) => {
                write!(f, "{insert}")?;
            }
            Statement::Copy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowSettings => {}
            Statement::ShowProcessList => write!(f, "SHOW PROCESSLIST")?,
            Statement::ShowMetrics => write!(f, "SHOW METRICS")?,
            Statement::ShowFunctions { limit } => {
                write!(f, "SHOW FUNCTIONS")?;
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
                write!(f, " {object_id}")?;
            }
            Statement::SetVariable { variable, value } => write!(f, "SET {variable} = {value}")?,
            Statement::ShowDatabases(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::AlterDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UseDatabase { database } => write!(f, "USE {database}")?,
            Statement::ShowTables(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTable(stmt) => {
                write!(f, "{stmt}")?;
            }
            Statement::ShowTablesStatus(stmt) => {
                write!(f, "{stmt}")?;
            }
            Statement::CreateTable(stmt) => {
                write!(f, "{stmt}")?;
            }
            Statement::DropTable(stmt) => write!(f, "{stmt}")?,
            Statement::UndropTable(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTable(stmt) => write!(f, "{stmt}")?,
            Statement::RenameTable(stmt) => write!(f, "{stmt}")?,
            Statement::TruncateTable(stmt) => write!(f, "{stmt}")?,
            Statement::OptimizeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ExistsTable(stmt) => write!(f, "{stmt}")?,

            Statement::CreateView(stmt) => write!(f, "{stmt}")?,
            Statement::AlterView(stmt) => write!(f, "{stmt}")?,
            Statement::DropView(stmt) => write!(f, "{stmt}")?,
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
            Statement::CreateUDF {
                if_not_exists,
                udf_name,
                parameters,
                definition,
                description,
            } => {
                write!(f, "CREATE FUNCTION")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {udf_name} AS (")?;
                write_comma_separated_list(f, parameters)?;
                write!(f, ") -> {definition}")?;
                if let Some(description) = description {
                    write!(f, " DESC = '{description}'")?;
                }
            }
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
            Statement::AlterUDF {
                udf_name,
                parameters,
                definition,
                description,
            } => {
                write!(f, "ALTER FUNCTION {udf_name} AS (")?;
                write_comma_separated_list(f, parameters)?;
                write!(f, ") -> {definition}")?;
                if let Some(description) = description {
                    write!(f, " DESC = '{description}'")?;
                }
            }
            Statement::ListStage { location, pattern } => {
                write!(f, "LIST @{location}")?;
                if !pattern.is_empty() {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::ShowStages => {
                write!(f, "SHOW STAGES")?;
            }
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
            Statement::CreateStage(stmt) => {
                write!(f, "CREATE STAGE")?;
                if stmt.if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {}", stmt.stage_name)?;

                if !stmt.location.is_empty() {
                    write!(f, " URL = '{}'", stmt.location)?;

                    if !stmt.credential_options.is_empty() {
                        write!(f, " CREDENTIALS = (")?;
                        for (k, v) in stmt.credential_options.iter() {
                            write!(f, " {} = '{}'", k, v)?;
                        }
                        write!(f, " )")?;
                    }

                    if !stmt.encryption_options.is_empty() {
                        write!(f, " ENCRYPTION = (")?;
                        for (k, v) in stmt.encryption_options.iter() {
                            write!(f, " {} = '{}'", k, v)?;
                        }
                        write!(f, " )")?;
                    }
                }

                if !stmt.file_format_options.is_empty() {
                    write!(f, " FILE_FORMAT = (")?;
                    for (k, v) in stmt.file_format_options.iter() {
                        write!(f, " {} = '{}'", k, v)?;
                    }
                    write!(f, " )")?;
                }

                if !stmt.on_error.is_empty() {
                    write!(f, " ON_ERROR = {}", stmt.on_error)?;
                }

                if stmt.size_limit != 0 {
                    write!(f, " SIZE_LIMIT = {}", stmt.size_limit)?;
                }

                if !stmt.validation_mode.is_empty() {
                    write!(f, " VALIDATION_MODE = {}", stmt.validation_mode)?;
                }

                if !stmt.comments.is_empty() {
                    write!(f, " COMMENTS = '{}'", stmt.comments)?;
                }
            }
            Statement::RemoveStage { location, pattern } => {
                write!(f, "REMOVE STAGE @{location}")?;
                if !pattern.is_empty() {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
            }
            Statement::DescribeStage { stage_name } => {
                write!(f, "DESC STAGE {stage_name}")?;
            }
        }
        Ok(())
    }
}
