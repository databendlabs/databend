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
use crate::ast::write_period_separated_list;
use crate::ast::write_quoted_comma_separated_list;
use crate::ast::write_space_seperated_list;
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
    Grant(AccountMgrStatement),
    ShowGrants {
        principal: Option<PrincipalIdentity>,
    },
    Revoke(AccountMgrStatement),

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
            Statement::Copy(stmt) => {
                write!(f, "COPY")?;
                write!(f, " INTO {}", stmt.dst)?;
                write!(f, " FROM {}", stmt.src)?;

                if !stmt.file_format.is_empty() {
                    write!(f, " FILE_FORMAT = (")?;
                    for (k, v) in stmt.file_format.iter() {
                        write!(f, " {} = '{}'", k, v)?;
                    }
                    write!(f, " )")?;
                }

                if !stmt.files.is_empty() {
                    write!(f, " FILES = (")?;
                    write_quoted_comma_separated_list(f, &stmt.files)?;
                    write!(f, " )")?;
                }

                if !stmt.pattern.is_empty() {
                    write!(f, " PATTERN = '{}'", stmt.pattern)?;
                }

                if stmt.size_limit != 0 {
                    write!(f, " SIZE_LIMIT = {}", stmt.size_limit)?;
                }

                if !stmt.validation_mode.is_empty() {
                    write!(f, "VALIDATION_MODE = {}", stmt.validation_mode)?;
                }
            }
            Statement::ShowSettings => {
                write!(f, "SHOW SETTINGS")?;
            }
            Statement::ShowProcessList => {
                write!(f, "SHOW PROCESSLIST")?;
            }
            Statement::ShowMetrics => {
                write!(f, "SHOW METRICS")?;
            }
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
            Statement::SetVariable { variable, value } => {
                write!(f, "SET {variable} = {value}")?;
            }
            Statement::ShowDatabases(ShowDatabasesStmt { limit }) => {
                write!(f, "SHOW DATABASES")?;
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::ShowCreateDatabase(ShowCreateDatabaseStmt { catalog, database }) => {
                write!(f, "SHOW CREATE DATABASE ")?;
                write_period_separated_list(f, catalog.iter().chain(Some(database)))?;
            }
            Statement::CreateDatabase(CreateDatabaseStmt {
                if_not_exists,
                catalog,
                database,
                engine,
                ..
            }) => {
                write!(f, "CREATE DATABASE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(Some(database)))?;
                if let Some(engine) = engine {
                    write!(f, " ENGINE = {engine}")?;
                }
                // TODO(leiysky): display rest information
            }
            Statement::DropDatabase(DropDatabaseStmt {
                if_exists,
                catalog,
                database,
            }) => {
                write!(f, "DROP DATABASE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(Some(database)))?;
            }
            Statement::AlterDatabase(AlterDatabaseStmt {
                if_exists,
                catalog,
                database,
                action,
            }) => {
                write!(f, "ALTER DATABASE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(Some(database)))?;
                match action {
                    AlterDatabaseAction::RenameDatabase { new_db } => {
                        write!(f, " RENAME TO {new_db}")?;
                    }
                }
            }
            Statement::UseDatabase { database } => {
                write!(f, "USE {database}")?;
            }
            Statement::ShowTables(ShowTablesStmt {
                database,
                full,
                limit,
                with_history,
            }) => {
                write!(f, "SHOW")?;
                if *full {
                    write!(f, " FULL")?;
                }
                write!(f, " TABLES")?;
                if *with_history {
                    write!(f, " HISTORY")?;
                }
                if let Some(database) = database {
                    write!(f, " FROM {database}")?;
                }
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::ShowCreateTable(ShowCreateTableStmt {
                catalog,
                database,
                table,
            }) => {
                write!(f, "SHOW CREATE TABLE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
            }
            Statement::DescribeTable(DescribeTableStmt {
                catalog,
                database,
                table,
            }) => {
                write!(f, "DESCRIBE ")?;
                write_period_separated_list(
                    f,
                    catalog.iter().chain(database.iter().chain(Some(table))),
                )?;
            }
            Statement::ShowTablesStatus(ShowTablesStatusStmt { database, limit }) => {
                write!(f, "SHOW TABLE STATUS")?;
                if let Some(database) = database {
                    write!(f, " FROM {database}")?;
                }
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::CreateTable(CreateTableStmt {
                if_not_exists,
                catalog,
                database,
                table,
                source,
                table_options,
                comment,
                cluster_by,
                as_query,
                transient,
            }) => {
                write!(f, "CREATE ")?;
                if *transient {
                    write!(f, "TRANSIENT ")?;
                }
                write!(f, "TABLE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                match source {
                    Some(CreateTableSource::Columns(columns)) => {
                        write!(f, " (")?;
                        write_comma_separated_list(f, columns)?;
                        write!(f, ")")?;
                    }
                    Some(CreateTableSource::Like {
                        catalog,
                        database,
                        table,
                    }) => {
                        write!(f, " LIKE ")?;
                        write_period_separated_list(
                            f,
                            catalog.iter().chain(database).chain(Some(table)),
                        )?;
                    }
                    None => (),
                }

                // Format table options
                write_space_seperated_list(f, table_options.iter())?;

                if let Some(comment) = comment {
                    write!(f, " COMMENT = {comment}")?;
                }
                if !cluster_by.is_empty() {
                    write!(f, " CLUSTER BY ")?;
                    write_comma_separated_list(f, cluster_by)?;
                }
                if let Some(as_query) = as_query {
                    write!(f, " AS {as_query}")?;
                }
            }
            Statement::DropTable(DropTableStmt {
                if_exists,
                catalog,
                database,
                table,
                all,
            }) => {
                write!(f, "DROP TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                if *all {
                    write!(f, " ALL")?;
                }
            }
            Statement::UndropTable(UndropTableStmt {
                catalog,
                database,
                table,
            }) => {
                write!(f, "UNDROP TABLE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
            }
            Statement::AlterTable(AlterTableStmt {
                if_exists,
                catalog,
                database,
                table,
                action,
            }) => {
                write!(f, "ALTER TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                match action {
                    AlterTableAction::RenameTable { new_table } => {
                        write!(f, " RENAME TO {new_table}")?;
                    }
                    AlterTableAction::AlterTableClusterKey { cluster_by } => {
                        write!(f, " CLUSTER BY ")?;
                        write_comma_separated_list(f, cluster_by)?;
                    }
                    AlterTableAction::DropTableClusterKey => {
                        write!(f, " DROP CLUSTER KEY")?;
                    }
                }
            }
            Statement::RenameTable(RenameTableStmt {
                if_exists,
                catalog,
                database,
                table,
                new_catalog,
                new_database,
                new_table,
            }) => {
                write!(f, "RENAME TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                write!(f, " TO ")?;
                write_period_separated_list(
                    f,
                    new_catalog
                        .iter()
                        .chain(new_database)
                        .chain(Some(new_table)),
                )?;
            }
            Statement::TruncateTable(TruncateTableStmt {
                catalog,
                database,
                table,
                purge,
            }) => {
                write!(f, "TRUNCATE TABLE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                if *purge {
                    write!(f, " PURGE")?;
                }
            }
            Statement::OptimizeTable(OptimizeTableStmt {
                catalog,
                database,
                table,
                action,
            }) => {
                write!(f, "OPTIMIZE TABLE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
                if let Some(action) = action {
                    write!(f, " {action}")?;
                }
            }
            Statement::ExistsTable(ExistsTableStmt {
                catalog,
                database,
                table,
            }) => {
                write!(f, "EXISTS TABLE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))?;
            }

            Statement::CreateView(CreateViewStmt {
                if_not_exists,
                catalog,
                database,
                view,
                query,
            }) => {
                write!(f, "CREATE VIEW ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(view)))?;
                write!(f, " AS {query}")?;
            }
            Statement::AlterView(AlterViewStmt {
                catalog,
                database,
                view,
                query,
            }) => {
                write!(f, "ALTER VIEW ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(view)))?;
                write!(f, " AS {query}")?;
            }
            Statement::DropView(DropViewStmt {
                if_exists,
                catalog,
                database,
                view,
            }) => {
                write!(f, "DROP VIEW ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(view)))?;
            }
            Statement::ShowUsers => {
                write!(f, "SHOW USERS")?;
            }
            Statement::ShowRoles => {
                write!(f, "SHOW ROLES")?;
            }
            Statement::CreateUser(CreateUserStmt {
                if_not_exists,
                user,
                auth_option,
                role_options,
            }) => {
                write!(f, "CREATE USER")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {user} IDENTIFIED")?;
                if let Some(auth_type) = &auth_option.auth_type {
                    write!(f, " WITH {}", auth_type.to_str())?;
                }
                if let Some(password) = &auth_option.password {
                    write!(f, " BY '{password}'")?;
                }
                if !role_options.is_empty() {
                    write!(f, " WITH")?;
                    for role_option in role_options {
                        write!(f, " {role_option}")?;
                    }
                }
            }
            Statement::AlterUser(AlterUserStmt {
                user,
                auth_option,
                role_options,
            }) => {
                write!(f, "ALTER USER")?;
                if let Some(user) = user {
                    write!(f, " {user}")?;
                } else {
                    write!(f, " USER()")?;
                }
                if let Some(auth_option) = &auth_option {
                    write!(f, " IDENTIFIED")?;
                    if let Some(auth_type) = &auth_option.auth_type {
                        write!(f, " WITH {}", auth_type.to_str())?;
                    }
                    if let Some(password) = &auth_option.password {
                        write!(f, " BY '{password}'")?;
                    }
                }
                if !role_options.is_empty() {
                    write!(f, " WITH")?;
                    for with_option in role_options {
                        write!(f, " {with_option}")?;
                    }
                }
            }
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
            Statement::Grant(AccountMgrStatement { source, principal }) => {
                write!(f, "GRANT")?;
                write!(f, "{source}")?;

                write!(f, " TO")?;
                write!(f, "{principal}")?;
            }
            Statement::ShowGrants { principal } => {
                write!(f, "SHOW GRANTS")?;
                if let Some(principal) = principal {
                    write!(f, " FOR")?;
                    write!(f, "{principal}")?;
                }
            }
            Statement::Revoke(AccountMgrStatement { source, principal }) => {
                write!(f, "REVOKE")?;
                write!(f, "{source}")?;

                write!(f, " FROM")?;
                write!(f, "{principal}")?;
            }
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
