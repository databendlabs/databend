// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use common_meta_types::AuthType;
use common_meta_types::UserIdentity;
use common_meta_types::UserOption;
use common_meta_types::UserOptionFlag;

use super::write_space_seperated_list;
use super::Expr;
use crate::ast::expr::Literal;
use crate::ast::expr::TypeName;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::parser::token::Token;

// SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    Explain {
        kind: ExplainKind,
        query: Box<Statement<'a>>,
    },
    Query(Box<Query<'a>>),

    // Databases
    ShowDatabases {
        limit: Option<ShowLimit<'a>>,
    },
    ShowCreateDatabase {
        database: Identifier<'a>,
    },
    CreateDatabase(CreateDatabaseStmt<'a>),
    DropDatabase(DropDatabaseStmt<'a>),
    AlterDatabase {
        if_exists: bool,
        database: Identifier<'a>,
        action: AlterDatabaseAction<'a>,
    },
    UseDatabase {
        database: Identifier<'a>,
    },

    // Tables
    ShowTables {
        database: Option<Identifier<'a>>,
        full: bool,
        limit: Option<ShowLimit<'a>>,
        with_history: bool,
    },
    ShowCreateTable {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
    ShowTablesStatus {
        database: Option<Identifier<'a>>,
        limit: Option<ShowLimit<'a>>,
    },
    CreateTable(CreateTableStmt<'a>),
    // Describe schema of a table
    // Like `SHOW CREATE TABLE`
    Describe {
        catalog: Option<Identifier<'a>>,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
    DropTable {
        if_exists: bool,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        all: bool,
    },
    UnDropTable {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
    AlterTable {
        if_exists: bool,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        action: AlterTableAction<'a>,
    },
    RenameTable {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        new_table: Identifier<'a>,
    },
    TruncateTable {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        purge: bool,
    },
    OptimizeTable {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        action: Option<OptimizeTableAction>,
    },

    // Views
    CreateView(CreateViewStmt<'a>),
    AlterView(AlterViewStmt<'a>),
    DropView {
        if_exists: bool,
        database: Option<Identifier<'a>>,
        view: Identifier<'a>,
    },

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

    Insert {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        columns: Vec<Identifier<'a>>,
        source: InsertSource<'a>,
        overwrite: bool,
    },

    // User
    CreateUser(CreateUserStmt),
    AlterUser {
        // None means current user
        user: Option<UserIdentity>,
        // None means no change to make
        auth_option: Option<AuthOption>,
        role_options: Vec<RoleOption>,
    },
    DropUser {
        if_exists: bool,
        user: UserIdentity,
    },

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
    // stages
    CreateStage(CreateStageStmt),
    ShowStages,
    DropStage {
        if_exists: bool,
        stage_name: String,
    },
    DescStage {
        stage_name: String,
    },
    RemoveStage {
        stage_name: String,
    },
    ListStage {
        stage_name: String,
        pattern: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExplainKind {
    Syntax,
    Graph,
    Pipeline,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource<'a> {
    Streaming { format: String },
    Values { values_tokens: &'a [Token<'a>] },
    Select { query: Box<Query<'a>> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabaseStmt<'a> {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
    pub engine: Option<DatabaseEngine>,
    pub options: Vec<SQLProperty>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropDatabaseStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Identifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt<'a> {
    pub if_not_exists: bool,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub source: Option<CreateTableSource<'a>>,
    pub table_options: Vec<TableOption>,
    pub cluster_by: Vec<Expr<'a>>,
    pub as_query: Option<Box<Query<'a>>>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateTableSource<'a> {
    Columns(Vec<ColumnDefinition<'a>>),
    Like {
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableOption {
    Engine(Engine),
    Comment(String),
}

impl TableOption {
    pub fn option_key(&self) -> String {
        match self {
            TableOption::Engine(_) => "ENGINE".to_string(),
            TableOption::Comment(_) => "COMMENT".to_string(),
        }
    }

    pub fn option_value(&self) -> String {
        match self {
            TableOption::Engine(engine) => engine.to_string(),
            TableOption::Comment(comment) => comment.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    Null,
    Memory,
    Fuse,
    Github,
    View,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseEngine {
    Default,
    Github(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt<'a> {
    pub if_not_exists: bool,
    pub database: Option<Identifier<'a>>,
    pub view: Identifier<'a>,
    pub query: Box<Query<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewStmt<'a> {
    pub database: Option<Identifier<'a>>,
    pub view: Identifier<'a>,
    pub query: Box<Query<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SQLProperty {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowLimit<'a> {
    Like { pattern: String },
    Where { selection: Box<Expr<'a>> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition<'a> {
    pub name: Identifier<'a>,
    pub data_type: TypeName,
    pub nullable: bool,
    pub default_expr: Option<Box<Expr<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterDatabaseAction<'a> {
    RenameDatabase { new_db: Identifier<'a> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction<'a> {
    RenameTable { new_table: Identifier<'a> },
    AlterClusterKey { cluster_by: Vec<Expr<'a>> },
    DropClusterKey,
    // TODO(wuzhiguo): AddColumn etc
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimizeTableAction {
    All,
    Purge,
    Compact,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStageStmt {
    pub if_not_exists: bool,
    pub stage_name: String,

    pub location: String,
    pub credential_options: BTreeMap<String, String>,
    pub encryption_options: BTreeMap<String, String>,

    pub file_format_options: BTreeMap<String, String>,
    pub on_error: String,
    pub size_limit: usize,
    pub validation_mode: String,
    pub comments: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KillTarget {
    Query,
    Connection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserStmt {
    pub if_not_exists: bool,
    pub user: UserIdentity,
    pub auth_option: AuthOption,
    pub role_options: Vec<RoleOption>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthOption {
    pub auth_type: Option<AuthType>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RoleOption {
    TenantSetting,
    NoTenantSetting,
    ConfigReload,
    NoConfigReload,
}

impl RoleOption {
    pub fn apply(&self, option: &mut UserOption) {
        match self {
            Self::TenantSetting => {
                option.set_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::NoTenantSetting => {
                option.unset_option_flag(UserOptionFlag::TenantSetting);
            }
            Self::ConfigReload => {
                option.set_option_flag(UserOptionFlag::ConfigReload);
            }
            Self::NoConfigReload => {
                option.unset_option_flag(UserOptionFlag::ConfigReload);
            }
        }
    }
}

impl<'a> Display for ShowLimit<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ShowLimit::Like { pattern } => write!(f, "LIKE {pattern}"),
            ShowLimit::Where { selection } => write!(f, "WHERE {selection}"),
        }
    }
}

impl<'a> Display for ColumnDefinition<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if self.nullable {
            write!(f, " NULL")?;
        } else {
            write!(f, " NOT NULL")?;
        }
        if let Some(default_expr) = &self.default_expr {
            write!(f, " DEFAULT {default_expr}")?;
        }
        Ok(())
    }
}

impl Display for TableOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOption::Engine(engine) => write!(f, "ENGINE = {engine}"),
            TableOption::Comment(comment) => write!(f, "COMMENT = {comment}"),
        }
    }
}

impl Display for DatabaseEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let DatabaseEngine::Github(token) = self {
            write!(f, "GITHUB(token=\'{token}\')")
        } else {
            write!(f, "DEFAULT")
        }
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Null => write!(f, "NULL"),
            Engine::Memory => write!(f, "MEMORY"),
            Engine::Fuse => write!(f, "FUSE"),
            Engine::Github => write!(f, "GITHUB"),
            Engine::View => write!(f, "VIEW"),
        }
    }
}

impl Display for KillTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KillTarget::Query => write!(f, "QUERY"),
            KillTarget::Connection => write!(f, "CONNECTION"),
        }
    }
}

impl Display for RoleOption {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RoleOption::TenantSetting => write!(f, "TENANTSETTING"),
            RoleOption::NoTenantSetting => write!(f, "NOTENANTSETTING"),
            RoleOption::ConfigReload => write!(f, "CONFIGRELOAD"),
            RoleOption::NoConfigReload => write!(f, "NOCONFIGRELOAD"),
        }
    }
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
            Statement::ShowDatabases { limit } => {
                write!(f, "SHOW DATABASES")?;
                if let Some(limit) = limit {
                    write!(f, " {limit}")?;
                }
            }
            Statement::ShowCreateDatabase { database } => {
                write!(f, "SHOW CREATE DATABASE {database}")?;
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
            Statement::AlterDatabase {
                if_exists,
                database,
                action,
            } => {
                write!(f, "ALTER DATABASE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {database}")?;
                match action {
                    AlterDatabaseAction::RenameDatabase { new_db } => {
                        write!(f, " RENAME TO {new_db}")?;
                    }
                }
            }
            Statement::UseDatabase { database } => {
                write!(f, "USE {database}")?;
            }
            Statement::ShowTables {
                database,
                full,
                limit,
                with_history,
            } => {
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
            Statement::ShowCreateTable { database, table } => {
                write!(f, "SHOW CREATE TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
            }
            Statement::ShowTablesStatus { database, limit } => {
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
                database,
                table,
                source,
                table_options,
                comment,
                cluster_by,
                as_query,
            }) => {
                write!(f, "CREATE TABLE ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                match source {
                    Some(CreateTableSource::Columns(columns)) => {
                        write!(f, " (")?;
                        write_comma_separated_list(f, columns)?;
                        write!(f, ")")?;
                    }
                    Some(CreateTableSource::Like { database, table }) => {
                        write!(f, " LIKE ")?;
                        write_period_separated_list(f, database.iter().chain(Some(table)))?;
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
            Statement::Describe {
                catalog,
                database,
                table,
            } => {
                write!(f, "DESCRIBE ")?;
                write_period_separated_list(
                    f,
                    catalog.iter().chain(database.iter().chain(Some(table))),
                )?;
            }
            Statement::DropTable {
                if_exists,
                database,
                table,
                all,
            } => {
                write!(f, "DROP TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                if *all {
                    write!(f, " ALL")?;
                }
            }
            Statement::UnDropTable { database, table } => {
                write!(f, "UNDROP TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
            }
            Statement::AlterTable {
                if_exists,
                database,
                table,
                action,
            } => {
                write!(f, "ALTER TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                match action {
                    AlterTableAction::RenameTable { new_table } => {
                        write!(f, " RENAME TO {new_table}")?;
                    }
                    AlterTableAction::AlterClusterKey { cluster_by } => {
                        write!(f, " CLUSTER BY ")?;
                        write_comma_separated_list(f, cluster_by)?;
                    }
                    AlterTableAction::DropClusterKey => {
                        write!(f, " DROP CLUSTER KEY")?;
                    }
                }
            }
            Statement::RenameTable {
                database,
                table,
                new_table,
            } => {
                write!(f, "RENAME TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                write!(f, " TO {new_table}")?;
            }
            Statement::TruncateTable {
                database,
                table,
                purge,
            } => {
                write!(f, "TRUNCATE TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                if *purge {
                    write!(f, " PURGE")?;
                }
            }
            Statement::OptimizeTable {
                database,
                table,
                action,
            } => {
                write!(f, "OPTIMIZE TABLE ")?;
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                if let Some(action) = action {
                    match action {
                        OptimizeTableAction::All => write!(f, " ALL")?,
                        OptimizeTableAction::Purge => write!(f, " PURGE")?,
                        OptimizeTableAction::Compact => write!(f, " COMPACT")?,
                    }
                }
            }
            Statement::CreateView(CreateViewStmt {
                if_not_exists,
                database,
                view,
                query,
            }) => {
                write!(f, "CREATE VIEW ")?;
                if *if_not_exists {
                    write!(f, "IF NOT EXISTS ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(view)))?;
                write!(f, " AS {query}")?;
            }
            Statement::AlterView(AlterViewStmt {
                database,
                view,
                query,
            }) => {
                write!(f, "ALTER VIEW ")?;
                write_period_separated_list(f, database.iter().chain(Some(view)))?;
                write!(f, " AS {query}")?;
            }
            Statement::DropView {
                if_exists,
                database,
                view,
            } => {
                write!(f, "DROP VIEW ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(view)))?;
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
            Statement::Insert {
                database,
                table,
                columns,
                source,
                overwrite,
            } => {
                write!(f, "INSERT ")?;
                if *overwrite {
                    write!(f, "OVERWRITE ")?;
                } else {
                    write!(f, "INTO ")?;
                }
                write_period_separated_list(f, database.iter().chain(Some(table)))?;
                if !columns.is_empty() {
                    write!(f, " (")?;
                    write_comma_separated_list(f, columns)?;
                    write!(f, ")")?;
                }
                match source {
                    InsertSource::Streaming { format } => write!(f, " FORMAT {format}")?,
                    InsertSource::Values { values_tokens } => write!(
                        f,
                        " VALUES {}",
                        &values_tokens[0].source[values_tokens.first().unwrap().span.start
                            ..values_tokens.last().unwrap().span.end]
                    )?,
                    InsertSource::Select { query } => write!(f, " {query}")?,
                }
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
            Statement::AlterUser {
                user,
                auth_option,
                role_options,
            } => {
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
            Statement::ListStage {
                stage_name,
                pattern,
            } => {
                write!(f, "LIST @{stage_name}")?;
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
            Statement::RemoveStage { stage_name } => {
                write!(f, "REMOVE STAGE @{stage_name}")?;
            }
            Statement::DescStage { stage_name } => {
                write!(f, "DESC STAGE {stage_name}")?;
            }
        }
        Ok(())
    }
}
