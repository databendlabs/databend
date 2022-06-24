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

use crate::ast::statements::show::ShowLimit;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::write_space_seperated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq)] // Tables
pub struct ShowTablesStmt<'a> {
    pub database: Option<Identifier<'a>>,
    pub full: bool,
    pub limit: Option<ShowLimit<'a>>,
    pub with_history: bool,
}

impl Display for ShowTablesStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW")?;
        if self.full {
            write!(f, " FULL")?;
        }
        write!(f, " TABLES")?;
        if self.with_history {
            write!(f, " HISTORY")?;
        }
        if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
}

impl Display for ShowCreateTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW CREATE TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStatusStmt<'a> {
    pub database: Option<Identifier<'a>>,
    pub limit: Option<ShowLimit<'a>>,
}

impl Display for ShowTablesStatusStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW TABLE STATUS")?;
        if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt<'a> {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub source: Option<CreateTableSource<'a>>,
    pub table_options: Vec<TableOption>,
    pub cluster_by: Vec<Expr<'a>>,
    pub as_query: Option<Box<Query<'a>>>,
    pub comment: Option<String>,
    pub transient: bool,
}

impl Display for CreateTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if self.transient {
            write!(f, "TRANSIENT ")?;
        }
        write!(f, "TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        if let Some(source) = &self.source {
            write!(f, " {source}")?;
        }

        // Format table options
        write_space_seperated_list(f, self.table_options.iter())?;

        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {comment}")?;
        }
        if !self.cluster_by.is_empty() {
            write!(f, " CLUSTER BY ")?;
            write_comma_separated_list(f, &self.cluster_by)?;
        }
        if let Some(as_query) = &self.as_query {
            write!(f, " AS {as_query}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateTableSource<'a> {
    Columns(Vec<ColumnDefinition<'a>>),
    Like {
        catalog: Option<Identifier<'a>>,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
}

impl Display for CreateTableSource<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CreateTableSource::Columns(columns) => {
                write!(f, "(")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                write!(f, "LIKE ")?;
                write_period_separated_list(f, catalog.iter().chain(database).chain(Some(table)))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
}

impl Display for DescribeTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DESCRIBE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter().chain(Some(&self.table))),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub all: bool,
}

impl Display for DropTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if self.all {
            write!(f, " ALL")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UndropTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
}

impl Display for UndropTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UNDROP TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub action: AlterTableAction<'a>,
}

impl Display for AlterTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " {}", self.action)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction<'a> {
    RenameTable { new_table: Identifier<'a> },
    AlterTableClusterKey { cluster_by: Vec<Expr<'a>> },
    DropTableClusterKey,
}

impl Display for AlterTableAction<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableAction::RenameTable { new_table } => {
                write!(f, "RENAME TO {new_table}")
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                write!(f, "CLUSTER BY ")?;
                write_comma_separated_list(f, cluster_by)
            }
            AlterTableAction::DropTableClusterKey => {
                write!(f, "DROP CLUSTER KEY")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt<'a> {
    pub if_exists: bool,
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub new_catalog: Option<Identifier<'a>>,
    pub new_database: Option<Identifier<'a>>,
    pub new_table: Identifier<'a>,
}

impl Display for RenameTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RENAME TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " TO ")?;
        write_period_separated_list(
            f,
            self.new_catalog
                .iter()
                .chain(&self.new_database)
                .chain(Some(&self.new_table)),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TruncateTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub purge: bool,
}

impl Display for TruncateTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TRUNCATE TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if self.purge {
            write!(f, " PURGE")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptimizeTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
    pub action: Option<OptimizeTableAction>,
}

impl Display for OptimizeTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OPTIMIZE TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        if let Some(action) = &self.action {
            write!(f, " {action}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExistsTableStmt<'a> {
    pub catalog: Option<Identifier<'a>>,
    pub database: Option<Identifier<'a>>,
    pub table: Identifier<'a>,
}

impl Display for ExistsTableStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXISTS TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    Null,
    Memory,
    Fuse,
    Github,
    View,
    Random,
}

impl Display for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Null => write!(f, "NULL"),
            Engine::Memory => write!(f, "MEMORY"),
            Engine::Fuse => write!(f, "FUSE"),
            Engine::Github => write!(f, "GITHUB"),
            Engine::View => write!(f, "VIEW"),
            Engine::Random => write!(f, "RANDOM"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptimizeTableAction {
    All,
    Purge,
    Compact,
}

impl Display for OptimizeTableAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizeTableAction::All => write!(f, "ALL"),
            OptimizeTableAction::Purge => write!(f, "PURGE"),
            OptimizeTableAction::Compact => write!(f, "COMPACT"),
        }
    }
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

impl Display for TableOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOption::Engine(engine) => write!(f, "ENGINE = {engine}"),
            TableOption::Comment(comment) => write!(f, "COMMENT = {comment}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition<'a> {
    pub name: Identifier<'a>,
    pub data_type: TypeName,
    pub nullable: bool,
    pub default_expr: Option<Box<Expr<'a>>>,
    pub comment: Option<String>,
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
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT '{comment}'")?;
        }
        Ok(())
    }
}
