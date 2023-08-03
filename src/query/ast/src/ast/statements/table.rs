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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::statements::show::ShowLimit;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::write_space_separated_map;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableReference;
use crate::ast::TimeTravelPoint;
use crate::ast::TypeName;
use crate::ast::UriLocation;

#[derive(Debug, Clone, PartialEq)] // Tables
pub struct ShowTablesStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub full: bool,
    pub limit: Option<ShowLimit>,
    pub with_history: bool,
}

impl Display for ShowTablesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW")?;
        if self.full {
            write!(f, " FULL")?;
        }
        write!(f, " TABLES")?;
        if self.with_history {
            write!(f, " HISTORY")?;
        }
        if let Some(database) = &self.database {
            write!(f, " FROM ")?;
            if let Some(catalog) = &self.catalog {
                write!(f, "{catalog}.",)?;
            }
            write!(f, "{database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCreateTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for ShowCreateTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
pub struct ShowTablesStatusStmt {
    pub database: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowTablesStatusStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
pub struct ShowDropTablesStmt {
    pub database: Option<Identifier>,
}

impl Display for ShowDropTablesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW DROP TABLE")?;
        if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub if_not_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub source: Option<CreateTableSource>,
    pub engine: Option<Engine>,
    pub uri_location: Option<UriLocation>,
    pub cluster_by: Vec<Expr>,
    pub table_options: BTreeMap<String, String>,
    pub as_query: Option<Box<Query>>,
    pub transient: bool,
}

impl Display for CreateTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

        if let Some(engine) = &self.engine {
            write!(f, " ENGINE = {engine}")?;
        }

        if !self.cluster_by.is_empty() {
            write!(f, " CLUSTER BY (")?;
            write_comma_separated_list(f, &self.cluster_by)?;
            write!(f, ")")?
        }

        // Format table options
        write_space_separated_map(f, self.table_options.iter())?;
        if let Some(as_query) = &self.as_query {
            write!(f, " AS {as_query}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub uri_location: UriLocation,
}

impl Display for AttachTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ATTACH TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        write!(f, " FROM {0}", self.uri_location)?;

        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum CreateTableSource {
    Columns(Vec<ColumnDefinition>),
    Like {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
    },
}

impl Display for CreateTableSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for DescribeTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter().chain(Some(&self.table))),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub all: bool,
}

impl Display for DropTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndropTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for UndropTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
pub struct AlterTableStmt {
    pub if_exists: bool,
    pub table_reference: TableReference,
    pub action: AlterTableAction,
}

impl Display for AlterTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.table_reference)?;
        write!(f, " {}", self.action)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    RenameTable {
        new_table: Identifier,
    },
    AddColumn {
        column: ColumnDefinition,
    },
    RenameColumn {
        old_column: Identifier,
        new_column: Identifier,
    },
    ModifyColumn {
        action: ModifyColumnAction,
    },
    DropColumn {
        column: Identifier,
    },
    AlterTableClusterKey {
        cluster_by: Vec<Expr>,
    },
    DropTableClusterKey,
    ReclusterTable {
        is_final: bool,
        selection: Option<Expr>,
        limit: Option<u64>,
    },
    RevertTo {
        point: TimeTravelPoint,
    },
    SetOptions {
        set_options: BTreeMap<String, String>,
    },
}

impl Display for AlterTableAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterTableAction::SetOptions { set_options } => {
                write!(f, "SET OPTIONS: ").expect("Set Options Write Error ");
                write_space_separated_map(f, set_options.iter())
            }
            AlterTableAction::RenameTable { new_table } => {
                write!(f, "RENAME TO {new_table}")
            }
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => {
                write!(f, "RENAME COLUMN {old_column} TO {new_column}")
            }
            AlterTableAction::AddColumn { column } => {
                write!(f, "ADD COLUMN {column}")
            }
            AlterTableAction::ModifyColumn { action } => {
                write!(f, "MODIFY COLUMN {action}")
            }
            AlterTableAction::DropColumn { column } => {
                write!(f, "DROP COLUMN {column}")
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                write!(f, "CLUSTER BY ")?;
                write_comma_separated_list(f, cluster_by)
            }
            AlterTableAction::DropTableClusterKey => {
                write!(f, "DROP CLUSTER KEY")
            }
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
                limit,
            } => {
                write!(f, "RECLUSTER")?;
                if *is_final {
                    write!(f, " FINAL")?;
                }
                if let Some(conditions) = selection {
                    write!(f, " WHERE {conditions}")?;
                }
                if let Some(limit) = limit {
                    write!(f, " LIMIT {limit}")?;
                }
                Ok(())
            }
            AlterTableAction::RevertTo { point } => {
                write!(f, "REVERT TO {}", point)?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameTableStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub new_catalog: Option<Identifier>,
    pub new_database: Option<Identifier>,
    pub new_table: Identifier,
}

impl Display for RenameTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
pub struct TruncateTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub purge: bool,
}

impl Display for TruncateTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
pub struct VacuumTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub option: VacuumTableOption,
}

impl Display for VacuumTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "VACUUM TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " {}", &self.option)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VacuumDropTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub option: VacuumTableOption,
}

impl Display for VacuumDropTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "VACUUM DROP TABLE ")?;
        if self.catalog.is_some() || self.database.is_some() {
            write!(f, "FROM ")?;
            write_period_separated_list(f, self.catalog.iter().chain(&self.database))?;
            write!(f, " ")?;
        }
        write!(f, "{}", &self.option)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptimizeTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub action: OptimizeTableAction,
    pub limit: Option<u64>,
}

impl Display for OptimizeTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OPTIMIZE TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;
        write!(f, " {}", &self.action)?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for AnalyzeTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ANALYZE TABLE ")?;
        write_period_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistsTableStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
}

impl Display for ExistsTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Null,
    Memory,
    Fuse,
    View,
    Random,
}

impl Display for Engine {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Engine::Null => write!(f, "NULL"),
            Engine::Memory => write!(f, "MEMORY"),
            Engine::Fuse => write!(f, "FUSE"),
            Engine::View => write!(f, "VIEW"),
            Engine::Random => write!(f, "RANDOM"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactTarget {
    Block,
    Segment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VacuumTableOption {
    pub retain_hours: Option<Expr>,
    pub dry_run: Option<()>,
}

impl Display for VacuumTableOption {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(retain_hours) = &self.retain_hours {
            write!(f, "RETAIN {} HOURS", retain_hours)?;
        }
        if self.dry_run.is_some() {
            if self.retain_hours.is_some() {
                write!(f, " DRY RUN")?;
            } else {
                write!(f, "DRY RUN")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimizeTableAction {
    All,
    Purge { before: Option<TimeTravelPoint> },
    Compact { target: CompactTarget },
}

impl Display for OptimizeTableAction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            OptimizeTableAction::All => write!(f, "ALL"),
            OptimizeTableAction::Purge { before } => {
                write!(f, "PURGE")?;
                if let Some(point) = before {
                    write!(f, " BEFORE {}", point)?;
                }
                Ok(())
            }
            OptimizeTableAction::Compact { target } => {
                match target {
                    CompactTarget::Block => {
                        write!(f, "COMPACT BLOCK")?;
                    }
                    CompactTarget::Segment => {
                        write!(f, "COMPACT SEGMENT")?;
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnExpr {
    Default(Box<Expr>),
    Virtual(Box<Expr>),
    Stored(Box<Expr>),
}

impl Display for ColumnExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ColumnExpr::Default(expr) => {
                write!(f, " DEFAULT {expr}")?;
            }
            ColumnExpr::Virtual(expr) => {
                write!(f, " AS ({expr}) VIRTUAL")?;
            }
            ColumnExpr::Stored(expr) => {
                write!(f, " AS ({expr}) STORED")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: TypeName,
    pub expr: Option<ColumnExpr>,
    pub comment: Option<String>,
}

impl Display for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        if !matches!(self.data_type, TypeName::Nullable(_)) {
            write!(f, " NOT NULL")?;
        }

        if let Some(expr) = &self.expr {
            write!(f, "{expr}")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT '{comment}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModifyColumnAction {
    // (column name id, masking policy name)
    SetMaskingPolicy(Identifier, String),
    // column name id
    UnsetMaskingPolicy(Identifier),
    // vec<(column name id, type name)>
    SetDataType(Vec<(Identifier, TypeName)>),
    // column name id
    ConvertStoredComputedColumn(Identifier),
}

impl Display for ModifyColumnAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self {
            ModifyColumnAction::SetMaskingPolicy(column, name) => {
                write!(f, "{} SET MASKING POLICY {}", column, name)?
            }
            ModifyColumnAction::UnsetMaskingPolicy(column) => {
                write!(f, "{} UNSET MASKING POLICY", column)?
            }
            ModifyColumnAction::SetDataType(column_type_name_vec) => {
                let ret = column_type_name_vec
                    .iter()
                    .enumerate()
                    .map(|(i, (column, type_name))| {
                        if i > 0 {
                            format!(" COLUMN {} {}", column, type_name)
                        } else {
                            format!("{} {}", column, type_name)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "{}", ret)?
            }
            ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                write!(f, "{} DROP STORED", column)?
            }
        }

        Ok(())
    }
}
