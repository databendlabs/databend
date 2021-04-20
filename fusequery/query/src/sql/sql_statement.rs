// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::DatabaseEngineType;
use common_planners::ExplainType;
use common_planners::TableEngineType;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SqlOption;
use sqlparser::ast::Statement as SQLStatement;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowTables;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowDatabases;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowSettings;

#[derive(Debug, Clone, PartialEq)]
pub struct DfExplain {
    pub typ: ExplainType,
    pub statement: Box<SQLStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateTable {
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: TableEngineType,
    pub options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateDatabase {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub engine: DatabaseEngineType,
    pub options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfUseDatabase {
    pub name: ObjectName,
}

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum DfStatement {
    // ANSI SQL AST node
    Statement(SQLStatement),
    Explain(DfExplain),

    // Databases.
    ShowDatabases(DfShowDatabases),
    CreateDatabase(DfCreateDatabase),
    UseDatabase(DfUseDatabase),

    // Tables.
    ShowTables(DfShowTables),
    CreateTable(DfCreateTable),

    // Settings.
    ShowSettings(DfShowSettings),
}
