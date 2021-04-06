// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use common_planners::{ExplainType, TableEngineType};
use sqlparser::ast::{ColumnDef, ObjectName, SqlOption, Statement as SQLStatement};

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateTable {
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: TableEngineType,
    pub table_properties: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowTables;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowSettings;

/// DataFusion extension DDL for `EXPLAIN` and `EXPLAIN VERBOSE`
#[derive(Debug, Clone, PartialEq)]
pub struct DfExplain {
    pub typ: ExplainType,
    /// The statement for which to generate an planning explanation
    pub statement: Box<SQLStatement>,
}

/// DataFusion Statement representations.
///
/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum DfStatement {
    /// ANSI SQL AST node
    Statement(SQLStatement),
    /// Extension: `EXPLAIN <SQL>`
    Explain(DfExplain),
    CreateTable(DfCreateTable),
    ShowTables(DfShowTables),
    ShowSettings(DfShowSettings),
}
