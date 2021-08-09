// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::DatabaseEngineType;
use common_planners::ExplainType;
use common_planners::TableEngineType;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_till1;
use nom::character::complete::digit1;
use nom::character::complete::multispace0;
use nom::character::complete::multispace1;
use nom::IResult;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::Ident;
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
pub struct DfShowProcessList;

#[derive(Debug, Clone, PartialEq)]
pub struct DfExplain {
    pub typ: ExplainType,
    pub statement: Box<SQLStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowCreateTable {
    pub name: ObjectName,
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
pub struct DfDescribeTable {
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropTable {
    pub if_exists: bool,
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfTruncateTable {
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateDatabase {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub engine: DatabaseEngineType,
    pub options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropDatabase {
    pub if_exists: bool,
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfUseDatabase {
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DfKillStatement {
    pub object_id: Ident,
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
    DropDatabase(DfDropDatabase),
    UseDatabase(DfUseDatabase),

    // Tables.
    ShowTables(DfShowTables),
    ShowCreateTable(DfShowCreateTable),
    CreateTable(DfCreateTable),
    DescribeTable(DfDescribeTable),
    DropTable(DfDropTable),
    TruncateTable(DfTruncateTable),

    // Settings.
    ShowSettings(DfShowSettings),

    // ProcessList
    ShowProcessList(DfShowProcessList),

    // Kill
    KillQuery(DfKillStatement),
    KillConn(DfKillStatement),
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq)]
pub struct DfHint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}

impl DfHint {
    pub fn create_from_comment(comment: &str, prefix: &str) -> Self {
        let error_code = match Self::parse_code(comment) {
            Ok((_, c)) => c,
            Err(_) => None,
        };

        Self {
            error_code,
            comment: comment.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    //  { ErrorCode 25 }
    pub fn parse_code(comment: &str) -> IResult<&str, Option<u16>> {
        let (comment, _) = take_till1(|c| c == '{')(comment)?;
        let (comment, _) = tag("{")(comment)?;
        let (comment, _) = multispace0(comment)?;
        let (comment, _) = tag("ErrorCode")(comment)?;
        let (comment, _) = multispace1(comment)?;
        let (comment, code) = digit1(comment)?;

        let code = code.parse::<u16>().ok();
        Ok((comment, code))
    }
}
