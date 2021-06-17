// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::DatabaseEngineType;
use common_planners::ExplainType;
use common_planners::TableEngineType;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_till;
use nom::bytes::complete::take_while1;
use nom::character::complete::multispace0;
use nom::character::is_digit;
use nom::sequence::tuple;
use nom::IResult;
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
pub struct DfDropTable {
    pub if_exists: bool,
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
    CreateTable(DfCreateTable),
    DropTable(DfDropTable),

    // Settings.
    ShowSettings(DfShowSettings),
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
            Ok((_, c)) => Some(c),
            Err(_) => None,
        };

        Self {
            error_code,
            comment: comment.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    //  { ErrorCode 25 }
    pub fn parse_code(
        comment: &str,
    ) -> std::result::Result<(&[u8], u16), Box<dyn std::error::Error + '_>> {
        let input = comment.as_bytes();

        let before = take_till(|c| c == b'{');
        let text = tag("ErrorCode");
        let code = take_while1(is_digit);

        let res: IResult<&[u8], (&[u8], &[u8], &[u8], &[u8], &[u8], &[u8])> =
            tuple((before, tag("{"), multispace0, text, multispace0, code))(input);

        let res = res?;

        let code = res.1 .5;
        let code = String::from_utf8_lossy(code).to_string();
        let code = code.parse::<u16>()?;

        Ok((res.0, code))
    }
}
