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
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;

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
        Self {
            error_code: Self::parse_code(comment),
            comment: comment.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    // todo: use nom parser
    pub fn parse_code(comment: &str) -> Option<u16> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, comment);
        let tokens = tokenizer.tokenize().unwrap_or_default();

        let mut index = 0;
        let mut next_token = || -> Token {
            loop {
                index += 1;
                match tokens.get(index - 1) {
                    Some(Token::Whitespace(_)) => continue,
                    token => return token.cloned().unwrap_or(Token::EOF),
                }
            }
        };

        loop {
            let token = next_token();
            match token {
                Token::Word(w) if w.value == "ErrorCode" => {
                    if let Token::Number(str, _) = next_token() {
                        return match str.parse::<u16>() {
                            Ok(code) => Some(code),
                            _ => None,
                        };
                    }
                }
                Token::EOF => break,
                _ => {}
            }
        }

        None
    }
}
