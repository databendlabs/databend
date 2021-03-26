// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See NOTICE.md

use common_planners::{DfExplainType, EngineType};
use sqlparser::ast::{Ident, ObjectName, SqlOption, Value};
use sqlparser::{
    ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string().into()))
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseCreateTable {
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    pub engine: EngineType,
    pub table_properties: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuseShowTables;
#[derive(Debug, Clone, PartialEq)]
pub struct FuseShowSettings;

/// DataFusion extension DDL for `EXPLAIN` and `EXPLAIN VERBOSE`
#[derive(Debug, Clone, PartialEq)]
pub struct DfExplainPlan {
    pub typ: DfExplainType,
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
    Explain(DfExplainPlan),
    Create(FuseCreateTable),
    ShowTables(FuseShowTables),
    ShowSettings(FuseShowSettings),
}

/// SQL Parser
pub struct DfParser<'a> {
    parser: Parser<'a>,
}

impl<'a> DfParser<'a> {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        DfParser::new_with_dialect(sql, dialect)
    }

    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(DfParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(sql: &str) -> Result<Vec<DfStatement>, ParserError> {
        let dialect = &GenericDialect {};
        DfParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<Vec<DfStatement>, ParserError> {
        let mut parser = DfParser::new_with_dialect(sql, dialect)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<DfStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_create()
                    }
                    Keyword::EXPLAIN => {
                        self.parser.next_token();
                        self.parse_explain()
                    }

                    Keyword::SHOW => {
                        self.parser.next_token();

                        if self.consume_token("TABLES") {
                            Ok(DfStatement::ShowTables(FuseShowTables))
                        } else if self.consume_token("SETTINGS") {
                            Ok(DfStatement::ShowSettings(FuseShowSettings))
                        } else {
                            self.expected("tables or settings", self.parser.peek_token())
                        }
                    }
                    _ => {
                        // use the native parser
                        Ok(DfStatement::Statement(self.parser.parse_statement()?))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(DfStatement::Statement(self.parser.parse_statement()?))
            }
        }
    }

    /// Parse an SQL EXPLAIN statement.
    pub fn parse_explain(&mut self) -> Result<DfStatement, ParserError> {
        // Parser is at the token immediately after EXPLAIN
        // Check for EXPLAIN VERBOSE
        let typ = match self.parser.peek_token() {
            Token::Word(w) => match w.value.to_uppercase().as_str() {
                "PIPELINE" => {
                    self.parser.next_token();
                    DfExplainType::Pipeline
                }
                "GRAPH" => {
                    self.parser.next_token();
                    DfExplainType::Graph
                }
                _ => DfExplainType::Syntax,
            },
            _ => DfExplainType::Syntax,
        };

        let statement = Box::new(self.parser.parse_statement()?);
        let explain_plan = DfExplainPlan { typ, statement };
        Ok(DfStatement::Explain(explain_plan))
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    /// This is a copy from sqlparser
    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok(Value::Boolean(true)),
                Keyword::FALSE => Ok(Value::Boolean(false)),
                Keyword::NULL => Ok(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value)),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value)),
                    _ => self.expected("A value?", Token::Word(w))?,
                },
                _ => self.expected("a concrete value", Token::Word(w)),
            },
            // The call to n.parse() returns a bigdecimal when the
            // bigdecimal feature is enabled, and is otherwise a no-op
            // (i.e., it returns the input string).
            Token::Number(ref n, l) => match n.parse() {
                Ok(n) => Ok(Value::Number(n, l)),
                Err(e) => parser_err!(format!("Could not parse '{}' as number: {}", n, e)),
            },
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            unexpected => self.expected("a value", unexpected),
        }
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parse_columns()?;
        let engine = self.parse_engine()?;

        let mut table_properties = vec![];

        // parse table options: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
        if self.consume_token("LOCATION") {
            self.parser.expect_token(&Token::Eq)?;
            let value = self.parse_value()?;
            table_properties.push(SqlOption {
                name: Ident::new("LOCATION"),
                value,
            })
        }

        let create = FuseCreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            table_properties,
        };

        Ok(DfStatement::Create(create))
    }

    /// Parses the set of valid formats
    fn parse_engine(&mut self) -> Result<EngineType, ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok(EngineType::Null);
        }

        self.parser.expect_token(&Token::Eq)?;

        match self.parser.next_token() {
            Token::Word(w) => match &*w.value {
                "Parquet" => Ok(EngineType::Parquet),
                "JSONEachRaw" => Ok(EngineType::JsonEachRaw),
                "CSV" => Ok(EngineType::Csv),
                "Null" => Ok(EngineType::Null),
                _ => self.expected("one of Parquet, JSONEachRaw, Null or CSV", Token::Word(w)),
            },
            unexpected => self.expected("one of Parquet, JSONEachRaw, Null or CSV", unexpected),
        }
    }

    fn consume_token(&mut self, expected: &str) -> bool {
        if self.parser.peek_token().to_string().to_uppercase() == *expected.to_uppercase() {
            self.parser.next_token();
            true
        } else {
            false
        }
    }
}
