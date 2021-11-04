// Copyright 2020 Datafuse Labs.
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
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use std::time::Instant;

use common_exception::ErrorCode;
use common_meta_types::AuthType;
use common_planners::ExplainType;
use metrics::histogram;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOptionDef;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::SqlOption;
use sqlparser::ast::TableConstraint;
use sqlparser::ast::Value;
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::Whitespace;

use crate::sql::DfCreateDatabase;
use crate::sql::DfCreateTable;
use crate::sql::DfCreateUser;
use crate::sql::DfDescribeTable;
use crate::sql::DfDropDatabase;
use crate::sql::DfDropTable;
use crate::sql::DfExplain;
use crate::sql::DfHint;
use crate::sql::DfKillStatement;
use crate::sql::DfShowCreateTable;
use crate::sql::DfShowDatabases;
use crate::sql::DfShowMetrics;
use crate::sql::DfShowProcessList;
use crate::sql::DfShowSettings;
use crate::sql::DfShowTables;
use crate::sql::DfStatement;
use crate::sql::DfTruncateTable;
use crate::sql::DfUseDatabase;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string().into()))
    };
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
    pub fn parse_sql(sql: &str) -> Result<(Vec<DfStatement>, Vec<DfHint>), ErrorCode> {
        let dialect = &GenericDialect {};
        let start = Instant::now();
        let result = DfParser::parse_sql_with_dialect(sql, dialect)?;
        histogram!(super::metrics::METRIC_PARSER_USEDTIME, start.elapsed());
        Ok(result)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<(Vec<DfStatement>, Vec<DfHint>), ParserError> {
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

        let mut hints = Vec::new();

        let mut parser = DfParser::new_with_dialect(sql, dialect)?;
        loop {
            let token = parser.parser.next_token_no_skip();
            match token {
                Some(Token::Whitespace(Whitespace::SingleLineComment { comment, prefix })) => {
                    hints.push(DfHint::create_from_comment(comment, prefix));
                }
                Some(Token::Whitespace(Whitespace::Newline)) | Some(Token::EOF) | None => break,
                _ => continue,
            }
        }
        Ok((stmts, hints))
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
                        self.parser.next_token();
                        self.parse_create()
                    }
                    Keyword::DESC => {
                        self.parser.next_token();
                        self.parse_describe()
                    }
                    Keyword::DESCRIBE => {
                        self.parser.next_token();
                        self.parse_describe()
                    }
                    Keyword::DROP => {
                        self.parser.next_token();
                        self.parse_drop()
                    }
                    Keyword::EXPLAIN => {
                        self.parser.next_token();
                        self.parse_explain()
                    }
                    Keyword::SHOW => {
                        self.parser.next_token();
                        if self.consume_token("TABLES") {
                            let tok = self.parser.next_token();
                            match &tok {
                                Token::EOF | Token::SemiColon => {
                                    Ok(DfStatement::ShowTables(DfShowTables::All))
                                }
                                Token::Word(w) => match w.keyword {
                                    Keyword::LIKE => Ok(DfStatement::ShowTables(
                                        DfShowTables::Like(self.parser.parse_identifier()?),
                                    )),
                                    Keyword::WHERE => Ok(DfStatement::ShowTables(
                                        DfShowTables::Where(self.parser.parse_expr()?),
                                    )),
                                    Keyword::FROM | Keyword::IN => Ok(DfStatement::ShowTables(
                                        DfShowTables::FromOrIn(self.parser.parse_object_name()?),
                                    )),
                                    _ => self.expected("like or where", tok),
                                },
                                _ => self.expected("like or where", tok),
                            }
                        } else if self.consume_token("DATABASES") {
                            self.parse_show_databases()
                        } else if self.consume_token("SETTINGS") {
                            Ok(DfStatement::ShowSettings(DfShowSettings))
                        } else if self.consume_token("CREATE") {
                            self.parse_show_create()
                        } else if self.consume_token("PROCESSLIST") {
                            Ok(DfStatement::ShowProcessList(DfShowProcessList))
                        } else if self.consume_token("METRICS") {
                            Ok(DfStatement::ShowMetrics(DfShowMetrics))
                        } else {
                            self.expected("tables or settings", self.parser.peek_token())
                        }
                    }
                    Keyword::TRUNCATE => {
                        self.parser.next_token();
                        self.parse_truncate()
                    }
                    Keyword::NoKeyword => match w.value.to_uppercase().as_str() {
                        // Use database
                        "USE" => self.parse_use_database(),
                        "KILL" => self.parse_kill_query(),
                        _ => self.expected("Keyword", self.parser.peek_token()),
                    },
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
                    ExplainType::Pipeline
                }
                "GRAPH" => {
                    self.parser.next_token();
                    ExplainType::Graph
                }
                _ => ExplainType::Syntax,
            },
            _ => ExplainType::Syntax,
        };

        let statement = Box::new(self.parser.parse_statement()?);
        let explain_plan = DfExplain { typ, statement };
        Ok(DfStatement::Explain(explain_plan))
    }

    // parse show databases where database = xxx or where database
    fn parse_show_databases(&mut self) -> Result<DfStatement, ParserError> {
        if self.parser.parse_keyword(Keyword::WHERE) {
            let mut expr = self.parser.parse_expr()?;

            expr = self.replace_show_database(expr);

            Ok(DfStatement::ShowDatabases(DfShowDatabases {
                where_opt: Some(expr),
            }))
        } else if self.parser.parse_keyword(Keyword::LIKE) {
            let pattern = self.parser.parse_expr()?;

            let like_expr = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("name"))),
                op: BinaryOperator::Like,
                right: Box::new(pattern),
            };
            Ok(DfStatement::ShowDatabases(DfShowDatabases {
                where_opt: Some(like_expr),
            }))
        } else if self.parser.peek_token() == Token::EOF
            || self.parser.peek_token() == Token::SemiColon
        {
            Ok(DfStatement::ShowDatabases(DfShowDatabases {
                where_opt: None,
            }))
        } else {
            self.expected("where or like", self.parser.peek_token())
        }
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
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_create_table(),
                Keyword::DATABASE => self.parse_create_database(),
                Keyword::USER => self.parse_create_user(),
                _ => self.expected("create statement", Token::Word(w)),
            },
            unexpected => self.expected("create statement", unexpected),
        }
    }

    fn parse_create_database(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parser.parse_object_name()?;

        let create = DfCreateDatabase {
            if_not_exists,
            name: db_name,
            options: vec![],
        };

        Ok(DfStatement::CreateDatabase(create))
    }

    fn parse_describe(&mut self) -> Result<DfStatement, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        let desc = DfDescribeTable { name: table_name };
        Ok(DfStatement::DescribeTable(desc))
    }

    /// Drop database/table.
    fn parse_drop(&mut self) -> Result<DfStatement, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::DATABASE => self.parse_drop_database(),
                Keyword::TABLE => self.parse_drop_table(),
                _ => self.expected("drop statement", Token::Word(w)),
            },
            unexpected => self.expected("drop statement", unexpected),
        }
    }

    /// Drop database.
    fn parse_drop_database(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let db_name = self.parser.parse_object_name()?;

        let drop = DfDropDatabase {
            if_exists,
            name: db_name,
        };

        Ok(DfStatement::DropDatabase(drop))
    }

    /// Drop table.
    fn parse_drop_table(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;

        let drop = DfDropTable {
            if_exists,
            name: table_name,
        };

        Ok(DfStatement::DropTable(drop))
    }

    // Parse 'use database' db name.
    fn parse_use_database(&mut self) -> Result<DfStatement, ParserError> {
        if !self.consume_token("USE") {
            return self.expected("Must USE", self.parser.peek_token());
        }

        let name = self.parser.parse_object_name()?;
        Ok(DfStatement::UseDatabase(DfUseDatabase { name }))
    }

    // Parse 'KILL statement'.
    fn parse_kill<F>(&mut self, f: F) -> Result<DfStatement, ParserError>
    where F: Fn(DfKillStatement) -> DfStatement {
        Ok(f(DfKillStatement {
            object_id: self.parser.parse_identifier()?,
        }))
    }

    // Parse 'KILL statement'.
    fn parse_kill_query(&mut self) -> Result<DfStatement, ParserError> {
        match self.consume_token("KILL") {
            true if self.consume_token("QUERY") => self.parse_kill(DfStatement::KillQuery),
            true if self.consume_token("CONNECTION") => self.parse_kill(DfStatement::KillConn),
            true => self.parse_kill(DfStatement::KillConn),
            false => self.expected("Must KILL", self.parser.peek_token()),
        }
    }

    fn parse_create_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let host_name = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };

        let exist_not_identified = self.parser.parse_keyword(Keyword::NOT);
        let exist_identified = self.consume_token("IDENTIFIED");
        let exist_with = self.consume_token("WITH");

        let (auth_type, password) = if exist_not_identified || !exist_identified {
            (AuthType::None, String::from(""))
        } else {
            let auth_type = if exist_with {
                match self.parser.parse_literal_string()?.as_str() {
                    "no_password" => AuthType::None,
                    "plaintext_password" => AuthType::PlainText,
                    "sha256_password" => AuthType::Sha256,
                    "double_sha1_password" => AuthType::DoubleSha1,
                    unexpected => return parser_err!(format!("Expected auth type {}, found: {}", "'no_password'|'plaintext_password'|'sha256_password'|'double_sha1_password'", unexpected))
                }
            } else {
                AuthType::Sha256
            };

            if AuthType::None == auth_type {
                (AuthType::None, String::from(""))
            } else if self.parser.parse_keyword(Keyword::BY) {
                let password = self.parser.parse_literal_string()?;
                if password.is_empty() {
                    return parser_err!("Missing password");
                }
                (auth_type, password)
            } else {
                return parser_err!("Expected keyword BY");
            }
        };

        let create = DfCreateUser {
            if_not_exists,
            name,
            host_name,
            auth_type,
            password,
        };

        Ok(DfStatement::CreateUser(create))
    }

    fn parse_create_table(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parse_columns()?;
        let engine = self.parse_table_engine()?;

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

        let create = DfCreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            options: table_properties,
        };

        Ok(DfStatement::CreateTable(create))
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String, ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok("NULL".to_string());
        }

        self.parser.expect_token(&Token::Eq)?;
        Ok(self.parser.next_token().to_string())
    }

    fn parse_show_create(&mut self) -> Result<DfStatement, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => {
                    let table_name = self.parser.parse_object_name()?;

                    let show_create_table = DfShowCreateTable { name: table_name };
                    Ok(DfStatement::ShowCreateTable(show_create_table))
                }
                _ => self.expected("show create statement", Token::Word(w)),
            },
            unexpected => self.expected("show create statement", unexpected),
        }
    }

    fn parse_truncate(&mut self) -> Result<DfStatement, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => {
                    let table_name = self.parser.parse_object_name()?;
                    let trunc = DfTruncateTable { name: table_name };
                    Ok(DfStatement::TruncateTable(trunc))
                }
                _ => self.expected("truncate statement", Token::Word(w)),
            },
            unexpected => self.expected("truncate statement", unexpected),
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

    fn replace_show_database(&self, expr: Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_expr = match *left {
                    Expr::Identifier(ref v) if v.value.to_uppercase() == *"DATABASE" => {
                        Expr::Identifier(Ident::new("name"))
                    }
                    _ => self.replace_show_database(*left),
                };

                let right_expr = match *right {
                    Expr::Identifier(ref v) if v.value.to_uppercase() == *"DATABASE" => {
                        Expr::Identifier(Ident::new("name"))
                    }
                    _ => self.replace_show_database(*right),
                };

                Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op,
                    right: Box::new(right_expr),
                }
            }
            _ => expr,
        }
    }
}
