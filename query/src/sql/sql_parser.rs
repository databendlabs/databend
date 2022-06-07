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
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use std::collections::BTreeMap;
use std::time::Instant;

use common_exception::ErrorCode;
use metrics::histogram;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::dialect::MySqlDialect;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::Whitespace;

use super::statements::DfShowRoles;
use crate::sessions::SessionType;
use crate::sql::statements::DfShowEngines;
use crate::sql::statements::DfShowMetrics;
use crate::sql::statements::DfShowProcessList;
use crate::sql::statements::DfShowSettings;
use crate::sql::statements::DfShowUsers;
use crate::sql::DfHint;
use crate::sql::DfStatement;

// Use `Parser::expected` instead, if possible
#[macro_export]
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string().into()))
    };
}

/// SQL Parser
pub struct DfParser<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl<'a> DfParser<'a> {
    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(sql: &'a str, dialect: &'a dyn Dialect) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let (tokens, position_map) = tokenizer.tokenize()?;

        Ok(DfParser {
            sql,
            parser: Parser::new(tokens, position_map, dialect),
        })
    }

    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(
        sql: &'a str,
        typ: SessionType,
    ) -> Result<(Vec<DfStatement<'a>>, Vec<DfHint>), ErrorCode> {
        match typ {
            SessionType::MySQL => {
                let dialect = &MySqlDialect {};
                let start = Instant::now();
                let result = DfParser::parse_sql_with_dialect(sql, dialect)?;
                histogram!(super::metrics::METRIC_PARSER_USEDTIME, start.elapsed());
                Ok(result)
            }
            _ => {
                let dialect = &GenericDialect {};
                let start = Instant::now();
                let result = DfParser::parse_sql_with_dialect(sql, dialect)?;
                histogram!(super::metrics::METRIC_PARSER_USEDTIME, start.elapsed());
                Ok(result)
            }
        }
    }

    pub fn parse_expr(expr: &str) -> Result<Expr, ParserError> {
        let dialect = &MySqlDialect {};
        let mut tokenizer = Tokenizer::new(dialect, expr);
        let (tokens, position_map) = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens, position_map, dialect);
        parser.parse_expr()
    }

    pub fn parse_exprs(expr: &str) -> Result<Vec<Expr>, ParserError> {
        let dialect = &MySqlDialect {};
        let mut tokenizer = Tokenizer::new(dialect, expr);
        let (tokens, position_map) = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens, position_map, dialect);

        parser.expect_token(&Token::LParen)?;
        let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
        parser.expect_token(&Token::RParen)?;

        Ok(exprs)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &'a str,
        dialect: &'a dyn Dialect,
    ) -> Result<(Vec<DfStatement<'a>>, Vec<DfHint>), ParserError> {
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
    pub(crate) fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token();
                        self.parse_create()
                    }
                    Keyword::ALTER => {
                        self.parser.next_token();
                        self.parse_alter()
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
                        self.parse_show()
                    }
                    Keyword::TRUNCATE => self.parse_truncate(),
                    Keyword::RENAME => self.parse_rename(),
                    Keyword::SET => self.parse_set(),
                    Keyword::INSERT => self.parse_insert(),
                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),
                    Keyword::DELETE => self.parse_delete(),
                    Keyword::GRANT => {
                        self.parser.next_token();
                        self.parse_grant()
                    }
                    Keyword::REVOKE => {
                        self.parser.next_token();
                        self.parse_revoke()
                    }
                    Keyword::COPY => {
                        *self = Self::new_with_dialect(self.sql, &SnowflakeDialect {})?;
                        self.parser.next_token();

                        self.parse_copy()
                    }
                    Keyword::CALL => {
                        self.parser.next_token();
                        self.parse_call()
                    }

                    // Change to snowflake dialect for list cmd
                    Keyword::LIST => {
                        *self = Self::new_with_dialect(self.sql, &SnowflakeDialect {})?;
                        self.parser.next_token();
                        self.parse_list_cmd()
                    }

                    Keyword::NoKeyword => match w.value.to_uppercase().as_str() {
                        // Use database
                        "USE" => self.parse_use_database(),
                        "KILL" => self.parse_kill_query(),
                        "OPTIMIZE" => self.parse_optimize(),
                        "UNDROP" => {
                            self.parser.next_token();
                            self.parse_undrop()
                        }
                        _ => self.expected("Keyword", self.parser.peek_token()),
                    },
                    _ => self.expected("an SQL statement", Token::Word(w)),
                }
            }
            Token::LParen => self.parse_query(),
            unexpected => self.expected("an SQL statement", unexpected),
        }
    }

    fn parse_create(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => {
                //TODO:make stage to sql parser keyword
                match w.keyword {
                    Keyword::TABLE => self.parse_create_table(),
                    Keyword::DATABASE | Keyword::SCHEMA => self.parse_create_database(),
                    Keyword::USER => self.parse_create_user(),
                    Keyword::ROLE => self.parse_create_role(),
                    Keyword::FUNCTION => self.parse_create_udf(),
                    Keyword::STAGE => self.parse_create_stage(),
                    Keyword::VIEW => self.parse_create_view(),
                    _ => self.expected("create statement", Token::Word(w)),
                }
            }
            unexpected => self.expected("create statement", unexpected),
        }
    }

    /// This is a copy from sqlparser
    /// Parse a literal value (numbers, strings, date/time, booleans)
    #[allow(dead_code)]
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

    pub(crate) fn parse_value_or_ident(&mut self) -> Result<String, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok("true".to_string()),
                Keyword::FALSE => Ok("false".to_string()),
                Keyword::NULL => Ok("null".to_string()),
                _ => Ok(w.value),
            },
            // The call to n.parse() returns a bigdecimal when the
            // bigdecimal feature is enabled, and is otherwise a no-op
            // (i.e., it returns the input string).
            Token::Number(n, _) => Ok(n),
            Token::SingleQuotedString(s) => Ok(s),
            Token::NationalStringLiteral(s) => Ok(s),
            Token::HexStringLiteral(s) => Ok(s),
            unexpected => self.expected("a value", unexpected),
        }
    }

    fn parse_alter(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::USER => self.parse_alter_user(),
                Keyword::FUNCTION => self.parse_alter_udf(),
                Keyword::TABLE => self.parse_alter_table(),
                Keyword::VIEW => self.parse_alter_view(),
                Keyword::DATABASE => self.parse_alter_database(),
                _ => self.expected("keyword USER or FUNCTION", Token::Word(w)),
            },
            unexpected => self.expected("alter statement", unexpected),
        }
    }

    fn parse_describe(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_desc_table(),
                Keyword::STAGE => self.parse_desc_stage(),

                _ => {
                    self.parser.prev_token();
                    self.parse_desc_table()
                }
            },
            unexpected => self.expected("describe statement", unexpected),
        }
    }

    fn parse_rename(&mut self) -> Result<DfStatement<'a>, ParserError> {
        self.parser.next_token();
        self.parse_rename_table()
    }

    /// Drop database/table/stage.
    fn parse_drop(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::DATABASE | Keyword::SCHEMA => self.parse_drop_database(),
                Keyword::TABLE => self.parse_drop_table(),
                Keyword::USER => self.parse_drop_user(),
                Keyword::ROLE => self.parse_drop_role(),
                Keyword::FUNCTION => self.parse_drop_udf(),
                Keyword::STAGE => self.parse_drop_stage(),
                Keyword::VIEW => self.parse_drop_view(),
                _ => self.expected("drop statement", Token::Word(w)),
            },
            unexpected => self.expected("drop statement", unexpected),
        }
    }

    fn parse_undrop(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_undrop_table(),
                Keyword::DATABASE => self.parse_undrop_database(),
                _ => self.expected("drop statement", Token::Word(w)),
            },
            unexpected => self.expected("drop statement", unexpected),
        }
    }

    fn parse_show(&mut self) -> Result<DfStatement<'a>, ParserError> {
        if self.consume_token("FULL") && self.consume_token("TABLES") {
            self.parse_show_tables(true)
        } else if self.consume_token("TABLES") {
            self.parse_show_tables(false)
        } else if self.consume_token("TABLE") && self.consume_token("STATUS") {
            self.parse_show_tab_stat()
        } else if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            self.parse_show_databases()
        } else if self.consume_token("SETTINGS") {
            Ok(DfStatement::ShowSettings(DfShowSettings))
        } else if self.consume_token("CREATE") {
            self.parse_show_create()
        } else if self.consume_token("FIELDS") {
            self.parse_show_fields()
        } else if self.consume_token("PROCESSLIST") {
            Ok(DfStatement::ShowProcessList(DfShowProcessList))
        } else if self.consume_token("METRICS") {
            Ok(DfStatement::ShowMetrics(DfShowMetrics))
        } else if self.consume_token("USERS") {
            Ok(DfStatement::ShowUsers(DfShowUsers))
        } else if self.consume_token("ROLES") {
            Ok(DfStatement::ShowRoles(DfShowRoles))
        } else if self.consume_token("GRANTS") {
            self.parse_show_grants()
        } else if self.consume_token("FUNCTIONS") {
            self.parse_show_functions()
        } else if self.consume_token("ENGINES") {
            Ok(DfStatement::ShowEngines(DfShowEngines))
        } else if self.consume_token("STAGES") {
            self.parse_show_stages()
        } else {
            self.expected("show statement", self.parser.peek_token())
        }
    }

    fn parse_show_create(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_show_create_table(),
                Keyword::DATABASE | Keyword::SCHEMA => self.parse_show_create_database(),
                _ => self.expected("show create statement", Token::Word(w)),
            },
            unexpected => self.expected("show create statement", unexpected),
        }
    }

    fn parse_truncate(&mut self) -> Result<DfStatement<'a>, ParserError> {
        self.parser.next_token();
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_truncate_table(),
                _ => self.expected("truncate statement", Token::Word(w)),
            },
            unexpected => self.expected("truncate statement", unexpected),
        }
    }

    pub(crate) fn parse_options(&mut self) -> Result<BTreeMap<String, String>, ParserError> {
        let mut options = BTreeMap::new();
        loop {
            let name = self.parser.parse_identifier();
            if name.is_err() {
                self.parser.prev_token();
                break;
            }
            let name = name.unwrap();
            if !self.parser.consume_token(&Token::Eq) {
                // only paired values are considered as options
                self.parser.prev_token();
                break;
            }
            let value = self.parse_value_or_ident()?;

            options.insert(name.to_string().to_lowercase(), value);
        }
        Ok(options)
    }

    pub(crate) fn parse_list(&mut self, token: &Token) -> Result<Vec<String>, ParserError> {
        let mut list: Vec<String> = vec![];
        loop {
            let value = self.parse_value_or_ident()?;
            list.push(value.to_string());

            if !self.parser.consume_token(token) {
                break;
            }
        }
        Ok(list)
    }

    pub(crate) fn consume_token(&mut self, expected: &str) -> bool {
        if self.parser.peek_token().to_string().to_uppercase() == *expected.to_uppercase() {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    pub(crate) fn consume_token_until_or_end(&mut self, until_tokens: Vec<&str>) -> Vec<String> {
        let mut tokens = vec![];

        loop {
            let next_token = self.parser.peek_token();

            if next_token == Token::EOF
                || next_token == Token::SemiColon
                || until_tokens.contains(&next_token.to_string().to_uppercase().as_str())
            {
                break;
            }

            tokens.push(self.parser.next_token().to_string());
        }

        tokens
    }

    pub(crate) fn expect_token(&mut self, expected: &str) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(expected, self.parser.peek_token())
        }
    }
}
