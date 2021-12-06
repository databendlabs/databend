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

use std::convert::TryFrom;
use std::str::FromStr;
use std::time::Instant;

use common_exception::ErrorCode;
use common_meta_types::AuthType;
use common_meta_types::Compression;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::StageParams;
use common_meta_types::UserPrivilege;
use common_meta_types::UserPrivilegeType;
use common_planners::ExplainType;
use metrics::histogram;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOptionDef;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::SqlOption;
use sqlparser::ast::Statement;
use sqlparser::ast::TableConstraint;
use sqlparser::ast::Value;
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::IsOptional;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::Whitespace;

use super::statements::DfCopy;
use crate::sql::statements::DfAlterUser;
use crate::sql::statements::DfCompactTable;
use crate::sql::statements::DfCreateDatabase;
use crate::sql::statements::DfCreateStage;
use crate::sql::statements::DfCreateTable;
use crate::sql::statements::DfCreateUser;
use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfDropDatabase;
use crate::sql::statements::DfDropTable;
use crate::sql::statements::DfDropUser;
use crate::sql::statements::DfExplain;
use crate::sql::statements::DfGrantObject;
use crate::sql::statements::DfGrantStatement;
use crate::sql::statements::DfInsertStatement;
use crate::sql::statements::DfKillStatement;
use crate::sql::statements::DfQueryStatement;
use crate::sql::statements::DfRevokeStatement;
use crate::sql::statements::DfSetVariable;
use crate::sql::statements::DfShowCreateTable;
use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowMetrics;
use crate::sql::statements::DfShowProcessList;
use crate::sql::statements::DfShowSettings;
use crate::sql::statements::DfShowTables;
use crate::sql::statements::DfShowUsers;
use crate::sql::statements::DfTruncateTable;
use crate::sql::statements::DfUseDatabase;
use crate::sql::DfHint;
use crate::sql::DfStatement;

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
                        } else if self.consume_token("USERS") {
                            Ok(DfStatement::ShowUsers(DfShowUsers))
                        } else {
                            self.expected("tables or settings", self.parser.peek_token())
                        }
                    }
                    Keyword::TRUNCATE => self.parse_truncate(),
                    Keyword::SET => self.parse_set(),
                    Keyword::INSERT => self.parse_insert(),
                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),
                    Keyword::GRANT => {
                        self.parser.next_token();
                        self.parse_grant()
                    }
                    Keyword::REVOKE => {
                        self.parser.next_token();
                        self.parse_revoke()
                    }
                    Keyword::COPY => {
                        self.parser.next_token();
                        self.parse_copy()
                    }
                    Keyword::NoKeyword => match w.value.to_uppercase().as_str() {
                        // Use database
                        "USE" => self.parse_use_database(),
                        "KILL" => self.parse_kill_query(),
                        "COMPACT" => self.parse_compact(),
                        _ => self.expected("Keyword", self.parser.peek_token()),
                    },
                    _ => self.expected("an SQL statement", Token::Word(w)),
                }
            }
            Token::LParen => self.parse_query(),
            unexpected => self.expected("an SQL statement", unexpected),
        }
    }

    fn parse_query(&mut self) -> Result<DfStatement, ParserError> {
        // self.parser.prev_token();
        let native_query = self.parser.parse_query()?;
        Ok(DfStatement::Query(Box::new(DfQueryStatement::try_from(
            native_query,
        )?)))
    }

    fn parse_set(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.next_token();
        match self.parser.parse_set()? {
            Statement::SetVariable {
                local,
                hivevar,
                variable,
                value,
            } => Ok(DfStatement::SetVariable(DfSetVariable {
                local,
                hivevar,
                variable,
                value,
            })),
            _ => parser_err!("Expect set Variable statement"),
        }
    }

    fn parse_insert(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.next_token();
        match self.parser.parse_insert()? {
            Statement::Insert {
                or,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                format,
                after_columns,
                table,
            } => Ok(DfStatement::InsertQuery(DfInsertStatement {
                or,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                format,
                after_columns,
                table,
            })),
            _ => parser_err!("Expect set insert statement"),
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

        let statement = Box::new(self.parse_query()?);
        Ok(DfStatement::Explain(DfExplain { typ, statement }))
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
            Token::Word(w) => {
                //TODO:make stage to sql parser keyword
                if w.value.to_uppercase() == "STAGE" {
                    self.parse_create_stage()
                } else {
                    match w.keyword {
                        Keyword::TABLE => self.parse_create_table(),
                        Keyword::DATABASE => self.parse_create_database(),
                        Keyword::USER => self.parse_create_user(),
                        _ => self.expected("create statement", Token::Word(w)),
                    }
                }
            }
            unexpected => self.expected("create statement", unexpected),
        }
    }

    fn parse_alter(&mut self) -> Result<DfStatement, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::USER => self.parse_alter_user(),
                _ => self.expected("alter statement", Token::Word(w)),
            },
            unexpected => self.expected("alter statement", unexpected),
        }
    }

    fn parse_create_database(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parser.parse_object_name()?;
        let db_engine = self.parse_database_engine()?;

        let create = DfCreateDatabase {
            if_not_exists,
            name: db_name,
            engine: db_engine,
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
                Keyword::USER => self.parse_drop_user(),
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
    fn parse_kill<const KILL_QUERY: bool>(&mut self) -> Result<DfStatement, ParserError> {
        Ok(DfStatement::KillStatement(DfKillStatement {
            object_id: self.parser.parse_identifier()?,
            kill_query: KILL_QUERY,
        }))
    }

    // Parse 'KILL statement'.
    fn parse_kill_query(&mut self) -> Result<DfStatement, ParserError> {
        match self.consume_token("KILL") {
            true if self.consume_token("QUERY") => self.parse_kill::<true>(),
            true if self.consume_token("CONNECTION") => self.parse_kill::<false>(),
            true => self.parse_kill::<false>(),
            false => self.expected("Must KILL", self.parser.peek_token()),
        }
    }

    fn parse_create_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };

        let (auth_type, password) = self.get_auth_option()?;

        let create = DfCreateUser {
            if_not_exists,
            name,
            hostname,
            auth_type,
            password,
        };

        Ok(DfStatement::CreateUser(create))
    }

    fn parse_alter_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_current_user = self.consume_token("USER")
            && self.parser.expect_token(&Token::LParen).is_ok()
            && self.parser.expect_token(&Token::RParen).is_ok();
        let name = if !if_current_user {
            self.parser.parse_literal_string()?
        } else {
            String::from("")
        };
        let hostname = if !if_current_user {
            if self.consume_token("@") {
                self.parser.parse_literal_string()?
            } else {
                String::from("%")
            }
        } else {
            String::from("")
        };

        let (auth_type, password) = self.get_auth_option()?;

        let alter = DfAlterUser {
            if_current_user,
            name,
            hostname,
            new_auth_type: auth_type,
            new_password: password,
        };

        Ok(DfStatement::AlterUser(alter))
    }

    fn parse_drop_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };
        let drop = DfDropUser {
            if_exists,
            name,
            hostname,
        };
        Ok(DfStatement::DropUser(drop))
    }

    fn get_auth_option(&mut self) -> Result<(AuthType, String), ParserError> {
        let exist_not_identified = self.parser.parse_keyword(Keyword::NOT);
        let exist_identified = self.consume_token("IDENTIFIED");
        let exist_with = self.consume_token("WITH");

        if exist_not_identified || !exist_identified {
            Ok((AuthType::None, String::from("")))
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
                Ok((AuthType::None, String::from("")))
            } else if self.parser.parse_keyword(Keyword::BY) {
                let password = self.parser.parse_literal_string()?;
                if password.is_empty() {
                    return parser_err!("Missing password");
                }
                Ok((auth_type, password))
            } else {
                return parser_err!("Expected keyword BY");
            }
        }
    }

    fn parse_stage_file_format(&mut self) -> Result<Option<FileFormat>, ParserError> {
        let file_format = if self.consume_token("FILE_FORMAT") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.expect_token(&Token::LParen)?;

            let format = if self.consume_token("FORMAT") {
                self.parser.expect_token(&Token::Eq)?;
                self.parser.next_token().to_string()
            } else {
                return parser_err!("Missing FORMAT");
            };

            let file_format = match format.to_uppercase().as_str() {
                "CSV" | "PARQUET" => {
                    let compression = if self.consume_token("COMPRESSION") {
                        self.parser.expect_token(&Token::Eq)?;
                        //TODO:check compression value correctness
                        let value = self.parser.next_token().to_string();
                        Compression::from_str(value.as_str())
                            .map_err(|e| ParserError::ParserError(e.to_string()))?
                    } else {
                        Compression::None
                    };
                    if "CSV" == format.to_uppercase().as_str() {
                        if self.consume_token("RECORD_DELIMITER") {
                            self.parser.expect_token(&Token::Eq)?;

                            let record_delimiter = match self.parser.next_token() {
                                Token::Word(w) => match w.value.to_uppercase().as_str() {
                                    "NONE" => String::from(""),
                                    _ => {
                                        return self
                                            .expected("record delimiter NONE", Token::Word(w))
                                    }
                                },
                                Token::SingleQuotedString(s) => s,
                                unexpected => {
                                    return self
                                        .expected("not supported record delimiter", unexpected)
                                }
                            };

                            Some(FileFormat::Csv {
                                compression,
                                record_delimiter,
                            })
                        } else {
                            Some(FileFormat::Csv {
                                compression,
                                record_delimiter: String::from(""),
                            })
                        }
                    } else {
                        Some(FileFormat::Parquet { compression })
                    }
                }
                "JSON" => Some(FileFormat::Json),
                unexpected => {
                    return parser_err!(format!(
                        "Expected format type {}, found: {}",
                        "CSV|PARQUET|JSON", unexpected
                    ))
                }
            };

            self.parser.expect_token(&Token::RParen)?;
            file_format
        } else {
            None
        };

        Ok(file_format)
    }

    fn parse_stage_credentials(&mut self, url: String) -> Result<Credentials, ParserError> {
        if !self.consume_token("CREDENTIALS") {
            return parser_err!("Missing CREDENTIALS");
        }
        self.parser.expect_token(&Token::Eq)?;
        self.parser.expect_token(&Token::LParen)?;

        let credentials = if url.to_uppercase().starts_with("S3") {
            //TODO: current credential field order is hard code
            let access_key_id = if self.consume_token("ACCESS_KEY_ID") {
                self.parser.expect_token(&Token::Eq)?;
                self.parser.parse_literal_string()?
            } else {
                return parser_err!("Missing S3 ACCESS_KEY_ID");
            };

            let secret_access_key = if self.consume_token("SECRET_ACCESS_KEY") {
                self.parser.expect_token(&Token::Eq)?;
                self.parser.parse_literal_string()?
            } else {
                return parser_err!("Missing S3 SECRET_ACCESS_KEY");
            };
            Credentials::S3 {
                access_key_id,
                secret_access_key,
            }
        } else {
            return parser_err!("Not supported storage");
        };
        self.parser.expect_token(&Token::RParen)?;
        Ok(credentials)
    }

    fn parse_create_stage(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let url = if self.consume_token("URL") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.parse_literal_string()?
        } else {
            return parser_err!("Missing URL");
        };

        let credentials = self.parse_stage_credentials(url.clone())?;
        let stage_params = StageParams::new(url.as_str(), credentials);
        let file_format = self.parse_stage_file_format()?;

        let comments = if self.consume_token("COMMENTS") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.parse_literal_string()?
        } else {
            String::from("")
        };

        let create = DfCreateStage {
            if_not_exists,
            stage_name: name,
            stage_params,
            file_format,
            comments,
        };

        Ok(DfStatement::CreateStage(create))
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

    fn parse_database_engine(&mut self) -> Result<String, ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok("".to_string());
        }

        self.parser.expect_token(&Token::Eq)?;
        Ok(self.parser.next_token().to_string())
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String, ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok("FUSE".to_string());
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
        self.parser.next_token();
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

    fn parse_privileges(&mut self) -> Result<UserPrivilege, ParserError> {
        let mut privileges = UserPrivilege::empty();
        loop {
            match self.parser.next_token() {
                Token::Word(w) => match w.keyword {
                    // Keyword::USAGE => privileges.set_privilege(UserPrivilegeType::Usage),
                    Keyword::CREATE => privileges.set_privilege(UserPrivilegeType::Create),
                    Keyword::SELECT => privileges.set_privilege(UserPrivilegeType::Select),
                    Keyword::INSERT => privileges.set_privilege(UserPrivilegeType::Insert),
                    Keyword::SET => privileges.set_privilege(UserPrivilegeType::Set),
                    Keyword::ALL => {
                        privileges.set_all_privileges();
                        // GRANT ALL [PRIVILEGES]
                        self.consume_token("PRIVILEGES");
                        break;
                    }
                    _ => return self.expected("privilege type", Token::Word(w)),
                },
                unexpected => return self.expected("privilege type", unexpected),
            };
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(privileges)
    }

    fn parse_grant(&mut self) -> Result<DfStatement, ParserError> {
        let privileges = self.parse_privileges()?;
        if !self.parser.parse_keyword(Keyword::ON) {
            return self.expected("keyword ON", self.parser.peek_token());
        }
        let on = self.parse_grant_object()?;
        if !self.parser.parse_keyword(Keyword::TO) {
            return self.expected("keyword TO", self.parser.peek_token());
        }
        let (name, hostname) = self.parse_user_identity()?;
        let grant = DfGrantStatement {
            name,
            hostname,
            on,
            priv_types: privileges,
        };
        Ok(DfStatement::GrantPrivilege(grant))
    }

    fn parse_revoke(&mut self) -> Result<DfStatement, ParserError> {
        let privileges = self.parse_privileges()?;
        if !self.parser.parse_keyword(Keyword::ON) {
            return self.expected("keyword ON", self.parser.peek_token());
        }
        let on = self.parse_grant_object()?;
        if !self.parser.parse_keyword(Keyword::FROM) {
            return self.expected("keyword FROM", self.parser.peek_token());
        }
        let (username, hostname) = self.parse_user_identity()?;
        let revoke = DfRevokeStatement {
            username,
            hostname,
            on,
            priv_types: privileges,
        };
        Ok(DfStatement::RevokePrivilege(revoke))
    }

    fn parse_user_identity(&mut self) -> Result<(String, String), ParserError> {
        let username = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };
        Ok((username, hostname))
    }

    /// Parse a possibly qualified, possibly quoted identifier or wild card, e.g.
    /// `*` or `myschema`.*. The sub string pattern like "db0%" is not in planned.
    fn parse_grant_object(&mut self) -> Result<DfGrantObject, ParserError> {
        let chunk0 = self.parse_grant_object_pattern_chunk()?;
        // "*" as current db or "table" with current db
        if !self.consume_token(".") {
            if chunk0.value == "*" {
                return Ok(DfGrantObject::Database(None));
            }
            return Ok(DfGrantObject::Table(None, chunk0.value));
        }
        let chunk1 = self.parse_grant_object_pattern_chunk()?;

        // *.* means global
        if chunk1.value == "*" && chunk0.value == "*" {
            return Ok(DfGrantObject::Global);
        }
        // *.table is not allowed
        if chunk0.value == "*" {
            return self.expected("whitespace", Token::Period);
        }
        // db.*
        if chunk1.value == "*" {
            return Ok(DfGrantObject::Database(Some(chunk0.value)));
        }
        // db.table
        Ok(DfGrantObject::Table(Some(chunk0.value), chunk1.value))
    }

    /// Parse a chunk from the object pattern, it might be * or an identifier
    pub fn parse_grant_object_pattern_chunk(&mut self) -> Result<Ident, ParserError> {
        if self.consume_token("*") {
            return Ok(Ident::new("*"));
        }
        let token = self.parser.peek_token();
        self.parser
            .parse_identifier()
            .or_else(|_| self.expected("identifier or *", token))
    }

    // copy into mycsvtable
    // from @my_ext_stage/tutorials/dataloading/contacts1.csv format CSV [options];
    fn parse_copy(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.expect_keyword(Keyword::INTO)?;
        let name = self.parser.parse_object_name()?;
        let columns = self
            .parser
            .parse_parenthesized_column_list(IsOptional::Optional)?;
        self.parser.expect_keyword(Keyword::FROM)?;
        let location = self.parser.parse_literal_string()?;

        self.parser.expect_keyword(Keyword::FORMAT)?;
        let format = self.parser.next_token().to_string();

        let options = self.parse_options()?;

        Ok(DfStatement::Copy(DfCopy {
            name,
            columns,
            location,
            format,
            options,
        }))
    }

    fn parse_options(&mut self) -> Result<Vec<SqlOption>, ParserError> {
        let mut options = vec![];
        loop {
            let name = self.parser.parse_identifier();
            if name.is_err() {
                break;
            }
            let name = name.unwrap();
            self.parser.expect_token(&Token::Eq)?;
            let value = self.parse_value()?;
            options.push(SqlOption { name, value });
        }
        Ok(options)
    }

    fn parse_compact(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.next_token();
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => {
                    let table_name = self.parser.parse_object_name()?;
                    let compact = DfCompactTable { name: table_name };
                    Ok(DfStatement::CompactTable(compact))
                }
                _ => self.expected("TABLE", Token::Word(w)),
            },
            unexpected => self.expected("compact statement", unexpected),
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
