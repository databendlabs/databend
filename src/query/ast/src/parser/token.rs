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

use common_exception::ErrorCode;
use common_exception::Result;
use logos::Lexer;
use logos::Logos;
use logos::Span;

pub use self::TokenKind::*;
use crate::DisplayError;

#[derive(Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub source: &'a str,
    pub kind: TokenKind,
    pub span: Span,
}

impl<'a> Token<'a> {
    pub fn text(&self) -> &'a str {
        &self.source[self.span.clone()]
    }
}

impl<'a> std::fmt::Debug for Token<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({:?})", self.kind, self.span)
    }
}

pub struct Tokenizer<'a> {
    source: &'a str,
    lexer: Lexer<'a, TokenKind>,
    eoi: bool,
}

impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            source,
            lexer: TokenKind::lexer(source),
            eoi: false,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Result<Token<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lexer.next() {
            Some(kind) if kind == TokenKind::Error => {
                let rest_span = Token {
                    source: self.source,
                    kind: TokenKind::Error,
                    span: self.lexer.span().start..self.source.len(),
                };
                Some(Err(ErrorCode::SyntaxException(rest_span.display_error(
                    "unable to recognize the rest tokens".to_string(),
                ))))
            }
            Some(kind) => Some(Ok(Token {
                source: self.source,
                kind,
                span: self.lexer.span(),
            })),
            None if !self.eoi => {
                self.eoi = true;
                Some(Ok(Token {
                    source: self.source,
                    kind: TokenKind::EOI,
                    span: (self.lexer.span().end)..(self.lexer.span().end),
                }))
            }
            None => None,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Logos, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenKind {
    #[error]
    Error,

    EOI,

    #[regex(r"[ \t\n\f]+", logos::skip)]
    Whitespace,

    #[regex(r"--[^\t\n\f]*", logos::skip)]
    Comment,

    #[regex(r"/\*([^\*]|(\*[^/]))*\*/", logos::skip)]
    CommentBlock,

    #[regex(r#"[_a-zA-Z][_$a-zA-Z0-9]*"#)]
    Ident,

    #[regex(r#"`[^`]*`"#)]
    #[regex(r#""([^"\\]|\\.|"")*""#)]
    #[regex(r#"'([^'\\]|\\.|'')*'"#)]
    QuotedString,

    #[regex(r#"@([^\s`;'"])+"#)]
    AtString,

    #[regex(r"[xX]'[a-fA-F0-9]*'")]
    PGLiteralHex,
    #[regex(r"0[xX][a-fA-F0-9]+")]
    MySQLLiteralHex,

    #[regex(r"[0-9]+")]
    LiteralInteger,

    #[regex(r"[0-9]+[eE][+-]?[0-9]+")]
    #[regex(r"([0-9]*\.[0-9]+([eE][+-]?[0-9]+)?)|([0-9]+\.[0-9]*([eE][+-]?[0-9]+)?)")]
    LiteralFloat,

    // Symbols
    #[token("==")]
    DoubleEq,
    #[token("=")]
    Eq,
    #[token("<>")]
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("<=")]
    Lte,
    #[token(">=")]
    Gte,
    #[token("<=>")]
    Spaceship,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Multiply,
    #[token("/")]
    Divide,
    #[token("%")]
    Modulo,
    #[token("||")]
    StringConcat,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token(",")]
    Comma,
    #[token(".")]
    Period,
    #[token(":")]
    Colon,
    #[token("::")]
    DoubleColon,
    #[token(";")]
    SemiColon,
    #[token("\\")]
    Backslash,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("&")]
    Ampersand,
    #[token("|")]
    Pipe,
    #[token("^")]
    Caret,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("->")]
    RArrow,
    #[token("=>")]
    FatRArrow,
    #[token("#")]
    Sharp,
    #[token("~")]
    Tilde,
    /// A case insensitive match regular expression operator in PostgreSQL
    #[token("~*")]
    TildeAsterisk,
    /// A case sensitive not match regular expression operator in PostgreSQL
    #[token("!*")]
    ExclamationMarkTilde,
    /// A case insensitive not match regular expression operator in PostgreSQL
    #[token("!~*")]
    ExclamationMarkTildeAsterisk,
    /// A bitwise shift left operator in PostgreSQL
    #[token("<<")]
    ShiftLeft,
    /// A bitwise shift right operator in PostgreSQL
    #[token(">>")]
    ShiftRight,
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    #[token("!")]
    ExclamationMark,
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    #[token("!!")]
    DoubleExclamationMark,
    /// AtSign `@` used for PostgreSQL abs operator
    #[token("@")]
    AtSign,
    /// A square root math operator in PostgreSQL
    #[token("|/")]
    PGSquareRoot,
    /// A cube root math operator in PostgreSQL
    #[token("||/")]
    PGCubeRoot,

    // Keywords
    //
    // Steps to add keyword:
    // 1. Add the keyword to token kind variants by alphabetical order.
    // 2. Search in this file to see if the new keyword is a commented
    //    out reserverd keyword. If so, uncomment the keyword in the
    //    reserved list.
    #[token("ALL", ignore(ascii_case))]
    ALL,
    #[token("ADD", ignore(ascii_case))]
    ADD,
    #[token("ANY", ignore(ascii_case))]
    ANY,
    #[token("SOME", ignore(ascii_case))]
    SOME,
    #[token("ALTER", ignore(ascii_case))]
    ALTER,
    #[token("ANALYZE", ignore(ascii_case))]
    ANALYZE,
    #[token("AND", ignore(ascii_case))]
    AND,
    #[token("ARRAY", ignore(ascii_case))]
    ARRAY,
    #[token("AS", ignore(ascii_case))]
    AS,
    #[token("AST", ignore(ascii_case))]
    AST,
    #[token("AT", ignore(ascii_case))]
    AT,
    #[token("ASC", ignore(ascii_case))]
    ASC,
    #[token("AWS_KEY_ID", ignore(ascii_case))]
    AWS_KEY_ID,
    #[token("AWS_SECRET_KEY", ignore(ascii_case))]
    AWS_SECRET_KEY,
    #[token("BETWEEN", ignore(ascii_case))]
    BETWEEN,
    #[token("BIGINT", ignore(ascii_case))]
    BIGINT,
    #[token("BOOL", ignore(ascii_case))]
    BOOL,
    #[token("BOOLEAN", ignore(ascii_case))]
    BOOLEAN,
    #[token("BOTH", ignore(ascii_case))]
    BOTH,
    #[token("BY", ignore(ascii_case))]
    BY,
    #[token("CALL", ignore(ascii_case))]
    CALL,
    #[token("CASE", ignore(ascii_case))]
    CASE,
    #[token("CAST", ignore(ascii_case))]
    CAST,
    #[token("CENTURY", ignore(ascii_case))]
    CENTURY,
    #[token("CLUSTER", ignore(ascii_case))]
    CLUSTER,
    #[token("COMMENT", ignore(ascii_case))]
    COMMENT,
    #[token("COMMENTS", ignore(ascii_case))]
    COMMENTS,
    #[token("COMPACT", ignore(ascii_case))]
    COMPACT,
    #[token("CONNECTION", ignore(ascii_case))]
    CONNECTION,
    #[token("CHAR", ignore(ascii_case))]
    CHAR,
    #[token("CHARACTER", ignore(ascii_case))]
    CHARACTER,
    #[token("COPY_OPTIONS", ignore(ascii_case))]
    COPY_OPTIONS,
    #[token("COPY", ignore(ascii_case))]
    COPY,
    #[token("COUNT", ignore(ascii_case))]
    COUNT,
    #[token("CREATE", ignore(ascii_case))]
    CREATE,
    #[token("CREDENTIALS", ignore(ascii_case))]
    CREDENTIALS,
    #[token("CROSS", ignore(ascii_case))]
    CROSS,
    #[token("CSV", ignore(ascii_case))]
    CSV,
    #[token("CURRENT_TIMESTAMP", ignore(ascii_case))]
    CURRENT_TIMESTAMP,
    #[token("DATABASE", ignore(ascii_case))]
    DATABASE,
    #[token("DATABASES", ignore(ascii_case))]
    DATABASES,
    #[token("DATE", ignore(ascii_case))]
    DATE,
    #[token("DATE_ADD", ignore(ascii_case))]
    DATE_ADD,
    #[token("DATE_SUB", ignore(ascii_case))]
    DATE_SUB,
    #[token("DATE_TRUNC", ignore(ascii_case))]
    DATE_TRUNC,
    #[token("DATETIME", ignore(ascii_case))]
    DATETIME,
    #[token("DAY", ignore(ascii_case))]
    DAY,
    #[token("DECADE", ignore(ascii_case))]
    DECADE,
    #[token("DEFAULT", ignore(ascii_case))]
    DEFAULT,
    #[token("DELETE", ignore(ascii_case))]
    DELETE,
    #[token("DESC", ignore(ascii_case))]
    DESC,
    #[token("DESCRIBE", ignore(ascii_case))]
    DESCRIBE,
    #[token("DISTINCT", ignore(ascii_case))]
    DISTINCT,
    #[token("DIV", ignore(ascii_case))]
    DIV,
    #[token("DOUBLE_SHA1_PASSWORD", ignore(ascii_case))]
    DOUBLE_SHA1_PASSWORD,
    #[token("DOUBLE", ignore(ascii_case))]
    DOUBLE,
    #[token("DOW", ignore(ascii_case))]
    DOW,
    #[token("DOY", ignore(ascii_case))]
    DOY,
    #[token("DOWNLOAD", ignore(ascii_case))]
    DOWNLOAD,
    #[token("DROP", ignore(ascii_case))]
    DROP,
    #[token("EXCEPT", ignore(ascii_case))]
    EXCEPT,
    #[token("ELSE", ignore(ascii_case))]
    ELSE,
    #[token("END", ignore(ascii_case))]
    END,
    #[token("ENDPOINT_URL", ignore(ascii_case))]
    ENDPOINT_URL,
    #[token("ENCRYPTION", ignore(ascii_case))]
    ENCRYPTION,
    #[token("ENGINE", ignore(ascii_case))]
    ENGINE,
    #[token("ENGINES", ignore(ascii_case))]
    ENGINES,
    #[token("EPOCH", ignore(ascii_case))]
    EPOCH,
    #[token("EXISTS", ignore(ascii_case))]
    EXISTS,
    #[token("EXPLAIN", ignore(ascii_case))]
    EXPLAIN,
    #[token("EXPIRE", ignore(ascii_case))]
    EXPIRE,
    #[token("EXTRACT", ignore(ascii_case))]
    EXTRACT,
    #[token("FALSE", ignore(ascii_case))]
    FALSE,
    #[token("FIELDS", ignore(ascii_case))]
    FIELDS,
    #[token("FIELD_DELIMITER", ignore(ascii_case))]
    FIELD_DELIMITER,
    #[token("FILE_FORMAT", ignore(ascii_case))]
    FILE_FORMAT,
    #[token("FILES", ignore(ascii_case))]
    FILES,
    #[token("FINAL", ignore(ascii_case))]
    FINAL,
    #[token("FLOAT", ignore(ascii_case))]
    FLOAT,
    #[token("FLOAT32", ignore(ascii_case))]
    FLOAT32,
    #[token("FLOAT64", ignore(ascii_case))]
    FLOAT64,
    #[token("FOR", ignore(ascii_case))]
    FOR,
    #[token("FORMAT", ignore(ascii_case))]
    FORMAT,
    #[token("FRAGMENTS", ignore(ascii_case))]
    FRAGMENTS,
    #[token("FROM", ignore(ascii_case))]
    FROM,
    #[token("FULL", ignore(ascii_case))]
    FULL,
    #[token("FUNCTION", ignore(ascii_case))]
    FUNCTION,
    #[token("FUNCTIONS", ignore(ascii_case))]
    FUNCTIONS,
    #[token("FUSE", ignore(ascii_case))]
    FUSE,
    #[token("GLOBAL", ignore(ascii_case))]
    GLOBAL,
    #[token("GRAPH", ignore(ascii_case))]
    GRAPH,
    #[token("GROUP", ignore(ascii_case))]
    GROUP,
    #[token("HAVING", ignore(ascii_case))]
    HAVING,
    #[token("HISTORY", ignore(ascii_case))]
    HISTORY,
    #[token("HOUR", ignore(ascii_case))]
    HOUR,
    #[token("INTERSECT", ignore(ascii_case))]
    INTERSECT,
    #[token("IDENTIFIED", ignore(ascii_case))]
    IDENTIFIED,
    #[token("IF", ignore(ascii_case))]
    IF,
    #[token("IN", ignore(ascii_case))]
    IN,
    #[token("INNER", ignore(ascii_case))]
    INNER,
    #[token("INSERT", ignore(ascii_case))]
    INSERT,
    #[token("INT", ignore(ascii_case))]
    INT,
    #[token("INT16", ignore(ascii_case))]
    INT16,
    #[token("INT32", ignore(ascii_case))]
    INT32,
    #[token("INT64", ignore(ascii_case))]
    INT64,
    #[token("INT8", ignore(ascii_case))]
    INT8,
    #[token("INTEGER", ignore(ascii_case))]
    INTEGER,
    #[token("INTERVAL", ignore(ascii_case))]
    INTERVAL,
    #[token("INTO", ignore(ascii_case))]
    INTO,
    #[token("IS", ignore(ascii_case))]
    IS,
    #[token("ISODOW", ignore(ascii_case))]
    ISODOW,
    #[token("ISOYEAR", ignore(ascii_case))]
    ISOYEAR,
    #[token("JOIN", ignore(ascii_case))]
    JOIN,
    #[token("JSON", ignore(ascii_case))]
    JSON,
    #[token("JULIAN", ignore(ascii_case))]
    JULIAN,
    #[token("JWT", ignore(ascii_case))]
    JWT,
    #[token("KEY", ignore(ascii_case))]
    KEY,
    #[token("KILL", ignore(ascii_case))]
    KILL,
    #[token("ROLES", ignore(ascii_case))]
    ROLES,
    #[token("LEADING", ignore(ascii_case))]
    LEADING,
    #[token("LEFT", ignore(ascii_case))]
    LEFT,
    #[token("LIKE", ignore(ascii_case))]
    LIKE,
    #[token("LIMIT", ignore(ascii_case))]
    LIMIT,
    #[token("LIST", ignore(ascii_case))]
    LIST,
    #[token("MAP", ignore(ascii_case))]
    MAP,
    #[token("MASTER_KEY", ignore(ascii_case))]
    MASTER_KEY,
    #[token("MEMORY", ignore(ascii_case))]
    MEMORY,
    #[token("METRICS", ignore(ascii_case))]
    METRICS,
    #[token("MICROSECONDS", ignore(ascii_case))]
    MICROSECONDS,
    #[token("MILLENIUM", ignore(ascii_case))]
    MILLENIUM,
    #[token("MILLISECONDS", ignore(ascii_case))]
    MILLISECONDS,
    #[token("MINUTE", ignore(ascii_case))]
    MINUTE,
    #[token("MONTH", ignore(ascii_case))]
    MONTH,
    #[token("NATURAL", ignore(ascii_case))]
    NATURAL,
    #[token("NO_PASSWORD", ignore(ascii_case))]
    NO_PASSWORD,
    #[token("NOT", ignore(ascii_case))]
    NOT,
    #[token("NOTENANTSETTING", ignore(ascii_case))]
    NOTENANTSETTING,
    #[token("NULL", ignore(ascii_case))]
    NULL,
    #[token("OBJECT", ignore(ascii_case))]
    OBJECT,
    #[token("OF", ignore(ascii_case))]
    OF,
    #[token("OFFSET", ignore(ascii_case))]
    OFFSET,
    #[token("ON", ignore(ascii_case))]
    ON,
    #[token("OPTIMIZE", ignore(ascii_case))]
    OPTIMIZE,
    #[token("OR", ignore(ascii_case))]
    OR,
    #[token("ORDER", ignore(ascii_case))]
    ORDER,
    #[token("OUTER", ignore(ascii_case))]
    OUTER,
    #[token("ON_ERROR", ignore(ascii_case))]
    ON_ERROR,
    #[token("OVERWRITE", ignore(ascii_case))]
    OVERWRITE,
    #[token("PARQUET", ignore(ascii_case))]
    PARQUET,
    #[token("PATTERN", ignore(ascii_case))]
    PATTERN,
    #[token("PIPELINE", ignore(ascii_case))]
    PIPELINE,
    #[token("PLAINTEXT_PASSWORD", ignore(ascii_case))]
    PLAINTEXT_PASSWORD,
    #[token("POSITION", ignore(ascii_case))]
    POSITION,
    #[token("PROCESSLIST", ignore(ascii_case))]
    PROCESSLIST,
    #[token("PURGE", ignore(ascii_case))]
    PURGE,
    #[token("QUARTER", ignore(ascii_case))]
    QUARTER,
    #[token("QUERY", ignore(ascii_case))]
    QUERY,
    #[token("RECLUSTER", ignore(ascii_case))]
    RECLUSTER,
    #[token("RECORD_DELIMITER", ignore(ascii_case))]
    RECORD_DELIMITER,
    #[token("REFERENCE_USAGE", ignore(ascii_case))]
    REFERENCE_USAGE,
    #[token("REGEXP", ignore(ascii_case))]
    REGEXP,
    #[token("RENAME", ignore(ascii_case))]
    RENAME,
    #[token("GRANT", ignore(ascii_case))]
    GRANT,
    #[token("ROLE", ignore(ascii_case))]
    ROLE,
    #[token("PRESIGN", ignore(ascii_case))]
    PRESIGN,
    #[token("PRIVILEGES", ignore(ascii_case))]
    PRIVILEGES,
    #[token("REMOVE", ignore(ascii_case))]
    REMOVE,
    #[token("REVOKE", ignore(ascii_case))]
    REVOKE,
    #[token("RECURSIVE", ignore(ascii_case))]
    RECURSIVE,
    #[token("GRANTS", ignore(ascii_case))]
    GRANTS,
    #[token("RIGHT", ignore(ascii_case))]
    RIGHT,
    #[token("RLIKE", ignore(ascii_case))]
    RLIKE,
    #[token("RAW", ignore(ascii_case))]
    RAW,
    #[token("SCHEMA", ignore(ascii_case))]
    SCHEMA,
    #[token("SCHEMAS", ignore(ascii_case))]
    SCHEMAS,
    #[token("SECOND", ignore(ascii_case))]
    SECOND,
    #[token("SELECT", ignore(ascii_case))]
    SELECT,
    #[token("SET", ignore(ascii_case))]
    SET,
    #[token("SETTINGS", ignore(ascii_case))]
    SETTINGS,
    #[token("STAGES", ignore(ascii_case))]
    STAGES,
    #[token("SHA256_PASSWORD", ignore(ascii_case))]
    SHA256_PASSWORD,
    #[token("SHOW", ignore(ascii_case))]
    SHOW,
    #[token("SIGNED", ignore(ascii_case))]
    SIGNED,
    #[token("SIZE_LIMIT", ignore(ascii_case))]
    SIZE_LIMIT,
    #[token("SKIP_HEADER", ignore(ascii_case))]
    SKIP_HEADER,
    #[token("SMALLINT", ignore(ascii_case))]
    SMALLINT,
    #[token("SNAPSHOT", ignore(ascii_case))]
    SNAPSHOT,
    #[token("STAGE", ignore(ascii_case))]
    STAGE,
    #[token("SYNTAX", ignore(ascii_case))]
    SYNTAX,
    #[token("USAGE", ignore(ascii_case))]
    USAGE,
    #[token("UPDATE", ignore(ascii_case))]
    UPDATE,
    #[token("UPLOAD", ignore(ascii_case))]
    UPLOAD,
    #[token("SHARE", ignore(ascii_case))]
    SHARE,
    #[token("SHARES", ignore(ascii_case))]
    SHARES,
    #[token("SUPER", ignore(ascii_case))]
    SUPER,
    #[token("STATUS", ignore(ascii_case))]
    STATUS,
    #[token("STRING", ignore(ascii_case))]
    STRING,
    #[token("SUBSTRING", ignore(ascii_case))]
    SUBSTRING,
    #[token("TABLE", ignore(ascii_case))]
    TABLE,
    #[token("TABLES", ignore(ascii_case))]
    TABLES,
    #[token("TEXT", ignore(ascii_case))]
    TEXT,
    #[token("TENANTSETTING", ignore(ascii_case))]
    TENANTSETTING,
    #[token("TENANTS", ignore(ascii_case))]
    TENANTS,
    #[token("THEN", ignore(ascii_case))]
    THEN,
    #[token("TIMESTAMP", ignore(ascii_case))]
    TIMESTAMP,
    #[token("TIMEZONE_HOUR", ignore(ascii_case))]
    TIMEZONE_HOUR,
    #[token("TIMEZONE_MINUTE", ignore(ascii_case))]
    TIMEZONE_MINUTE,
    #[token("TIMEZONE", ignore(ascii_case))]
    TIMEZONE,
    #[token("TINYINT", ignore(ascii_case))]
    TINYINT,
    #[token("TO", ignore(ascii_case))]
    TO,
    #[token("TOKEN", ignore(ascii_case))]
    TOKEN,
    #[token("TRAILING", ignore(ascii_case))]
    TRAILING,
    #[token("TRANSIENT", ignore(ascii_case))]
    TRANSIENT,
    #[token("TRIM", ignore(ascii_case))]
    TRIM,
    #[token("TRUE", ignore(ascii_case))]
    TRUE,
    #[token("TRUNCATE", ignore(ascii_case))]
    TRUNCATE,
    #[token("TRY_CAST", ignore(ascii_case))]
    TRY_CAST,
    #[token("TUPLE", ignore(ascii_case))]
    TUPLE,
    #[token("TYPE", ignore(ascii_case))]
    TYPE,
    #[token("UNION", ignore(ascii_case))]
    UNION,
    #[token("UINT16", ignore(ascii_case))]
    UINT16,
    #[token("UINT32", ignore(ascii_case))]
    UINT32,
    #[token("UINT64", ignore(ascii_case))]
    UINT64,
    #[token("UINT8", ignore(ascii_case))]
    UINT8,
    #[token("UNDROP", ignore(ascii_case))]
    UNDROP,
    #[token("UNSIGNED", ignore(ascii_case))]
    UNSIGNED,
    #[token("URL", ignore(ascii_case))]
    URL,
    #[token("USE", ignore(ascii_case))]
    USE,
    #[token("USER", ignore(ascii_case))]
    USER,
    #[token("USERS", ignore(ascii_case))]
    USERS,
    #[token("USING", ignore(ascii_case))]
    USING,
    #[token("VALUES", ignore(ascii_case))]
    VALUES,
    #[token("VALIDATION_MODE", ignore(ascii_case))]
    VALIDATION_MODE,
    #[token("VARCHAR", ignore(ascii_case))]
    VARCHAR,
    #[token("VARIANT", ignore(ascii_case))]
    VARIANT,
    #[token("VIEW", ignore(ascii_case))]
    VIEW,
    #[token("WEEK", ignore(ascii_case))]
    WEEK,
    #[token("WHEN", ignore(ascii_case))]
    WHEN,
    #[token("WHERE", ignore(ascii_case))]
    WHERE,
    #[token("WITH", ignore(ascii_case))]
    WITH,
    #[token("XOR", ignore(ascii_case))]
    XOR,
    #[token("YEAR", ignore(ascii_case))]
    YEAR,
    #[token("NULLIF", ignore(ascii_case))]
    NULLIF,
    #[token("COALESCE", ignore(ascii_case))]
    COALESCE,
    #[token("RANDOM", ignore(ascii_case))]
    RANDOM,
    #[token("IFNULL", ignore(ascii_case))]
    IFNULL,
    #[token("NULLS", ignore(ascii_case))]
    NULLS,
    #[token("FIRST", ignore(ascii_case))]
    FIRST,
    #[token("LAST", ignore(ascii_case))]
    LAST,
}

// Reference: https://www.postgresql.org/docs/current/sql-keywords-appendix.html
impl TokenKind {
    pub fn is_keyword(&self) -> bool {
        !matches!(
            self,
            Ident
                | QuotedString
                | PGLiteralHex
                | MySQLLiteralHex
                | LiteralInteger
                | LiteralFloat
                | DoubleEq
                | Eq
                | NotEq
                | Lt
                | Gt
                | Lte
                | Gte
                | Spaceship
                | Plus
                | Minus
                | Multiply
                | Divide
                | Modulo
                | StringConcat
                | LParen
                | RParen
                | Comma
                | Period
                | Colon
                | DoubleColon
                | SemiColon
                | Backslash
                | LBracket
                | RBracket
                | Ampersand
                | Pipe
                | Caret
                | LBrace
                | RBrace
                | RArrow
                | FatRArrow
                | Sharp
                | Tilde
                | TildeAsterisk
                | ExclamationMarkTilde
                | ExclamationMarkTildeAsterisk
                | ShiftLeft
                | ShiftRight
                | ExclamationMark
                | DoubleExclamationMark
                | AtSign
                | PGSquareRoot
                | PGCubeRoot
                | EOI
        )
    }

    pub fn is_reserved_function_name(&self, after_as: bool) -> bool {
        match self {
            | TokenKind::ALL
            // | TokenKind::ANALYSE
            | TokenKind::ANALYZE
            | TokenKind::AND
            | TokenKind::ANY
            | TokenKind::ASC
            // | TokenKind::ASYMMETRIC
            | TokenKind::BETWEEN
            | TokenKind::BIGINT
            // | TokenKind::BIT
            | TokenKind::BOOLEAN
            | TokenKind::BOTH
            | TokenKind::CASE
            | TokenKind::CAST
            // | TokenKind::CHECK
            // | TokenKind::COALESCE
            // | TokenKind::COLLATE
            // | TokenKind::COLUMN
            // | TokenKind::CONSTRAINT
            // | TokenKind::CURRENT_CATALOG
            // | TokenKind::CURRENT_DATE
            // | TokenKind::CURRENT_ROLE
            // | TokenKind::CURRENT_TIME
            | TokenKind::CURRENT_TIMESTAMP
            // | TokenKind::CURRENT_USER
            // | TokenKind::DEC
            // | TokenKind::DECIMAL
            | TokenKind::DEFAULT
            // | TokenKind::DEFERRABLE
            | TokenKind::DESC
            | TokenKind::DISTINCT
            // | TokenKind::DO
            | TokenKind::ELSE
            | TokenKind::END
            | TokenKind::EXISTS
            | TokenKind::EXTRACT
            | TokenKind::FALSE
            | TokenKind::FLOAT
            // | TokenKind::FOREIGN
            // | TokenKind::GREATEST
            // | TokenKind::GROUPING
            // | TokenKind::IFNULL
            | TokenKind::IN
            // | TokenKind::INITIALLY
            // | TokenKind::INOUT
            | TokenKind::INT
            | TokenKind::INTEGER
            | TokenKind::INTERVAL
            // | TokenKind::LATERAL
            | TokenKind::LEADING
            // | TokenKind::LEAST
            // | TokenKind::LOCALTIME
            // | TokenKind::LOCALTIMESTAMP
            // | TokenKind::NATIONAL
            // | TokenKind::NCHAR
            // | TokenKind::NONE
            // | TokenKind::NORMALIZE
            | TokenKind::NOT
            | TokenKind::NULL
            // | TokenKind::NULLIF
            // | TokenKind::NUMERIC
            // | TokenKind::ONLY
            | TokenKind::OR
            // | TokenKind::OUT
            // | TokenKind::OVERLAY
            // | TokenKind::PLACING
            | TokenKind::POSITION
            // | TokenKind::PRIMARY
            // | TokenKind::REAL
            // | TokenKind::REFERENCES
            // | TokenKind::ROW
            | TokenKind::SELECT
            // | TokenKind::SESSION_USER
            // | TokenKind::SETOF
            | TokenKind::SHARE
            | TokenKind::SHARES
            | TokenKind::SMALLINT
            | TokenKind::SOME
            | TokenKind::SUBSTRING
            // | TokenKind::SYMMETRIC
            | TokenKind::TABLE
            | TokenKind::THEN
            // | TokenKind::TIME
            | TokenKind::TIMESTAMP
            | TokenKind::TRAILING
            // | TokenKind::TREAT
            | TokenKind::TRIM
            | TokenKind::TRUE
            | TokenKind::TRY_CAST
            // | TokenKind::UNIQUE
            //| TokenKind::USER
            | TokenKind::USING
            | TokenKind::VALUES
            | TokenKind::VARCHAR
            // | TokenKind::VARIADIC
            // | TokenKind::XMLATTRIBUTES
            // | TokenKind::XMLCONCAT
            // | TokenKind::XMLELEMENT
            // | TokenKind::XMLEXISTS
            // | TokenKind::XMLFOREST
            // | TokenKind::XMLNAMESPACES
            // | TokenKind::XMLPARSE
            // | TokenKind::XMLPI
            // | TokenKind::XMLROOT
            // | TokenKind::XMLSERIALIZE
            // | TokenKind::XMLTABLE
            | TokenKind::WHEN => true,
            | TokenKind::ARRAY
            | TokenKind::AS
            // | TokenKind::CHAR
            | TokenKind::CHARACTER
            | TokenKind::CREATE
            | TokenKind::EXCEPT
            // | TokenKind::FETCH
            | TokenKind::FOR
            | TokenKind::FROM
            // | TokenKind::GRANT
            | TokenKind::GROUP
            | TokenKind::HAVING
            | TokenKind::INTERSECT
            | TokenKind::INTO
            | TokenKind::LIMIT
            | TokenKind::OFFSET
            | TokenKind::ON
            | TokenKind::OF
            | TokenKind::ORDER
            // | TokenKind::PRECISION
            // | TokenKind::RETURNING
            | TokenKind::TO
            | TokenKind::UNION
            | TokenKind::WHERE
            // | TokenKind::WINDOW
            | TokenKind::WITH
            | TokenKind::DATE_ADD
            | TokenKind::DATE_SUB
            | TokenKind::DATE_TRUNC
            if !after_as => true,
            _ => false
        }
    }

    pub fn is_reserved_ident(&self, after_as: bool) -> bool {
        match self {
            | TokenKind::ALL
            // | TokenKind::ANALYSE
            | TokenKind::ANALYZE
            | TokenKind::AND
            | TokenKind::ANY
            | TokenKind::ASC
            // | TokenKind::ASYMMETRIC
            // | TokenKind::AUTHORIZATION
            // | TokenKind::BINARY
            | TokenKind::BOTH
            | TokenKind::CASE
            | TokenKind::CAST
            // | TokenKind::CHECK
            // | TokenKind::COLLATE
            // | TokenKind::COLLATION
            // | TokenKind::COLUMN
            // | TokenKind::CONCURRENTLY
            // | TokenKind::CONSTRAINT
            | TokenKind::CROSS
            // | TokenKind::CURRENT_CATALOG
            // | TokenKind::CURRENT_DATE
            // | TokenKind::CURRENT_ROLE
            // | TokenKind::CURRENT_SCHEMA
            // | TokenKind::CURRENT_TIME
            | TokenKind::CURRENT_TIMESTAMP
            // | TokenKind::CURRENT_USER
            // | TokenKind::DEFERRABLE
            | TokenKind::DESC
            | TokenKind::DISTINCT
            // | TokenKind::DO
            | TokenKind::ELSE
            | TokenKind::END
            | TokenKind::FALSE
            // | TokenKind::FOREIGN
            // | TokenKind::FREEZE
            | TokenKind::FULL
            // | TokenKind::ILIKE
            | TokenKind::IN
            // | TokenKind::INITIALLY
            | TokenKind::INNER
            | TokenKind::IS
            | TokenKind::JOIN
            // | TokenKind::LATERAL
            | TokenKind::LEADING
            | TokenKind::LEFT
            | TokenKind::LIKE
            // | TokenKind::LOCALTIME
            // | TokenKind::LOCALTIMESTAMP
            | TokenKind::NATURAL
            | TokenKind::NOT
            | TokenKind::NULL
            // | TokenKind::ONLY
            | TokenKind::OR
            | TokenKind::OUTER
            // | TokenKind::PLACING
            // | TokenKind::PRIMARY
            // | TokenKind::REFERENCES
            | TokenKind::RIGHT
            | TokenKind::SELECT
            // | TokenKind::SESSION_USER
            // | TokenKind::SIMILAR
            | TokenKind::SOME
            // | TokenKind::SYMMETRIC
            // | TokenKind::TABLE
            // | TokenKind::TABLESAMPLE
            | TokenKind::THEN
            | TokenKind::TRAILING
            | TokenKind::TRUE
            // | TokenKind::UNIQUE
            //| TokenKind::USER
            | TokenKind::USING
            // | TokenKind::VARIADIC
            // | TokenKind::VERBOSE
            | TokenKind::WHEN => true,
            | TokenKind::ARRAY
            | TokenKind::AS
            | TokenKind::CREATE
            | TokenKind::EXCEPT
            // | TokenKind::FETCH
            | TokenKind::FOR
            | TokenKind::FROM
            // | TokenKind::GRANT
            | TokenKind::GROUP
            | TokenKind::HAVING
            | TokenKind::INTERSECT
            | TokenKind::INTO
            // | TokenKind::ISNULL
            | TokenKind::LIMIT
            | TokenKind::FORMAT
            // | TokenKind::NOTNULL
            | TokenKind::OFFSET
            | TokenKind::ON
            | TokenKind::OF
            | TokenKind::ORDER
            // | TokenKind::OVERLAPS
            // | TokenKind::RETURNING
            | TokenKind::STAGE
            | TokenKind::SHARE
            | TokenKind::SHARES
            | TokenKind::TO
            | TokenKind::UNION
            | TokenKind::WHERE
            // | TokenKind::WINDOW
            | TokenKind::WITH
            if !after_as => true,
            _ => false
        }
    }
}
