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

use logos::Logos;
use logos::Span;

pub use self::TokenKind::*;
use crate::error::Error;
use crate::error::Result;

#[allow(non_camel_case_types)]
#[derive(Logos, Clone, Copy, Debug, PartialEq)]
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

    #[regex(r#""[_a-zA-Z][_$a-zA-Z0-9]*""#)]
    QuotedIdent,

    #[regex(r#"'([^'\\]|\\.|'')*'"#)]
    LiteralString,

    #[regex(r"[xX]'[a-fA-F0-9]*'")]
    LiteralHex,

    #[regex(r"[0-9]+")]
    #[regex(r"[0-9]+e[+-]?[0-9]+")]
    #[regex(r"([0-9]*\.[0-9]+(e[+-]?[0-9]+)?)|([0-9]+\.[0-9]*(e[+-]?[0-9]+)?)")]
    LiteralNumber,

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
    #[token("=>")]
    RArrow,
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
    #[token("ABS", ignore(ascii_case))]
    ABS,
    #[token("ABORT", ignore(ascii_case))]
    ABORT,
    #[token("ACTION", ignore(ascii_case))]
    ACTION,
    #[token("ADD", ignore(ascii_case))]
    ADD,
    #[token("ALL", ignore(ascii_case))]
    ALL,
    #[token("ALLOCATE", ignore(ascii_case))]
    ALLOCATE,
    #[token("ALTER", ignore(ascii_case))]
    ALTER,
    #[token("ANALYZE", ignore(ascii_case))]
    ANALYZE,
    #[token("AND", ignore(ascii_case))]
    AND,
    #[token("ANY", ignore(ascii_case))]
    ANY,
    #[token("APPLY", ignore(ascii_case))]
    APPLY,
    #[token("ARE", ignore(ascii_case))]
    ARE,
    #[token("ARRAY", ignore(ascii_case))]
    ARRAY,
    #[token("ARRAY_AGG", ignore(ascii_case))]
    ARRAY_AGG,
    #[token("ARRAY_MAX_CARDINALITY", ignore(ascii_case))]
    ARRAY_MAX_CARDINALITY,
    #[token("AS", ignore(ascii_case))]
    AS,
    #[token("ASC", ignore(ascii_case))]
    ASC,
    #[token("ASENSITIVE", ignore(ascii_case))]
    ASENSITIVE,
    #[token("ASSERT", ignore(ascii_case))]
    ASSERT,
    #[token("ASYMMETRIC", ignore(ascii_case))]
    ASYMMETRIC,
    #[token("AT", ignore(ascii_case))]
    AT,
    #[token("ATOMIC", ignore(ascii_case))]
    ATOMIC,
    #[token("AUTHORIZATION", ignore(ascii_case))]
    AUTHORIZATION,
    #[token("AUTOINCREMENT", ignore(ascii_case))]
    AUTOINCREMENT,
    #[token("AUTO_INCREMENT", ignore(ascii_case))]
    AUTO_INCREMENT,
    #[token("AVG", ignore(ascii_case))]
    AVG,
    #[token("AVRO", ignore(ascii_case))]
    AVRO,
    #[token("BEGIN", ignore(ascii_case))]
    BEGIN,
    #[token("BEGIN_FRAME", ignore(ascii_case))]
    BEGIN_FRAME,
    #[token("BEGIN_PARTITION", ignore(ascii_case))]
    BEGIN_PARTITION,
    #[token("BETWEEN", ignore(ascii_case))]
    BETWEEN,
    #[token("BIGINT", ignore(ascii_case))]
    BIGINT,
    #[token("BINARY", ignore(ascii_case))]
    BINARY,
    #[token("BLOB", ignore(ascii_case))]
    BLOB,
    #[token("BOOLEAN", ignore(ascii_case))]
    BOOLEAN,
    #[token("BOTH", ignore(ascii_case))]
    BOTH,
    #[token("BY", ignore(ascii_case))]
    BY,
    #[token("BYTEA", ignore(ascii_case))]
    BYTEA,
    #[token("CACHE", ignore(ascii_case))]
    CACHE,
    #[token("CALL", ignore(ascii_case))]
    CALL,
    #[token("CALLED", ignore(ascii_case))]
    CALLED,
    #[token("CARDINALITY", ignore(ascii_case))]
    CARDINALITY,
    #[token("CASCADE", ignore(ascii_case))]
    CASCADE,
    #[token("CASCADED", ignore(ascii_case))]
    CASCADED,
    #[token("CASE", ignore(ascii_case))]
    CASE,
    #[token("CAST", ignore(ascii_case))]
    CAST,
    #[token("CEIL", ignore(ascii_case))]
    CEIL,
    #[token("CEILING", ignore(ascii_case))]
    CEILING,
    #[token("CHAIN", ignore(ascii_case))]
    CHAIN,
    #[token("CHANGE", ignore(ascii_case))]
    CHANGE,
    #[token("CHAR", ignore(ascii_case))]
    CHAR,
    #[token("CHARACTER", ignore(ascii_case))]
    CHARACTER,
    #[token("CHARACTER_LENGTH", ignore(ascii_case))]
    CHARACTER_LENGTH,
    #[token("CHAR_LENGTH", ignore(ascii_case))]
    CHAR_LENGTH,
    #[token("CHECK", ignore(ascii_case))]
    CHECK,
    #[token("CLOB", ignore(ascii_case))]
    CLOB,
    #[token("CLOSE", ignore(ascii_case))]
    CLOSE,
    #[token("CLUSTER", ignore(ascii_case))]
    CLUSTER,
    #[token("COALESCE", ignore(ascii_case))]
    COALESCE,
    #[token("COLLATE", ignore(ascii_case))]
    COLLATE,
    #[token("COLLECT", ignore(ascii_case))]
    COLLECT,
    #[token("COLUMN", ignore(ascii_case))]
    COLUMN,
    #[token("COLUMNS", ignore(ascii_case))]
    COLUMNS,
    #[token("COMMENT", ignore(ascii_case))]
    COMMENT,
    #[token("COMMIT", ignore(ascii_case))]
    COMMIT,
    #[token("COMMITTED", ignore(ascii_case))]
    COMMITTED,
    #[token("COMPUTE", ignore(ascii_case))]
    COMPUTE,
    #[token("CONDITION", ignore(ascii_case))]
    CONDITION,
    #[token("CONNECT", ignore(ascii_case))]
    CONNECT,
    #[token("CONSTRAINT", ignore(ascii_case))]
    CONSTRAINT,
    #[token("CONTAINS", ignore(ascii_case))]
    CONTAINS,
    #[token("CONVERT", ignore(ascii_case))]
    CONVERT,
    #[token("COPY", ignore(ascii_case))]
    COPY,
    #[token("CORR", ignore(ascii_case))]
    CORR,
    #[token("CORRESPONDING", ignore(ascii_case))]
    CORRESPONDING,
    #[token("COUNT", ignore(ascii_case))]
    COUNT,
    #[token("COVAR_POP", ignore(ascii_case))]
    COVAR_POP,
    #[token("COVAR_SAMP", ignore(ascii_case))]
    COVAR_SAMP,
    #[token("CREATE", ignore(ascii_case))]
    CREATE,
    #[token("CROSS", ignore(ascii_case))]
    CROSS,
    #[token("CSV", ignore(ascii_case))]
    CSV,
    #[token("CUBE", ignore(ascii_case))]
    CUBE,
    #[token("CUME_DIST", ignore(ascii_case))]
    CUME_DIST,
    #[token("CURRENT", ignore(ascii_case))]
    CURRENT,
    #[token("CURRENT_CATALOG", ignore(ascii_case))]
    CURRENT_CATALOG,
    #[token("CURRENT_DATE", ignore(ascii_case))]
    CURRENT_DATE,
    #[token("CURRENT_DEFAULT_TRANSFORM_GROUP", ignore(ascii_case))]
    CURRENT_DEFAULT_TRANSFORM_GROUP,
    #[token("CURRENT_PATH", ignore(ascii_case))]
    CURRENT_PATH,
    #[token("CURRENT_ROLE", ignore(ascii_case))]
    CURRENT_ROLE,
    #[token("CURRENT_ROW", ignore(ascii_case))]
    CURRENT_ROW,
    #[token("CURRENT_SCHEMA", ignore(ascii_case))]
    CURRENT_SCHEMA,
    #[token("CURRENT_TIME", ignore(ascii_case))]
    CURRENT_TIME,
    #[token("CURRENT_TIMESTAMP", ignore(ascii_case))]
    CURRENT_TIMESTAMP,
    #[token("CURRENT_TRANSFORM_GROUP_FOR_TYPE", ignore(ascii_case))]
    CURRENT_TRANSFORM_GROUP_FOR_TYPE,
    #[token("CURRENT_USER", ignore(ascii_case))]
    CURRENT_USER,
    #[token("CURSOR", ignore(ascii_case))]
    CURSOR,
    #[token("CYCLE", ignore(ascii_case))]
    CYCLE,
    #[token("DATA", ignore(ascii_case))]
    DATA,
    #[token("DATABASE", ignore(ascii_case))]
    DATABASE,
    #[token("DATE", ignore(ascii_case))]
    DATE,
    #[token("DAY", ignore(ascii_case))]
    DAY,
    #[token("DEALLOCATE", ignore(ascii_case))]
    DEALLOCATE,
    #[token("DEC", ignore(ascii_case))]
    DEC,
    #[token("DECIMAL", ignore(ascii_case))]
    DECIMAL,
    #[token("DECLARE", ignore(ascii_case))]
    DECLARE,
    #[token("DEFAULT", ignore(ascii_case))]
    DEFAULT,
    #[token("DELETE", ignore(ascii_case))]
    DELETE,
    #[token("DELIMITED", ignore(ascii_case))]
    DELIMITED,
    #[token("DENSE_RANK", ignore(ascii_case))]
    DENSE_RANK,
    #[token("DEREF", ignore(ascii_case))]
    DEREF,
    #[token("DESC", ignore(ascii_case))]
    DESC,
    #[token("DESCRIBE", ignore(ascii_case))]
    DESCRIBE,
    #[token("DETERMINISTIC", ignore(ascii_case))]
    DETERMINISTIC,
    #[token("DIRECTORY", ignore(ascii_case))]
    DIRECTORY,
    #[token("DISCONNECT", ignore(ascii_case))]
    DISCONNECT,
    #[token("DISTINCT", ignore(ascii_case))]
    DISTINCT,
    #[token("DISTRIBUTE", ignore(ascii_case))]
    DISTRIBUTE,
    #[token("DIV", ignore(ascii_case))]
    DIV,
    #[token("DOUBLE", ignore(ascii_case))]
    DOUBLE,
    #[token("DROP", ignore(ascii_case))]
    DROP,
    #[token("DUPLICATE", ignore(ascii_case))]
    DUPLICATE,
    #[token("DYNAMIC", ignore(ascii_case))]
    DYNAMIC,
    #[token("EACH", ignore(ascii_case))]
    EACH,
    #[token("ELEMENT", ignore(ascii_case))]
    ELEMENT,
    #[token("ELSE", ignore(ascii_case))]
    ELSE,
    #[token("END", ignore(ascii_case))]
    END,
    #[token("END-EXEC", ignore(ascii_case))]
    END_EXEC,
    #[token("END_FRAME", ignore(ascii_case))]
    END_FRAME,
    #[token("END_PARTITION", ignore(ascii_case))]
    END_PARTITION,
    #[token("EQUALS", ignore(ascii_case))]
    EQUALS,
    #[token("ERROR", ignore(ascii_case))]
    ERROR,
    #[token("ESCAPE", ignore(ascii_case))]
    ESCAPE,
    #[token("EVENT", ignore(ascii_case))]
    EVENT,
    #[token("EVERY", ignore(ascii_case))]
    EVERY,
    #[token("EXCEPT", ignore(ascii_case))]
    EXCEPT,
    #[token("EXEC", ignore(ascii_case))]
    EXEC,
    #[token("EXECUTE", ignore(ascii_case))]
    EXECUTE,
    #[token("EXISTS", ignore(ascii_case))]
    EXISTS,
    #[token("EXP", ignore(ascii_case))]
    EXP,
    #[token("EXPLAIN", ignore(ascii_case))]
    EXPLAIN,
    #[token("EXTENDED", ignore(ascii_case))]
    EXTENDED,
    #[token("EXTERNAL", ignore(ascii_case))]
    EXTERNAL,
    #[token("EXTRACT", ignore(ascii_case))]
    EXTRACT,
    #[token("FAIL", ignore(ascii_case))]
    FAIL,
    #[token("FALSE", ignore(ascii_case))]
    FALSE,
    #[token("FETCH", ignore(ascii_case))]
    FETCH,
    #[token("FIELDS", ignore(ascii_case))]
    FIELDS,
    #[token("FILTER", ignore(ascii_case))]
    FILTER,
    #[token("FIRST", ignore(ascii_case))]
    FIRST,
    #[token("FIRST_VALUE", ignore(ascii_case))]
    FIRST_VALUE,
    #[token("FLOAT", ignore(ascii_case))]
    FLOAT,
    #[token("FLOOR", ignore(ascii_case))]
    FLOOR,
    #[token("FOLLOWING", ignore(ascii_case))]
    FOLLOWING,
    #[token("FOR", ignore(ascii_case))]
    FOR,
    #[token("FOREIGN", ignore(ascii_case))]
    FOREIGN,
    #[token("FORMAT", ignore(ascii_case))]
    FORMAT,
    #[token("FRAME_ROW", ignore(ascii_case))]
    FRAME_ROW,
    #[token("FREE", ignore(ascii_case))]
    FREE,
    #[token("FROM", ignore(ascii_case))]
    FROM,
    #[token("FULL", ignore(ascii_case))]
    FULL,
    #[token("FUNCTION", ignore(ascii_case))]
    FUNCTION,
    #[token("FUSION", ignore(ascii_case))]
    FUSION,
    #[token("GET", ignore(ascii_case))]
    GET,
    #[token("GLOBAL", ignore(ascii_case))]
    GLOBAL,
    #[token("GRANT", ignore(ascii_case))]
    GRANT,
    #[token("GRANTED", ignore(ascii_case))]
    GRANTED,
    #[token("GROUP", ignore(ascii_case))]
    GROUP,
    #[token("GROUPING", ignore(ascii_case))]
    GROUPING,
    #[token("GROUPS", ignore(ascii_case))]
    GROUPS,
    #[token("HAVING", ignore(ascii_case))]
    HAVING,
    #[token("HEADER", ignore(ascii_case))]
    HEADER,
    #[token("HIVEVAR", ignore(ascii_case))]
    HIVEVAR,
    #[token("HOLD", ignore(ascii_case))]
    HOLD,
    #[token("HOUR", ignore(ascii_case))]
    HOUR,
    #[token("IDENTITY", ignore(ascii_case))]
    IDENTITY,
    #[token("IF", ignore(ascii_case))]
    IF,
    #[token("IGNORE", ignore(ascii_case))]
    IGNORE,
    #[token("ILIKE", ignore(ascii_case))]
    ILIKE,
    #[token("IN", ignore(ascii_case))]
    IN,
    #[token("INDEX", ignore(ascii_case))]
    INDEX,
    #[token("INDICATOR", ignore(ascii_case))]
    INDICATOR,
    #[token("INNER", ignore(ascii_case))]
    INNER,
    #[token("INOUT", ignore(ascii_case))]
    INOUT,
    #[token("INPUTFORMAT", ignore(ascii_case))]
    INPUTFORMAT,
    #[token("INSENSITIVE", ignore(ascii_case))]
    INSENSITIVE,
    #[token("INSERT", ignore(ascii_case))]
    INSERT,
    #[token("INT", ignore(ascii_case))]
    INT,
    #[token("INTEGER", ignore(ascii_case))]
    INTEGER,
    #[token("INTERSECT", ignore(ascii_case))]
    INTERSECT,
    #[token("INTERSECTION", ignore(ascii_case))]
    INTERSECTION,
    #[token("INTERVAL", ignore(ascii_case))]
    INTERVAL,
    #[token("INTO", ignore(ascii_case))]
    INTO,
    #[token("IS", ignore(ascii_case))]
    IS,
    #[token("ISOLATION", ignore(ascii_case))]
    ISOLATION,
    #[token("JOIN", ignore(ascii_case))]
    JOIN,
    #[token("JSONFILE", ignore(ascii_case))]
    JSONFILE,
    #[token("KEY", ignore(ascii_case))]
    KEY,
    #[token("LAG", ignore(ascii_case))]
    LAG,
    #[token("LANGUAGE", ignore(ascii_case))]
    LANGUAGE,
    #[token("LARGE", ignore(ascii_case))]
    LARGE,
    #[token("LAST", ignore(ascii_case))]
    LAST,
    #[token("LAST_VALUE", ignore(ascii_case))]
    LAST_VALUE,
    #[token("LATERAL", ignore(ascii_case))]
    LATERAL,
    #[token("LEAD", ignore(ascii_case))]
    LEAD,
    #[token("LEADING", ignore(ascii_case))]
    LEADING,
    #[token("LEFT", ignore(ascii_case))]
    LEFT,
    #[token("LEVEL", ignore(ascii_case))]
    LEVEL,
    #[token("LIKE", ignore(ascii_case))]
    LIKE,
    #[token("LIKE_REGEX", ignore(ascii_case))]
    LIKE_REGEX,
    #[token("LIMIT", ignore(ascii_case))]
    LIMIT,
    #[token("LISTAGG", ignore(ascii_case))]
    LISTAGG,
    #[token("LN", ignore(ascii_case))]
    LN,
    #[token("LOCAL", ignore(ascii_case))]
    LOCAL,
    #[token("LOCALTIME", ignore(ascii_case))]
    LOCALTIME,
    #[token("LOCALTIMESTAMP", ignore(ascii_case))]
    LOCALTIMESTAMP,
    #[token("LOCATION", ignore(ascii_case))]
    LOCATION,
    #[token("LOWER", ignore(ascii_case))]
    LOWER,
    #[token("MANAGEDLOCATION", ignore(ascii_case))]
    MANAGEDLOCATION,
    #[token("MATCH", ignore(ascii_case))]
    MATCH,
    #[token("MATERIALIZED", ignore(ascii_case))]
    MATERIALIZED,
    #[token("MAX", ignore(ascii_case))]
    MAX,
    #[token("MEMBER", ignore(ascii_case))]
    MEMBER,
    #[token("MERGE", ignore(ascii_case))]
    MERGE,
    #[token("METADATA", ignore(ascii_case))]
    METADATA,
    #[token("METHOD", ignore(ascii_case))]
    METHOD,
    #[token("MIN", ignore(ascii_case))]
    MIN,
    #[token("MINUTE", ignore(ascii_case))]
    MINUTE,
    #[token("MOD", ignore(ascii_case))]
    MOD,
    #[token("MODIFIES", ignore(ascii_case))]
    MODIFIES,
    #[token("MODULE", ignore(ascii_case))]
    MODULE,
    #[token("MONTH", ignore(ascii_case))]
    MONTH,
    #[token("MSCK", ignore(ascii_case))]
    MSCK,
    #[token("MULTISET", ignore(ascii_case))]
    MULTISET,
    #[token("NATIONAL", ignore(ascii_case))]
    NATIONAL,
    #[token("NATURAL", ignore(ascii_case))]
    NATURAL,
    #[token("NCHAR", ignore(ascii_case))]
    NCHAR,
    #[token("NCLOB", ignore(ascii_case))]
    NCLOB,
    #[token("NEW", ignore(ascii_case))]
    NEW,
    #[token("NEXT", ignore(ascii_case))]
    NEXT,
    #[token("NO", ignore(ascii_case))]
    NO,
    #[token("NONE", ignore(ascii_case))]
    NONE,
    #[token("NORMALIZE", ignore(ascii_case))]
    NORMALIZE,
    #[token("NOSCAN", ignore(ascii_case))]
    NOSCAN,
    #[token("NOT", ignore(ascii_case))]
    NOT,
    #[token("NTH_VALUE", ignore(ascii_case))]
    NTH_VALUE,
    #[token("NTILE", ignore(ascii_case))]
    NTILE,
    #[token("NULL", ignore(ascii_case))]
    NULL,
    #[token("NULLIF", ignore(ascii_case))]
    NULLIF,
    #[token("NULLS", ignore(ascii_case))]
    NULLS,
    #[token("NUMERIC", ignore(ascii_case))]
    NUMERIC,
    #[token("OBJECT", ignore(ascii_case))]
    OBJECT,
    #[token("OCCURRENCES_REGEX", ignore(ascii_case))]
    OCCURRENCES_REGEX,
    #[token("OCTET_LENGTH", ignore(ascii_case))]
    OCTET_LENGTH,
    #[token("OF", ignore(ascii_case))]
    OF,
    #[token("OFFSET", ignore(ascii_case))]
    OFFSET,
    #[token("OLD", ignore(ascii_case))]
    OLD,
    #[token("ON", ignore(ascii_case))]
    ON,
    #[token("ONLY", ignore(ascii_case))]
    ONLY,
    #[token("OPEN", ignore(ascii_case))]
    OPEN,
    #[token("OPTION", ignore(ascii_case))]
    OPTION,
    #[token("OR", ignore(ascii_case))]
    OR,
    #[token("ORC", ignore(ascii_case))]
    ORC,
    #[token("ORDER", ignore(ascii_case))]
    ORDER,
    #[token("OUT", ignore(ascii_case))]
    OUT,
    #[token("OUTER", ignore(ascii_case))]
    OUTER,
    #[token("OUTPUTFORMAT", ignore(ascii_case))]
    OUTPUTFORMAT,
    #[token("OVER", ignore(ascii_case))]
    OVER,
    #[token("OVERFLOW", ignore(ascii_case))]
    OVERFLOW,
    #[token("OVERLAPS", ignore(ascii_case))]
    OVERLAPS,
    #[token("OVERLAY", ignore(ascii_case))]
    OVERLAY,
    #[token("OVERWRITE", ignore(ascii_case))]
    OVERWRITE,
    #[token("PARAMETER", ignore(ascii_case))]
    PARAMETER,
    #[token("PARQUET", ignore(ascii_case))]
    PARQUET,
    #[token("PARTITION", ignore(ascii_case))]
    PARTITION,
    #[token("PARTITIONED", ignore(ascii_case))]
    PARTITIONED,
    #[token("PARTITIONS", ignore(ascii_case))]
    PARTITIONS,
    #[token("PERCENT", ignore(ascii_case))]
    PERCENT,
    #[token("PERCENTILE_CONT", ignore(ascii_case))]
    PERCENTILE_CONT,
    #[token("PERCENTILE_DISC", ignore(ascii_case))]
    PERCENTILE_DISC,
    #[token("PERCENT_RANK", ignore(ascii_case))]
    PERCENT_RANK,
    #[token("PERIOD", ignore(ascii_case))]
    PERIOD,
    #[token("PORTION", ignore(ascii_case))]
    PORTION,
    #[token("POSITION", ignore(ascii_case))]
    POSITION,
    #[token("POSITION_REGEX", ignore(ascii_case))]
    POSITION_REGEX,
    #[token("POWER", ignore(ascii_case))]
    POWER,
    #[token("PRECEDES", ignore(ascii_case))]
    PRECEDES,
    #[token("PRECEDING", ignore(ascii_case))]
    PRECEDING,
    #[token("PRECISION", ignore(ascii_case))]
    PRECISION,
    #[token("PREPARE", ignore(ascii_case))]
    PREPARE,
    #[token("PRIMARY", ignore(ascii_case))]
    PRIMARY,
    #[token("PRIVILEGES", ignore(ascii_case))]
    PRIVILEGES,
    #[token("PROCEDURE", ignore(ascii_case))]
    PROCEDURE,
    #[token("PURGE", ignore(ascii_case))]
    PURGE,
    #[token("RANGE", ignore(ascii_case))]
    RANGE,
    #[token("RANK", ignore(ascii_case))]
    RANK,
    #[token("RCFILE", ignore(ascii_case))]
    RCFILE,
    #[token("READ", ignore(ascii_case))]
    READ,
    #[token("READS", ignore(ascii_case))]
    READS,
    #[token("REAL", ignore(ascii_case))]
    REAL,
    #[token("RECURSIVE", ignore(ascii_case))]
    RECURSIVE,
    #[token("REF", ignore(ascii_case))]
    REF,
    #[token("REFERENCES", ignore(ascii_case))]
    REFERENCES,
    #[token("REFERENCING", ignore(ascii_case))]
    REFERENCING,
    #[token("REGCLASS", ignore(ascii_case))]
    REGCLASS,
    #[token("REGR_AVGX", ignore(ascii_case))]
    REGR_AVGX,
    #[token("REGR_AVGY", ignore(ascii_case))]
    REGR_AVGY,
    #[token("REGR_COUNT", ignore(ascii_case))]
    REGR_COUNT,
    #[token("REGR_INTERCEPT", ignore(ascii_case))]
    REGR_INTERCEPT,
    #[token("REGR_R2", ignore(ascii_case))]
    REGR_R2,
    #[token("REGR_SLOPE", ignore(ascii_case))]
    REGR_SLOPE,
    #[token("REGR_SXX", ignore(ascii_case))]
    REGR_SXX,
    #[token("REGR_SXY", ignore(ascii_case))]
    REGR_SXY,
    #[token("REGR_SYY", ignore(ascii_case))]
    REGR_SYY,
    #[token("RELEASE", ignore(ascii_case))]
    RELEASE,
    #[token("RENAME", ignore(ascii_case))]
    RENAME,
    #[token("REPAIR", ignore(ascii_case))]
    REPAIR,
    #[token("REPEATABLE", ignore(ascii_case))]
    REPEATABLE,
    #[token("REPLACE", ignore(ascii_case))]
    REPLACE,
    #[token("RESTRICT", ignore(ascii_case))]
    RESTRICT,
    #[token("RESULT", ignore(ascii_case))]
    RESULT,
    #[token("RETURN", ignore(ascii_case))]
    RETURN,
    #[token("RETURNS", ignore(ascii_case))]
    RETURNS,
    #[token("REVOKE", ignore(ascii_case))]
    REVOKE,
    #[token("RIGHT", ignore(ascii_case))]
    RIGHT,
    #[token("ROLLBACK", ignore(ascii_case))]
    ROLLBACK,
    #[token("ROLLUP", ignore(ascii_case))]
    ROLLUP,
    #[token("ROW", ignore(ascii_case))]
    ROW,
    #[token("ROWID", ignore(ascii_case))]
    ROWID,
    #[token("ROWS", ignore(ascii_case))]
    ROWS,
    #[token("ROW_NUMBER", ignore(ascii_case))]
    ROW_NUMBER,
    #[token("SAVEPOINT", ignore(ascii_case))]
    SAVEPOINT,
    #[token("SCHEMA", ignore(ascii_case))]
    SCHEMA,
    #[token("SCOPE", ignore(ascii_case))]
    SCOPE,
    #[token("SCROLL", ignore(ascii_case))]
    SCROLL,
    #[token("SEARCH", ignore(ascii_case))]
    SEARCH,
    #[token("SECOND", ignore(ascii_case))]
    SECOND,
    #[token("SELECT", ignore(ascii_case))]
    SELECT,
    #[token("SENSITIVE", ignore(ascii_case))]
    SENSITIVE,
    #[token("SEQUENCE", ignore(ascii_case))]
    SEQUENCE,
    #[token("SEQUENCEFILE", ignore(ascii_case))]
    SEQUENCEFILE,
    #[token("SEQUENCES", ignore(ascii_case))]
    SEQUENCES,
    #[token("SERDE", ignore(ascii_case))]
    SERDE,
    #[token("SERIALIZABLE", ignore(ascii_case))]
    SERIALIZABLE,
    #[token("SESSION", ignore(ascii_case))]
    SESSION,
    #[token("SESSION_USER", ignore(ascii_case))]
    SESSION_USER,
    #[token("SET", ignore(ascii_case))]
    SET,
    #[token("SETS", ignore(ascii_case))]
    SETS,
    #[token("SHOW", ignore(ascii_case))]
    SHOW,
    #[token("SIMILAR", ignore(ascii_case))]
    SIMILAR,
    #[token("SMALLINT", ignore(ascii_case))]
    SMALLINT,
    #[token("SNAPSHOT", ignore(ascii_case))]
    SNAPSHOT,
    #[token("SOME", ignore(ascii_case))]
    SOME,
    #[token("SORT", ignore(ascii_case))]
    SORT,
    #[token("SPECIFIC", ignore(ascii_case))]
    SPECIFIC,
    #[token("SPECIFICTYPE", ignore(ascii_case))]
    SPECIFICTYPE,
    #[token("SQL", ignore(ascii_case))]
    SQL,
    #[token("SQLEXCEPTION", ignore(ascii_case))]
    SQLEXCEPTION,
    #[token("SQLSTATE", ignore(ascii_case))]
    SQLSTATE,
    #[token("SQLWARNING", ignore(ascii_case))]
    SQLWARNING,
    #[token("SQRT", ignore(ascii_case))]
    SQRT,
    #[token("START", ignore(ascii_case))]
    START,
    #[token("STATIC", ignore(ascii_case))]
    STATIC,
    #[token("STATISTICS", ignore(ascii_case))]
    STATISTICS,
    #[token("STDDEV_POP", ignore(ascii_case))]
    STDDEV_POP,
    #[token("STDDEV_SAMP", ignore(ascii_case))]
    STDDEV_SAMP,
    #[token("STDIN", ignore(ascii_case))]
    STDIN,
    #[token("STORED", ignore(ascii_case))]
    STORED,
    #[token("STRING", ignore(ascii_case))]
    STRING,
    #[token("SUBMULTISET", ignore(ascii_case))]
    SUBMULTISET,
    #[token("SUBSTRING", ignore(ascii_case))]
    SUBSTRING,
    #[token("SUBSTRING_REGEX", ignore(ascii_case))]
    SUBSTRING_REGEX,
    #[token("SUCCEEDS", ignore(ascii_case))]
    SUCCEEDS,
    #[token("SUM", ignore(ascii_case))]
    SUM,
    #[token("SYMMETRIC", ignore(ascii_case))]
    SYMMETRIC,
    #[token("SYNC", ignore(ascii_case))]
    SYNC,
    #[token("SYSTEM", ignore(ascii_case))]
    SYSTEM,
    #[token("SYSTEM_TIME", ignore(ascii_case))]
    SYSTEM_TIME,
    #[token("SYSTEM_USER", ignore(ascii_case))]
    SYSTEM_USER,
    #[token("TABLE", ignore(ascii_case))]
    TABLE,
    #[token("TABLES", ignore(ascii_case))]
    TABLES,
    #[token("TABLESAMPLE", ignore(ascii_case))]
    TABLESAMPLE,
    #[token("TBLPROPERTIES", ignore(ascii_case))]
    TBLPROPERTIES,
    #[token("TEMP", ignore(ascii_case))]
    TEMP,
    #[token("TEMPORARY", ignore(ascii_case))]
    TEMPORARY,
    #[token("TEXT", ignore(ascii_case))]
    TEXT,
    #[token("TEXTFILE", ignore(ascii_case))]
    TEXTFILE,
    #[token("THEN", ignore(ascii_case))]
    THEN,
    #[token("TIES", ignore(ascii_case))]
    TIES,
    #[token("TIME", ignore(ascii_case))]
    TIME,
    #[token("TIMESTAMP", ignore(ascii_case))]
    TIMESTAMP,
    #[token("TIMEZONE_HOUR", ignore(ascii_case))]
    TIMEZONE_HOUR,
    #[token("TIMEZONE_MINUTE", ignore(ascii_case))]
    TIMEZONE_MINUTE,
    #[token("TINYINT", ignore(ascii_case))]
    TINYINT,
    #[token("TO", ignore(ascii_case))]
    TO,
    #[token("TOP", ignore(ascii_case))]
    TOP,
    #[token("TRAILING", ignore(ascii_case))]
    TRAILING,
    #[token("TRANSACTION", ignore(ascii_case))]
    TRANSACTION,
    #[token("TRANSLATE", ignore(ascii_case))]
    TRANSLATE,
    #[token("TRANSLATE_REGEX", ignore(ascii_case))]
    TRANSLATE_REGEX,
    #[token("TRANSLATION", ignore(ascii_case))]
    TRANSLATION,
    #[token("TREAT", ignore(ascii_case))]
    TREAT,
    #[token("TRIGGER", ignore(ascii_case))]
    TRIGGER,
    #[token("TRIM", ignore(ascii_case))]
    TRIM,
    #[token("TRIM_ARRAY", ignore(ascii_case))]
    TRIM_ARRAY,
    #[token("TRUE", ignore(ascii_case))]
    TRUE,
    #[token("TRUNCATE", ignore(ascii_case))]
    TRUNCATE,
    #[token("TRY_CAST", ignore(ascii_case))]
    TRY_CAST,
    #[token("TYPE", ignore(ascii_case))]
    TYPE,
    #[token("UESCAPE", ignore(ascii_case))]
    UESCAPE,
    #[token("UNBOUNDED", ignore(ascii_case))]
    UNBOUNDED,
    #[token("UNCOMMITTED", ignore(ascii_case))]
    UNCOMMITTED,
    #[token("UNION", ignore(ascii_case))]
    UNION,
    #[token("UNIQUE", ignore(ascii_case))]
    UNIQUE,
    #[token("UNKNOWN", ignore(ascii_case))]
    UNKNOWN,
    #[token("UNNEST", ignore(ascii_case))]
    UNNEST,
    #[token("UPDATE", ignore(ascii_case))]
    UPDATE,
    #[token("UPPER", ignore(ascii_case))]
    UPPER,
    #[token("USAGE", ignore(ascii_case))]
    USAGE,
    #[token("USER", ignore(ascii_case))]
    USER,
    #[token("USING", ignore(ascii_case))]
    USING,
    #[token("UUID", ignore(ascii_case))]
    UUID,
    #[token("VALUE", ignore(ascii_case))]
    VALUE,
    #[token("VALUES", ignore(ascii_case))]
    VALUES,
    #[token("VALUE_OF", ignore(ascii_case))]
    VALUE_OF,
    #[token("VARBINARY", ignore(ascii_case))]
    VARBINARY,
    #[token("VARCHAR", ignore(ascii_case))]
    VARCHAR,
    #[token("VARYING", ignore(ascii_case))]
    VARYING,
    #[token("VAR_POP", ignore(ascii_case))]
    VAR_POP,
    #[token("VAR_SAMP", ignore(ascii_case))]
    VAR_SAMP,
    #[token("VERBOSE", ignore(ascii_case))]
    VERBOSE,
    #[token("VERSIONING", ignore(ascii_case))]
    VERSIONING,
    #[token("VIEW", ignore(ascii_case))]
    VIEW,
    #[token("VIRTUAL", ignore(ascii_case))]
    VIRTUAL,
    #[token("WHEN", ignore(ascii_case))]
    WHEN,
    #[token("WHENEVER", ignore(ascii_case))]
    WHENEVER,
    #[token("WHERE", ignore(ascii_case))]
    WHERE,
    #[token("WIDTH_BUCKET", ignore(ascii_case))]
    WIDTH_BUCKET,
    #[token("WINDOW", ignore(ascii_case))]
    WINDOW,
    #[token("WITH", ignore(ascii_case))]
    WITH,
    #[token("WITHIN", ignore(ascii_case))]
    WITHIN,
    #[token("WITHOUT", ignore(ascii_case))]
    WITHOUT,
    #[token("WORK", ignore(ascii_case))]
    WORK,
    #[token("WRITE", ignore(ascii_case))]
    WRITE,
    #[token("XOR", ignore(ascii_case))]
    XOR,
    #[token("YEAR", ignore(ascii_case))]
    YEAR,
    #[token("ZONE", ignore(ascii_case))]
    ZONE,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    pub text: &'a str,
    pub span: Span,
}

pub fn tokenise(input: &str) -> Result<Vec<Token>> {
    let mut lex = TokenKind::lexer(input);
    let mut tokens = Vec::new();

    while let Some(kind) = lex.next() {
        if kind == TokenKind::Error {
            let position = lex.span().start;
            let rest = input[position..].to_string();
            return Err(Error::UnrecognisedToken { rest, position });
        } else {
            tokens.push(Token {
                kind,
                text: lex.slice(),
                span: lex.span(),
            })
        }
    }

    tokens.push(Token {
        kind: TokenKind::EOI,
        text: "",
        span: (lex.span().end)..(lex.span().end),
    });

    Ok(tokens)
}
