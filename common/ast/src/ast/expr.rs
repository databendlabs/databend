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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq)]

pub enum Expr {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        database: Option<Identifier>,
        table: Option<Identifier>,
        column: Identifier,
    },
    /// `IS [ NOT ] NULL` expression
    IsNull { expr: Box<Expr>, not: bool },
    /// `[ NOT ] IN (expr, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        expr: Box<Expr>,
        target_type: TypeName,
        pg_style: bool,
    },
    /// `TRY_CAST` expression`
    TryCast {
        expr: Box<Expr>,
        target_type: TypeName,
    },
    /// EXTRACT(DateTimeField FROM <expr>)
    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        substr_expr: Box<Expr>,
        str_expr: Box<Expr>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal(Literal),
    /// `COUNT(*)` expression
    CountAll,
    /// `(foo, bar)`
    Tuple { exprs: Vec<Expr> },
    /// Scalar function call
    FunctionCall {
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: Identifier,
        args: Vec<Expr>,
        params: Vec<Literal>,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// `EXISTS` expression
    Exists(Box<Query>),
    /// Scalar subquery, which will only return a single row with a single column.
    Subquery(Box<Query>),
    // TODO(andylokandy): allow interval, function, and others alike to be a key
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        expr: Box<Expr>,
        accessor: MapAccessor,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    // Numeric literal value
    Number(String),
    // Quoted string literal value
    String(String),
    Boolean(bool),
    Interval(Interval),
    CurrentTimestamp,
    Null,
}

/// The display style for a map access expression
#[derive(Debug, Clone, PartialEq)]
pub enum MapAccessor {
    /// `[0][1]`
    Bracket { key: Literal },
    /// `.a.b`
    Period { key: Identifier },
    /// `:a:b`
    Colon { key: Identifier },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeName {
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Date,
    DateTime { precision: Option<u64> },
    Timestamp,
    String,
    Array { item_type: Option<Box<TypeName>> },
    Object,
    Variant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    pub value: String,
    pub field: DateTimeField,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DateTimeField {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Century,
    Decade,
    Dow,
    Doy,
    Epoch,
    Isodow,
    Isoyear,
    Julian,
    Microseconds,
    Millenium,
    Milliseconds,
    Quarter,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TrimWhere {
    Both,
    Leading,
    Trailing,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Div,
    Divide,
    Modulo,
    StringConcat,
    // `>` operator
    Gt,
    // `<` operator
    Lt,
    // `>=` operator
    Gte,
    // `<=` operator
    Lte,
    Eq,
    NotEq,
    And,
    Or,
    Xor,
    Like,
    NotLike,
    Regexp,
    RLike,
    NotRegexp,
    NotRLike,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Plus => {
                write!(f, "+")
            }
            UnaryOperator::Minus => {
                write!(f, "-")
            }
            UnaryOperator::Not => {
                write!(f, "NOT")
            }
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Plus => {
                write!(f, "+")
            }
            BinaryOperator::Minus => {
                write!(f, "-")
            }
            BinaryOperator::Multiply => {
                write!(f, "*")
            }
            BinaryOperator::Div => {
                write!(f, "DIV")
            }
            BinaryOperator::Divide => {
                write!(f, "/")
            }
            BinaryOperator::Modulo => {
                write!(f, "%")
            }
            BinaryOperator::StringConcat => {
                write!(f, "||")
            }
            BinaryOperator::Gt => {
                write!(f, ">")
            }
            BinaryOperator::Lt => {
                write!(f, "<")
            }
            BinaryOperator::Gte => {
                write!(f, ">=")
            }
            BinaryOperator::Lte => {
                write!(f, "<=")
            }
            BinaryOperator::Eq => {
                write!(f, "=")
            }
            BinaryOperator::NotEq => {
                write!(f, "<>")
            }
            BinaryOperator::And => {
                write!(f, "AND")
            }
            BinaryOperator::Or => {
                write!(f, "OR")
            }
            BinaryOperator::Xor => {
                write!(f, "XOR")
            }
            BinaryOperator::Like => {
                write!(f, "LIKE")
            }
            BinaryOperator::NotLike => {
                write!(f, "NOT LIKE")
            }
            BinaryOperator::Regexp => {
                write!(f, "REGEXP")
            }
            BinaryOperator::RLike => {
                write!(f, "RLIKE")
            }
            BinaryOperator::NotRegexp => {
                write!(f, "NOT REGEXP")
            }
            BinaryOperator::NotRLike => {
                write!(f, "NOT RLIKE")
            }
            BinaryOperator::BitwiseOr => {
                write!(f, "|")
            }
            BinaryOperator::BitwiseAnd => {
                write!(f, "&")
            }
            BinaryOperator::BitwiseXor => {
                write!(f, "^")
            }
        }
    }
}

impl Display for TypeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeName::Boolean => {
                write!(f, "BOOLEAN")?;
            }
            TypeName::UInt8 => {
                write!(f, "UInt8")?;
            }
            TypeName::UInt16 => {
                write!(f, "UInt16")?;
            }
            TypeName::UInt32 => {
                write!(f, "UInt32")?;
            }
            TypeName::UInt64 => {
                write!(f, "UInt64")?;
            }
            TypeName::Int8 => {
                write!(f, "Int8")?;
            }
            TypeName::Int16 => {
                write!(f, "Int16")?;
            }
            TypeName::Int32 => {
                write!(f, "Int32")?;
            }
            TypeName::Int64 => {
                write!(f, "Int64")?;
            }
            TypeName::Float32 => {
                write!(f, "Float32")?;
            }
            TypeName::Float64 => {
                write!(f, "Float64")?;
            }
            TypeName::Date => {
                write!(f, "DATE")?;
            }
            TypeName::DateTime { precision } => {
                write!(f, "DATETIME")?;
                if let Some(precision) = precision {
                    write!(f, "({})", *precision)?;
                }
            }
            TypeName::Timestamp => {
                write!(f, "TIMESTAMP")?;
            }
            TypeName::String => {
                write!(f, "STRING")?;
            }
            TypeName::Array { item_type } => {
                write!(f, "ARRAY")?;
                if let Some(item_type) = item_type {
                    write!(f, "({})", *item_type)?;
                }
            }
            TypeName::Object => {
                write!(f, "OBJECT")?;
            }
            TypeName::Variant => {
                write!(f, "VARIANT")?;
            }
        }
        Ok(())
    }
}

impl Display for DateTimeField {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Week => "WEEK",
            DateTimeField::Day => "DAY",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
            DateTimeField::Century => "CENTURY",
            DateTimeField::Decade => "DECADE",
            DateTimeField::Dow => "DOW",
            DateTimeField::Doy => "DOY",
            DateTimeField::Epoch => "EPOCH",
            DateTimeField::Isodow => "ISODOW",
            DateTimeField::Isoyear => "ISOYEAR",
            DateTimeField::Julian => "JULIAN",
            DateTimeField::Microseconds => "MICROSECONDS",
            DateTimeField::Millenium => "MILLENIUM",
            DateTimeField::Milliseconds => "MILLISECONDS",
            DateTimeField::Quarter => "QUARTER",
            DateTimeField::Timezone => "TIMEZONE",
            DateTimeField::TimezoneHour => "TIMEZONE_HOUR",
            DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE",
        })
    }
}

impl Display for TrimWhere {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(match self {
            TrimWhere::Both => "BOTH",
            TrimWhere::Leading => "LEADING",
            TrimWhere::Trailing => "TRAILING",
        })
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Number(val) => {
                write!(f, "{val}")
            }
            Literal::String(val) => {
                write!(f, "\'{val}\'")
            }
            Literal::Boolean(val) => {
                if *val {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Literal::CurrentTimestamp => {
                write!(f, "CURRENT_TIMESTAMP")
            }
            Literal::Interval(interval) => {
                write!(f, "INTERVAL {} {}", interval.value, interval.field)
            }
            Literal::Null => {
                write!(f, "NULL")
            }
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::ColumnRef {
                database,
                table,
                column,
            } => {
                write_period_separated_list(f, database.iter().chain(table).chain(Some(column)))?;
            }
            Expr::IsNull { expr, not } => {
                write!(f, "{expr} IS")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " NULL")?;
            }
            Expr::InList { expr, list, not } => {
                write!(f, "{expr}")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " IN(")?;
                write_comma_separated_list(f, list)?;
                write!(f, ")")?;
            }
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => {
                write!(f, "{expr}")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " IN({subquery})")?;
            }
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => {
                write!(f, "{expr}")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {low} AND {high}")?;
            }
            Expr::UnaryOp { op, expr } => {
                write!(f, "{op} {expr}")?;
            }
            Expr::BinaryOp { op, left, right } => {
                write!(f, "{left} {op} {right}")?;
            }
            Expr::Cast {
                expr,
                target_type,
                pg_style,
            } => {
                if *pg_style {
                    write!(f, "{expr}::{target_type}")?;
                } else {
                    write!(f, "CAST({expr} AS {target_type})")?;
                }
            }
            Expr::TryCast { expr, target_type } => {
                write!(f, "TRY_CAST({expr} AS {target_type})")?;
            }
            Expr::Extract { field, expr } => {
                write!(f, "EXTRACT({field} FROM {expr})")?;
            }
            Expr::Position {
                substr_expr,
                str_expr,
            } => {
                write!(f, "POSITION({substr_expr} IN {str_expr})")?;
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                write!(f, "SUBSTRING({expr}")?;
                if let Some(substring_from) = substring_from {
                    write!(f, " FROM {substring_from}")?;
                }
                if let Some(substring_for) = substring_for {
                    write!(f, " FOR {substring_for}")?;
                }
                write!(f, ")")?;
            }
            Expr::Trim { expr, trim_where } => {
                write!(f, "TRIM(")?;
                if let Some((trim_where, trim_str)) = trim_where {
                    write!(f, "{trim_where} {trim_str} FROM ")?;
                }
                write!(f, "{expr})")?;
            }
            Expr::Literal(lit) => {
                write!(f, "{lit}")?;
            }
            Expr::CountAll => {
                write!(f, "COUNT(*)")?;
            }
            Expr::Tuple { exprs } => {
                write!(f, "(")?;
                write_comma_separated_list(f, exprs)?;
                if exprs.len() == 1 {
                    write!(f, ",")?;
                }
                write!(f, ")")?;
            }
            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => {
                write!(f, "{name}")?;
                if !params.is_empty() {
                    write!(f, "(")?;
                    write_comma_separated_list(f, params)?;
                    write!(f, ")")?;
                }
                write!(f, "(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                write_comma_separated_list(f, args)?;
                write!(f, ")")?;
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op} ")?;
                }
                for (cond, res) in conditions.iter().zip(results) {
                    write!(f, " WHEN {cond} THEN {res}")?;
                }
                if let Some(el) = else_result {
                    write!(f, " ELSE {el}")?;
                }
                write!(f, " END")?;
            }
            Expr::Exists(subquery) => {
                write!(f, "EXITS ({subquery})")?;
            }
            Expr::Subquery(subquery) => {
                write!(f, "({subquery})")?;
            }
            Expr::MapAccess { expr, accessor } => {
                write!(f, "{}", expr)?;
                match accessor {
                    MapAccessor::Bracket { key } => write!(f, "[{key}]")?,
                    MapAccessor::Period { key } => write!(f, ".{key}")?,
                    MapAccessor::Colon { key } => write!(f, ":{key}")?,
                }
            }
        }

        Ok(())
    }
}
