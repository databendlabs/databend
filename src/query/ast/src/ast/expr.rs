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

use common_datavalues::IntervalKind;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::parser::token::Token;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'a> {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        span: &'a [Token<'a>],
        database: Option<Identifier<'a>>,
        table: Option<Identifier<'a>>,
        column: Identifier<'a>,
    },
    /// `IS [ NOT ] NULL` expression
    IsNull {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        not: bool,
    },
    /// `IS [NOT] DISTINCT` expression
    IsDistinctFrom {
        span: &'a [Token<'a>],
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
        not: bool,
    },
    /// `[ NOT ] IN (expr, ...)`
    InList {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        list: Vec<Expr<'a>>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        subquery: Box<Query<'a>>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        low: Box<Expr<'a>>,
        high: Box<Expr<'a>>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        span: &'a [Token<'a>],
        op: BinaryOperator,
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
    },
    /// Unary operation
    UnaryOp {
        span: &'a [Token<'a>],
        op: UnaryOperator,
        expr: Box<Expr<'a>>,
    },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        target_type: TypeName,
        pg_style: bool,
    },
    /// `TRY_CAST` expression`
    TryCast {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        span: &'a [Token<'a>],
        kind: IntervalKind,
        expr: Box<Expr<'a>>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        span: &'a [Token<'a>],
        substr_expr: Box<Expr<'a>>,
        str_expr: Box<Expr<'a>>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        substring_from: Option<Box<Expr<'a>>>,
        substring_for: Option<Box<Expr<'a>>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr<'a>>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal { span: &'a [Token<'a>], lit: Literal },
    /// `COUNT(*)` expression
    CountAll { span: &'a [Token<'a>] },
    /// `(foo, bar)`
    Tuple {
        span: &'a [Token<'a>],
        exprs: Vec<Expr<'a>>,
    },
    /// Scalar function call
    FunctionCall {
        span: &'a [Token<'a>],
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: Identifier<'a>,
        args: Vec<Expr<'a>>,
        params: Vec<Literal>,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        span: &'a [Token<'a>],
        operand: Option<Box<Expr<'a>>>,
        conditions: Vec<Expr<'a>>,
        results: Vec<Expr<'a>>,
        else_result: Option<Box<Expr<'a>>>,
    },
    /// `EXISTS` expression
    Exists {
        span: &'a [Token<'a>],
        /// Indicate if this is a `NOT EXISTS`
        not: bool,
        subquery: Box<Query<'a>>,
    },
    /// Scalar/ANY/ALL/SOME subquery
    Subquery {
        span: &'a [Token<'a>],
        modifier: Option<SubqueryModifier>,
        subquery: Box<Query<'a>>,
    },
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        accessor: MapAccessor<'a>,
    },
    /// The `Array` expr
    Array {
        span: &'a [Token<'a>],
        exprs: Vec<Expr<'a>>,
    },
    /// The `Interval 1 DAY` expr
    Interval {
        span: &'a [Token<'a>],
        expr: Box<Expr<'a>>,
        unit: IntervalKind,
    },
    DateAdd {
        span: &'a [Token<'a>],
        unit: IntervalKind,
        interval: Box<Expr<'a>>,
        date: Box<Expr<'a>>,
    },
    DateSub {
        span: &'a [Token<'a>],
        unit: IntervalKind,
        interval: Box<Expr<'a>>,
        date: Box<Expr<'a>>,
    },
    DateTrunc {
        span: &'a [Token<'a>],
        unit: IntervalKind,
        date: Box<Expr<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubqueryModifier {
    Any,
    All,
    Some,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(u64),
    Float(f64),
    BigInt { lit: String, is_hex: bool },
    // Quoted string literal value
    String(String),
    Boolean(bool),
    CurrentTimestamp,
    Null,
}

/// The display style for a map access expression
#[derive(Debug, Clone, PartialEq)]
pub enum MapAccessor<'a> {
    /// `[0][1]`
    Bracket { key: Literal },
    /// `.a.b`
    Period { key: Identifier<'a> },
    /// `:a:b`
    Colon { key: Identifier<'a> },
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
    Timestamp {
        precision: Option<u64>,
    },
    String,
    Array {
        item_type: Option<Box<TypeName>>,
    },
    Tuple {
        fields_name: Option<Vec<String>>,
        fields_type: Vec<TypeName>,
    },
    Object,
    Variant,
    Nullable(Box<TypeName>),
}

impl TypeName {
    pub fn wrap_nullable(self) -> Self {
        if !matches!(&self, &Self::Nullable(_)) {
            Self::Nullable(Box::new(self))
        } else {
            self
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrimWhere {
    Both,
    Leading,
    Trailing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl BinaryOperator {
    pub fn to_contrary(&self) -> Result<Self> {
        match &self {
            BinaryOperator::Gt => Ok(BinaryOperator::Lte),
            BinaryOperator::Lt => Ok(BinaryOperator::Gte),
            BinaryOperator::Gte => Ok(BinaryOperator::Lt),
            BinaryOperator::Lte => Ok(BinaryOperator::Gt),
            BinaryOperator::Eq => Ok(BinaryOperator::NotEq),
            BinaryOperator::NotEq => Ok(BinaryOperator::Eq),
            _ => Err(ErrorCode::UnImplement(format!(
                "Converting {self} to its relative is not currently supported"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl<'a> Expr<'a> {
    pub fn span(&self) -> &'a [Token<'a>] {
        match self {
            Expr::ColumnRef { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::IsDistinctFrom { span, .. }
            | Expr::InList { span, .. }
            | Expr::InSubquery { span, .. }
            | Expr::Between { span, .. }
            | Expr::BinaryOp { span, .. }
            | Expr::UnaryOp { span, .. }
            | Expr::Cast { span, .. }
            | Expr::TryCast { span, .. }
            | Expr::Extract { span, .. }
            | Expr::Position { span, .. }
            | Expr::Substring { span, .. }
            | Expr::Trim { span, .. }
            | Expr::Literal { span, .. }
            | Expr::CountAll { span }
            | Expr::Tuple { span, .. }
            | Expr::FunctionCall { span, .. }
            | Expr::Case { span, .. }
            | Expr::Exists { span, .. }
            | Expr::Subquery { span, .. }
            | Expr::MapAccess { span, .. }
            | Expr::Array { span, .. }
            | Expr::Interval { span, .. }
            | Expr::DateAdd { span, .. }
            | Expr::DateSub { span, .. }
            | Expr::DateTrunc { span, .. } => span,
        }
    }
}

impl Display for SubqueryModifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubqueryModifier::Any => write!(f, "ANY"),
            SubqueryModifier::All => write!(f, "ALL"),
            SubqueryModifier::Some => write!(f, "SOME"),
        }
    }
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
            TypeName::Timestamp { precision } => {
                write!(f, "Timestamp")?;
                if let Some(precision) = precision {
                    write!(f, "({})", *precision)?;
                }
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
            TypeName::Tuple {
                fields_name,
                fields_type,
            } => {
                write!(f, "TUPLE(")?;
                let mut first = true;
                match fields_name {
                    Some(fields_name) => {
                        for (name, ty) in fields_name.iter().zip(fields_type.iter()) {
                            if !first {
                                write!(f, ", ")?;
                            }
                            first = false;
                            write!(f, "{} {}", name, ty)?;
                        }
                    }
                    None => {
                        for ty in fields_type.iter() {
                            if !first {
                                write!(f, ", ")?;
                            }
                            first = false;
                            write!(f, "{}", ty)?;
                        }
                    }
                }
                write!(f, ")")?;
            }
            TypeName::Object => {
                write!(f, "OBJECT")?;
            }
            TypeName::Variant => {
                write!(f, "VARIANT")?;
            }
            TypeName::Nullable(ty) => {
                write!(f, "{} NULL", ty)?;
            }
        }
        Ok(())
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
            Literal::Integer(val) => {
                write!(f, "{val}")
            }
            Literal::Float(val) => {
                write!(f, "{val}")
            }
            Literal::BigInt { lit, is_hex } => {
                if *is_hex {
                    write!(f, "0x")?;
                }
                write!(f, "{lit}")
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
            Literal::Null => {
                write!(f, "NULL")
            }
        }
    }
}

impl<'a> Display for Expr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::ColumnRef {
                database,
                table,
                column,
                ..
            } => {
                if f.alternate() {
                    write!(f, "{}", column.name)?;
                } else {
                    write_period_separated_list(
                        f,
                        database.iter().chain(table).chain(Some(column)),
                    )?;
                }
            }
            Expr::IsNull { expr, not, .. } => {
                write!(f, "{expr} IS")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " NULL")?;
            }
            Expr::IsDistinctFrom {
                left, right, not, ..
            } => {
                write!(f, "{left} IS")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " DISTINCT FROM {right}")?;
            }

            Expr::InList {
                expr, list, not, ..
            } => {
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
                ..
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
                ..
            } => {
                write!(f, "{expr}")?;
                if *not {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {low} AND {high}")?;
            }
            Expr::UnaryOp { op, expr, .. } => {
                write!(f, "{op} {expr}")?;
            }
            Expr::BinaryOp {
                op, left, right, ..
            } => {
                write!(f, "{left} {op} {right}")?;
            }
            Expr::Cast {
                expr,
                target_type,
                pg_style,
                ..
            } => {
                if *pg_style {
                    write!(f, "{expr}::{target_type}")?;
                } else {
                    write!(f, "CAST({expr} AS {target_type})")?;
                }
            }
            Expr::TryCast {
                expr, target_type, ..
            } => {
                write!(f, "TRY_CAST({expr} AS {target_type})")?;
            }
            Expr::Extract {
                kind: field, expr, ..
            } => {
                write!(f, "EXTRACT({field} FROM {expr})")?;
            }
            Expr::Position {
                substr_expr,
                str_expr,
                ..
            } => {
                write!(f, "POSITION({substr_expr} IN {str_expr})")?;
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
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
            Expr::Trim {
                expr, trim_where, ..
            } => {
                write!(f, "TRIM(")?;
                if let Some((trim_where, trim_str)) = trim_where {
                    write!(f, "{trim_where} {trim_str} FROM ")?;
                }
                write!(f, "{expr})")?;
            }
            Expr::Literal { lit, .. } => {
                write!(f, "{lit}")?;
            }
            Expr::CountAll { .. } => {
                write!(f, "COUNT(*)")?;
            }
            Expr::Tuple { exprs, .. } => {
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
                ..
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
                ..
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
            Expr::Exists { not, subquery, .. } => {
                if *not {
                    write!(f, "NOT ")?;
                }
                write!(f, "EXISTS ({subquery})")?;
            }
            Expr::Subquery {
                subquery, modifier, ..
            } => {
                if let Some(m) = modifier {
                    write!(f, "{m} ")?;
                }
                write!(f, "({subquery})")?;
            }
            Expr::MapAccess { expr, accessor, .. } => {
                write!(f, "{}", expr)?;
                match accessor {
                    MapAccessor::Bracket { key } => write!(f, "[{key}]")?,
                    MapAccessor::Period { key } => write!(f, ".{key}")?,
                    MapAccessor::Colon { key } => write!(f, ":{key}")?,
                }
            }
            Expr::Array { exprs, .. } => {
                write!(f, "[")?;
                write_comma_separated_list(f, exprs)?;
                write!(f, "]")?;
            }
            Expr::Interval { expr, unit, .. } => {
                write!(f, "INTERVAL {expr} {unit}")?;
            }
            Expr::DateAdd {
                unit,
                interval,
                date,
                ..
            } => {
                write!(f, "DATE_ADD({unit}, INTERVAL {interval}, {date})")?;
            }
            Expr::DateSub {
                unit,
                interval,
                date,
                ..
            } => {
                write!(f, "DATE_SUB({unit}, INTERVAL {interval}, {date})")?;
            }
            Expr::DateTrunc { unit, date, .. } => {
                write!(f, "DATE_TRUNC({unit}, {date})")?;
            }
        }

        Ok(())
    }
}
