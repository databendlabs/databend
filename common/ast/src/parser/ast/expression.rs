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

use super::Identifier;
use super::Query;
use crate::parser::ast::display_identifier_vec;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
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
    },
    /// A literal value, such as string, number, date or NULL
    Literal(Literal),
    /// `COUNT(*)` expression
    CountAll,
    /// Scalar function call
    FunctionCall {
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: String,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeName {
    Char(Option<u64>),
    Varchar(Option<u64>),
    Decimal(Option<u64>, Option<u64>),
    Float(Option<u64>),
    Int(Option<u64>),
    TinyInt(Option<u64>),
    SmallInt(Option<u64>),
    BigInt(Option<u64>),
    Real,
    Double,
    Boolean,
    Date,
    Time,
    Timestamp,
    Interval,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    // Numeric literal value
    Number(String),
    // Quoted string literal value
    String(String),
    Boolean(bool),
    Null,
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
    Like,
    NotLike,
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
            BinaryOperator::Like => {
                write!(f, "LIKE")
            }
            BinaryOperator::NotLike => {
                write!(f, "NOT LIKE")
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
            TypeName::Char(length) => {
                write!(f, "CHAR")?;
                if let Some(len) = length {
                    write!(f, "({})", len)?;
                }
            }
            TypeName::Varchar(length) => {
                write!(f, "VARCHAR")?;
                if let Some(len) = length {
                    write!(f, "({})", len)?;
                }
            }
            TypeName::Decimal(prec, scale) => {
                write!(f, "DECIMAL")?;
                if let (Some(p), Some(s)) = (prec, scale) {
                    write!(f, "({}, {})", p, s)?;
                }
            }
            TypeName::Float(prec) => {
                write!(f, "FLOAT")?;
                if let Some(p) = prec {
                    write!(f, "({})", p)?;
                }
            }
            TypeName::Int(display) => {
                write!(f, "INTEGER")?;
                if let Some(d) = display {
                    write!(f, "({})", d)?;
                }
            }
            TypeName::TinyInt(display) => {
                write!(f, "TINYINT")?;
                if let Some(d) = display {
                    write!(f, "({})", d)?;
                }
            }
            TypeName::SmallInt(display) => {
                write!(f, "SMALLINT")?;
                if let Some(d) = display {
                    write!(f, "({})", d)?;
                }
            }
            TypeName::BigInt(display) => {
                write!(f, "BIGINT")?;
                if let Some(d) = display {
                    write!(f, "({})", d)?;
                }
            }
            TypeName::Real => {
                write!(f, "REAL")?;
            }
            TypeName::Double => {
                write!(f, "DOUBLE")?;
            }
            TypeName::Boolean => {
                write!(f, "BOOLEAN")?;
            }
            TypeName::Date => {
                write!(f, "DATE")?;
            }
            TypeName::Time => {
                write!(f, "TIME")?;
            }
            TypeName::Timestamp => {
                write!(f, "TIMESTAMP")?;
            }
            TypeName::Interval => {
                todo!()
            }
            TypeName::Text => {
                write!(f, "TEXT")?;
            }
        }
        Ok(())
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Number(val) => {
                write!(f, "{}", val)
            }
            Literal::String(val) => {
                write!(f, "\'{}\'", val)
            }
            Literal::Boolean(val) => {
                if *val {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
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
                display_identifier_vec(
                    f,
                    vec![
                        database.to_owned(),
                        table.to_owned(),
                        Some(column.to_owned()),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .as_slice(),
                )?;
            }
            Expr::IsNull { expr, not } => {
                write!(f, "{} IS ", expr)?;
                if *not {
                    write!(f, "NOT ")?;
                }
                write!(f, "NULL")?;
            }
            Expr::InList { expr, list, not } => {
                write!(f, "{} ", expr)?;
                if *not {
                    write!(f, "NOT ")?;
                }
                write!(f, "IN(")?;
                for i in 0..list.len() {
                    write!(f, "{}", list[i])?;
                    if i != list.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")?;
            }
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => {
                write!(f, "{} ", expr)?;
                if *not {
                    write!(f, "NOT ")?;
                }
                write!(f, "IN({})", subquery)?;
            }
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => {
                write!(f, "{} ", expr)?;
                if *not {
                    write!(f, "NOT ")?;
                }
                write!(f, "BETWEEN {} AND {}", low, high)?;
            }
            Expr::BinaryOp { op, left, right } => {
                write!(f, "{} {} {}", left, op, right)?;
            }
            Expr::UnaryOp { op, expr } => {
                write!(f, "{} {}", op, expr)?;
            }
            Expr::Cast { expr, target_type } => {
                write!(f, "CAST({} AS {})", expr, target_type)?;
            }
            Expr::Literal(lit) => {
                write!(f, "{}", lit)?;
            }
            Expr::CountAll => {
                write!(f, "COUNT(*)")?;
            }
            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => {
                write!(f, "{}", name)?;
                if !params.is_empty() {
                    write!(f, "(")?;
                    for i in 0..params.len() {
                        write!(f, "{}", params[i])?;
                        if i != params.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ")")?;
                }
                write!(f, "(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                for i in 0..args.len() {
                    write!(f, "{}", args[i])?;
                    if i != args.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")?;
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE ")?;
                if let Some(op) = operand {
                    write!(f, "{} ", op)?;
                }
                for (cond, res) in conditions.iter().zip(results) {
                    write!(f, "WHEN {} THEN {} ", cond, res)?;
                }
                if let Some(el) = else_result {
                    write!(f, "ELSE {} ", el)?;
                }
                write!(f, "END")?;
            }
            Expr::Exists(subquery) => {
                write!(f, "EXITS ({})", subquery)?;
            }
            Expr::Subquery(subquery) => {
                write!(f, "({})", subquery)?;
            }
        }

        Ok(())
    }
}
