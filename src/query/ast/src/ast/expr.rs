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

use std::cmp::Ordering;
use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_io::display_decimal_128;
use common_io::display_decimal_256;
use enum_as_inner::EnumAsInner;
use ethnum::i256;

use super::OrderByExpr;
use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ErrorKind;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IntervalKind {
    Year,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Doy,
    Dow,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        span: Span,
        database: Option<Identifier>,
        table: Option<Identifier>,
        column: Identifier,
    },
    /// `IS [ NOT ] NULL` expression
    IsNull {
        span: Span,
        expr: Box<Expr>,
        not: bool,
    },
    /// `IS [NOT] DISTINCT` expression
    IsDistinctFrom {
        span: Span,
        left: Box<Expr>,
        right: Box<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (expr, ...)`
    InList {
        span: Span,
        expr: Box<Expr>,
        list: Vec<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        span: Span,
        expr: Box<Expr>,
        subquery: Box<Query>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        span: Span,
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        span: Span,
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp {
        span: Span,
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        span: Span,
        expr: Box<Expr>,
        target_type: TypeName,
        pg_style: bool,
    },
    /// `TRY_CAST` expression`
    TryCast {
        span: Span,
        expr: Box<Expr>,
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        span: Span,
        kind: IntervalKind,
        expr: Box<Expr>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        span: Span,
        substr_expr: Box<Expr>,
        str_expr: Box<Expr>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        span: Span,
        expr: Box<Expr>,
        substring_from: Box<Expr>,
        substring_for: Option<Box<Expr>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        span: Span,
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal { span: Span, lit: Literal },
    /// `COUNT(*)` expression
    CountAll { span: Span },
    /// `(foo, bar)`
    Tuple { span: Span, exprs: Vec<Expr> },
    /// Scalar/Agg/Window function call
    FunctionCall {
        span: Span,
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: Identifier,
        args: Vec<Expr>,
        params: Vec<Literal>,
        window: Option<Window>,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        span: Span,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// `EXISTS` expression
    Exists {
        span: Span,
        /// Indicate if this is a `NOT EXISTS`
        not: bool,
        subquery: Box<Query>,
    },
    /// Scalar/ANY/ALL/SOME subquery
    Subquery {
        span: Span,
        modifier: Option<SubqueryModifier>,
        subquery: Box<Query>,
    },
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        span: Span,
        expr: Box<Expr>,
        accessor: MapAccessor,
    },
    /// The `Array` expr
    Array { span: Span, exprs: Vec<Expr> },
    ArraySort {
        span: Span,
        expr: Box<Expr>,
        asc: bool,
        null_first: bool,
    },
    /// The `Map` expr
    Map { span: Span, kvs: Vec<(Expr, Expr)> },
    /// The `Interval 1 DAY` expr
    Interval {
        span: Span,
        expr: Box<Expr>,
        unit: IntervalKind,
    },
    DateAdd {
        span: Span,
        unit: IntervalKind,
        interval: Box<Expr>,
        date: Box<Expr>,
    },
    DateSub {
        span: Span,
        unit: IntervalKind,
        interval: Box<Expr>,
        date: Box<Expr>,
    },
    DateTrunc {
        span: Span,
        unit: IntervalKind,
        date: Box<Expr>,
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
    UInt64(u64),
    Int64(i64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: u8,
    },
    Decimal256 {
        value: i256,
        precision: u8,
        scale: u8,
    },
    Float(f64),
    // Quoted string literal value
    String(String),
    Boolean(bool),
    CurrentTimestamp,
    Null,
}

impl Literal {
    pub(crate) fn neg(&self) -> Self {
        match self {
            Literal::UInt64(u) => match u.cmp(&(i64::MAX as u64 + 1)) {
                Ordering::Greater => Literal::Decimal128 {
                    value: -(*u as i128),
                    precision: 19,
                    scale: 0,
                },
                Ordering::Less => Literal::Int64(-(*u as i64)),
                Ordering::Equal => Literal::Int64(i64::MIN),
            },
            Literal::Float(f) => Literal::Float(-*f),
            Literal::Decimal128 {
                value,
                precision,
                scale,
            } => Literal::Decimal128 {
                value: -*value,
                precision: *precision,
                scale: *scale,
            },
            Literal::Decimal256 {
                value,
                precision,
                scale,
            } => Literal::Decimal256 {
                value: -*value,
                precision: *precision,
                scale: *scale,
            },
            _ => unreachable!(),
        }
    }

    /// assume text is from
    /// used only for expr, so put more weight on readability
    pub fn parse_decimal(text: &str) -> std::result::Result<Self, ErrorKind> {
        let mut start = 0;
        let bytes = text.as_bytes();
        while bytes[start] == b'0' {
            start += 1
        }
        let text = &text[start..];
        let point_pos = text.find('.');
        let e_pos = text.find(|c| c == 'e' || c == 'E');
        let (i_part, f_part, e_part) = match (point_pos, e_pos) {
            (Some(p1), Some(p2)) => (&text[..p1], &text[(p1 + 1)..p2], Some(&text[(p2 + 1)..])),
            (Some(p), None) => (&text[..p], &text[(p + 1)..], None),
            (None, Some(p)) => (&text[..p], "", Some(&text[(p + 1)..])),
            _ => {
                unreachable!()
            }
        };
        let exp = match e_part {
            Some(s) => match s.parse::<i32>() {
                Ok(i) => i,
                Err(_) => return Ok(Literal::Float(fast_float::parse(text)?)),
            },
            None => 0,
        };
        if i_part.len() as i32 + exp > 76 {
            Ok(Literal::Float(fast_float::parse(text)?))
        } else {
            let mut digits = Vec::with_capacity(76);
            digits.extend_from_slice(i_part.as_bytes());
            digits.extend_from_slice(f_part.as_bytes());
            if digits.is_empty() {
                digits.push(b'0')
            }
            let mut scale = f_part.len() as i32 - exp;
            if scale < 0 {
                // e.g 123.1e3
                for _ in 0..(-scale) {
                    digits.push(b'0')
                }
                scale = 0;
            };

            // truncate
            if digits.len() > 76 {
                scale -= digits.len() as i32 - 76;
            }
            let precision = std::cmp::min(digits.len(), 76);
            let digits = unsafe { std::str::from_utf8_unchecked(&digits[..precision]) };

            let scale = scale as u8;
            let precision = std::cmp::max(precision as u8, scale);
            if precision > 38 {
                Ok(Literal::Decimal256 {
                    value: i256::from_str_radix(digits, 10)?,
                    precision,
                    scale,
                })
            } else {
                Ok(Literal::Decimal128 {
                    value: digits.parse::<i128>()?,
                    precision,
                    scale,
                })
            }
        }
    }

    pub fn parse_decimal_uint(text: &str) -> std::result::Result<Self, ErrorKind> {
        let mut start = 0;
        let bytes = text.as_bytes();
        while start < bytes.len() && bytes[start] == b'0' {
            start += 1
        }
        let text = &text[start..];
        if text.is_empty() {
            return Ok(Literal::UInt64(0));
        }
        let precision = text.len() as u8;
        match precision {
            0..=19 => Ok(Literal::UInt64(text.parse::<u64>()?)),
            20 => {
                if text <= "18446744073709551615" {
                    Ok(Literal::UInt64(text.parse::<u64>()?))
                } else {
                    Ok(Literal::Decimal128 {
                        value: text.parse::<i128>()?,
                        precision,
                        scale: 0,
                    })
                }
            }
            21..=38 => Ok(Literal::Decimal128 {
                value: text.parse::<i128>()?,
                precision,
                scale: 0,
            }),
            39..=76 => Ok(Literal::Decimal256 {
                value: i256::from_str_radix(text, 10)?,
                precision,
                scale: 0,
            }),
            _ => {
                // lost precision
                // 2.2250738585072014 E - 308 to 1.7976931348623158 E + 308
                Ok(Literal::Float(fast_float::parse(text)?))
            }
        }
    }
}

/// The display style for a map access expression
#[derive(Debug, Clone, PartialEq)]
pub enum MapAccessor {
    /// `[0][1]`
    Bracket { key: Box<Expr> },
    /// `.a.b`
    Period { key: Identifier },
    /// `.1`
    PeriodNumber { key: u64 },
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
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Timestamp,
    String,
    Array(Box<TypeName>),
    Map {
        key_type: Box<TypeName>,
        val_type: Box<TypeName>,
    },
    Tuple {
        fields_name: Option<Vec<String>>,
        fields_type: Vec<TypeName>,
    },
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

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Window {
    WindowReference(WindowRef),
    WindowSpec(WindowSpec),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowDefinition {
    pub name: Identifier,
    pub spec: WindowSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowRef {
    pub window_name: Identifier,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub existing_window_name: Option<Identifier>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumAsInner)]
pub enum WindowFrameUnits {
    Rows,
    Range,
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Box<Expr>>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Box<Expr>>),
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
    Caret,
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
    BitwiseShiftLeft,
    BitwiseShiftRight,
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
            _ => Err(ErrorCode::Unimplemented(format!(
                "Converting {self} to its contrary is not currently supported"
            ))),
        }
    }

    pub fn to_func_name(&self) -> String {
        match self {
            BinaryOperator::StringConcat => "concat".to_string(),
            BinaryOperator::NotLike => "NOT LIKE".to_string(),
            BinaryOperator::NotRegexp => "NOT REGEXP".to_string(),
            BinaryOperator::NotRLike => "NOT RLIKE".to_string(),
            BinaryOperator::BitwiseOr => "bit_or".to_string(),
            BinaryOperator::BitwiseAnd => "bit_and".to_string(),
            BinaryOperator::BitwiseXor => "bit_xor".to_string(),
            BinaryOperator::BitwiseShiftLeft => "bit_shift_left".to_string(),
            BinaryOperator::BitwiseShiftRight => "bit_shift_right".to_string(),
            BinaryOperator::Caret => "pow".to_string(),
            _ => {
                let name = format!("{:?}", self);
                name.to_lowercase()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    Factorial,
    SquareRoot,
    CubeRoot,
    Abs,
    BitwiseNot,
}

impl UnaryOperator {
    pub fn to_func_name(&self) -> String {
        match self {
            UnaryOperator::SquareRoot => "sqrt".to_string(),
            UnaryOperator::CubeRoot => "cbrt".to_string(),
            UnaryOperator::BitwiseNot => "bit_not".to_string(),
            _ => {
                let name = format!("{:?}", self);
                name.to_lowercase()
            }
        }
    }
}

impl Expr {
    pub fn span(&self) -> Span {
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
            | Expr::ArraySort { span, .. }
            | Expr::Map { span, .. }
            | Expr::Interval { span, .. }
            | Expr::DateAdd { span, .. }
            | Expr::DateSub { span, .. }
            | Expr::DateTrunc { span, .. } => *span,
        }
    }
}

impl Display for IntervalKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            IntervalKind::Year => "YEAR",
            IntervalKind::Quarter => "QUARTER",
            IntervalKind::Month => "MONTH",
            IntervalKind::Day => "DAY",
            IntervalKind::Hour => "HOUR",
            IntervalKind::Minute => "MINUTE",
            IntervalKind::Second => "SECOND",
            IntervalKind::Doy => "DOY",
            IntervalKind::Dow => "DOW",
        })
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
            UnaryOperator::SquareRoot => {
                write!(f, "|/")
            }
            UnaryOperator::CubeRoot => {
                write!(f, "||/")
            }
            UnaryOperator::Factorial => {
                write!(f, "!")
            }
            UnaryOperator::Abs => {
                write!(f, "@")
            }
            UnaryOperator::BitwiseNot => {
                write!(f, "~")
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
            BinaryOperator::Caret => {
                write!(f, "^")
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
                write!(f, "#")
            }
            BinaryOperator::BitwiseShiftLeft => {
                write!(f, "<<")
            }
            BinaryOperator::BitwiseShiftRight => {
                write!(f, ">>")
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
            TypeName::Decimal { precision, scale } => {
                write!(f, "Decimal({}, {})", precision, scale)?;
            }
            TypeName::Date => {
                write!(f, "DATE")?;
            }
            TypeName::Timestamp => {
                write!(f, "TIMESTAMP")?;
            }
            TypeName::String => {
                write!(f, "STRING")?;
            }
            TypeName::Array(ty) => {
                write!(f, "ARRAY({})", ty)?;
            }
            TypeName::Map { key_type, val_type } => {
                write!(f, "MAP({}, {})", key_type, val_type)?;
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
            Literal::UInt64(val) => {
                write!(f, "{val}")
            }
            Literal::Int64(val) => {
                write!(f, "{val}")
            }
            Literal::Decimal128 { value, scale, .. } => {
                write!(f, "{}", display_decimal_128(*value, *scale))
            }
            Literal::Decimal256 { value, scale, .. } => {
                write!(f, "{}", display_decimal_256(*value, *scale))
            }
            Literal::Float(val) => {
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
            Literal::Null => {
                write!(f, "NULL")
            }
        }
    }
}

impl Display for WindowDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WINDOW {} {}", self.name, self.spec)
    }
}

impl Display for Window {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let window_fmt = match *self {
            Window::WindowSpec(ref window_spec) => format!("{}", window_spec),
            Window::WindowReference(ref window_ref) => format!("{}", window_ref),
        };
        write!(f, "{}", window_fmt)
    }
}

impl Display for WindowRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WINDOW {}", self.window_name)
    }
}

impl Display for WindowSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        if !self.partition_by.is_empty() {
            first = false;
            write!(f, "PARTITION BY ")?;
            for (i, p) in self.partition_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{p}")?;
            }
        }

        if !self.order_by.is_empty() {
            if !first {
                write!(f, " ")?;
            }
            first = false;
            write!(f, "ORDER BY ")?;
            for (i, o) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{o}")?;
            }
        }

        if let Some(frame) = &self.window_frame {
            if !first {
                write!(f, " ")?;
            }
            match frame.units {
                WindowFrameUnits::Rows => {
                    write!(f, "ROWS")?;
                }
                WindowFrameUnits::Range => {
                    write!(f, "RANGE")?;
                }
            }

            let format_frame = |frame: &WindowFrameBound| -> String {
                match frame {
                    WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
                    WindowFrameBound::Preceding(None) => "UNBOUNDED PRECEDING".to_string(),
                    WindowFrameBound::Following(None) => "UNBOUNDED FOLLOWING".to_string(),
                    WindowFrameBound::Preceding(Some(n)) => format!("{} PRECEDING", n),
                    WindowFrameBound::Following(Some(n)) => format!("{} FOLLOWING", n),
                }
            };
            write!(
                f,
                " BETWEEN {} AND {}",
                format_frame(&frame.start_bound),
                format_frame(&frame.end_bound)
            )?
        }
        Ok(())
    }
}

impl Display for Expr {
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
                match op {
                    // TODO (xieqijun) Maybe special attribute are provided to check whether the symbol is before or after.
                    UnaryOperator::Factorial => {
                        write!(f, "({expr} {op})")?;
                    }
                    _ => {
                        write!(f, "({op} {expr})")?;
                    }
                }
            }
            Expr::BinaryOp {
                op, left, right, ..
            } => {
                write!(f, "({left} {op} {right})")?;
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
                write!(f, "SUBSTRING({expr} FROM {substring_from}")?;
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
                window,
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

                if let Some(window) = window {
                    write!(f, " OVER ({window})")?;
                }
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
                    MapAccessor::PeriodNumber { key } => write!(f, ".{key}")?,
                    MapAccessor::Colon { key } => write!(f, ":{key}")?,
                }
            }
            Expr::Array { exprs, .. } => {
                write!(f, "[")?;
                write_comma_separated_list(f, exprs)?;
                write!(f, "]")?;
            }
            Expr::ArraySort {
                expr,
                asc,
                null_first,
                ..
            } => {
                write!(f, "ARRAY_SORT(")?;
                write!(f, "{expr})")?;
                if *asc {
                    write!(f, " , 'ASC'")?;
                } else {
                    write!(f, " , 'DESC'")?;
                }
                if *null_first {
                    write!(f, " , 'NULLS FIRST'")?;
                } else {
                    write!(f, " , 'NULLS LAST'")?;
                }
                write!(f, ")")?;
            }
            Expr::Map { kvs, .. } => {
                write!(f, "{{")?;
                for (i, (k, v)) in kvs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{k}:{v}")?;
                }
                write!(f, "}}")?;
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

pub fn split_conjunctions_expr(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op, left, right, ..
        } if op == &BinaryOperator::And => {
            let mut result = split_conjunctions_expr(left);
            result.extend(split_conjunctions_expr(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

pub fn split_equivalent_predicate_expr(expr: &Expr) -> Option<(Expr, Expr)> {
    match expr {
        Expr::BinaryOp {
            op, left, right, ..
        } if op == &BinaryOperator::Eq => Some((*left.clone(), *right.clone())),
        _ => None,
    }
}
