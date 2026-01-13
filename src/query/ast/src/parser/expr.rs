// Copyright 2021 Datafuse Labs
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

use ethnum::i256;
use itertools::Itertools;
use nom::Parser;
use nom::combinator::consumed;
use nom::combinator::verify;
use nom::error::context;
use nom_rule::rule;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use crate::Span;
use crate::ast::quote::AtString;
use crate::ast::*;
use crate::parser::Error;
use crate::parser::ErrorKind;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::input::WithSpan;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::span::merge_span;

macro_rules! with_span {
    ($parser:expr_2021) => {
        map(consumed($parser), |(span, elem)| WithSpan { span, elem })
    };
}

pub fn expr(i: Input) -> IResult<Expr> {
    context("expression", subexpr(0)).parse(i)
}

pub fn values(i: Input) -> IResult<Vec<Expr>> {
    let values = comma_separated_list0(expr);
    map(rule! { ( "(" ~ #values ~ ")" ) }, |(_, v, _)| v).parse(i)
}

pub fn subexpr(min_precedence: u32) -> impl FnMut(Input) -> IResult<Expr> {
    move |i| {
        let higher_prec_expr_element = |i| {
            expr_element(i).and_then(|(rest, elem)| match elem.elem.affix() {
                Affix::Infix(prec, _) | Affix::Prefix(prec) | Affix::Postfix(prec)
                    if prec <= Precedence(min_precedence) =>
                {
                    Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other("expected more tokens for expression"),
                    )))
                }
                _ => Ok((rest, elem)),
            })
        };

        let (rest, mut expr_elements) = rule! { #higher_prec_expr_element+ }.parse(i)?;

        for (prev, curr) in (-1..(expr_elements.len() as isize)).tuple_windows() {
            // If it's following a prefix or infix element or it's the first element, ...
            if prev == -1
                || matches!(
                    expr_elements[prev as usize].elem.affix(),
                    Affix::Prefix(_) | Affix::Infix(_, _)
                )
            {
                let span = expr_elements[curr as usize].span;
                let elem = &mut expr_elements[curr as usize].elem;
                match elem {
                    // replace bracket map access to an array, ...
                    ExprElement::MapAccess {
                        accessor: MapAccessor::Bracket { key },
                    } => {
                        *elem = ExprElement::Array {
                            exprs: vec![(**key).clone()],
                        };
                    }
                    // replace binary `+` and `-` to unary one, ...
                    ExprElement::BinaryOp {
                        op: BinaryOperator::Plus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Plus,
                        };
                    }
                    ExprElement::BinaryOp {
                        op: BinaryOperator::Minus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Minus,
                        };
                    }
                    // replace `:ident` to hole, ...
                    ExprElement::MapAccess {
                        accessor: MapAccessor::Colon { key },
                    } => {
                        if !key.is_quoted() && !key.is_hole() {
                            *elem = ExprElement::Hole {
                                name: key.to_string(),
                            };
                        }
                    }
                    // and replace `.<number>` map access to floating point literal.
                    ExprElement::MapAccess {
                        accessor: MapAccessor::DotNumber { .. },
                    } => {
                        *elem = ExprElement::Literal {
                            value: literal(span)?.1,
                        };
                    }
                    // replace json operator `?` to placeholder.
                    ExprElement::JsonOp { op } => {
                        if *op == JsonOperator::Question {
                            *elem = ExprElement::Placeholder;
                        }
                    }
                    _ => {}
                }
            }
        }

        run_pratt_parser(ExprParser, expr_elements, rest, i)
    }
}

/// A 'flattened' AST of expressions.
///
/// This is used to parse expressions in Pratt parser.
/// The Pratt parser is not able to parse expressions by grammar. So we need to extract
/// the expression operands and operators to be the input of Pratt parser, by running a
/// nom parser in advance.
///
/// For example, `a + b AND c is null` is parsed as `[col(a), PLUS, col(b), AND, col(c), ISNULL]` by nom parsers.
/// Then the Pratt parser is able to parse the expression into `AND(PLUS(col(a), col(b)), ISNULL(col(c)))`.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ExprElement {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        column: ColumnRef,
    },
    /// `.a.b` after column ref, currently it'll be taken as column reference
    DotAccess {
        key: ColumnID,
    },
    /// `IS [NOT] NULL` expression
    IsNull {
        not: bool,
    },
    /// `IS [NOT] DISTINCT FROM` expression
    IsDistinctFrom {
        not: bool,
    },
    /// `[ NOT ] IN (list, ...)`
    InList {
        list: Vec<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        subquery: Box<Query>,
        not: bool,
    },
    /// `LIKE (SELECT ...) [ESCAPE '<escape>']`
    LikeSubquery {
        modifier: SubqueryModifier,
        subquery: Box<Query>,
        escape: Option<String>,
    },
    /// `ESCAPE '<escape>'`
    Escape {
        escape: String,
    },
    /// `BETWEEN ... AND ...`
    Between {
        low: Box<Expr>,
        high: Box<Expr>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        op: BinaryOperator,
    },
    /// JSON operation
    JsonOp {
        op: JsonOperator,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
    },
    VariableAccess(String),
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        expr: Box<Expr>,
        target_type: TypeName,
    },
    /// `TRY_CAST` expression`
    TryCast {
        expr: Box<Expr>,
        target_type: TypeName,
    },
    /// `::<type_name>` expression
    PgCast {
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        field: IntervalKind,
        expr: Box<Expr>,
    },
    /// DATE_PART(IntervalKind, <expr>)
    DatePart {
        field: IntervalKind,
        expr: Box<Expr>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        substr_expr: Box<Expr>,
        str_expr: Box<Expr>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    SubString {
        expr: Box<Expr>,
        substring_from: Box<Expr>,
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
    Literal {
        value: Literal,
    },
    /// `Count(*)` expression
    CountAll {
        qualified: QualifiedName,
        window: Option<Window>,
    },
    /// `(foo, bar)`
    Tuple {
        exprs: Vec<Expr>,
    },
    /// Scalar function call
    FunctionCall {
        func: FunctionCall,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// `EXISTS` expression
    Exists {
        subquery: Query,
        not: bool,
    },
    /// Scalar/ANY/ALL/SOME subquery
    Subquery {
        modifier: Option<SubqueryModifier>,
        subquery: Query,
    },
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        accessor: MapAccessor,
    },
    /// python/rust style function call, like `a.foo(b).bar(c)` ---> `bar(foo(a, b), c)`
    ChainFunctionCall {
        name: Identifier,
        args: Vec<Expr>,
        lambda: Option<Lambda>,
    },
    /// python/rust list comprehension
    ListComprehension {
        source: Expr,
        param: Identifier,
        filter: Option<Expr>,
        result: Expr,
    },
    /// An expression between parentheses
    Group(Expr),
    /// `[1, 2, 3]`
    Array {
        exprs: Vec<Expr>,
    },
    /// `{'k1':'v1','k2':'v2'}`
    Map {
        kvs: Vec<(Literal, Expr)>,
    },
    Interval {
        expr: Expr,
        unit: IntervalKind,
    },
    DateAdd {
        unit: IntervalKind,
        interval: Expr,
        date: Expr,
    },
    DateDiff {
        unit: IntervalKind,
        date_start: Expr,
        date_end: Expr,
    },
    DateBetween {
        unit: IntervalKind,
        date_start: Expr,
        date_end: Expr,
    },
    DateSub {
        unit: IntervalKind,
        interval: Expr,
        date: Expr,
    },
    DateTrunc {
        unit: IntervalKind,
        date: Expr,
    },
    TimeSlice {
        unit: IntervalKind,
        date: Expr,
        slice_length: u64,
        start_or_end: Option<String>,
    },
    LastDay {
        unit: IntervalKind,
        date: Expr,
    },
    PreviousDay {
        unit: Weekday,
        date: Expr,
    },
    NextDay {
        unit: Weekday,
        date: Expr,
    },
    Hole {
        name: String,
    },
    Placeholder,
    StageLocation {
        location: String,
    },
}

pub const BETWEEN_PREC: u32 = 20;
pub const NOT_PREC: u32 = 15;
const CHAIN_FUNCTION_AFFIX: Affix = Affix::Postfix(Precedence(61));
const DOT_ACCESS_AFFIX: Affix = Affix::Postfix(Precedence(60));
const MAP_ACCESS_AFFIX: Affix = Affix::Postfix(Precedence(60));
const IS_NULL_AFFIX: Affix = Affix::Postfix(Precedence(17));
const BETWEEN_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const IS_DISTINCT_FROM_AFFIX: Affix = Affix::Infix(Precedence(BETWEEN_PREC), Associativity::Left);
const IN_LIST_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const IN_SUBQUERY_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const LIKE_SUBQUERY_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const LIKE_ANY_WITH_ESCAPE_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const LIKE_WITH_ESCAPE_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const ESCAPE_AFFIX: Affix = Affix::Postfix(Precedence(BETWEEN_PREC));
const JSON_OP_AFFIX: Affix = Affix::Infix(Precedence(40), Associativity::Left);
const PG_CAST_AFFIX: Affix = Affix::Postfix(Precedence(60));

const fn unary_affix(op: &UnaryOperator) -> Affix {
    match op {
        UnaryOperator::Not => Affix::Prefix(Precedence(NOT_PREC)),
        UnaryOperator::Plus => Affix::Prefix(Precedence(50)),
        UnaryOperator::Minus => Affix::Prefix(Precedence(50)),
        UnaryOperator::BitwiseNot => Affix::Prefix(Precedence(50)),
        UnaryOperator::SquareRoot => Affix::Prefix(Precedence(60)),
        UnaryOperator::CubeRoot => Affix::Prefix(Precedence(60)),
        UnaryOperator::Abs => Affix::Prefix(Precedence(60)),
        UnaryOperator::Factorial => Affix::Postfix(Precedence(60)),
    }
}

const fn binary_affix(op: &BinaryOperator) -> Affix {
    match op {
        BinaryOperator::Or => Affix::Infix(Precedence(5), Associativity::Left),
        BinaryOperator::And => Affix::Infix(Precedence(10), Associativity::Left),
        BinaryOperator::Eq => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotEq => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Gt => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Lt => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Gte => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Lte => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Like(_) => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::LikeAny(_) => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotLike(_) => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Regexp => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotRegexp => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::RLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotRLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::SoundsLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::BitwiseOr => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::BitwiseAnd => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::BitwiseXor => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::CosineDistance => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::L1Distance => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::L2Distance => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::BitwiseShiftLeft => Affix::Infix(Precedence(23), Associativity::Left),
        BinaryOperator::BitwiseShiftRight => Affix::Infix(Precedence(23), Associativity::Left),
        BinaryOperator::Xor => Affix::Infix(Precedence(24), Associativity::Left),
        BinaryOperator::Plus => Affix::Infix(Precedence(30), Associativity::Left),
        BinaryOperator::Minus => Affix::Infix(Precedence(30), Associativity::Left),
        BinaryOperator::Multiply => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::Div => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::Divide => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::IntDiv => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::Modulo => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::StringConcat => Affix::Infix(Precedence(40), Associativity::Left),
        BinaryOperator::Caret => Affix::Infix(Precedence(40), Associativity::Right),
    }
}

impl ExprElement {
    pub fn affix(&self) -> Affix {
        match &self {
            ExprElement::ChainFunctionCall { .. } => CHAIN_FUNCTION_AFFIX,
            ExprElement::DotAccess { .. } => DOT_ACCESS_AFFIX,
            ExprElement::MapAccess { .. } => MAP_ACCESS_AFFIX,
            ExprElement::IsNull { .. } => IS_NULL_AFFIX,
            ExprElement::Between { .. } => BETWEEN_AFFIX,
            ExprElement::IsDistinctFrom { .. } => IS_DISTINCT_FROM_AFFIX,
            ExprElement::InList { .. } => IN_LIST_AFFIX,
            ExprElement::InSubquery { .. } => IN_SUBQUERY_AFFIX,
            ExprElement::LikeSubquery { .. } => LIKE_SUBQUERY_AFFIX,
            ExprElement::Escape { .. } => ESCAPE_AFFIX,
            ExprElement::UnaryOp { op } => unary_affix(op),
            ExprElement::BinaryOp { op } => binary_affix(op),
            ExprElement::JsonOp { .. } => JSON_OP_AFFIX,
            ExprElement::PgCast { .. } => PG_CAST_AFFIX,
            ExprElement::ColumnRef { .. } => Affix::Nilfix,
            ExprElement::Cast { .. } => Affix::Nilfix,
            ExprElement::TryCast { .. } => Affix::Nilfix,
            ExprElement::Extract { .. } => Affix::Nilfix,
            ExprElement::DatePart { .. } => Affix::Nilfix,
            ExprElement::Position { .. } => Affix::Nilfix,
            ExprElement::SubString { .. } => Affix::Nilfix,
            ExprElement::Trim { .. } => Affix::Nilfix,
            ExprElement::Literal { .. } => Affix::Nilfix,
            ExprElement::CountAll { .. } => Affix::Nilfix,
            ExprElement::Tuple { .. } => Affix::Nilfix,
            ExprElement::FunctionCall { .. } => Affix::Nilfix,
            ExprElement::Case { .. } => Affix::Nilfix,
            ExprElement::Exists { .. } => Affix::Nilfix,
            ExprElement::Subquery { .. } => Affix::Nilfix,
            ExprElement::ListComprehension { .. } => Affix::Nilfix,
            ExprElement::Group(_) => Affix::Nilfix,
            ExprElement::Array { .. } => Affix::Nilfix,
            ExprElement::Map { .. } => Affix::Nilfix,
            ExprElement::Interval { .. } => Affix::Nilfix,
            ExprElement::DateAdd { .. } => Affix::Nilfix,
            ExprElement::DateDiff { .. } => Affix::Nilfix,
            ExprElement::DateBetween { .. } => Affix::Nilfix,
            ExprElement::DateSub { .. } => Affix::Nilfix,
            ExprElement::DateTrunc { .. } => Affix::Nilfix,
            ExprElement::TimeSlice { .. } => Affix::Nilfix,
            ExprElement::LastDay { .. } => Affix::Nilfix,
            ExprElement::PreviousDay { .. } => Affix::Nilfix,
            ExprElement::NextDay { .. } => Affix::Nilfix,
            ExprElement::Hole { .. } => Affix::Nilfix,
            ExprElement::Placeholder => Affix::Nilfix,
            ExprElement::VariableAccess { .. } => Affix::Nilfix,
            ExprElement::StageLocation { .. } => Affix::Nilfix,
        }
    }
}

impl Expr {
    pub fn affix(&self) -> Affix {
        match self {
            Expr::MapAccess { .. } => MAP_ACCESS_AFFIX,
            Expr::IsNull { .. } => IS_NULL_AFFIX,
            Expr::Between { .. } => BETWEEN_AFFIX,
            Expr::IsDistinctFrom { .. } => Affix::Nilfix,
            Expr::InList { .. } => IN_LIST_AFFIX,
            Expr::InSubquery { .. } => IN_SUBQUERY_AFFIX,
            Expr::LikeSubquery { .. } => LIKE_SUBQUERY_AFFIX,
            Expr::LikeAnyWithEscape { .. } => LIKE_ANY_WITH_ESCAPE_AFFIX,
            Expr::LikeWithEscape { .. } => LIKE_WITH_ESCAPE_AFFIX,
            Expr::UnaryOp { op, .. } => unary_affix(op),
            Expr::BinaryOp { op, .. } => binary_affix(op),
            Expr::JsonOp { .. } => JSON_OP_AFFIX,
            Expr::Cast { pg_style: true, .. } => PG_CAST_AFFIX,
            Expr::Cast {
                pg_style: false, ..
            } => Affix::Nilfix,
            Expr::TryCast { .. } => Affix::Nilfix,
            Expr::Extract { .. } => Affix::Nilfix,
            Expr::DatePart { .. } => Affix::Nilfix,
            Expr::Position { .. } => Affix::Nilfix,
            Expr::Substring { .. } => Affix::Nilfix,
            Expr::ColumnRef { .. } => Affix::Nilfix,
            Expr::Trim { .. } => Affix::Nilfix,
            Expr::Literal { .. } => Affix::Nilfix,
            Expr::CountAll { .. } => Affix::Nilfix,
            Expr::Tuple { .. } => Affix::Nilfix,
            Expr::FunctionCall { .. } => Affix::Nilfix,
            Expr::Case { .. } => Affix::Nilfix,
            Expr::Exists { .. } => Affix::Nilfix,
            Expr::Subquery { .. } => Affix::Nilfix,
            Expr::Array { .. } => Affix::Nilfix,
            Expr::Map { .. } => Affix::Nilfix,
            Expr::Interval { .. } => Affix::Nilfix,
            Expr::DateAdd { .. } => Affix::Nilfix,
            Expr::DateDiff { .. } => Affix::Nilfix,
            Expr::DateBetween { .. } => Affix::Nilfix,
            Expr::DateSub { .. } => Affix::Nilfix,
            Expr::DateTrunc { .. } => Affix::Nilfix,
            Expr::TimeSlice { .. } => Affix::Nilfix,
            Expr::LastDay { .. } => Affix::Nilfix,
            Expr::PreviousDay { .. } => Affix::Nilfix,
            Expr::NextDay { .. } => Affix::Nilfix,
            Expr::Hole { .. } => Affix::Nilfix,
            Expr::Placeholder { .. } => Affix::Nilfix,
            Expr::StageLocation { .. } => Affix::Nilfix,
        }
    }
}

struct ExprParser;

impl<'a, I: Iterator<Item = WithSpan<'a, ExprElement>>> PrattParser<I> for ExprParser {
    type Error = &'static str;
    type Input = WithSpan<'a, ExprElement>;
    type Output = Expr;

    fn query(&mut self, elem: &WithSpan<ExprElement>) -> Result<Affix, &'static str> {
        Ok(elem.elem.affix())
    }

    fn primary(&mut self, elem: WithSpan<'a, ExprElement>) -> Result<Expr, &'static str> {
        let expr = match elem.elem {
            ExprElement::ColumnRef { column } => Expr::ColumnRef {
                span: transform_span(elem.span.tokens),
                column,
            },
            ExprElement::Cast { expr, target_type } => Expr::Cast {
                span: transform_span(elem.span.tokens),
                expr,
                target_type,
                pg_style: false,
            },
            ExprElement::TryCast { expr, target_type } => Expr::TryCast {
                span: transform_span(elem.span.tokens),
                expr,
                target_type,
            },
            ExprElement::Extract { field, expr } => Expr::Extract {
                span: transform_span(elem.span.tokens),
                kind: field,
                expr,
            },
            ExprElement::DatePart { field, expr } => Expr::DatePart {
                span: transform_span(elem.span.tokens),
                kind: field,
                expr,
            },
            ExprElement::Position {
                substr_expr,
                str_expr,
            } => Expr::Position {
                span: transform_span(elem.span.tokens),
                substr_expr,
                str_expr,
            },
            ExprElement::SubString {
                expr,
                substring_from,
                substring_for,
            } => Expr::Substring {
                span: transform_span(elem.span.tokens),
                expr,
                substring_from,
                substring_for,
            },
            ExprElement::Trim { expr, trim_where } => Expr::Trim {
                span: transform_span(elem.span.tokens),
                expr,
                trim_where,
            },
            ExprElement::Literal { value } => Expr::Literal {
                span: transform_span(elem.span.tokens),
                value,
            },
            ExprElement::CountAll { qualified, window } => Expr::CountAll {
                span: transform_span(elem.span.tokens),
                qualified,
                window,
            },
            ExprElement::Tuple { exprs } => Expr::Tuple {
                span: transform_span(elem.span.tokens),
                exprs,
            },
            ExprElement::FunctionCall { func } => Expr::FunctionCall {
                span: transform_span(elem.span.tokens),
                func,
            },
            ExprElement::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Expr::Case {
                span: transform_span(elem.span.tokens),
                operand,
                conditions,
                results,
                else_result,
            },
            ExprElement::Exists { subquery, not } => Expr::Exists {
                span: transform_span(elem.span.tokens),
                not,
                subquery: Box::new(subquery),
            },
            ExprElement::Subquery { subquery, modifier } => Expr::Subquery {
                span: transform_span(elem.span.tokens),
                modifier,
                subquery: Box::new(subquery),
            },
            ExprElement::Group(expr) => expr,
            ExprElement::Array { exprs } => Expr::Array {
                span: transform_span(elem.span.tokens),
                exprs,
            },
            ExprElement::ListComprehension {
                source,
                param,
                filter,
                result,
            } => {
                let span = transform_span(elem.span.tokens);
                let mut source = source;

                // array_filter(source, filter)
                if let Some(filter) = filter {
                    source = Expr::FunctionCall {
                        span,
                        func: FunctionCall {
                            distinct: false,
                            name: Identifier::from_name(
                                transform_span(elem.span.tokens),
                                "array_filter",
                            ),
                            args: vec![source],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: Some(Lambda {
                                params: vec![param.clone()],
                                expr: Box::new(filter),
                            }),
                        },
                    };
                }
                // array_map(source, result)
                Expr::FunctionCall {
                    span,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(transform_span(elem.span.tokens), "array_map"),
                        args: vec![source],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: Some(Lambda {
                            params: vec![param.clone()],
                            expr: Box::new(result),
                        }),
                    },
                }
            }
            ExprElement::Map { kvs } => Expr::Map {
                span: transform_span(elem.span.tokens),
                kvs,
            },
            ExprElement::Interval { expr, unit } => Expr::Interval {
                span: transform_span(elem.span.tokens),
                expr: Box::new(expr),
                unit,
            },
            ExprElement::DateAdd {
                unit,
                interval,
                date,
            } => Expr::DateAdd {
                span: transform_span(elem.span.tokens),
                unit,
                interval: Box::new(interval),
                date: Box::new(date),
            },
            ExprElement::DateDiff {
                unit,
                date_start,
                date_end,
            } => Expr::DateDiff {
                span: transform_span(elem.span.tokens),
                unit,
                date_start: Box::new(date_start),
                date_end: Box::new(date_end),
            },
            ExprElement::DateBetween {
                unit,
                date_start,
                date_end,
            } => Expr::DateBetween {
                span: transform_span(elem.span.tokens),
                unit,
                date_start: Box::new(date_start),
                date_end: Box::new(date_end),
            },
            ExprElement::DateSub {
                unit,
                interval,
                date,
            } => Expr::DateSub {
                span: transform_span(elem.span.tokens),
                unit,
                interval: Box::new(interval),
                date: Box::new(date),
            },
            ExprElement::DateTrunc { unit, date } => Expr::DateTrunc {
                span: transform_span(elem.span.tokens),
                unit,
                date: Box::new(date),
            },
            ExprElement::TimeSlice {
                unit,
                date,
                slice_length,
                start_or_end,
            } => Expr::TimeSlice {
                span: transform_span(elem.span.tokens),
                unit,
                date: Box::new(date),
                slice_length,
                start_or_end: start_or_end.unwrap_or("start".to_string()),
            },
            ExprElement::LastDay { unit, date } => Expr::LastDay {
                span: transform_span(elem.span.tokens),
                unit,
                date: Box::new(date),
            },
            ExprElement::PreviousDay { unit, date } => Expr::PreviousDay {
                span: transform_span(elem.span.tokens),
                unit,
                date: Box::new(date),
            },
            ExprElement::NextDay { unit, date } => Expr::NextDay {
                span: transform_span(elem.span.tokens),
                unit,
                date: Box::new(date),
            },
            ExprElement::Hole { name } => Expr::Hole {
                span: transform_span(elem.span.tokens),
                name,
            },
            ExprElement::Placeholder => Expr::Placeholder {
                span: transform_span(elem.span.tokens),
            },
            ExprElement::VariableAccess(name) => {
                let span = transform_span(elem.span.tokens);
                make_func_get_variable(span, name)
            }
            ExprElement::StageLocation { location } => Expr::StageLocation {
                span: transform_span(elem.span.tokens),
                location,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn infix(
        &mut self,
        lhs: Expr,
        elem: WithSpan<'a, ExprElement>,
        rhs: Expr,
    ) -> Result<Expr, &'static str> {
        let expr = match elem.elem {
            ExprElement::BinaryOp { op } => Expr::BinaryOp {
                span: transform_span(elem.span.tokens),
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            ExprElement::IsDistinctFrom { not } => Expr::IsDistinctFrom {
                span: transform_span(elem.span.tokens),
                left: Box::new(lhs),
                right: Box::new(rhs),
                not,
            },
            ExprElement::JsonOp { op } => Expr::JsonOp {
                span: transform_span(elem.span.tokens),
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn prefix(&mut self, elem: WithSpan<'a, ExprElement>, rhs: Expr) -> Result<Expr, &'static str> {
        match elem.elem {
            ExprElement::UnaryOp { op } => {
                let op_span = transform_span(elem.span.tokens);
                match (op, rhs) {
                    (
                        UnaryOperator::Minus,
                        Expr::Literal {
                            span: rhs_span,
                            value,
                        },
                    ) => {
                        if let Some(value) = try_negate_literal(&value) {
                            Ok(Expr::Literal {
                                span: merge_span(op_span, rhs_span),
                                value,
                            })
                        } else {
                            Ok(Expr::UnaryOp {
                                span: op_span,
                                op: UnaryOperator::Minus,
                                expr: Box::new(Expr::Literal {
                                    span: rhs_span,
                                    value,
                                }),
                            })
                        }
                    }
                    (op, rhs_expr) => Ok(Expr::UnaryOp {
                        span: op_span,
                        op,
                        expr: Box::new(rhs_expr),
                    }),
                }
            }
            _ => unreachable!(),
        }
    }

    fn postfix(
        &mut self,
        mut lhs: Expr,
        elem: WithSpan<'a, ExprElement>,
    ) -> Result<Expr, &'static str> {
        let expr = match elem.elem {
            ExprElement::MapAccess { accessor } => Expr::MapAccess {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                accessor,
            },
            ExprElement::DotAccess { key } => {
                // `database.table.column` is parsed into [database] [.table] [.column],
                // so we need to transform it into the right `ColumnRef` form.
                if let Expr::ColumnRef { column, .. } = &mut lhs
                    && let ColumnID::Name(name) = &column.column
                {
                    column.database = column.table.take();
                    column.table = Some(name.clone());
                    column.column = key.clone();
                    return Ok(lhs);
                }

                match key {
                    ColumnID::Name(id) => Expr::MapAccess {
                        span: transform_span(elem.span.tokens),
                        expr: Box::new(lhs),
                        accessor: MapAccessor::Colon { key: id },
                    },
                    _ => {
                        return Err("dot access position must be after ident");
                    }
                }
            }
            ExprElement::ChainFunctionCall { name, args, lambda } => Expr::FunctionCall {
                span: transform_span(elem.span.tokens),
                func: FunctionCall {
                    distinct: false,
                    name,
                    args: [vec![lhs], args].concat(),
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda,
                },
            },
            ExprElement::IsNull { not } => Expr::IsNull {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                not,
            },
            ExprElement::InList { list, not } => Expr::InList {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                list,
                not,
            },
            ExprElement::InSubquery { subquery, not } => Expr::InSubquery {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                subquery,
                not,
            },
            ExprElement::LikeSubquery {
                subquery,
                modifier,
                escape,
            } => Expr::LikeSubquery {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                subquery,
                modifier,
                escape,
            },
            ExprElement::Escape { escape } => match lhs {
                Expr::BinaryOp {
                    span,
                    op: BinaryOperator::Like(_),
                    left,
                    right,
                } => Expr::LikeWithEscape {
                    span,
                    left,
                    right,
                    is_not: false,
                    escape,
                },
                Expr::BinaryOp {
                    span,
                    op: BinaryOperator::NotLike(_),
                    left,
                    right,
                } => Expr::LikeWithEscape {
                    span,
                    left,
                    right,
                    is_not: true,
                    escape,
                },
                Expr::BinaryOp {
                    span,
                    op: BinaryOperator::LikeAny(_),
                    left,
                    right,
                } => Expr::LikeAnyWithEscape {
                    span,
                    left,
                    right,
                    escape,
                },
                _ => return Err("escape clause must be after LIKE/NOT LIKE/LIKE ANY binary expr"),
            },
            ExprElement::Between { low, high, not } => Expr::Between {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                low,
                high,
                not,
            },
            ExprElement::PgCast { target_type } => Expr::Cast {
                span: transform_span(elem.span.tokens),
                expr: Box::new(lhs),
                target_type,
                pg_style: true,
            },
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                span: transform_span(elem.span.tokens),
                op,
                expr: Box::new(lhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }
}
#[allow(unreachable_code)]
pub fn expr_element(i: Input) -> IResult<WithSpan<ExprElement>> {
    let column_ref = map(column_id, |column| ExprElement::ColumnRef {
        column: ColumnRef {
            database: None,
            table: None,
            column,
        },
    });
    let is_null = map(
        rule! {
            IS ~ NOT? ~ NULL
        },
        |(_, opt_not, _)| ExprElement::IsNull {
            not: opt_not.is_some(),
        },
    );
    let in_list = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #comma_separated_list1(subexpr(0)) ~ ^")"
        },
        |(opt_not, _, _, list, _)| ExprElement::InList {
            list,
            not: opt_not.is_some(),
        },
    );
    let in_subquery = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #query  ~ ^")"
        },
        |(opt_not, _, _, subquery, _)| ExprElement::InSubquery {
            subquery: Box::new(subquery),
            not: opt_not.is_some(),
        },
    );
    let like_subquery = map(
        rule! {
            LIKE ~ ( ANY | SOME | ALL ) ~ "(" ~ #query ~ ^")" ~ (ESCAPE ~  ^#literal_string)?
        },
        |(_, m, _, subquery, _, option_escape)| {
            let modifier = match m.kind {
                ALL => SubqueryModifier::All,
                ANY => SubqueryModifier::Any,
                SOME => SubqueryModifier::Some,
                _ => unreachable!(),
            };
            ExprElement::LikeSubquery {
                modifier,
                subquery: Box::new(subquery),
                escape: option_escape.map(|(_, escape)| escape),
            }
        },
    );
    let escape = map(
        rule! {
            ESCAPE ~  ^#literal_string
        },
        |(_, escape)| ExprElement::Escape { escape },
    );
    let between = map(
        rule! {
            NOT? ~ BETWEEN ~ ^#subexpr(BETWEEN_PREC) ~ ^AND ~ ^#subexpr(BETWEEN_PREC)
        },
        |(opt_not, _, low, _, high)| ExprElement::Between {
            low: Box::new(low),
            high: Box::new(high),
            not: opt_not.is_some(),
        },
    );
    let cast = map(
        rule! {
            ( CAST | TRY_CAST )
            ~ "("
            ~ ^#subexpr(0)
            ~ ^( AS | "," )
            ~ ^#type_name
            ~ ^")"
        },
        |(cast, _, expr, _, target_type, _)| {
            if cast.kind == CAST {
                ExprElement::Cast {
                    expr: Box::new(expr),
                    target_type,
                }
            } else {
                ExprElement::TryCast {
                    expr: Box::new(expr),
                    target_type,
                }
            }
        },
    );
    let pg_cast = map(
        rule! {
            "::" ~ ^#type_name
        },
        |(_, target_type)| ExprElement::PgCast { target_type },
    );
    let date_part = map(
        rule! {
            (DATE_PART | DATEPART) ~ "(" ~ ^#interval_kind ~ "," ~ ^#subexpr(0) ~ ^")"
        },
        |(_, _, field, _, expr, _)| ExprElement::DatePart {
            field,
            expr: Box::new(expr),
        },
    );
    let extract = map(
        rule! {
            EXTRACT ~ "(" ~ ^#interval_kind ~ ^FROM ~ ^#subexpr(0) ~ ^")"
        },
        |(_, _, field, _, expr, _)| ExprElement::Extract {
            field,
            expr: Box::new(expr),
        },
    );
    let position = map(
        rule! {
            POSITION
            ~ "("
            ~ ^#subexpr(BETWEEN_PREC)
            ~ ^IN
            ~ ^#subexpr(0)
            ~ ^")"
        },
        |(_, _, substr_expr, _, str_expr, _)| ExprElement::Position {
            substr_expr: Box::new(substr_expr),
            str_expr: Box::new(str_expr),
        },
    );
    let substring = map(
        rule! {
            ( SUBSTRING | SUBSTR )
            ~ "("
            ~ ^#subexpr(0)
            ~ ( FROM | "," )
            ~ ^#subexpr(0)
            ~ ( ( FOR | "," ) ~ ^#subexpr(0) )?
            ~ ^")"
        },
        |(_, _, expr, _, substring_from, opt_substring_for, _)| ExprElement::SubString {
            expr: Box::new(expr),
            substring_from: Box::new(substring_from),
            substring_for: opt_substring_for.map(|(_, expr)| Box::new(expr)),
        },
    );
    let trim_where = alt((
        value(TrimWhere::Both, rule! { BOTH }),
        value(TrimWhere::Leading, rule! { LEADING }),
        value(TrimWhere::Trailing, rule! { TRAILING }),
    ));
    let trim_from = map(
        rule! {
            TRIM
            ~ "("
            ~ #trim_where
            ~ ^#subexpr(0)
            ~ ^FROM
            ~ ^#subexpr(0)
            ~ ^")"
        },
        |(_, _, trim_where, trim_str, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: Some((trim_where, Box::new(trim_str))),
        },
    );

    let count_all_with_window = map(
        rule! {
            COUNT ~ "(" ~  ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*" ~ ")" ~ ( OVER ~ #window_spec_ident )?
        },
        |(_, _, res, star, _, window)| match res {
            Some((fst, _, Some((snd, _)))) => ExprElement::CountAll {
                qualified: vec![
                    Indirection::Identifier(fst),
                    Indirection::Identifier(snd),
                    Indirection::Star(Some(star.span)),
                ],
                window: window.map(|w| w.1),
            },
            Some((fst, _, None)) => ExprElement::CountAll {
                qualified: vec![
                    Indirection::Identifier(fst),
                    Indirection::Star(Some(star.span)),
                ],
                window: window.map(|w| w.1),
            },
            None => ExprElement::CountAll {
                qualified: vec![Indirection::Star(Some(star.span))],
                window: window.map(|w| w.1),
            },
        },
    );

    let tuple = map(
        rule! {
            "(" ~ #comma_separated_list1_ignore_trailing(subexpr(0)) ~ ","? ~ ^")"
        },
        |(_, mut exprs, opt_trail, _)| {
            if exprs.len() == 1 && opt_trail.is_none() {
                ExprElement::Group(exprs.remove(0))
            } else {
                ExprElement::Tuple { exprs }
            }
        },
    );
    let subquery = map(
        rule! {
            ( ANY | SOME | ALL )? ~ "(" ~ #query ~ ^")"
        },
        |(modifier, _, subquery, _)| {
            let modifier = modifier.map(|m| match m.kind {
                ALL => SubqueryModifier::All,
                ANY => SubqueryModifier::Any,
                SOME => SubqueryModifier::Some,
                _ => unreachable!(),
            });
            ExprElement::Subquery { modifier, subquery }
        },
    );

    let case = map(
        rule! {
            CASE ~ #subexpr(0)?
            ~ ( WHEN ~ ^#subexpr(0) ~ ^THEN ~ ^#subexpr(0) )+
            ~ ( ELSE ~ ^#subexpr(0) )? ~ ^END
        },
        |(_, operand, branches, else_result, _)| {
            let (conditions, results) = branches
                .into_iter()
                .map(|(_, cond, _, result)| (cond, result))
                .unzip();
            let else_result = else_result.map(|(_, result)| result);
            ExprElement::Case {
                operand: operand.map(Box::new),
                conditions,
                results,
                else_result: else_result.map(Box::new),
            }
        },
    );
    let exists = map(
        rule! {
            NOT? ~ EXISTS ~ "(" ~ ^#query ~ ^")"
        },
        |(opt_not, _, _, subquery, _)| ExprElement::Exists {
            subquery,
            not: opt_not.is_some(),
        },
    );
    let binary_op = map(binary_op, |op| ExprElement::BinaryOp { op });
    let json_op = map(json_op, |op| ExprElement::JsonOp { op });
    let variable_access = map(variable_ident, ExprElement::VariableAccess);

    let unary_op = map(unary_op, |op| ExprElement::UnaryOp { op });
    let dot_number_map_access = map(map_access_dot_number, |accessor| ExprElement::MapAccess {
        accessor,
    });
    let colon_map_access = map(map_access_colon, |accessor| ExprElement::MapAccess {
        accessor,
    });
    let dot_access = map(
        rule! {
           "." ~ #column_id
        },
        |(_, column)| ExprElement::DotAccess { key: column },
    );

    let chain_function_call = check_experimental_chain_function(
        true,
        alt((
            map(
                rule! {
                    "." ~ #function_name
                    ~ "(" ~ #ident ~ "->" ~ #subexpr(0) ~ ")"
                },
                |(_, name, _, param, _, expr, _)| ExprElement::ChainFunctionCall {
                    name,
                    args: vec![],
                    lambda: Some(Lambda {
                        params: vec![param],
                        expr: Box::new(expr),
                    }),
                },
            ),
            map(
                rule! {
                    "." ~ #function_name ~ "(" ~ #comma_separated_list0(subexpr(0)) ~ ^")"
                },
                |(_, name, _, args, _)| ExprElement::ChainFunctionCall {
                    name,
                    args,
                    lambda: None,
                },
            ),
        )),
    );

    // python style list comprehensions
    // python: [i for i in range(10) if i%2==0 ]
    // sql: [i for i in range(10) if i%2 = 0 ]
    let list_comprehensions = check_experimental_list_comprehension(
        true,
        map(
            rule! {
                "[" ~ #subexpr(0) ~ FOR ~ #ident ~ IN
                ~ #subexpr(0) ~ (IF ~ #subexpr(2))? ~ "]"
            },
            |(_, result, _, param, _, source, opt_filter, _)| {
                let filter = opt_filter.map(|(_, filter)| filter);
                ExprElement::ListComprehension {
                    source,
                    param,
                    filter,
                    result,
                }
            },
        ),
    );

    // Floating point literal with leading dot will be parsed as a period map access,
    // and then will be converted back to a floating point literal if the map access
    // is not following a primary element nor a postfix element.
    let literal = map(literal, |value| ExprElement::Literal { value });
    let array = map(
        // Array that contains a single literal item will be parsed as a bracket map access,
        // and then will be converted back to an array if the map access is not following
        // a primary element nor a postfix element.
        rule! {
            "[" ~ #comma_separated_list0_ignore_trailing(subexpr(0))? ~ ","? ~ ^"]"
        },
        |(_, opt_args, _, _)| {
            let mut exprs = opt_args.unwrap_or_default();

            if exprs.len() == 1 {
                let expr = exprs.pop().unwrap();
                return ExprElement::MapAccess {
                    accessor: MapAccessor::Bracket {
                        key: Box::new(expr),
                    },
                };
            }
            ExprElement::Array { exprs }
        },
    );

    let map_expr = map(
        rule! { "{" ~ #comma_separated_list0(map_element) ~ "}" },
        |(_, kvs, _)| ExprElement::Map { kvs },
    );

    let date_add = map(
        rule! {
            (DATEADD | DATE_ADD) ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, interval, _, date, _)| ExprElement::DateAdd {
            unit,
            interval,
            date,
        },
    );

    let date_diff = map(
        rule! {
            (DATE_DIFF | DATEDIFF) ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, date_start, _, date_end, _)| ExprElement::DateDiff {
            unit,
            date_start,
            date_end,
        },
    );

    let date_sub = map(
        rule! {
            (DATESUB | DATE_SUB) ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, interval, _, date, _)| ExprElement::DateSub {
            unit,
            interval,
            date,
        },
    );

    let date_between = map(
        rule! {
            (DATEBETWEEN | DATE_BETWEEN) ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, date_start, _, date_end, _)| ExprElement::DateBetween {
            unit,
            date_start,
            date_end,
        },
    );

    let interval = map(
        rule! {
            INTERVAL ~ ^#subexpr(0) ~ #interval_kind?
        },
        |(_, expr, unit)| match unit {
            None => ExprElement::Cast {
                expr: Box::new(expr),
                target_type: TypeName::Interval,
            },
            Some(unit) => ExprElement::Interval { expr, unit },
        },
    );

    let date_trunc = map(
        rule! {
            DATE_TRUNC ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, date, _)| ExprElement::DateTrunc { unit, date },
    );

    let time_slice = map(
        rule! {
            TIME_SLICE ~ "(" ~ #subexpr(0) ~ "," ~ ^#literal_u64 ~ "," ~ #interval_kind ~ ("," ~ ^#literal_string)? ~ ")"
        },
        |(_, _, date, _, slice_length, _, unit, opt_start_or_end, _)| ExprElement::TimeSlice {
            unit,
            date,
            slice_length,
            start_or_end: opt_start_or_end.map(|(_, start_or_end)| start_or_end),
        },
    );

    let trunc = map(
        rule! {
            TRUNC ~ "(" ~  (#subexpr(0) ~ "," ~  #interval_kind)? ~ (#subexpr(0) ~ ("," ~  #subexpr(0))?)? ~ ")"
        },
        |(s, _, opt_date, opt_numeric, _)| match (opt_date, opt_numeric) {
            (Some((date, _, unit)), None) => ExprElement::DateTrunc { unit, date },
            (None, Some((expr, opt_expr2))) => {
                if let Some((_, expr2)) = opt_expr2 {
                    ExprElement::FunctionCall {
                        func: FunctionCall {
                            distinct: false,
                            name: Identifier::from_name(Some(s.span), "TRUNCATE"),
                            args: vec![expr, expr2],
                            ..Default::default()
                        },
                    }
                } else {
                    ExprElement::FunctionCall {
                        func: FunctionCall {
                            distinct: false,
                            name: Identifier::from_name(Some(s.span), "TRUNCATE"),
                            args: vec![expr],
                            ..Default::default()
                        },
                    }
                }
            }
            _ => ExprElement::DateTrunc {
                unit: IntervalKind::UnknownIntervalKind,
                date: Expr::Literal {
                    span: None,
                    value: Literal::Null,
                },
            },
        },
    );

    let last_day = map(
        rule! {
            LAST_DAY ~ "(" ~ #subexpr(0) ~ ("," ~ #interval_kind)? ~ ")"
        },
        |(_, _, date, opt_unit, _)| {
            if let Some((_, unit)) = opt_unit {
                ExprElement::LastDay { unit, date }
            } else {
                ExprElement::LastDay {
                    unit: IntervalKind::Month,
                    date,
                }
            }
        },
    );

    let previous_day = map(
        rule! {
            PREVIOUS_DAY ~ "(" ~ #subexpr(0) ~ "," ~ #weekday ~ ")"
        },
        |(_, _, date, _, unit, _)| ExprElement::PreviousDay { unit, date },
    );

    let next_day = map(
        rule! {
            NEXT_DAY ~ "(" ~ #subexpr(0) ~ "," ~ #weekday ~ ")"
        },
        |(_, _, date, _, unit, _)| ExprElement::NextDay { unit, date },
    );

    let date_expr = map(
        rule! {
            DATE ~ #consumed(literal_string)
        },
        |(_, (span, date))| ExprElement::Cast {
            expr: Box::new(Expr::Literal {
                span: transform_span(span.tokens),
                value: Literal::String(date),
            }),
            target_type: TypeName::Date,
        },
    );

    let timestamp_expr = map(
        rule! {
            TIMESTAMP ~ #consumed(literal_string)
        },
        |(_, (span, date))| ExprElement::Cast {
            expr: Box::new(Expr::Literal {
                span: transform_span(span.tokens),
                value: Literal::String(date),
            }),
            target_type: TypeName::Timestamp,
        },
    );

    let timestamp_tz_expr = map(
        rule! {
            TIMESTAMP_TZ ~ #consumed(literal_string)
        },
        |(_, (span, date))| ExprElement::Cast {
            expr: Box::new(Expr::Literal {
                span: transform_span(span.tokens),
                value: Literal::String(date),
            }),
            target_type: TypeName::TimestampTz,
        },
    );

    let is_distinct_from = map(
        rule! {
            IS ~ NOT? ~ DISTINCT ~ FROM
        },
        |(_, not, _, _)| ExprElement::IsDistinctFrom { not: not.is_some() },
    );

    let current_date = map(consumed(rule! { CURRENT_DATE }), |(span, _)| {
        ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: false,
                name: Identifier::from_name(transform_span(span.tokens), "current_date"),
                args: vec![],
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }
    });

    let current_time = map(consumed(rule! { CURRENT_TIME }), |(span, _)| {
        ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: false,
                name: Identifier::from_name(transform_span(span.tokens), "current_time"),
                args: vec![],
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }
    });

    let current_timestamp = map(consumed(rule! { CURRENT_TIMESTAMP }), |(span, _)| {
        ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: false,
                name: Identifier::from_name(transform_span(span.tokens), "current_timestamp"),
                args: vec![],
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }
    });

    let stage_location = map(rule! { #at_string }, |location| {
        ExprElement::StageLocation { location }
    });
    let string = map(literal_string, |literal| ExprElement::Literal {
        value: Literal::String(literal),
    });
    let code_string = map(code_string, |literal| ExprElement::Literal {
        value: Literal::String(literal),
    });
    let boolean = map(literal_bool, |literal| ExprElement::Literal {
        value: Literal::Boolean(literal),
    });
    let null = value(
        ExprElement::Literal {
            value: Literal::Null,
        },
        rule! { NULL },
    );
    let decimal_uint = map_res(
        rule! {
            LiteralInteger
        },
        |token| {
            Ok(ExprElement::Literal {
                value: parse_uint(token.text(), 10).map_err(nom::Err::Failure)?,
            })
        },
    );
    let hex_uint = map_res(literal_hex_str, |str| {
        Ok(ExprElement::Literal {
            value: parse_uint(str, 16).map_err(nom::Err::Failure)?,
        })
    });
    let decimal_float = map_res(
        verify(
            rule! {
               LiteralFloat
            },
            |token: &Token| !token.text().starts_with('.'),
        ),
        |token| {
            Ok(ExprElement::Literal {
                value: parse_float(token.text()).map_err(nom::Err::Failure)?,
            })
        },
    );
    let column_position = map(column_position, |column| ExprElement::ColumnRef {
        column: ColumnRef {
            database: None,
            table: None,
            column,
        },
    });
    let column_row = map(column_row, |column| ExprElement::ColumnRef {
        column: ColumnRef {
            database: None,
            table: None,
            column,
        },
    });
    let column_ident = map(column_ident, |column| ExprElement::ColumnRef {
        column: ColumnRef {
            database: None,
            table: None,
            column,
        },
    });

    if i.tokens.first().map(|token| token.kind) == Some(ColumnPosition) {
        return with_span!(column_position).parse(i);
    }

    try_dispatch!(i, true,
        IS => with_span!(rule!(#is_null | #is_distinct_from)).parse(i),
        NOT => with_span!(rule!(
            #in_list
                | #in_subquery
                | #exists
                | #between
                | #binary_op
                | #unary_op
        ))
        .parse(i),
        IN => with_span!(rule!(#in_list | #in_subquery)).parse(i),
        LIKE => with_span!(rule!(#like_subquery | #binary_op)).parse(i),
        EXISTS => with_span!(exists).parse(i),
        BETWEEN => with_span!(between).parse(i),
        CAST | TRY_CAST => with_span!(cast).parse(i),
        DoubleColon => with_span!(pg_cast).parse(i),
        POSITION => with_span!(position).parse(i),
        IDENTIFIER => {
            return with_span!(column_ref).parse(i);
        },
        IdentVariable => with_span!(variable_access).parse(i),
        ESCAPE => with_span!(escape).parse(i),
        COUNT => with_span!(rule!{ #count_all_with_window | #function_call}).parse(i),
        SUBSTRING | SUBSTR => with_span!(substring).parse(i),
        TRIM => with_span!(trim_from).parse(i),
        CASE => with_span!(case).parse(i),
        LParen => with_span!(rule!(#tuple | #subquery)).parse(i),
        ANY | SOME | ALL => with_span!(subquery).parse(i),
        Dot => {
            return with_span!(rule!(#chain_function_call | #dot_access | #dot_number_map_access))
                .parse(i);
        },
        Colon => {
            return with_span!(colon_map_access).parse(i);
        },
        LBracket => {
            return with_span!(rule!(
                #list_comprehensions | #array
            ))
            .parse(i);
        },
        LBrace => with_span!(map_expr).parse(i),
        LiteralAtString => with_span!(stage_location).parse(i),
        DATEADD | DATE_ADD => with_span!(date_add).parse(i),
        DATE_DIFF | DATEDIFF => with_span!(date_diff).parse(i),
        DATESUB | DATE_SUB => with_span!(date_sub).parse(i),
        DATEBETWEEN | DATE_BETWEEN => with_span!(date_between).parse(i),
        DATE_TRUNC => with_span!(date_trunc).parse(i),
        TIME_SLICE => with_span!(time_slice).parse(i),
        TRUNC => with_span!(trunc).parse(i),
        LAST_DAY => with_span!(last_day).parse(i),
        PREVIOUS_DAY => with_span!(previous_day).parse(i),
        NEXT_DAY => with_span!(next_day).parse(i),
        DATE => with_span!(date_expr).parse(i),
        TIMESTAMP => with_span!(timestamp_expr).parse(i),
        TIMESTAMP_TZ => with_span!(timestamp_tz_expr).parse(i),
        INTERVAL => with_span!(interval).parse(i),
        DATE_PART | DATEPART => with_span!(date_part).parse(i),
        EXTRACT => with_span!(extract).parse(i),
        CURRENT_DATE => with_span!(rule!{ #function_call | #current_date }).parse(i),
        CURRENT_TIME => with_span!(rule!{ #function_call | #current_time }).parse(i),
        CURRENT_TIMESTAMP => with_span!(rule!{ #function_call | #current_timestamp }).parse(i),
        Plus
            | Minus
            | Multiply
            | Divide
            | IntDiv
            | DIV
            | Modulo
            | StringConcat
            | Spaceship
            | L1DISTANCE
            | L2DISTANCE
            | Gt
            | Lt
            | Gte
            | Lte
            | Eq
            | NotEq
            | Caret
            | AND
            | OR
            | XOR
            | REGEXP
            | RLIKE
            | BitWiseOr
            | BitWiseAnd
            | BitWiseXor
            | ShiftLeft
            | ShiftRight
            | SOUNDS => with_span!(rule!{ #binary_op | #unary_op }).parse(i),
        RArrow
            | LongRArrow
            | HashRArrow
            | HashLongRArrow
            | Placeholder
            | QuestionOr
            | QuestionAnd
            | AtArrow
            | ArrowAt
            | AtQuestion
            | AtAt
            | HashMinus => with_span!(json_op).parse(i),
        Factorial | SquareRoot | BitWiseNot | CubeRoot | Abs => with_span!(unary_op).parse(i),
        LiteralString => with_span!(string).parse(i),
        LiteralCodeString => with_span!(code_string).parse(i),
        LiteralInteger => with_span!(decimal_uint).parse(i),
        LiteralFloat => with_span!(rule!{ #decimal_float | #dot_number_map_access }).parse(i),
        MySQLLiteralHex | PGLiteralHex => with_span!(hex_uint).parse(i),
        TRUE | FALSE => with_span!(boolean).parse(i),
        NULL => with_span!(null).parse(i),
        ROW => with_span!(column_row).parse(i),
    );

    // The try-parse operation in the function call is very expensive, easy to stack overflow
    // so we manually check here whether the second token exists in LParen to avoid entering the loop
    if i.tokens
        .get(1)
        .map(|token| token.kind == LParen)
        .unwrap_or(false)
    {
        return with_span!(function_call).parse(i);
    }

    with_span!(alt((rule!(
        #column_ident : "<column>"
        | #literal : "<literal>"
    ),)))
    .parse(i)
}

#[inline]
fn return_op<T>(i: Input, start: usize, op: T) -> IResult<T> {
    Ok((i.slice(start..), op))
}

macro_rules! op_branch {
    ($i:ident, $token_0:ident, $($kind:ident => $op:expr_2021),+ $(,)?) => {
        match $token_0.kind {
            $(
                TokenKind::$kind => return return_op($i, 1, $op),
            )+
            _ => (),
        }
    };
}

pub fn unary_op(i: Input) -> IResult<UnaryOperator> {
    // Plus and Minus are parsed as binary op at first.
    if let Some(token_0) = i.tokens.first() {
        op_branch!(
            i, token_0,
            NOT => UnaryOperator::Not,
            Factorial => UnaryOperator::Factorial,
            SquareRoot => UnaryOperator::SquareRoot,
            BitWiseNot => UnaryOperator::BitwiseNot,
            CubeRoot => UnaryOperator::CubeRoot,
            Abs => UnaryOperator::Abs,
        );
    }
    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::Other("expecting `NOT`, '!', '|/', '~', '||/', '@', or more ..."),
    )))
}

pub fn binary_op(i: Input) -> IResult<BinaryOperator> {
    if let Some(token_0) = i.tokens.first() {
        op_branch!(
            i, token_0,
            Plus => BinaryOperator::Plus,
            Minus => BinaryOperator::Minus,
            Multiply => BinaryOperator::Multiply,
            Divide => BinaryOperator::Divide,
            IntDiv => BinaryOperator::IntDiv,
            DIV => BinaryOperator::Div,
            Modulo => BinaryOperator::Modulo,
            StringConcat => BinaryOperator::StringConcat,
            Spaceship => BinaryOperator::CosineDistance,
            L1DISTANCE => BinaryOperator::L1Distance,
            L2DISTANCE => BinaryOperator::L2Distance,
            Gt => BinaryOperator::Gt,
            Lt => BinaryOperator::Lt,
            Gte => BinaryOperator::Gte,
            Lte => BinaryOperator::Lte,
            Eq => BinaryOperator::Eq,
            NotEq => BinaryOperator::NotEq,
            Caret => BinaryOperator::Caret,
            AND => BinaryOperator::And,
            OR => BinaryOperator::Or,
            XOR => BinaryOperator::Xor,
            REGEXP => BinaryOperator::Regexp,
            RLIKE => BinaryOperator::RLike,
            BitWiseOr => BinaryOperator::BitwiseOr,
            BitWiseAnd => BinaryOperator::BitwiseAnd,
            BitWiseXor => BinaryOperator::BitwiseXor,
            ShiftLeft => BinaryOperator::BitwiseShiftLeft,
            ShiftRight => BinaryOperator::BitwiseShiftRight,
        );
        match token_0.kind {
            LIKE => {
                return if matches!(i.tokens.get(1).map(|first| first.kind == ANY), Some(true)) {
                    return_op(i, 2, BinaryOperator::LikeAny(None))
                } else {
                    return_op(i, 1, BinaryOperator::Like(None))
                };
            }
            NOT => match i.tokens.get(1).map(|first| first.kind) {
                Some(LIKE) => {
                    return return_op(i, 2, BinaryOperator::NotLike(None));
                }
                Some(REGEXP) => {
                    return return_op(i, 2, BinaryOperator::NotRegexp);
                }
                Some(RLIKE) => {
                    return return_op(i, 2, BinaryOperator::NotRLike);
                }
                _ => (),
            },
            SOUNDS => {
                if let Some(LIKE) = i.tokens.get(1).map(|first| first.kind) {
                    return return_op(i, 2, BinaryOperator::SoundsLike);
                }
            }
            _ => (),
        }
    }
    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::Other(
            "expecting `IS`, `IN`, `LIKE`, `EXISTS`, `BETWEEN`, `+`, `-`, `*`, `/`, `//`, `DIV`, `%`, `||`, `<=>`, `<+>`, `<->`, `>`, `<`, `>=`, `<=`, `=`, `<>`, `!=`, `^`, `AND`, `OR`, `XOR`, `NOT`, `REGEXP`, `RLIKE`, `SOUNDS`, or more ...",
        ),
    )))
}

pub fn json_op(i: Input) -> IResult<JsonOperator> {
    if let Some(token_0) = i.tokens.first() {
        op_branch!(
            i, token_0,
            RArrow => JsonOperator::Arrow,
            LongRArrow => JsonOperator::LongArrow,
            HashRArrow => JsonOperator::HashArrow,
            HashLongRArrow => JsonOperator::HashLongArrow,
            Placeholder => JsonOperator::Question,
            QuestionOr => JsonOperator::QuestionOr,
            QuestionAnd => JsonOperator::QuestionAnd,
            AtArrow => JsonOperator::AtArrow,
            ArrowAt => JsonOperator::ArrowAt,
            AtQuestion => JsonOperator::AtQuestion,
            AtAt => JsonOperator::AtAt,
            HashMinus => JsonOperator::HashMinus,
        );
    }
    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::Other(
            "expecting `->`, '->>', '#>', '#>>', '?', '?|', '?&', '@>', '<@', '@?', '@@', '#-', or more ...",
        ),
    )))
}

pub fn literal(i: Input) -> IResult<Literal> {
    let mut string = map(literal_string, Literal::String);
    let mut code_string = map(code_string, Literal::String);
    let mut boolean = map(literal_bool, Literal::Boolean);
    let mut null = value(Literal::Null, rule! { NULL });
    let mut decimal_uint = map_res(
        rule! {
            LiteralInteger
        },
        |token| parse_uint(token.text(), 10).map_err(nom::Err::Failure),
    );
    let mut hex_uint = map_res(literal_hex_str, |str| {
        parse_uint(str, 16).map_err(nom::Err::Failure)
    });
    let mut decimal_float = map_res(
        rule! {
           LiteralFloat
        },
        |token| parse_float(token.text()).map_err(nom::Err::Failure),
    );

    try_dispatch!(i, true,
        LiteralString => string.parse(i),
        LiteralCodeString => code_string.parse(i),
        LiteralInteger => decimal_uint.parse(i),
        LiteralFloat => decimal_float.parse(i),
        MySQLLiteralHex | PGLiteralHex => hex_uint(i),
        TRUE | FALSE => boolean.parse(i),
        NULL => null.parse(i),
    );

    Err(nom::Err::Error(Error::from_error_kind(
        i,
        ErrorKind::Other(
            "expecting `<LiteralString>`, '<LiteralCodeString>', '<LiteralInteger>', '<LiteralFloat>', 'TRUE', 'FALSE', or more ...",
        ),
    )))
}

pub fn literal_hex_str(i: Input<'_>) -> IResult<'_, &str> {
    // 0XFFFF
    let mysql_hex = map(
        rule! {
            MySQLLiteralHex
        },
        |token| &token.text()[2..],
    );
    // x'FFFF'
    let pg_hex = map(
        rule! {
            PGLiteralHex
        },
        |token| &token.text()[2..token.text().len() - 1],
    );

    rule!(
        #mysql_hex
        | #pg_hex
    )
    .parse(i)
}

#[allow(clippy::from_str_radix_10)]
pub fn literal_u64(i: Input) -> IResult<u64> {
    let decimal = map_res(
        rule! {
            LiteralInteger
        },
        |token| u64::from_str_radix(token.text(), 10).map_err(|e| nom::Err::Failure(e.into())),
    );
    let hex = map_res(literal_hex_str, |lit| {
        u64::from_str_radix(lit, 16).map_err(|e| nom::Err::Failure(e.into()))
    });

    rule!(
        #decimal
        | #hex
    )
    .parse(i)
}

#[allow(clippy::from_str_radix_10)]
pub fn literal_i64(i: Input) -> IResult<i64> {
    let decimal = map_res(
        rule! {
            LiteralInteger
        },
        |token| i64::from_str_radix(token.text(), 10).map_err(|e| nom::Err::Failure(e.into())),
    );
    let hex = map_res(literal_hex_str, |lit| {
        i64::from_str_radix(lit, 16).map_err(|e| nom::Err::Failure(e.into()))
    });

    rule!(
        #decimal
        | #hex
    )
    .parse(i)
}

pub fn literal_bool(i: Input) -> IResult<bool> {
    alt((value(true, rule! { TRUE }), value(false, rule! { FALSE }))).parse(i)
}

pub fn literal_string(i: Input) -> IResult<String> {
    map_res(
        rule! {
            LiteralString
        },
        |token| {
            let quote::QuotedString(s, quote) = token
                .text()
                .parse()
                .map_err(|_| nom::Err::Failure(ErrorKind::Other("invalid escape or unicode")))?;

            if !i.dialect.is_string_quote(quote) {
                return Err(nom::Err::Error(ErrorKind::ExpectToken(LiteralString)));
            }

            Ok(s)
        },
    )(i)
}

pub fn literal_string_eq_ignore_case(s: &str) -> impl FnMut(Input) -> IResult<()> + '_ {
    move |i| {
        map_res(rule! { LiteralString }, |token| {
            if token.text()[1..token.text().len() - 1].eq_ignore_ascii_case(s) {
                Ok(())
            } else {
                Err(nom::Err::Error(ErrorKind::ExpectToken(LiteralString)))
            }
        })(i)
    }
}

pub fn at_string(i: Input) -> IResult<String> {
    map_res(rule! { LiteralAtString }, |token| {
        let AtString(s) = token
            .text()
            .parse()
            .map_err(|_| nom::Err::Failure(ErrorKind::Other("invalid at string")))?;
        Ok(s)
    })(i)
}

pub fn code_string(i: Input) -> IResult<String> {
    map_res(rule! { LiteralCodeString }, |token| {
        let content = &token.text()[2..token.text().len() - 2];
        let trimmed = unindent::unindent(content).trim().to_string();
        Ok(trimmed)
    })(i)
}

pub fn nullable(i: Input) -> IResult<bool> {
    alt((
        value(true, rule! { NULL }),
        value(false, rule! { NOT ~ NULL }),
    ))
    .parse(i)
}

pub fn type_name(i: Input) -> IResult<TypeName> {
    let ty_boolean = value(TypeName::Boolean, rule! { BOOLEAN | BOOL });
    let ty_uint8 = value(TypeName::UInt8, rule! { (
            #map(rule! { UINT8 ~ ( "(" ~ ^#literal_u64 ~ ^")" )? }, |(t, _)| t) |
            #map(rule! { TINYINT ~ ( "(" ~ ^#literal_u64 ~ ^")" )? ~ UNSIGNED }, |(t, _, _)| t)
        )
    });
    let ty_uint16 = value(TypeName::UInt16, rule! { (
            #map(rule! { UINT16 ~ ( "(" ~ ^#literal_u64 ~ ^")" )? }, |(t, _)| t) |
            #map(rule! { SMALLINT ~ ( "(" ~ ^#literal_u64 ~ ^")" )? ~ UNSIGNED }, |(t, _, _)| t)
        )
    });
    let ty_uint32 = value(TypeName::UInt32, rule! { (
            #map(rule! { UINT32 ~ ( "(" ~ ^#literal_u64 ~ ^")" )? }, |(t, _)| t) |
            #map(rule! { ( INT | INTEGER ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? ~ UNSIGNED }, |(t, _, _)| t)
        )
    });
    let ty_uint64 = value(TypeName::UInt64, rule! { (
            #map(rule! { ( UINT64 | UNSIGNED) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? }, |(t, _)| t) |
            #map(rule! { BIGINT ~ ( "(" ~ ^#literal_u64 ~ ^")" )? ~ UNSIGNED }, |(t, _, _)| t)
        )
    });
    let ty_int8 = value(
        TypeName::Int8,
        rule! { ( INT8 | TINYINT ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_int16 = value(
        TypeName::Int16,
        rule! { ( INT16 | SMALLINT ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_int32 = value(
        TypeName::Int32,
        rule! { ( INT32 | INT | INTEGER ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_int64 = value(
        TypeName::Int64,
        rule! { ( INT64 | SIGNED | BIGINT ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_float32 = value(TypeName::Float32, rule! { FLOAT32 | FLOAT | REAL });
    let ty_float64 = value(
        TypeName::Float64,
        rule! { (FLOAT64 | DOUBLE)  ~ PRECISION? },
    );
    let ty_decimal = map_res(
        rule! { DECIMAL ~ ( "(" ~ #literal_u64 ~ ( "," ~ ^#literal_u64 )? ~ ")" )? },
        |(_, opt_precision)| {
            let (precision, scale) = match opt_precision {
                Some((_, precision, scale, _)) => {
                    (precision, scale.map(|(_, scale)| scale).unwrap_or(0))
                }
                None => (18, 3),
            };

            Ok(TypeName::Decimal {
                precision: precision
                    .try_into()
                    .map_err(|_| nom::Err::Failure(ErrorKind::Other("precision is too large")))?,
                scale: scale
                    .try_into()
                    .map_err(|_| nom::Err::Failure(ErrorKind::Other("scale is too large")))?,
            })
        },
    );
    let ty_numeric = value(
        TypeName::Decimal {
            precision: 18,
            scale: 3,
        },
        rule! { NUMERIC },
    );

    let ty_array = map(
        rule! { ARRAY ~ "(" ~ #type_name ~ ")" },
        |(_, _, item_type, _)| TypeName::Array(Box::new(item_type)),
    );
    let ty_map = map(
        rule! { MAP ~ "(" ~ #type_name ~ "," ~ #type_name ~ ")" },
        |(_, _, key_type, _, val_type, _)| TypeName::Map {
            key_type: Box::new(key_type),
            val_type: Box::new(val_type),
        },
    );
    let ty_bitmap = value(TypeName::Bitmap, rule! { BITMAP });
    let ty_nullable = map(
        rule! { NULLABLE ~ ( "(" ~ #type_name ~ ")" ) },
        |(_, item_type)| TypeName::Nullable(Box::new(item_type.1)),
    );
    let ty_tuple = map(
        rule! { TUPLE ~ "(" ~ #comma_separated_list1(type_name) ~ ")" },
        |(_, _, fields_type, _)| TypeName::Tuple {
            fields_name: None,
            fields_type,
        },
    );
    let ty_named_tuple = map_res(
        rule! { TUPLE ~ "(" ~ #comma_separated_list1(rule! { #ident ~ #type_name }) ~ ")" },
        |(_, _, fields, _)| {
            let (fields_name, fields_type): (Vec<Identifier>, Vec<TypeName>) =
                fields.into_iter().unzip();
            Ok(TypeName::Tuple {
                fields_name: Some(fields_name),
                fields_type,
            })
        },
    );
    let ty_date = value(TypeName::Date, rule! { DATE });
    let ty_interval = value(TypeName::Interval, rule! { INTERVAL });
    let ty_datetime = map(
        rule! { ( DATETIME | TIMESTAMP ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
        |(_, _)| TypeName::Timestamp,
    );
    let ty_binary = value(
        TypeName::Binary,
        rule! { ( BINARY | VARBINARY | LONGBLOB | MEDIUMBLOB |  TINYBLOB | BLOB ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_string = value(
        TypeName::String,
        rule! { ( STRING | VARCHAR | CHAR | CHARACTER | TEXT ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_variant = value(TypeName::Variant, rule! { VARIANT | JSON });
    let ty_geometry = value(TypeName::Geometry, rule! { GEOMETRY });
    let ty_geography = value(TypeName::Geography, rule! { GEOGRAPHY });
    let ty_vector = map(
        rule! { VECTOR ~ ^"(" ~ ^#literal_u64 ~ ^")" },
        |(_, _, dimension, _)| TypeName::Vector(dimension),
    );
    let ty_stage_location = value(TypeName::StageLocation, rule! { STAGE_LOCATION });
    let ty_timestamp_tz = value(
        TypeName::TimestampTz,
        rule! { TIMESTAMP ~ WITH ~ TIME ~ ZONE },
    );
    let ty_timestamp_tz_simply = value(TypeName::TimestampTz, rule! { TIMESTAMP_TZ });
    map_res(
        alt((
            rule! {
            ( #ty_boolean
            | #ty_uint8
            | #ty_uint16
            | #ty_uint32
            | #ty_uint64
            | #ty_int8
            | #ty_int16
            | #ty_int32
            | #ty_int64
            | #ty_float32
            | #ty_float64
            | #ty_decimal
            | #ty_array
            | #ty_map
            | #ty_bitmap
            | #ty_tuple : "TUPLE(<type>, ...)"
            | #ty_named_tuple : "TUPLE(<name> <type>, ...)"
            ) ~ #nullable? : "type name"
            },
            rule! {
            ( #ty_date
            | #ty_timestamp_tz
            | #ty_timestamp_tz_simply
            | #ty_datetime
            | #ty_interval
            | #ty_numeric
            | #ty_binary
            | #ty_string
            | #ty_variant
            | #ty_geometry
            | #ty_geography
            | #ty_nullable
            | #ty_vector
            | #ty_stage_location
            ) ~ #nullable? : "type name" },
        )),
        |(ty, opt_nullable)| match opt_nullable {
            Some(true) => Ok(ty.wrap_nullable()),
            Some(false) => {
                if matches!(ty, TypeName::Nullable(_)) {
                    Err(nom::Err::Failure(ErrorKind::Other(
                        "ambiguous NOT NULL constraint",
                    )))
                } else {
                    Ok(ty.wrap_not_null())
                }
            }
            None => Ok(ty),
        },
    )(i)
}

pub fn weekday(i: Input) -> IResult<Weekday> {
    alt((
        value(Weekday::Sunday, rule! { SUNDAY }),
        value(Weekday::Monday, rule! { MONDAY }),
        value(Weekday::Tuesday, rule! { TUESDAY }),
        value(Weekday::Wednesday, rule! { WEDNESDAY }),
        value(Weekday::Thursday, rule! { THURSDAY }),
        value(Weekday::Friday, rule! { FRIDAY }),
        value(Weekday::Saturday, rule! { SATURDAY }),
        value(
            Weekday::Sunday,
            rule! { #literal_string_eq_ignore_case("SUNDAY") },
        ),
        value(
            Weekday::Monday,
            rule! { #literal_string_eq_ignore_case("MONDAY") },
        ),
        value(
            Weekday::Tuesday,
            rule! { #literal_string_eq_ignore_case("TUESDAY") },
        ),
        value(
            Weekday::Wednesday,
            rule! { #literal_string_eq_ignore_case("WEDNESDAY") },
        ),
        value(
            Weekday::Thursday,
            rule! { #literal_string_eq_ignore_case("THURSDAY") },
        ),
        value(
            Weekday::Friday,
            rule! { #literal_string_eq_ignore_case("FRIDAY") },
        ),
        value(
            Weekday::Saturday,
            rule! { #literal_string_eq_ignore_case("SATURDAY") },
        ),
    ))
    .parse(i)
}

pub fn interval_kind(i: Input) -> IResult<IntervalKind> {
    let iso_year = value(IntervalKind::ISOYear, rule! { ISOYEAR });
    let year = value(IntervalKind::Year, rule! { YEAR });
    let quarter = value(IntervalKind::Quarter, rule! { QUARTER });
    let month = value(IntervalKind::Month, rule! { MONTH });
    let day = value(IntervalKind::Day, rule! { DAY });
    let hour = value(IntervalKind::Hour, rule! { HOUR });
    let minute = value(IntervalKind::Minute, rule! { MINUTE });
    let second = value(IntervalKind::Second, rule! { SECOND });
    let doy = value(IntervalKind::Doy, rule! { DOY });
    let dow = value(IntervalKind::Dow, rule! { DOW });
    let isodow = value(IntervalKind::ISODow, rule! { ISODOW });
    let isoweek = value(IntervalKind::ISOWeek, rule! { ISOWEEK });
    let week = value(IntervalKind::Week, rule! { WEEK });
    let epoch = value(IntervalKind::Epoch, rule! { EPOCH });
    let microsecond = value(IntervalKind::MicroSecond, rule! { MICROSECOND });
    let millennium = value(IntervalKind::Millennium, rule! { MILLENNIUM });
    let yearweek = value(IntervalKind::YearWeek, rule! { YEARWEEK });

    let iso_year_str = value(
        IntervalKind::ISOYear,
        rule! { #literal_string_eq_ignore_case("ISOYEAR") },
    );

    let year_str = value(
        IntervalKind::Year,
        rule! { #literal_string_eq_ignore_case("YEAR")
            | #literal_string_eq_ignore_case("Y")
            | #literal_string_eq_ignore_case("YY")
            | #literal_string_eq_ignore_case("YYY")
            | #literal_string_eq_ignore_case("YYYY")
            | #literal_string_eq_ignore_case("YR")
            | #literal_string_eq_ignore_case("YEARS")
            | #literal_string_eq_ignore_case("YRS")
        },
    );

    let quarter_str = value(
        IntervalKind::Quarter,
        rule! { #literal_string_eq_ignore_case("QUARTER")
            | #literal_string_eq_ignore_case("Q")
            | #literal_string_eq_ignore_case("QTR")
            | #literal_string_eq_ignore_case("QTRS")
            | #literal_string_eq_ignore_case("QUARTERS")
        },
    );

    let month_str = value(
        IntervalKind::Month,
        rule! { #literal_string_eq_ignore_case("MONTH")
            | #literal_string_eq_ignore_case("MM")
            | #literal_string_eq_ignore_case("MON")
            | #literal_string_eq_ignore_case("MONS")
            | #literal_string_eq_ignore_case("MONTHS")
        },
    );

    let day_str = value(
        IntervalKind::Day,
        rule! { #literal_string_eq_ignore_case("DAY")
            | #literal_string_eq_ignore_case("D")
            | #literal_string_eq_ignore_case("DD")
            | #literal_string_eq_ignore_case("DAYS")
            | #literal_string_eq_ignore_case("DAYOFMONTH")
        },
    );

    let hour_str = value(
        IntervalKind::Hour,
        rule! { #literal_string_eq_ignore_case("HOUR")
            | #literal_string_eq_ignore_case("H")
            | #literal_string_eq_ignore_case("HH")
            | #literal_string_eq_ignore_case("HH24")
            | #literal_string_eq_ignore_case("HR")
            | #literal_string_eq_ignore_case("HOURS")
            | #literal_string_eq_ignore_case("HRS")
        },
    );

    let minute_str = value(
        IntervalKind::Minute,
        rule! { #literal_string_eq_ignore_case("MINUTE")
            | #literal_string_eq_ignore_case("M")
            | #literal_string_eq_ignore_case("MI")
            | #literal_string_eq_ignore_case("MIN")
            | #literal_string_eq_ignore_case("MINUTES")
            | #literal_string_eq_ignore_case("MINS")
        },
    );

    let second_str = value(
        IntervalKind::Second,
        rule! { #literal_string_eq_ignore_case("SECOND")
            | #literal_string_eq_ignore_case("S")
            | #literal_string_eq_ignore_case("SEC")
            | #literal_string_eq_ignore_case("SECONDS")
            | #literal_string_eq_ignore_case("SECS")
        },
    );

    let doy_str = value(
        IntervalKind::Doy,
        rule! { #literal_string_eq_ignore_case("DOY")
            | #literal_string_eq_ignore_case("DAYOFYEAR")
            | #literal_string_eq_ignore_case("YEARDAY")
            | #literal_string_eq_ignore_case("DY")
        },
    );

    let dow_str = value(
        IntervalKind::Dow,
        rule! { (#literal_string_eq_ignore_case("DOW")
            | #literal_string_eq_ignore_case("WEEKDAY")
            | #literal_string_eq_ignore_case("DW")
            | #literal_string_eq_ignore_case("DAYOFWEEK"))
        },
    );

    let isodow_str = value(
        IntervalKind::ISODow,
        rule! { #literal_string_eq_ignore_case("ISODOW")
            | #literal_string_eq_ignore_case("DAYOFWEEK_ISO")
            | #literal_string_eq_ignore_case("DAYOFWEEKISO")
            | #literal_string_eq_ignore_case("WEEKDAY_ISO")
            | #literal_string_eq_ignore_case("DOW_ISO")
            | #literal_string_eq_ignore_case("DW_ISO")
        },
    );

    let week_str = value(
        IntervalKind::Week,
        rule! { (#literal_string_eq_ignore_case("WEEK") | #literal_string_eq_ignore_case("WEEKS") | #literal_string_eq_ignore_case("W"))
            | #literal_string_eq_ignore_case("WK")
            | #literal_string_eq_ignore_case("WEEKOFYEAR")
            | #literal_string_eq_ignore_case("WOY")
            | #literal_string_eq_ignore_case("WY")
        },
    );

    let isoweek_str = value(
        IntervalKind::ISOWeek,
        rule! { #literal_string_eq_ignore_case("IW") },
    );

    let epoch_str = value(
        IntervalKind::Epoch,
        rule! { #literal_string_eq_ignore_case("EPOCH")
            | #literal_string_eq_ignore_case("EPOCH_SECOND")
            | #literal_string_eq_ignore_case("EPOCH")
            | #literal_string_eq_ignore_case("EPOCH_SECONDS")
        },
    );

    let microsecond_str = value(
        IntervalKind::MicroSecond,
        rule! { #literal_string_eq_ignore_case("MICROSECOND")
            | #literal_string_eq_ignore_case("MICROSECONDS")
            | #literal_string_eq_ignore_case("US")
            | #literal_string_eq_ignore_case("USEC")
        },
    );

    let yearweek_str = value(
        IntervalKind::YearWeek,
        rule! { #literal_string_eq_ignore_case("YEARWEEK")
            | #literal_string_eq_ignore_case("YEAROFWEEK")
        },
    );

    let millennium_str = value(
        IntervalKind::Millennium,
        rule! { #literal_string_eq_ignore_case("MILLENNIUM") },
    );

    alt((
        rule!(
            #year
            | #iso_year
            | #quarter
            | #month
            | #day
            | #hour
            | #minute
            | #second
            | #doy
            | #dow
            | #week
            | #epoch
            | #microsecond
            | #isodow
            | #isoweek
            | #millennium
            | #yearweek
        ),
        rule!(
            #year_str
            | #iso_year_str
            | #quarter_str
            | #month_str
            | #day_str
            | #hour_str
            | #minute_str
            | #second_str
            | #doy_str
            | #dow_str
            | #week_str
            | #epoch_str
            | #microsecond_str
            | #isodow_str
            | #isoweek_str
            | #yearweek_str
            | #millennium_str
        ),
    ))
    .parse(i)
}

fn map_access_dot_number(i: Input) -> IResult<MapAccessor> {
    map_res(rule! { LiteralFloat }, |key| {
        if key.text().starts_with('.')
            && let Ok(key) = (key.text()[1..]).parse::<u64>()
        {
            return Ok(MapAccessor::DotNumber { key });
        }
        Err(nom::Err::Error(ErrorKind::ExpectText(".")))
    })
    .parse(i)
}

fn map_access_colon(i: Input) -> IResult<MapAccessor> {
    map(
        rule! {
            ":" ~ #ident
        },
        |(_, key)| MapAccessor::Colon { key },
    )
    .parse(i)
}

pub fn map_element(i: Input) -> IResult<(Literal, Expr)> {
    map(
        rule! {
            #literal ~ ":" ~ #subexpr(0)
        },
        |(key, _, value)| (key, value),
    )
    .parse(i)
}

pub fn function_call(i: Input) -> IResult<ExprElement> {
    enum FunctionCallSuffix {
        Simple {
            distinct: bool,
            args: Vec<Expr>,
        },
        Lambda {
            arg: Expr,
            params: Vec<Identifier>,
            expr: Box<Expr>,
        },
        Window {
            distinct: bool,
            args: Vec<Expr>,
            window: WindowDesc,
        },
        WithInGroupWindow {
            distinct: bool,
            args: Vec<Expr>,
            order_by: Vec<OrderByExpr>,
            window: Option<WindowDesc>,
        },
        ParamsWindow {
            distinct: bool,
            params: Vec<Expr>,
            args: Vec<Expr>,
            window: Option<WindowDesc>,
        },
    }
    let function_call_body = map_res(
        rule! {
            "(" ~ DISTINCT? ~ #subexpr(0)? ~ ","? ~ (#lambda_params ~ "->" ~ #subexpr(0))? ~ #comma_separated_list1(subexpr(0))? ~ ")"
            ~ ("(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")")?
            ~ #within_group?
            ~ #window_function?
        },
        |(
            _,
            opt_distinct_0,
            first_param,
            _,
            opt_lambda,
            params_0,
            _,
            params_1,
            order_by,
            window,
        )| {
            match (
                first_param,
                opt_lambda,
                opt_distinct_0,
                params_0,
                params_1,
                order_by,
                window,
            ) {
                (
                    Some(first_param),
                    Some((lambda_params, _, arg_1)),
                    None,
                    None,
                    None,
                    None,
                    None,
                ) => Ok(FunctionCallSuffix::Lambda {
                    arg: first_param,
                    params: lambda_params,
                    expr: Box::new(arg_1),
                }),
                (
                    Some(first_param),
                    None,
                    None,
                    params_0,
                    Some((_, opt_distinct_1, params_1, _)),
                    None,
                    window,
                ) => {
                    let params = params_0
                        .map(|mut params| {
                            params.insert(0, first_param.clone());
                            params
                        })
                        .unwrap_or_else(|| vec![first_param]);

                    Ok(FunctionCallSuffix::ParamsWindow {
                        distinct: opt_distinct_1.is_some(),
                        params,
                        args: params_1.unwrap_or_default(),
                        window,
                    })
                }
                (first_param, None, opt_distinct, params, None, Some(order_by), window) => {
                    let mut args = params.unwrap_or_default();
                    if let Some(first_param) = first_param {
                        args.insert(0, first_param)
                    }

                    Ok(FunctionCallSuffix::WithInGroupWindow {
                        distinct: opt_distinct.is_some(),
                        args,
                        order_by,
                        window,
                    })
                }
                (first_param, None, opt_distinct, params, None, None, Some(window)) => {
                    let mut args = params.unwrap_or_default();
                    if let Some(first_param) = first_param {
                        args.insert(0, first_param)
                    }

                    Ok(FunctionCallSuffix::Window {
                        distinct: opt_distinct.is_some(),
                        args,
                        window,
                    })
                }
                (first_param, None, opt_distinct, params, None, None, None) => {
                    let mut args = params.unwrap_or_default();
                    if let Some(first_param) = first_param {
                        args.insert(0, first_param)
                    }

                    Ok(FunctionCallSuffix::Simple {
                        distinct: opt_distinct.is_some(),
                        args,
                    })
                }
                _ => Err(nom::Err::Error(ErrorKind::Other(
                    "Unsupported function format",
                ))),
            }
        },
    );

    map(
        rule!(
            #function_name
            ~ #function_call_body : "`function(... [ , x -> ... ] ) [ (...) ] [ WITHIN GROUP ( ORDER BY <expr>, ... ) ] [ OVER ([ PARTITION BY <expr>, ... ] [ ORDER BY <expr>, ... ] [ <window frame> ]) ]`"
        ),
        |(name, suffix)| match suffix {
            FunctionCallSuffix::Simple { distinct, args } => ExprElement::FunctionCall {
                func: FunctionCall {
                    distinct,
                    name,
                    args,
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda: None,
                },
            },
            FunctionCallSuffix::Lambda { arg, params, expr } => ExprElement::FunctionCall {
                func: FunctionCall {
                    distinct: false,
                    name,
                    args: vec![arg],
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda: Some(Lambda { params, expr }),
                },
            },
            FunctionCallSuffix::Window {
                distinct,
                args,
                window,
            } => ExprElement::FunctionCall {
                func: FunctionCall {
                    distinct,
                    name,
                    args,
                    params: vec![],
                    order_by: vec![],
                    window: Some(window),
                    lambda: None,
                },
            },
            FunctionCallSuffix::WithInGroupWindow {
                distinct,
                args,
                order_by,
                window,
            } => ExprElement::FunctionCall {
                func: FunctionCall {
                    distinct,
                    name,
                    args,
                    params: vec![],
                    order_by,
                    window,
                    lambda: None,
                },
            },
            FunctionCallSuffix::ParamsWindow {
                distinct,
                params,
                args,
                window,
            } => ExprElement::FunctionCall {
                func: FunctionCall {
                    distinct,
                    name,
                    args,
                    params,
                    order_by: vec![],
                    window,
                    lambda: None,
                },
            },
        },
    ).parse(i)
}

pub fn parse_float(text: &str) -> Result<Literal, ErrorKind> {
    let text = text.trim_start_matches('0');
    let point_pos = text.find('.');
    let e_pos = text.find(['e', 'E']);
    let (i_part, f_part, e_part) = match (point_pos, e_pos) {
        (Some(p1), Some(p2)) => (&text[..p1], &text[(p1 + 1)..p2], Some(&text[(p2 + 1)..])),
        (Some(p), None) => (&text[..p], &text[(p + 1)..], None),
        (None, Some(p)) => (&text[..p], "", Some(&text[(p + 1)..])),
        _ => unreachable!(),
    };
    let exp = match e_part {
        Some(s) => match s.parse::<i32>() {
            Ok(i) => i,
            Err(_) => return Ok(Literal::Float64(fast_float2::parse(text)?)),
        },
        None => 0,
    };

    let i_part_len = i_part.len() as i32;
    let f_part_len = f_part.len() as i32;
    let mut precision = i_part_len + f_part_len;
    if exp > f_part_len {
        precision += exp - f_part_len;
    } else if i_part_len + exp < 0 {
        precision -= i_part_len + exp;
    }

    if precision > 76 {
        Ok(Literal::Float64(fast_float2::parse(text)?))
    } else {
        let mut digits = String::with_capacity(precision as usize);
        digits.push_str(i_part);
        digits.push_str(f_part);
        if digits.is_empty() {
            digits.push('0')
        }
        let mut scale = f_part_len - exp;
        if scale < 0 {
            // e.g 123.1e3
            for _ in 0..(-scale) {
                digits.push('0')
            }
            scale = 0;
        };
        Ok(Literal::Decimal256 {
            value: i256::from_str_radix(&digits, 10)?,
            precision: 76,
            scale: scale as u8,
        })
    }
}

pub fn parse_uint(text: &str, radix: u32) -> Result<Literal, ErrorKind> {
    let text = text.trim_start_matches('0');
    let contains_underscore = text.contains('_');
    if contains_underscore {
        let text = text.replace('_', "");
        return parse_uint(&text, radix);
    }

    if text.is_empty() {
        return Ok(Literal::UInt64(0));
    } else if text.len() > 76 {
        return Ok(Literal::Float64(fast_float2::parse(text)?));
    }

    let value = i256::from_str_radix(text, radix)?;
    if value <= i256::from(u64::MAX) {
        Ok(Literal::UInt64(value.as_u64()))
    } else {
        Ok(Literal::Decimal256 {
            value,
            precision: 76,
            scale: 0,
        })
    }
}

fn try_negate_literal(literal: &Literal) -> Option<Literal> {
    match literal {
        Literal::UInt64(value) => Some(Literal::Decimal256 {
            value: -i256::from(*value),
            precision: 76,
            scale: 0,
        }),
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => Some(Literal::Decimal256 {
            value: -*value,
            precision: *precision,
            scale: *scale,
        }),
        Literal::Float64(value) => Some(Literal::Float64(-value)),
        _ => None,
    }
}

pub(crate) fn make_func_get_variable(span: Span, name: String) -> Expr {
    Expr::FunctionCall {
        span,
        func: FunctionCall {
            distinct: false,
            name: Identifier::from_name(span, "getvariable"),
            args: vec![Expr::Literal {
                span,
                value: Literal::String(name),
            }],
            params: vec![],
            order_by: vec![],
            window: None,
            lambda: None,
        },
    }
}
