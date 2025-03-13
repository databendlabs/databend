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
use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use nom_rule::rule;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::quote::AtString;
use crate::ast::*;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::input::WithSpan;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::Error;
use crate::parser::ErrorKind;
use crate::Span;

pub fn expr(i: Input) -> IResult<Expr> {
    context("expression", subexpr(0))(i)
}

pub fn values(i: Input) -> IResult<Vec<Expr>> {
    let values = comma_separated_list0(expr);
    map(rule! { ( "(" ~ #values ~ ")" ) }, |(_, v, _)| v)(i)
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

        let (rest, mut expr_elements) = rule! { #higher_prec_expr_element+ }(i)?;

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

        run_pratt_parser(ExprParser, &expr_elements.into_iter(), rest, i)
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
    DateSub {
        unit: IntervalKind,
        interval: Expr,
        date: Expr,
    },
    DateTrunc {
        unit: IntervalKind,
        date: Expr,
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
        BinaryOperator::Like => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::Regexp => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotRegexp => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::RLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::NotRLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::SoundsLike => Affix::Infix(Precedence(20), Associativity::Left),
        BinaryOperator::BitwiseOr => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::BitwiseAnd => Affix::Infix(Precedence(22), Associativity::Left),
        BinaryOperator::BitwiseXor => Affix::Infix(Precedence(22), Associativity::Left),
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
            ExprElement::DateSub { .. } => Affix::Nilfix,
            ExprElement::DateTrunc { .. } => Affix::Nilfix,
            ExprElement::LastDay { .. } => Affix::Nilfix,
            ExprElement::PreviousDay { .. } => Affix::Nilfix,
            ExprElement::NextDay { .. } => Affix::Nilfix,
            ExprElement::Hole { .. } => Affix::Nilfix,
            ExprElement::Placeholder { .. } => Affix::Nilfix,
            ExprElement::VariableAccess { .. } => Affix::Nilfix,
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
            Expr::DateSub { .. } => Affix::Nilfix,
            Expr::DateTrunc { .. } => Affix::Nilfix,
            Expr::LastDay { .. } => Affix::Nilfix,
            Expr::PreviousDay { .. } => Affix::Nilfix,
            Expr::NextDay { .. } => Affix::Nilfix,
            Expr::Hole { .. } => Affix::Nilfix,
            Expr::Placeholder { .. } => Affix::Nilfix,
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
            ExprElement::CountAll { window } => Expr::CountAll {
                span: transform_span(elem.span.tokens),
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
        let expr = match elem.elem {
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                span: transform_span(elem.span.tokens),
                op,
                expr: Box::new(rhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
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
                if let Expr::ColumnRef { column, .. } = &mut lhs {
                    if let ColumnID::Name(name) = &column.column {
                        column.database = column.table.take();
                        column.table = Some(name.clone());
                        column.column = key.clone();
                        return Ok(lhs);
                    }
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
            DATE_PART ~ "(" ~ ^#interval_kind ~ "," ~ ^#subexpr(0) ~ ^")"
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
            COUNT ~ "(" ~ "*" ~ ")" ~ ( OVER ~ #window_spec_ident )?
        },
        |(_, _, _, _, window)| ExprElement::CountAll {
            window: window.map(|w| w.1),
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
                TokenKind::ALL => SubqueryModifier::All,
                TokenKind::ANY => SubqueryModifier::Any,
                TokenKind::SOME => SubqueryModifier::Some,
                _ => unreachable!(),
            });
            ExprElement::Subquery { modifier, subquery }
        },
    );

    let function_call = map(
        rule! {
            #function_name
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
        },
        |(name, _, opt_distinct, opt_args, _)| ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: opt_distinct.is_some(),
                name,
                args: opt_args.unwrap_or_default(),
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        },
    );
    let function_call_with_lambda = map(
        rule! {
            #function_name
            ~ "(" ~ #subexpr(0) ~ "," ~ #lambda_params ~ "->" ~ #subexpr(0) ~ ")"
        },
        |(name, _, arg, _, params, _, expr, _)| ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: false,
                name,
                args: vec![arg],
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: Some(Lambda {
                    params,
                    expr: Box::new(expr),
                }),
            },
        },
    );
    let function_call_with_window = map(
        rule! {
            #function_name
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
            ~ #window_function
        },
        |(name, _, opt_distinct, opt_args, _, window)| ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: opt_distinct.is_some(),
                name,
                args: opt_args.unwrap_or_default(),
                params: vec![],
                order_by: vec![],
                window: Some(window),
                lambda: None,
            },
        },
    );
    let function_call_with_within_group_window = map(
        rule! {
            #function_name
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
            ~ #within_group
            ~ #window_function?
        },
        |(name, _, opt_distinct, opt_args, _, order_by, window)| ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: opt_distinct.is_some(),
                name,
                args: opt_args.unwrap_or_default(),
                params: vec![],
                order_by,
                window,
                lambda: None,
            },
        },
    );
    let function_call_with_params_window = map(
        rule! {
            #function_name
            ~ "(" ~ #comma_separated_list1(subexpr(0)) ~ ")"
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
            ~ #window_function?
        },
        |(name, _, params, _, _, opt_distinct, opt_args, _, window)| ExprElement::FunctionCall {
            func: FunctionCall {
                distinct: opt_distinct.is_some(),
                name,
                args: opt_args.unwrap_or_default(),
                params,
                order_by: vec![],
                window,
                lambda: None,
            },
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
    let map_access = map(map_access, |accessor| ExprElement::MapAccess { accessor });
    let dot_access = map(
        rule! {
           "." ~ #column_id
        },
        |(_, key)| ExprElement::DotAccess { key },
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
            let exprs = opt_args.unwrap_or_default();
            ExprElement::Array { exprs }
        },
    );

    let map_expr = map(
        rule! { "{" ~ #comma_separated_list0(map_element) ~ "}" },
        |(_, kvs, _)| ExprElement::Map { kvs },
    );

    let date_add = map(
        rule! {
            DATE_ADD ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, interval, _, date, _)| ExprElement::DateAdd {
            unit,
            interval,
            date,
        },
    );

    let date_diff = map(
        rule! {
            DATE_DIFF ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, date_start, _, date_end, _)| ExprElement::DateDiff {
            unit,
            date_start,
            date_end,
        },
    );

    let date_sub = map(
        rule! {
            DATE_SUB ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, interval, _, date, _)| ExprElement::DateSub {
            unit,
            interval,
            date,
        },
    );
    let interval = map(
        rule! {
            INTERVAL ~ #subexpr(0) ~ #interval_kind
        },
        |(_, operand, unit)| ExprElement::Interval {
            expr: operand,
            unit,
        },
    );
    let date_trunc = map(
        rule! {
            DATE_TRUNC ~ "(" ~ #interval_kind ~ "," ~ #subexpr(0) ~ ")"
        },
        |(_, _, unit, _, date, _)| ExprElement::DateTrunc { unit, date },
    );

    let last_day = map(
        rule! {
            LAST_DAY ~ "(" ~ #subexpr(0) ~ "," ~ #interval_kind ~ ")"
        },
        |(_, _, date, _, unit, _)| ExprElement::LastDay { unit, date },
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

    let interval_expr = map(
        rule! {
            INTERVAL ~ #consumed(literal_string)
        },
        |(_, (span, date))| ExprElement::Cast {
            expr: Box::new(Expr::Literal {
                span: transform_span(span.tokens),
                value: Literal::String(date),
            }),
            target_type: TypeName::Interval,
        },
    );

    let is_distinct_from = map(
        rule! {
            IS ~ NOT? ~ DISTINCT ~ FROM
        },
        |(_, not, _, _)| ExprElement::IsDistinctFrom { not: not.is_some() },
    );

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

    map(
        consumed(alt((
            // Note: each `alt` call supports maximum of 21 parsers
            rule!(
                #is_null : "`... IS [NOT] NULL`"
                | #in_list : "`[NOT] IN (<expr>, ...)`"
                | #in_subquery : "`[NOT] IN (SELECT ...)`"
                | #exists : "`[NOT] EXISTS (SELECT ...)`"
                | #between : "`[NOT] BETWEEN ... AND ...`"
                | #binary_op : "<operator>"
                | #json_op : "<operator>"
                | #unary_op : "<operator>"
                | #cast : "`CAST(... AS ...)`"
                | #pg_cast : "`::<type_name>`"
                | #position : "`POSITION(... IN ...)`"
                | #variable_access: "`$<ident>`"
            ),
            rule! (
                #date_add : "`DATE_ADD(..., ..., (YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW))`"
                | #date_diff : "`DATE_DIFF(..., ..., (YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW))`"
                | #date_sub : "`DATE_SUB(..., ..., (YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW))`"
                | #date_trunc : "`DATE_TRUNC((YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND), ...)`"
                | #last_day : "`LAST_DAY(..., (YEAR | QUARTER | MONTH | WEEK)))`"
                | #previous_day : "`PREVIOUS_DAY(..., (Sunday | Monday | Tuesday | Wednesday | Thursday | Friday | Saturday))`"
                | #next_day : "`NEXT_DAY(..., (Sunday | Monday | Tuesday | Wednesday | Thursday | Friday | Saturday))`"
                | #date_expr : "`DATE <str_literal>`"
                | #timestamp_expr : "`TIMESTAMP <str_literal>`"
                | #interval : "`INTERVAL ... (YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW)`"
                | #interval_expr : "`INTERVAL <str_literal>`"
                | #extract : "`EXTRACT((YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | WEEK) FROM ...)`"
                | #date_part : "`DATE_PART((YEAR | QUARTER | MONTH | DAY | HOUR | MINUTE | SECOND | WEEK), ...)`"
            ),
            rule!(
                #substring : "`SUBSTRING(... [FROM ...] [FOR ...])`"
                | #trim_from : "`TRIM([(BOTH | LEADEING | TRAILING) ... FROM ...)`"
                | #is_distinct_from: "`... IS [NOT] DISTINCT FROM ...`"
                | #chain_function_call : "x.function(...)"
                | #list_comprehensions: "[expr for x in ... [if ...]]"
                | #count_all_with_window : "`COUNT(*) OVER ...`"
                | #function_call_with_lambda : "`function(..., x -> ...)`"
                | #function_call_with_window : "`function(...) OVER ([ PARTITION BY <expr>, ... ] [ ORDER BY <expr>, ... ] [ <window frame> ])`"
                | #function_call_with_within_group_window: "`function(...) [ WITHIN GROUP ( ORDER BY <expr>, ... ) ] OVER ([ PARTITION BY <expr>, ... ] [ ORDER BY <expr>, ... ] [ <window frame> ])`"
                | #function_call_with_params_window : "`function(...)(...) OVER ([ PARTITION BY <expr>, ... ] [ ORDER BY <expr>, ... ] [ <window frame> ])`"
                | #function_call : "`function(...)`"
            ),
            rule!(
                #case : "`CASE ... END`"
                | #tuple : "`(<expr> [, ...])`"
                | #subquery : "`(SELECT ...)`"
                | #column_ref : "<column>"
                | #dot_access : "<dot_access>"
                | #map_access : "[<key>] | .<key> | :<key>"
                | #literal : "<literal>"
                | #current_timestamp: "CURRENT_TIMESTAMP"
                | #array : "`[<expr>, ...]`"
                | #map_expr : "`{ <literal> : <expr>, ... }`"
            ),
        ))),
        |(span, elem)| WithSpan { span, elem },
    )(i)
}

pub fn unary_op(i: Input) -> IResult<UnaryOperator> {
    // Plus and Minus are parsed as binary op at first.
    alt((
        value(UnaryOperator::Not, rule! { NOT }),
        value(UnaryOperator::Factorial, rule! { Factorial }),
        value(UnaryOperator::SquareRoot, rule! { SquareRoot }),
        value(UnaryOperator::BitwiseNot, rule! { BitWiseNot }),
        value(UnaryOperator::CubeRoot, rule! { CubeRoot }),
        value(UnaryOperator::Abs, rule! { Abs }),
    ))(i)
}

pub fn binary_op(i: Input) -> IResult<BinaryOperator> {
    alt((
        alt((
            value(BinaryOperator::Plus, rule! { "+" }),
            value(BinaryOperator::Minus, rule! { "-" }),
            value(BinaryOperator::Multiply, rule! { "*" }),
            value(BinaryOperator::Divide, rule! { "/" }),
            value(BinaryOperator::IntDiv, rule! { "//" }),
            value(BinaryOperator::Div, rule! { DIV }),
            value(BinaryOperator::Modulo, rule! { "%" }),
            value(BinaryOperator::StringConcat, rule! { "||" }),
            value(BinaryOperator::L2Distance, rule! { "<->" }),
            value(BinaryOperator::Gt, rule! { ">" }),
            value(BinaryOperator::Lt, rule! { "<" }),
            value(BinaryOperator::Gte, rule! { ">=" }),
            value(BinaryOperator::Lte, rule! { "<=" }),
            value(BinaryOperator::Eq, rule! { "=" }),
            value(BinaryOperator::NotEq, rule! { "<>" | "!=" }),
            value(BinaryOperator::Caret, rule! { "^" }),
        )),
        alt((
            value(BinaryOperator::And, rule! { AND }),
            value(BinaryOperator::Or, rule! { OR }),
            value(BinaryOperator::Xor, rule! { XOR }),
            value(BinaryOperator::Like, rule! { LIKE }),
            value(BinaryOperator::NotLike, rule! { NOT ~ LIKE }),
            value(BinaryOperator::Regexp, rule! { REGEXP }),
            value(BinaryOperator::NotRegexp, rule! { NOT ~ REGEXP }),
            value(BinaryOperator::RLike, rule! { RLIKE }),
            value(BinaryOperator::NotRLike, rule! { NOT ~ RLIKE }),
            value(BinaryOperator::SoundsLike, rule! { SOUNDS ~ LIKE }),
            value(BinaryOperator::BitwiseOr, rule! { BitWiseOr }),
            value(BinaryOperator::BitwiseAnd, rule! { BitWiseAnd }),
            value(BinaryOperator::BitwiseXor, rule! { BitWiseXor }),
            value(BinaryOperator::BitwiseShiftLeft, rule! { ShiftLeft }),
            value(BinaryOperator::BitwiseShiftRight, rule! { ShiftRight }),
        )),
    ))(i)
}

pub fn json_op(i: Input) -> IResult<JsonOperator> {
    alt((
        value(JsonOperator::Arrow, rule! { "->" }),
        value(JsonOperator::LongArrow, rule! { "->>" }),
        value(JsonOperator::HashArrow, rule! { "#>" }),
        value(JsonOperator::HashLongArrow, rule! { "#>>" }),
        value(JsonOperator::Question, rule! { "?" }),
        value(JsonOperator::QuestionOr, rule! { "?|" }),
        value(JsonOperator::QuestionAnd, rule! { "?&" }),
        value(JsonOperator::AtArrow, rule! { "@>" }),
        value(JsonOperator::ArrowAt, rule! { "<@" }),
        value(JsonOperator::AtQuestion, rule! { "@?" }),
        value(JsonOperator::AtAt, rule! { "@@" }),
        value(JsonOperator::HashMinus, rule! { "#-" }),
    ))(i)
}

pub fn literal(i: Input) -> IResult<Literal> {
    let string = map(literal_string, Literal::String);
    let code_string = map(code_string, Literal::String);
    let boolean = map(literal_bool, Literal::Boolean);
    let null = value(Literal::Null, rule! { NULL });

    rule!(
        #string
        | #code_string
        | #boolean
        | #literal_number
        | #null
    )(i)
}

pub fn literal_hex_str(i: Input) -> IResult<&str> {
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
    )(i)
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
    )(i)
}

pub fn literal_number(i: Input) -> IResult<Literal> {
    let decimal_uint = map_res(
        rule! {
            LiteralInteger
        },
        |token| parse_uint(token.text(), 10).map_err(nom::Err::Failure),
    );

    let hex_uint = map_res(literal_hex_str, |str| {
        parse_uint(str, 16).map_err(nom::Err::Failure)
    });

    let decimal_float = map_res(
        rule! {
           LiteralFloat
        },
        |token| parse_float(token.text()).map_err(nom::Err::Failure),
    );

    rule!(
        #decimal_uint
        | #decimal_float
        | #hex_uint
    )(i)
}

pub fn literal_bool(i: Input) -> IResult<bool> {
    alt((value(true, rule! { TRUE }), value(false, rule! { FALSE })))(i)
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
    ))(i)
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
    let ty_float32 = value(TypeName::Float32, rule! { FLOAT32 | FLOAT });
    let ty_float64 = value(
        TypeName::Float64,
        rule! { (FLOAT64 | DOUBLE)  ~ PRECISION? },
    );
    let ty_decimal = map_res(
        rule! { DECIMAL ~ "(" ~ #literal_u64 ~ ( "," ~ ^#literal_u64 )? ~ ")" },
        |(_, _, precision, opt_scale, _)| {
            Ok(TypeName::Decimal {
                precision: precision
                    .try_into()
                    .map_err(|_| nom::Err::Failure(ErrorKind::Other("precision is too large")))?,
                scale: if let Some((_, scale)) = opt_scale {
                    scale
                        .try_into()
                        .map_err(|_| nom::Err::Failure(ErrorKind::Other("scale is too large")))?
                } else {
                    0
                },
            })
        },
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
        rule! { ( BINARY | VARBINARY | LONGBLOB | MEDIUMBLOB |  TINYBLOB| BLOB ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_string = value(
        TypeName::String,
        rule! { ( STRING | VARCHAR | CHAR | CHARACTER | TEXT ) ~ ( "(" ~ ^#literal_u64 ~ ^")" )? },
    );
    let ty_variant = value(TypeName::Variant, rule! { VARIANT | JSON });
    let ty_geometry = value(TypeName::Geometry, rule! { GEOMETRY });
    let ty_geography = value(TypeName::Geography, rule! { GEOGRAPHY });
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
            | #ty_datetime
            | #ty_interval
            | #ty_binary
            | #ty_string
            | #ty_variant
            | #ty_geometry
            | #ty_geography
            | #ty_nullable
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
    ))(i)
}

pub fn interval_kind(i: Input) -> IResult<IntervalKind> {
    let year = value(IntervalKind::Year, rule! { YEAR });
    let quarter = value(IntervalKind::Quarter, rule! { QUARTER });
    let month = value(IntervalKind::Month, rule! { MONTH });
    let day = value(IntervalKind::Day, rule! { DAY });
    let hour = value(IntervalKind::Hour, rule! { HOUR });
    let minute = value(IntervalKind::Minute, rule! { MINUTE });
    let second = value(IntervalKind::Second, rule! { SECOND });
    let doy = value(IntervalKind::Doy, rule! { DOY });
    let dow = value(IntervalKind::Dow, rule! { DOW });
    let week = value(IntervalKind::Week, rule! { WEEK });
    let epoch = value(IntervalKind::Epoch, rule! { EPOCH });
    let microsecond = value(IntervalKind::MicroSecond, rule! { MICROSECOND });
    let year_str = value(
        IntervalKind::Year,
        rule! { #literal_string_eq_ignore_case("YEAR")  },
    );
    let quarter_str = value(
        IntervalKind::Quarter,
        rule! { #literal_string_eq_ignore_case("QUARTER") },
    );
    let month_str = value(
        IntervalKind::Month,
        rule! { #literal_string_eq_ignore_case("MONTH")  },
    );
    let day_str = value(
        IntervalKind::Day,
        rule! { #literal_string_eq_ignore_case("DAY")  },
    );
    let hour_str = value(
        IntervalKind::Hour,
        rule! { #literal_string_eq_ignore_case("HOUR")  },
    );
    let minute_str = value(
        IntervalKind::Minute,
        rule! { #literal_string_eq_ignore_case("MINUTE")  },
    );
    let second_str = value(
        IntervalKind::Second,
        rule! { #literal_string_eq_ignore_case("SECOND")  },
    );
    let doy_str = value(
        IntervalKind::Doy,
        rule! { #literal_string_eq_ignore_case("DOY")  },
    );
    let dow_str = value(
        IntervalKind::Dow,
        rule! { #literal_string_eq_ignore_case("DOW")  },
    );
    let week_str = value(
        IntervalKind::Week,
        rule! { #literal_string_eq_ignore_case("WEEK")  },
    );
    let epoch_str = value(
        IntervalKind::Epoch,
        rule! { #literal_string_eq_ignore_case("EPOCH")  },
    );
    let microsecond_str = value(
        IntervalKind::MicroSecond,
        rule! { #literal_string_eq_ignore_case("MICROSECOND")  },
    );
    alt((
        rule!(
            #year
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
        ),
        rule!(
            #year_str
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
        ),
    ))(i)
}

pub fn map_access(i: Input) -> IResult<MapAccessor> {
    let bracket = map(
        rule! {
           "[" ~ #subexpr(0) ~ "]"
        },
        |(_, key, _)| MapAccessor::Bracket { key: Box::new(key) },
    );
    let dot_number = map_res(
        rule! {
           LiteralFloat
        },
        |key| {
            if key.text().starts_with('.') {
                if let Ok(key) = (key.text()[1..]).parse::<u64>() {
                    return Ok(MapAccessor::DotNumber { key });
                }
            }
            Err(nom::Err::Error(ErrorKind::ExpectText(".")))
        },
    );
    let colon = map(
        rule! {
         ":" ~ #ident
        },
        |(_, key)| MapAccessor::Colon { key },
    );

    rule!(
        #bracket
        | #dot_number
        | #colon
    )(i)
}

pub fn map_element(i: Input) -> IResult<(Literal, Expr)> {
    map(
        rule! {
            #literal ~ ":" ~ #subexpr(0)
        },
        |(key, _, value)| (key, value),
    )(i)
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

    let p = i_part.len() as i32 + exp - f_part.len() as i32;
    if !(-76..=76).contains(&p) {
        Ok(Literal::Float64(fast_float2::parse(text)?))
    } else {
        let mut digits = String::with_capacity(76);
        digits.push_str(i_part);
        digits.push_str(f_part);
        if digits.is_empty() {
            digits.push('0')
        }
        let mut scale = f_part.len() as i32 - exp;
        if scale < 0 {
            // e.g 123.1e3
            for _ in 0..(-scale) {
                digits.push('0')
            }
            scale = 0;
        };

        // truncate
        if digits.len() > 76 {
            scale -= digits.len() as i32 - 76;
            digits.truncate(76);
        }

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
