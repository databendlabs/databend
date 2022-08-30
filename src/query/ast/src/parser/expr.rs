// Copyright 2022 Datafuse Labs.
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

use common_datavalues::IntervalKind;
use itertools::Itertools;
use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::*;
use crate::input::Input;
use crate::input::WithSpan;
use crate::match_token;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::unescape::unescape;
use crate::rule;
use crate::util::*;
use crate::Error;
use crate::ErrorKind;

pub const BETWEEN_PREC: u32 = 20;
pub const NOT_PREC: u32 = 15;

pub fn expr(i: Input) -> IResult<Expr> {
    context("expression", subexpr(0))(i)
}

pub fn subexpr(min_precedence: u32) -> impl FnMut(Input) -> IResult<Expr> {
    move |i| {
        let higher_prec_expr_element =
            |i| {
                expr_element(i).and_then(|(rest, elem)| {
                    match PrattParser::<std::iter::Once<_>>::query(&mut ExprParser, &elem).unwrap()
                    {
                        Affix::Infix(prec, _) | Affix::Prefix(prec) | Affix::Postfix(prec)
                            if prec <= Precedence(min_precedence) =>
                        {
                            Err(nom::Err::Error(Error::from_error_kind(
                                i,
                                ErrorKind::Other("expected more tokens for expression"),
                            )))
                        }
                        _ => Ok((rest, elem)),
                    }
                })
            };

        let (rest, mut expr_elements) = rule! { #higher_prec_expr_element+ }(i)?;

        for (prev, curr) in (-1..(expr_elements.len() as isize)).tuple_windows() {
            // Replace binary Plus and Minus to the unary one, if it's following another op
            // or it's the first element.
            if prev == -1
                || matches!(
                    expr_elements[prev as usize].elem,
                    ExprElement::UnaryOp { .. } | ExprElement::BinaryOp { .. }
                )
            {
                match &mut expr_elements[curr as usize].elem {
                    elem @ ExprElement::BinaryOp {
                        op: BinaryOperator::Plus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Plus,
                        };
                    }
                    elem @ ExprElement::BinaryOp {
                        op: BinaryOperator::Minus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Minus,
                        };
                    }
                    _ => {}
                }
            }

            // Replace bracket map access to an array, if it's following a prefix or infix
            // element or it's the first element.
            if prev == -1
                || matches!(
                    PrattParser::<std::iter::Once<_>>::query(
                        &mut ExprParser,
                        &expr_elements[prev as usize]
                    )
                    .unwrap(),
                    Affix::Prefix(_) | Affix::Infix(_, _)
                )
            {
                let key = match &expr_elements[curr as usize].elem {
                    ExprElement::MapAccess {
                        accessor: MapAccessor::Bracket { key },
                    } => Some(key),
                    _ => None,
                };
                if let Some(key) = key {
                    let span = expr_elements[curr as usize].span;
                    expr_elements[curr as usize] = WithSpan {
                        span,
                        elem: ExprElement::Array {
                            exprs: vec![Expr::Literal {
                                span: span.0,
                                lit: key.clone(),
                            }],
                        },
                    };
                }
            }
        }
        let iter = &mut expr_elements.into_iter();
        run_pratt_parser(ExprParser, iter, rest, i)
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
pub enum ExprElement<'a> {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        database: Option<Identifier<'a>>,
        table: Option<Identifier<'a>>,
        column: Identifier<'a>,
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
        list: Vec<Expr<'a>>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        subquery: Box<Query<'a>>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        low: Box<Expr<'a>>,
        high: Box<Expr<'a>>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        op: BinaryOperator,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
    },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        expr: Box<Expr<'a>>,
        target_type: TypeName,
    },
    /// `TRY_CAST` expression`
    TryCast {
        expr: Box<Expr<'a>>,
        target_type: TypeName,
    },
    /// `::<type_name>` expression
    PgCast {
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        field: IntervalKind,
        expr: Box<Expr<'a>>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        substr_expr: Box<Expr<'a>>,
        str_expr: Box<Expr<'a>>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    SubString {
        expr: Box<Expr<'a>>,
        substring_from: Option<Box<Expr<'a>>>,
        substring_for: Option<Box<Expr<'a>>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        expr: Box<Expr<'a>>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr<'a>>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal {
        lit: Literal,
    },
    /// `Count(*)` expression
    CountAll,
    /// `(foo, bar)`
    Tuple {
        exprs: Vec<Expr<'a>>,
    },
    /// Scalar function call
    FunctionCall {
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: Identifier<'a>,
        args: Vec<Expr<'a>>,
        params: Vec<Literal>,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        operand: Option<Box<Expr<'a>>>,
        conditions: Vec<Expr<'a>>,
        results: Vec<Expr<'a>>,
        else_result: Option<Box<Expr<'a>>>,
    },
    /// `EXISTS` expression
    Exists {
        subquery: Query<'a>,
        not: bool,
    },
    /// Scalar/ANY/ALL/SOME subquery
    Subquery {
        modifier: Option<SubqueryModifier>,
        subquery: Query<'a>,
    },
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        accessor: MapAccessor<'a>,
    },
    /// An expression between parentheses
    Group(Expr<'a>),
    /// `[1, 2, 3]`
    Array {
        exprs: Vec<Expr<'a>>,
    },
    Interval {
        expr: Expr<'a>,
        unit: IntervalKind,
    },
    DateAdd {
        unit: IntervalKind,
        interval: Expr<'a>,
        date: Expr<'a>,
    },
    DateSub {
        unit: IntervalKind,
        interval: Expr<'a>,
        date: Expr<'a>,
    },
    DateTrunc {
        unit: IntervalKind,
        date: Expr<'a>,
    },
}

struct ExprParser;

impl<'a, I: Iterator<Item = WithSpan<'a, ExprElement<'a>>>> PrattParser<I> for ExprParser {
    type Error = &'static str;
    type Input = WithSpan<'a, ExprElement<'a>>;
    type Output = Expr<'a>;

    fn query(&mut self, elem: &WithSpan<ExprElement>) -> Result<Affix, &'static str> {
        let affix = match &elem.elem {
            ExprElement::MapAccess { .. } => Affix::Postfix(Precedence(25)),
            ExprElement::IsNull { .. } => Affix::Postfix(Precedence(17)),
            ExprElement::Between { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::IsDistinctFrom { .. } => {
                Affix::Infix(Precedence(BETWEEN_PREC), Associativity::Left)
            }
            ExprElement::InList { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::InSubquery { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::UnaryOp { op } => match op {
                UnaryOperator::Not => Affix::Prefix(Precedence(NOT_PREC)),

                UnaryOperator::Plus => Affix::Prefix(Precedence(30)),
                UnaryOperator::Minus => Affix::Prefix(Precedence(30)),
            },
            ExprElement::BinaryOp { op } => match op {
                BinaryOperator::Or => Affix::Infix(Precedence(5), Associativity::Left),

                BinaryOperator::And => Affix::Infix(Precedence(10), Associativity::Left),

                BinaryOperator::Eq => Affix::Infix(Precedence(20), Associativity::Right),
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

                BinaryOperator::BitwiseOr => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseAnd => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseXor => Affix::Infix(Precedence(22), Associativity::Left),

                BinaryOperator::Xor => Affix::Infix(Precedence(24), Associativity::Left),

                BinaryOperator::Plus => Affix::Infix(Precedence(30), Associativity::Left),
                BinaryOperator::Minus => Affix::Infix(Precedence(30), Associativity::Left),

                BinaryOperator::Multiply => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Div => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Divide => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Modulo => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::StringConcat => Affix::Infix(Precedence(40), Associativity::Left),
            },
            ExprElement::PgCast { .. } => Affix::Postfix(Precedence(50)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, elem: WithSpan<'a, ExprElement<'a>>) -> Result<Expr<'a>, &'static str> {
        let expr = match elem.elem {
            ExprElement::ColumnRef {
                database,
                table,
                column,
            } => Expr::ColumnRef {
                span: elem.span.0,
                database,
                table,
                column,
            },
            ExprElement::Cast { expr, target_type } => Expr::Cast {
                span: elem.span.0,
                expr,
                target_type,
                pg_style: false,
            },
            ExprElement::TryCast { expr, target_type } => Expr::TryCast {
                span: elem.span.0,
                expr,
                target_type,
            },
            ExprElement::Extract { field, expr } => Expr::Extract {
                span: elem.span.0,
                kind: field,
                expr,
            },
            ExprElement::Position {
                substr_expr,
                str_expr,
            } => Expr::Position {
                span: elem.span.0,
                substr_expr,
                str_expr,
            },
            ExprElement::SubString {
                expr,
                substring_from,
                substring_for,
            } => Expr::Substring {
                span: elem.span.0,
                expr,
                substring_from,
                substring_for,
            },
            ExprElement::Trim { expr, trim_where } => Expr::Trim {
                span: elem.span.0,
                expr,
                trim_where,
            },
            ExprElement::Literal { lit } => Expr::Literal {
                span: elem.span.0,
                lit,
            },
            ExprElement::CountAll => Expr::CountAll { span: elem.span.0 },
            ExprElement::Tuple { exprs } => Expr::Tuple {
                span: elem.span.0,
                exprs,
            },
            ExprElement::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => Expr::FunctionCall {
                span: elem.span.0,
                distinct,
                name,
                args,
                params,
            },
            ExprElement::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Expr::Case {
                span: elem.span.0,
                operand,
                conditions,
                results,
                else_result,
            },
            ExprElement::Exists { subquery, not } => Expr::Exists {
                span: elem.span.0,
                not,
                subquery: Box::new(subquery),
            },
            ExprElement::Subquery { subquery, modifier } => Expr::Subquery {
                span: elem.span.0,
                modifier,
                subquery: Box::new(subquery),
            },
            ExprElement::Group(expr) => expr,
            ExprElement::Array { exprs } => Expr::Array {
                span: elem.span.0,
                exprs,
            },
            ExprElement::Interval { expr, unit } => Expr::Interval {
                span: elem.span.0,
                expr: Box::new(expr),
                unit,
            },
            ExprElement::DateAdd {
                unit,
                interval,
                date,
            } => Expr::DateAdd {
                span: elem.span.0,
                unit,
                interval: Box::new(interval),
                date: Box::new(date),
            },
            ExprElement::DateSub {
                unit,
                interval,
                date,
            } => Expr::DateSub {
                span: elem.span.0,
                unit,
                interval: Box::new(interval),
                date: Box::new(date),
            },
            ExprElement::DateTrunc { unit, date } => Expr::DateTrunc {
                span: elem.span.0,
                unit,
                date: Box::new(date),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn infix(
        &mut self,
        lhs: Expr<'a>,
        elem: WithSpan<'a, ExprElement>,
        rhs: Expr<'a>,
    ) -> Result<Expr<'a>, &'static str> {
        let expr = match elem.elem {
            ExprElement::BinaryOp { op } => Expr::BinaryOp {
                span: elem.span.0,
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            ExprElement::IsDistinctFrom { not } => Expr::IsDistinctFrom {
                span: elem.span.0,
                left: Box::new(lhs),
                right: Box::new(rhs),
                not,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn prefix(
        &mut self,
        elem: WithSpan<'a, ExprElement>,
        rhs: Expr<'a>,
    ) -> Result<Expr<'a>, &'static str> {
        let expr = match elem.elem {
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                span: elem.span.0,
                op,
                expr: Box::new(rhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn postfix(
        &mut self,
        lhs: Expr<'a>,
        elem: WithSpan<'a, ExprElement<'a>>,
    ) -> Result<Expr<'a>, &'static str> {
        let expr = match elem.elem {
            ExprElement::MapAccess { accessor } => Expr::MapAccess {
                span: elem.span.0,
                expr: Box::new(lhs),
                accessor,
            },
            ExprElement::IsNull { not } => Expr::IsNull {
                span: elem.span.0,
                expr: Box::new(lhs),
                not,
            },
            ExprElement::InList { list, not } => Expr::InList {
                span: elem.span.0,
                expr: Box::new(lhs),
                list,
                not,
            },
            ExprElement::InSubquery { subquery, not } => Expr::InSubquery {
                span: elem.span.0,
                expr: Box::new(lhs),
                subquery,
                not,
            },
            ExprElement::Between { low, high, not } => Expr::Between {
                span: elem.span.0,
                expr: Box::new(lhs),
                low,
                high,
                not,
            },
            ExprElement::PgCast { target_type } => Expr::Cast {
                span: elem.span.0,
                expr: Box::new(lhs),
                target_type,
                pg_style: true,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }
}

pub fn expr_element(i: Input) -> IResult<WithSpan<ExprElement>> {
    let column_ref = map(
        peroid_separated_idents_1_to_3,
        |(database, table, column)| ExprElement::ColumnRef {
            database,
            table,
            column,
        },
    );
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
    let extract = map(
        rule! {
            EXTRACT ~ ^"(" ~ ^#interval_kind ~ ^FROM ~ ^#subexpr(0) ~ ^")"
        },
        |(_, _, field, _, expr, _)| ExprElement::Extract {
            field,
            expr: Box::new(expr),
        },
    );
    let position = map(
        rule! {
            POSITION
            ~ ^"("
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
            SUBSTRING
            ~ ^"("
            ~ ^#subexpr(0)
            ~ ( ( FROM | "," ) ~ ^#subexpr(0) )?
            ~ ( ( FOR | "," ) ~ ^#subexpr(0) )?
            ~ ^")"
        },
        |(_, _, expr, opt_substring_from, opt_substring_for, _)| ExprElement::SubString {
            expr: Box::new(expr),
            substring_from: opt_substring_from.map(|(_, expr)| Box::new(expr)),
            substring_for: opt_substring_for.map(|(_, expr)| Box::new(expr)),
        },
    );
    let trim_where = alt((
        value(TrimWhere::Both, rule! { BOTH }),
        value(TrimWhere::Leading, rule! { LEADING }),
        value(TrimWhere::Trailing, rule! { TRAILING }),
    ));
    let trim = map(
        rule! {
            TRIM
            ~ ^"("
            ~ #subexpr(0)
            ~ ^")"
        },
        |(_, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: None,
        },
    );
    let trim_from = map(
        rule! {
            TRIM
            ~ ^"("
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
    let count_all = value(ExprElement::CountAll, rule! {
        COUNT ~ "(" ~ "*" ~ ^")"
    });
    let tuple = map(
        rule! {
            "(" ~ #comma_separated_list0_ignore_trailling(subexpr(0)) ~ ","? ~ ^")"
        },
        |(_, mut exprs, opt_trail, _)| {
            if exprs.len() == 1 && opt_trail.is_none() {
                ExprElement::Group(exprs.remove(0))
            } else {
                ExprElement::Tuple { exprs }
            }
        },
    );
    let function_call = map(
        rule! {
            #function_name
            ~ "("
            ~ DISTINCT?
            ~ #comma_separated_list0(subexpr(0))?
            ~ ")"
        },
        |(name, _, opt_distinct, opt_args, _)| ExprElement::FunctionCall {
            distinct: opt_distinct.is_some(),
            name,
            args: opt_args.unwrap_or_default(),
            params: vec![],
        },
    );
    let function_call_with_param = map(
        rule! {
            #function_name
            ~ "(" ~ #comma_separated_list1(literal) ~ ")"
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
        },
        |(name, _, params, _, _, opt_distinct, opt_args, _)| ExprElement::FunctionCall {
            distinct: opt_distinct.is_some(),
            name,
            args: opt_args.unwrap_or_default(),
            params,
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
        rule! { NOT? ~ EXISTS ~ ^"(" ~ ^#query ~ ^")" },
        |(opt_not, _, _, subquery, _)| ExprElement::Exists {
            subquery,
            not: opt_not.is_some(),
        },
    );
    let subquery = map(
        rule! {
            (ANY | SOME | ALL)? ~
            "("
            ~ #query
            ~ ^")"
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
    let binary_op = map(binary_op, |op| ExprElement::BinaryOp { op });
    let unary_op = map(unary_op, |op| ExprElement::UnaryOp { op });
    let literal = map(literal, |lit| ExprElement::Literal { lit });
    let map_access = map(map_access, |accessor| ExprElement::MapAccess { accessor });
    let array = map(
        // Array that contains a single literal item will be parsed as a bracket map access,
        // and then will be converted back to an array if the map access is not following
        // a primary element or postfix element.
        rule! {
            "[" ~ #comma_separated_list0_ignore_trailling(subexpr(0))? ~ ","? ~ ^"]"
        },
        |(_, opt_args, _, _)| {
            let exprs = opt_args.unwrap_or_default();
            ExprElement::Array { exprs }
        },
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
    let is_distinct_from = map(
        rule! {
            IS ~ NOT? ~ DISTINCT ~ FROM
        },
        |(_, not, _, _)| ExprElement::IsDistinctFrom { not: not.is_some() },
    );
    let (rest, (span, elem)) = consumed(alt((
        // Note: each `alt` call supports maximum of 21 parsers
        rule!(
            #is_null : "`... IS [NOT] NULL`"
            | #in_list : "`[NOT] IN (<expr>, ...)`"
            | #in_subquery : "`[NOT] IN (SELECT ...)`"
            | #exists : "`[NOT] EXISTS (SELECT ...)`"
            | #between : "`[NOT] BETWEEN ... AND ...`"
            | #binary_op : "<operator>"
            | #unary_op : "<operator>"
            | #cast : "`CAST(... AS ...)`"
            | #date_add: "`DATE_ADD(..., ..., (YEAR| MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW))`"
            | #date_sub: "`DATE_SUB(..., ..., (YEAR| MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW))`"
            | #date_trunc: "`DATE_TRUNC((YEAR | MONTH | DAY | HOUR | MINUTE | SECOND), ...)`"
            | #interval: "`INTERVAL ... (YEAR| MONTH | DAY | HOUR | MINUTE | SECOND | DOY | DOW)`"
            | #pg_cast : "`::<type_name>`"
            | #extract : "`EXTRACT((YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) FROM ...)`"
            | #position : "`POSITION(... IN ...)`"
            | #substring : "`SUBSTRING(... [FROM ...] [FOR ...])`"
            | #trim : "`TRIM(...)`"
            | #trim_from : "`TRIM([(BOTH | LEADEING | TRAILING) ... FROM ...)`"
        ),
        rule!(
            #is_distinct_from: "`... IS [NOT] DISTINCT FROM ...`"
            | #count_all : "COUNT(*)"
            | #function_call_with_param : "<function>"
            | #function_call : "<function>"
            | #literal : "<literal>"
            | #case : "`CASE ... END`"
            | #subquery : "`(SELECT ...)`"
            | #tuple : "`(<expr> [, ...])`"
            | #column_ref : "<column>"
            | #map_access : "[<key>] | .<key> | :<key>"
            | #array : "`[...]`"
        ),
    )))(i)?;

    Ok((rest, WithSpan { span, elem }))
}

pub fn unary_op(i: Input) -> IResult<UnaryOperator> {
    // Plus and Minus are parsed as binary op at first.
    value(UnaryOperator::Not, rule! { NOT })(i)
}

pub fn binary_op(i: Input) -> IResult<BinaryOperator> {
    alt((
        alt((
            value(BinaryOperator::Plus, rule! { "+" }),
            value(BinaryOperator::Minus, rule! { "-" }),
            value(BinaryOperator::Multiply, rule! { "*" }),
            value(BinaryOperator::Divide, rule! { "/" }),
            value(BinaryOperator::Div, rule! { DIV }),
            value(BinaryOperator::Modulo, rule! { "%" }),
            value(BinaryOperator::StringConcat, rule! { "||" }),
            value(BinaryOperator::Gt, rule! { ">" }),
            value(BinaryOperator::Lt, rule! { "<" }),
            value(BinaryOperator::Gte, rule! { ">=" }),
            value(BinaryOperator::Lte, rule! { "<=" }),
            value(BinaryOperator::Eq, rule! { "=" }),
            value(BinaryOperator::NotEq, rule! { "<>" | "!=" }),
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
            value(BinaryOperator::BitwiseOr, rule! { "|" }),
            value(BinaryOperator::BitwiseAnd, rule! { "&" }),
            value(BinaryOperator::BitwiseXor, rule! { "^" }),
        )),
    ))(i)
}

pub fn literal(i: Input) -> IResult<Literal> {
    let string = map(literal_string, Literal::String);
    let integer = map(literal_u64, Literal::Integer);
    let float = map(literal_f64, Literal::Float);
    let bigint = map(rule!(LiteralInteger), |lit| Literal::BigInt {
        lit: lit.text().to_string(),
        is_hex: false,
    });
    let bigint_hex = map(literal_hex_str, |lit| Literal::BigInt {
        lit: lit.to_string(),
        is_hex: true,
    });

    let boolean = alt((
        value(Literal::Boolean(true), rule! { TRUE }),
        value(Literal::Boolean(false), rule! { FALSE }),
    ));
    let current_timestamp = value(Literal::CurrentTimestamp, rule! { CURRENT_TIMESTAMP });
    let null = value(Literal::Null, rule! { NULL });

    rule!(
        #string
        | #integer
        | #float
        | #bigint
        | #bigint_hex
        | #boolean
        | #current_timestamp
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
        |token| Ok(u64::from_str_radix(token.text(), 10)?),
    );
    let hex = map_res(literal_hex_str, |lit| Ok(u64::from_str_radix(lit, 16)?));

    rule!(
        #decimal
        | #hex
    )(i)
}

pub fn literal_f64(i: Input) -> IResult<f64> {
    map_res(
        rule! {
            LiteralFloat
        },
        |token| Ok(fast_float::parse(token.text())?),
    )(i)
}

pub fn literal_string(i: Input) -> IResult<String> {
    map_res(
        rule! {
            QuotedString
        },
        |token| {
            if token
                .text()
                .chars()
                .next()
                .filter(|c| i.1.is_string_quote(*c))
                .is_some()
            {
                let str = &token.text()[1..token.text().len() - 1];
                let unescaped =
                    unescape(str, '\'').ok_or(ErrorKind::Other("invalid escape or unicode"))?;
                Ok(unescaped)
            } else {
                Err(ErrorKind::ExpectToken(QuotedString))
            }
        },
    )(i)
}

pub fn literal_string_eq_ignore_case(s: &str) -> impl FnMut(Input) -> IResult<()> + '_ {
    move |i| {
        map_res(rule! { QuotedString }, |token| {
            if token.text()[1..token.text().len() - 1].eq_ignore_ascii_case(s) {
                Ok(())
            } else {
                Err(ErrorKind::ExpectToken(QuotedString))
            }
        })(i)
    }
}

pub fn at_string(i: Input) -> IResult<String> {
    match_token(AtString)(i)
        .map(|(i2, token)| (i2, token.text()[1..token.text().len()].to_string()))
}

pub fn type_name(i: Input) -> IResult<TypeName> {
    let ty_boolean = value(TypeName::Boolean, rule! { BOOLEAN | BOOL });
    let ty_uint8 = value(
        TypeName::UInt8,
        rule! { ( UINT8 | #map(rule! { TINYINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )?  },
    );
    let ty_uint16 = value(
        TypeName::UInt16,
        rule! { ( UINT16 | #map(rule! { SMALLINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_uint32 = value(
        TypeName::UInt32,
        rule! { ( UINT32 | #map(rule! { ( INT | INTEGER ) ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_uint64 = value(
        TypeName::UInt64,
        rule! { ( UINT64 | UNSIGNED | #map(rule! { BIGINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int8 = value(
        TypeName::Int8,
        rule! { ( INT8 | TINYINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int16 = value(
        TypeName::Int16,
        rule! { ( INT16 | SMALLINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int32 = value(
        TypeName::Int32,
        rule! { ( INT32 | INT | INTEGER ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int64 = value(
        TypeName::Int64,
        rule! { ( INT64 | SIGNED | BIGINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_float32 = value(TypeName::Float32, rule! { FLOAT32 | FLOAT });
    let ty_float64 = value(TypeName::Float64, rule! { FLOAT64 | DOUBLE });
    let ty_array = map(
        rule! { ARRAY ~ ( "(" ~ #type_name ~ ")" )? },
        |(_, opt_item_type)| TypeName::Array {
            item_type: opt_item_type.map(|(_, opt_item_type, _)| Box::new(opt_item_type)),
        },
    );
    let ty_tuple = map(
        rule! { TUPLE ~ "(" ~ #comma_separated_list1(tuple_types) ~ ")" },
        |(_, _, tuple_types, _)| {
            let mut fields_name = Vec::with_capacity(tuple_types.len());
            let mut fields_type = Vec::with_capacity(tuple_types.len());
            for tuple_type in tuple_types {
                if let Some(field_name) = tuple_type.0 {
                    fields_name.push(field_name.name);
                }
                fields_type.push(tuple_type.1);
            }
            if fields_name.is_empty() {
                TypeName::Tuple {
                    fields_name: None,
                    fields_type,
                }
            } else {
                TypeName::Tuple {
                    fields_name: Some(fields_name),
                    fields_type,
                }
            }
        },
    );
    let ty_date = value(TypeName::Date, rule! { DATE });
    let ty_datetime = map(
        rule! { (DATETIME | TIMESTAMP) ~ ( "(" ~ #literal_u64 ~ ")" )? },
        |(_, opt_precision)| TypeName::Timestamp {
            precision: opt_precision.map(|(_, precision, _)| precision),
        },
    );
    let ty_string = value(
        TypeName::String,
        rule! { ( STRING | VARCHAR | CHAR | CHARACTER | TEXT  ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_object = value(TypeName::Object, rule! { OBJECT | MAP });
    let ty_variant = value(TypeName::Variant, rule! { VARIANT | JSON });
    map(
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
            | #ty_array
            | #ty_tuple
            | #ty_date
            | #ty_datetime
            | #ty_string
            | #ty_object
            | #ty_variant
            ) ~ NULL? : "type name"
        },
        |(ty, null_opt)| {
            if null_opt.is_some() {
                TypeName::Nullable(Box::new(ty))
            } else {
                ty
            }
        },
    )(i)
}

pub fn tuple_types(i: Input) -> IResult<(Option<Identifier>, TypeName)> {
    let tuple_types = map(
        rule! {
           #type_name
        },
        |type_name| (None, type_name),
    );
    let named_tuple_types = map(
        rule! {
           #ident ~ #type_name
        },
        |(name, type_name)| (Some(name), type_name),
    );

    rule!(
        #tuple_types
        | #named_tuple_types
    )(i)
}

pub fn interval_kind(i: Input) -> IResult<IntervalKind> {
    alt((
        value(IntervalKind::Year, rule! { YEAR }),
        value(IntervalKind::Month, rule! { MONTH }),
        value(IntervalKind::Day, rule! { DAY }),
        value(IntervalKind::Hour, rule! { HOUR }),
        value(IntervalKind::Minute, rule! { MINUTE }),
        value(IntervalKind::Second, rule! { SECOND }),
        value(IntervalKind::Doy, rule! { DOY }),
        value(IntervalKind::Dow, rule! { DOW }),
        value(
            IntervalKind::Year,
            rule! { #literal_string_eq_ignore_case("YEAR")  },
        ),
        value(
            IntervalKind::Month,
            rule! { #literal_string_eq_ignore_case("MONTH")  },
        ),
        value(
            IntervalKind::Day,
            rule! { #literal_string_eq_ignore_case("DAY")  },
        ),
        value(
            IntervalKind::Hour,
            rule! { #literal_string_eq_ignore_case("HOUR")  },
        ),
        value(
            IntervalKind::Minute,
            rule! { #literal_string_eq_ignore_case("MINUTE")  },
        ),
        value(
            IntervalKind::Second,
            rule! { #literal_string_eq_ignore_case("SECOND")  },
        ),
        value(
            IntervalKind::Doy,
            rule! { #literal_string_eq_ignore_case("DOY")  },
        ),
        value(
            IntervalKind::Dow,
            rule! { #literal_string_eq_ignore_case("DOW")  },
        ),
    ))(i)
}

pub fn map_access(i: Input) -> IResult<MapAccessor> {
    let bracket = map(
        rule! {
           "[" ~ #literal ~ "]"
        },
        |(_, key, _)| MapAccessor::Bracket { key },
    );
    let bracket_ident = map(
        rule! {
           "[" ~ #ident ~ "]"
        },
        |(_, key, _)| MapAccessor::Bracket {
            key: Literal::String(key.name),
        },
    );
    let period = map(
        rule! {
           "." ~ #ident
        },
        |(_, key)| MapAccessor::Period { key },
    );
    let colon = map(
        rule! {
         ":" ~ #ident
        },
        |(_, key)| MapAccessor::Colon { key },
    );

    rule!(
        #bracket
        | #bracket_ident
        | #period
        | #colon
    )(i)
}
