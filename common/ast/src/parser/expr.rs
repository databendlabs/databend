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

use itertools::Itertools;
use nom::branch::alt;
use nom::combinator::cut;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::*;
use crate::parser::error::Error;
use crate::parser::error::ErrorKind;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

const BETWEEN_PREC: u32 = 20;

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
                                rest,
                                ErrorKind::Other("expected more tokens for expression"),
                            )))
                        }
                        _ => Ok((rest, elem)),
                    }
                })
            };

        let (rest, mut expr_elements) = rule! { #higher_prec_expr_element+ }(i)?;

        // Replace binary Plus and Minus to the unary one, if it's following another op or it's the first element.
        for (prev, curr) in (-1..(expr_elements.len() as isize)).tuple_windows() {
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
        }

        let mut iter = expr_elements.iter().cloned();
        let expr = ExprParser
            .parse(&mut iter)
            .map_err(|err| {
                map_pratt_error(
                    iter.next()
                        .map(|elem| elem.span)
                        // It's safe to slice one more token because EOI is always added.
                        .unwrap_or(&rest[..1]),
                    err,
                )
            })
            .map_err(nom::Err::Error)?;

        if let Some(elem) = iter.next() {
            dbg!(&elem);
            dbg!(&expr_elements);
            return Err(nom::Err::Error(Error::from_error_kind(
                elem.span,
                ErrorKind::Other("unable to parse rest of the expression"),
            )));
        }

        Ok((rest, expr))
    }
}

fn map_pratt_error<'a>(
    next_token: Input<'a>,
    err: PrattError<WithSpan<'a>, pratt::NoError>,
) -> Error<'a> {
    match err {
        PrattError::EmptyInput => Error::from_error_kind(
            next_token,
            ErrorKind::Other("expected more tokens for expression"),
        ),
        PrattError::UnexpectedNilfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the expression value"),
        ),
        PrattError::UnexpectedPrefix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the prefix operator"),
        ),
        PrattError::UnexpectedInfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the binary operator"),
        ),
        PrattError::UnexpectedPostfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the postfix operator"),
        ),
        PrattError::UserError(_) => unreachable!(),
    }
}

#[derive(Debug, Clone)]
pub struct WithSpan<'a> {
    elem: ExprElement,
    span: Input<'a>,
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
#[allow(dead_code)]
pub enum ExprElement {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        database: Option<Identifier>,
        table: Option<Identifier>,
        column: Identifier,
    },
    /// `IS NULL` expression
    IsNull { not: bool },
    /// `IS NOT NULL` expression
    /// `[ NOT ] IN (list, ...)`
    InList { list: Vec<Expr>, not: bool },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery { subquery: Box<Query>, not: bool },
    /// `BETWEEN ... AND ...`
    Between {
        low: Box<Expr>,
        high: Box<Expr>,
        not: bool,
    },
    /// Binary operation
    BinaryOp { op: BinaryOperator },
    /// Unary operation
    UnaryOp { op: UnaryOperator },
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
    PgCast { target_type: TypeName },
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
    SubString {
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
    /// `Count(*)` expression
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
    Exists(Query),
    /// Scalar subquery, which will only return a single row with a single column.
    Subquery(Query),
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess { accessor: MapAccessor },
    /// An expression between parentheses
    Group(Expr),
}

struct ExprParser;

impl<'a, I: Iterator<Item = WithSpan<'a>>> PrattParser<I> for ExprParser {
    type Error = pratt::NoError;
    type Input = WithSpan<'a>;
    type Output = Expr;

    fn query(&mut self, elem: &WithSpan) -> pratt::Result<Affix> {
        let affix = match &elem.elem {
            ExprElement::MapAccess { .. } => Affix::Postfix(Precedence(10)),
            ExprElement::IsNull { .. } => Affix::Postfix(Precedence(17)),
            ExprElement::Between { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::InList { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::InSubquery { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::UnaryOp { op } => match op {
                UnaryOperator::Not => Affix::Prefix(Precedence(15)),

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

    fn primary(&mut self, elem: WithSpan) -> pratt::Result<Expr> {
        let expr = match elem.elem {
            ExprElement::ColumnRef {
                database,
                table,
                column,
            } => Expr::ColumnRef {
                database,
                table,
                column,
            },
            ExprElement::Cast { expr, target_type } => Expr::Cast {
                expr,
                target_type,
                pg_style: false,
            },
            ExprElement::TryCast { expr, target_type } => Expr::TryCast { expr, target_type },
            ExprElement::Extract { field, expr } => Expr::Extract { field, expr },
            ExprElement::Position {
                substr_expr,
                str_expr,
            } => Expr::Position {
                substr_expr,
                str_expr,
            },
            ExprElement::SubString {
                expr,
                substring_from,
                substring_for,
            } => Expr::Substring {
                expr,
                substring_from,
                substring_for,
            },
            ExprElement::Trim { expr, trim_where } => Expr::Trim { expr, trim_where },
            ExprElement::Literal(lit) => Expr::Literal(lit),
            ExprElement::CountAll => Expr::CountAll,
            ExprElement::Tuple { exprs } => Expr::Tuple { exprs },
            ExprElement::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => Expr::FunctionCall {
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
                operand,
                conditions,
                results,
                else_result,
            },
            ExprElement::Exists(subquery) => Expr::Exists(Box::new(subquery)),
            ExprElement::Subquery(subquery) => Expr::Subquery(Box::new(subquery)),
            ExprElement::Group(expr) => expr,
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn infix(&mut self, lhs: Expr, elem: WithSpan, rhs: Expr) -> pratt::Result<Expr> {
        let expr = match elem.elem {
            ExprElement::BinaryOp { op } => Expr::BinaryOp {
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn prefix(&mut self, elem: WithSpan, rhs: Expr) -> pratt::Result<Expr> {
        let expr = match elem.elem {
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                op,
                expr: Box::new(rhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn postfix(&mut self, lhs: Expr, elem: WithSpan) -> pratt::Result<Expr> {
        let expr = match elem.elem {
            ExprElement::MapAccess { accessor } => Expr::MapAccess {
                expr: Box::new(lhs),
                accessor,
            },
            ExprElement::IsNull { not } => Expr::IsNull {
                expr: Box::new(lhs),
                not,
            },
            ExprElement::InList { list, not } => Expr::InList {
                expr: Box::new(lhs),
                list,
                not,
            },
            ExprElement::InSubquery { subquery, not } => Expr::InSubquery {
                expr: Box::new(lhs),
                subquery,
                not,
            },
            ExprElement::Between { low, high, not } => Expr::Between {
                expr: Box::new(lhs),
                low,
                high,
                not,
            },
            ExprElement::PgCast { target_type } => Expr::Cast {
                expr: Box::new(lhs),
                target_type,
                pg_style: true,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }
}

pub fn expr_element(i: Input) -> IResult<WithSpan> {
    let column_ref = map(
        rule! {
            #ident ~ ("." ~ #ident ~ ("." ~ #ident)?)?
        },
        |res| match res {
            (column, None) => ExprElement::ColumnRef {
                database: None,
                table: None,
                column,
            },
            (table, Some((_, column, None))) => ExprElement::ColumnRef {
                database: None,
                table: Some(table),
                column,
            },
            (database, Some((_, table, Some((_, column))))) => ExprElement::ColumnRef {
                database: Some(database),
                table: Some(table),
                column,
            },
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
            NOT? ~ IN ~ "(" ~ #comma_separated_list1(subexpr(0)) ~ ")"
        },
        |(opt_not, _, _, list, _)| ExprElement::InList {
            list,
            not: opt_not.is_some(),
        },
    );
    let in_subquery = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #query  ~ ")"
        },
        |(opt_not, _, _, subquery, _)| ExprElement::InSubquery {
            subquery: Box::new(subquery),
            not: opt_not.is_some(),
        },
    );
    let between = map(
        rule! {
            NOT? ~ BETWEEN ~ #cut(subexpr(BETWEEN_PREC)) ~ AND ~ #cut(subexpr(BETWEEN_PREC))
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
            ~ #subexpr(0)
            ~ ( AS | "," )
            ~ #type_name
            ~ ")"
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
            "::" ~ #type_name
        },
        |(_, target_type)| ExprElement::PgCast { target_type },
    );
    let extract = map(
        rule! {
            EXTRACT ~ "(" ~ #date_time_field ~ FROM ~ #subexpr(0) ~ ")"
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
            ~ #subexpr(BETWEEN_PREC)
            ~ IN
            ~ #subexpr(0)
            ~ ")"
        },
        |(_, _, substr_expr, _, str_expr, _)| ExprElement::Position {
            substr_expr: Box::new(substr_expr),
            str_expr: Box::new(str_expr),
        },
    );
    let substring = map(
        rule! {
            SUBSTRING
            ~ "("
            ~ #subexpr(0)
            ~ ( ( FROM | "," ) ~ #cut(subexpr(0)) )?
            ~ ( ( FOR | "," ) ~ #cut(subexpr(0)) )?
            ~ ")"
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
            ~ "("
            ~ #subexpr(0)
            ~ ")"
        },
        |(_, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: None,
        },
    );
    let trim_from = map(
        rule! {
            TRIM
            ~ "("
            ~ #trim_where
            ~ #subexpr(0)
            ~ FROM
            ~ #subexpr(0)
            ~ ")"
        },
        |(_, _, trim_where, trim_str, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: Some((trim_where, Box::new(trim_str))),
        },
    );
    let count_all = value(ExprElement::CountAll, rule! {
        COUNT ~ "(" ~ "*" ~ ")"
    });
    let tuple = map(
        rule! {
            "(" ~ #subexpr(0) ~ "," ~ #comma_separated_list1_allow_trailling(subexpr(0))? ~ ","? ~ ")"
        },
        |(_, head, _, opt_tail, _, _)| {
            let mut exprs = opt_tail.unwrap_or_default();
            exprs.insert(0, head);
            ExprElement::Tuple { exprs }
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
            ~ ( WHEN ~ #subexpr(0) ~ THEN ~ #subexpr(0) )+
            ~ ( ELSE ~ #cut(subexpr(0)) )? ~ END
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
        rule! { EXISTS ~ "(" ~ #query ~ ")" },
        |(_, _, subquery, _)| ExprElement::Exists(subquery),
    );
    let subquery = map(
        rule! {
            "("
            ~ #query
            ~ ")"
        },
        |(_, subquery, _)| ExprElement::Subquery(subquery),
    );
    let group = map(
        rule! {
           "("
           ~ #subexpr(0)
           ~ ")"
        },
        |(_, expr, _)| ExprElement::Group(expr),
    );
    let binary_op = map(binary_op, |op| ExprElement::BinaryOp { op });
    let unary_op = map(unary_op, |op| ExprElement::UnaryOp { op });
    let literal = map(literal, ExprElement::Literal);
    let map_access = map(map_access, |accessor| ExprElement::MapAccess { accessor });

    let (rest, elem) = alt((
        rule! (
            #is_null : "`... IS [NOT] NULL`"
            | #in_list : "`[NOT] IN (<expr>, ...)`"
            | #in_subquery : "`[NOT] IN (SELECT ...)`"
            | #between : "`[NOT] BETWEEN ... AND ...`"
            | #binary_op : "<operator>"
            | #unary_op : "<operator>"
            | #cast : "`CAST(... AS ...)`"
            | #pg_cast : "`::<type_name>`"
            | #extract : "`EXTRACT((YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) FROM ...)`"
            | #position : "`POSITION(... IN ...)`"
            | #substring : "`SUBSTRING(... [FROM ...] [FOR ...])`"
            | #trim : "`TRIM(...)`"
            | #trim_from : "`TRIM([(BOTH | LEADEING | TRAILING) ... FROM ...)`"
        ),
        rule!(
            #count_all : "COUNT(*)"
            | #tuple : "`(<expr>, ...)`"
            | #literal : "<literal>"
            | #function_call_with_param : "<function>"
            | #function_call : "<function>"
            | #case : "`CASE ... END`"
            | #exists : "`EXISTS (SELECT ...)`"
            | #subquery : "`(SELECT ...)`"
            | #map_access : "[<key>] | .<key> | :<key>"
            | #group
            | #column_ref : "<column>"
        ),
    ))(i)?;

    let input_ptr = i.as_ptr();
    let rest_ptr = rest.as_ptr();
    let offset = (rest_ptr as usize - input_ptr as usize) / std::mem::size_of::<Token>();
    let span = &i[..offset];

    Ok((rest, WithSpan { elem, span }))
}

pub fn unary_op(i: Input) -> IResult<UnaryOperator> {
    // Plus and Minus are parsed as binary op at first.
    value(UnaryOperator::Not, rule! { NOT })(i)
}

pub fn binary_op(i: Input) -> IResult<BinaryOperator> {
    alt((
        alt((
            value(BinaryOperator::Plus, rule! { Plus }),
            value(BinaryOperator::Minus, rule! { Minus }),
            value(BinaryOperator::Multiply, rule! { Multiply }),
            value(BinaryOperator::Divide, rule! { Divide }),
            value(BinaryOperator::Div, rule! { DIV }),
            value(BinaryOperator::Modulo, rule! { Modulo }),
            value(BinaryOperator::StringConcat, rule! { StringConcat }),
            value(BinaryOperator::Gt, rule! { Gt }),
            value(BinaryOperator::Lt, rule! { Lt }),
            value(BinaryOperator::Gte, rule! { Gte }),
            value(BinaryOperator::Lte, rule! { Lte }),
            value(BinaryOperator::Eq, rule! { Eq }),
            value(BinaryOperator::NotEq, rule! { NotEq }),
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
    let string = map(
        rule! {
            LiteralString
        },
        |quoted| Literal::String(quoted.text()[1..quoted.text().len() - 1].to_string()),
    );
    // TODO(andylokandy): handle hex numbers in parser
    let number = map(
        rule! {
            LiteralHex | LiteralNumber
        },
        |number| Literal::Number(number.text().to_string()),
    );
    let boolean = alt((
        value(Literal::Boolean(true), rule! { TRUE }),
        value(Literal::Boolean(false), rule! { FALSE }),
    ));
    let interval = map(
        rule! {
            INTERVAL ~ #literal_string ~ #date_time_field
        },
        |(_, value, field)| Literal::Interval(Interval { value, field }),
    );
    let null = value(Literal::Null, rule! { NULL });

    rule!(
        #string
        | #number
        | #boolean
        | #interval : "`INTERVAL '...' (YEAR | MONTH | DAY | ...)`"
        | #null
    )(i)
}

pub fn type_name(i: Input) -> IResult<TypeName> {
    let ty_boolean = value(TypeName::Boolean, rule! { BOOLEAN });
    let ty_uint8 = value(
        TypeName::UInt8,
        rule! { UINT8 | #map(rule! { TINYINT ~ UNSIGNED }, |(t, _)| t) },
    );
    let ty_uint16 = value(
        TypeName::UInt16,
        rule! { UINT16 | #map(rule! { SMALLINT ~ UNSIGNED }, |(t, _)| t) },
    );
    let ty_uint32 = value(
        TypeName::UInt32,
        rule! { UINT32 | #map(rule! { ( INT | INTEGER ) ~ UNSIGNED }, |(t, _)| t) },
    );
    let ty_uint64 = value(
        TypeName::UInt64,
        rule! { UINT64 | UNSIGNED | #map(rule! { BIGINT ~ UNSIGNED }, |(t, _)| t) },
    );
    let ty_int8 = value(TypeName::Int8, rule! { INT8 | TINYINT });
    let ty_int16 = value(TypeName::Int16, rule! { INT16 | SMALLINT });
    let ty_int32 = value(TypeName::Int32, rule! { INT32 | ( INT | INTEGER ) });
    let ty_int64 = value(TypeName::Int64, rule! { INT64 | SIGNED | BIGINT });
    let ty_float32 = value(TypeName::Float32, rule! { FLOAT32 | FLOAT });
    let ty_float64 = value(TypeName::Float64, rule! { FLOAT64 | DOUBLE });
    let ty_array = map(
        rule! { ARRAY ~ ( "(" ~ #type_name ~ ")" )? },
        |(_, opt_item_type)| TypeName::Array {
            item_type: opt_item_type.map(|(_, opt_item_type, _)| Box::new(opt_item_type)),
        },
    );
    let ty_date = value(TypeName::Date, rule! { DATE });
    let ty_datetime = map(
        rule! { DATETIME ~ ( "(" ~ #literal_u64 ~ ")" )? },
        |(_, opt_precision)| TypeName::DateTime {
            precision: opt_precision.map(|(_, precision, _)| precision),
        },
    );
    let ty_timestamp = value(TypeName::Timestamp, rule! { TIMESTAMP });
    let ty_string = value(TypeName::String, rule! { STRING | VARCHAR });
    let ty_object = value(TypeName::Object, rule! { OBJECT });
    let ty_variant = value(TypeName::Variant, rule! { VARIANT });

    rule!(
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
        | #ty_date
        | #ty_datetime
        | #ty_timestamp
        | #ty_string
        | #ty_object
        | #ty_variant
        ) : "type name"
    )(i)
}

pub fn date_time_field(i: Input) -> IResult<DateTimeField> {
    let year = value(DateTimeField::Year, rule! { YEAR });
    let month = value(DateTimeField::Month, rule! { MONTH });
    let week = value(DateTimeField::Week, rule! { WEEK });
    let day = value(DateTimeField::Day, rule! { DAY });
    let hour = value(DateTimeField::Hour, rule! { HOUR });
    let minute = value(DateTimeField::Minute, rule! { MINUTE });
    let second = value(DateTimeField::Second, rule! { SECOND });
    let century = value(DateTimeField::Century, rule! { CENTURY });
    let decade = value(DateTimeField::Decade, rule! { DECADE });
    let doy = value(DateTimeField::Doy, rule! { DOY });
    let dow = value(DateTimeField::Dow, rule! { DOW });
    let epoch = value(DateTimeField::Epoch, rule! { EPOCH });
    let isodow = value(DateTimeField::Isodow, rule! { ISODOW });
    let isoyear = value(DateTimeField::Isoyear, rule! { ISOYEAR });
    let julian = value(DateTimeField::Julian, rule! { JULIAN });
    let microseconds = value(DateTimeField::Microseconds, rule! { MICROSECONDS });
    let millenium = value(DateTimeField::Millenium, rule! { MILLENIUM });
    let milliseconds = value(DateTimeField::Milliseconds, rule! { MILLISECONDS });
    let quarter = value(DateTimeField::Quarter, rule! { QUARTER });
    let timezone = value(DateTimeField::Timezone, rule! { TIMEZONE });
    let timezone_hour = value(DateTimeField::TimezoneHour, rule! { TIMEZONE_HOUR });
    let timezone_minute = value(DateTimeField::TimezoneMinute, rule! { TIMEZONE_MINUTE });

    alt((
        rule!(
            #year
            | #month
            | #week
            | #day
            | #hour
            | #minute
            | #second
            | #century
            | #decade
            | #doy
            | #dow
        ),
        rule!(
            #epoch
            | #isodow
            | #isoyear
            | #julian
            | #microseconds
            | #millenium
            | #milliseconds
            | #quarter
            | #timezone
            | #timezone_hour
            | #timezone_minute
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
        | #period
        | #colon
    )(i)
}
