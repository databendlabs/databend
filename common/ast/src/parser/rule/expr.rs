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

use nom::branch::alt;
use nom::combinator::map;
use nom::combinator::value;
use nom::combinator::verify;
use nom::error::make_error;
use nom::error::ContextError;
use nom::error::ErrorKind;
use nom::IResult;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

use crate::parser::ast::BinaryOperator;
use crate::parser::ast::Expr;
use crate::parser::ast::Identifier;
use crate::parser::ast::Literal;
use crate::parser::ast::Query;
use crate::parser::ast::TypeName;
use crate::parser::ast::UnaryOperator;
use crate::parser::rule::util::ident;
use crate::parser::rule::util::literal_u64;
use crate::parser::rule::util::Input;
use crate::parser::rule::util::ParseError;
use crate::parser::token::*;
use crate::rule;

const BETWEEN_PREC: Precedence = Precedence(20);

pub fn query<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Query, Error>
where Error: ParseError<Input<'a>> {
    // TODO: unimplemented
    nom::combinator::fail(i)
}

pub fn expr<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Expr, Error>
where Error: ParseError<Input<'a>> {
    subexpr(Precedence(0))(i)
}

pub fn subexpr<'a, Error>(
    min_precedence: Precedence,
) -> impl FnMut(Input<'a>) -> IResult<Input<'a>, Expr, Error> + Copy
where Error: ParseError<Input<'a>> {
    move |i| {
        let expr_element_limited =
            verify(
                expr_element,
                |elem| match PrattParser::<std::iter::Once<_>>::query(&mut ExprParser, elem)
                    .unwrap()
                {
                    Affix::Infix(prec, _) | Affix::Prefix(prec) | Affix::Postfix(prec)
                        if prec <= min_precedence =>
                    {
                        false
                    }
                    _ => true,
                },
            );

        let (i, expr_elements) = rule! { #expr_element_limited+ }(i)?;

        let mut iter = expr_elements.into_iter();
        let expr = ExprParser
            .parse(&mut iter)
            .map_err(|err| match err {
                PrattError::EmptyInput => {
                    ContextError::add_context(i, "empty expresssion", make_error(i, ErrorKind::Eof))
                }
                // TODO (andylokandy): report the proper span
                PrattError::UnexpectedNilfix(_) => ContextError::add_context(
                    i,
                    "unable to parse the value",
                    make_error(i, ErrorKind::Complete),
                ),
                PrattError::UnexpectedPrefix(_) => ContextError::add_context(
                    i,
                    "unable to parse the prefix operator",
                    make_error(i, ErrorKind::Complete),
                ),
                PrattError::UnexpectedInfix(_) => ContextError::add_context(
                    i,
                    "unable to parse the binary operator",
                    make_error(i, ErrorKind::Complete),
                ),
                PrattError::UnexpectedPostfix(_) => ContextError::add_context(
                    i,
                    "unable to parse the postfix operator",
                    make_error(i, ErrorKind::Complete),
                ),
                PrattError::UserError(_) => unreachable!(),
            })
            .map_err(nom::Err::Error)?;

        if iter.next().is_some() {
            return Err(nom::Err::Error(ContextError::add_context(
                i,
                "unable to parse the rest of expression",
                make_error(i, ErrorKind::Complete),
            )));
        }

        Ok((i, expr))
    }
}

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
    InSubquery { subquery: Query, not: bool },
    /// `BETWEEN ... AND ...`
    Between { low: Expr, high: Expr, not: bool },
    /// Binary operation
    BinaryOp { op: BinaryOperator },
    /// Unary operation
    UnaryOp { op: UnaryOperator },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast { expr: Expr, target_type: TypeName },
    /// A literal value, such as string, number, date or NULL
    Literal(Literal),
    /// `Count(*)` expression
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
        operand: Option<Expr>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Expr>,
    },
    /// `EXISTS` expression
    Exists(Query),
    /// Scalar subquery, which will only return a single row with a single column.
    Subquery(Query),
    /// An expression between parentheses
    Group(Expr),
}

struct ExprParser;

impl<I: Iterator<Item = ExprElement>> PrattParser<I> for ExprParser {
    type Error = pratt::NoError;
    type Input = ExprElement;
    type Output = Expr;

    fn query(&mut self, elem: &ExprElement) -> pratt::Result<Affix> {
        let affix = match elem {
            ExprElement::IsNull { .. } => Affix::Postfix(Precedence(17)),
            ExprElement::Between { .. } => Affix::Postfix(BETWEEN_PREC),
            ExprElement::InList { .. } => Affix::Postfix(Precedence(20)),
            ExprElement::InSubquery { .. } => Affix::Postfix(Precedence(20)),
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

                BinaryOperator::BitwiseOr => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseAnd => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseXor => Affix::Infix(Precedence(22), Associativity::Left),

                BinaryOperator::Plus => Affix::Infix(Precedence(30), Associativity::Left),
                BinaryOperator::Minus => Affix::Infix(Precedence(30), Associativity::Left),

                BinaryOperator::Multiply => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Div => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Divide => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Modulo => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::StringConcat => Affix::Infix(Precedence(40), Associativity::Left),
            },
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, elem: ExprElement) -> pratt::Result<Expr> {
        let expr = match elem {
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
                expr: Box::new(expr),
                target_type,
            },
            ExprElement::Literal(lit) => Expr::Literal(lit),
            ExprElement::CountAll => Expr::CountAll,
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
                operand: operand.map(Box::new),
                conditions,
                results,
                else_result: else_result.map(Box::new),
            },
            ExprElement::Exists(subquery) => Expr::Exists(Box::new(subquery)),
            ExprElement::Subquery(subquery) => Expr::Subquery(Box::new(subquery)),
            ExprElement::Group(expr) => expr,
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn infix(&mut self, lhs: Expr, elem: ExprElement, rhs: Expr) -> pratt::Result<Expr> {
        let expr = match elem {
            ExprElement::BinaryOp { op } => Expr::BinaryOp {
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn prefix(&mut self, elem: ExprElement, rhs: Expr) -> pratt::Result<Expr> {
        let expr = match elem {
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                op,
                expr: Box::new(rhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn postfix(&mut self, lhs: Expr, elem: ExprElement) -> pratt::Result<Expr> {
        let expr = match elem {
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
                subquery: Box::new(subquery),
                not,
            },
            ExprElement::Between { low, high, not } => Expr::Between {
                expr: Box::new(lhs),
                low: Box::new(low),
                high: Box::new(high),
                not,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }
}

pub fn expr_element<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, ExprElement, Error>
where Error: ParseError<Input<'a>> {
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
        |(_, not, _)| ExprElement::IsNull { not: not.is_some() },
    );
    let in_list = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #expr ~ ("," ~ #expr)*  ~ ")"
        },
        |(not, _, _, head, tail, _)| {
            let mut list = vec![head];
            list.extend(tail.into_iter().map(|(_, expr)| expr));
            ExprElement::InList {
                list,
                not: not.is_some(),
            }
        },
    );
    let in_subquery = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #query  ~ ")"
        },
        |(not, _, _, subquery, _)| ExprElement::InSubquery {
            subquery,
            not: not.is_some(),
        },
    );
    let between_subexpr = subexpr(BETWEEN_PREC);
    let between = map(
        rule! {
            NOT? ~ BETWEEN ~ #between_subexpr ~ AND ~  #between_subexpr
        },
        |(not, _, low, _, high)| ExprElement::Between {
            low,
            high,
            not: not.is_some(),
        },
    );
    let cast = map(
        rule! {
            CAST ~ "(" ~ #expr ~ AS ~ #type_name ~ ")"
        },
        |(_, _, expr, _, target_type, _)| ExprElement::Cast { expr, target_type },
    );
    let count_all = value(ExprElement::CountAll, rule! {
        COUNT ~ "(" ~ "*" ~ ")"
    });
    let function_call = map(
        rule! {
            #function_name ~ "(" ~ (DISTINCT? ~ #expr ~ ("," ~ #expr)*)? ~ ")"
        },
        |(name, _, args, _)| {
            let (distinct, args) = args
                .map(|(distinct, head, tail)| {
                    let mut args = vec![head];
                    args.extend(tail.into_iter().map(|(_, arg)| arg));
                    (distinct.is_some(), args)
                })
                .unwrap_or_default();

            ExprElement::FunctionCall {
                distinct,
                name,
                args,
                params: vec![],
            }
        },
    );
    let function_call_with_param = map(
        rule! {
            #function_name ~ "(" ~ (#literal ~ ("," ~ #literal)*)? ~ ")" ~ "(" ~ (DISTINCT? ~ #expr ~ ("," ~ #expr)*)? ~ ")"
        },
        |(name, _, params, _, _, args, _)| {
            let params = params
                .map(|(head, tail)| {
                    let mut params = vec![head];
                    params.extend(tail.into_iter().map(|(_, param)| param));
                    params
                })
                .unwrap_or_default();

            let (distinct, args) = args
                .map(|(distinct, head, tail)| {
                    let mut args = vec![head];
                    args.extend(tail.into_iter().map(|(_, arg)| arg));
                    (distinct.is_some(), args)
                })
                .unwrap_or_default();

            ExprElement::FunctionCall {
                distinct,
                name,
                args,
                params,
            }
        },
    );
    let case = map(
        rule! {
            CASE ~ #expr? ~ (WHEN ~ #expr ~ THEN ~ #expr)+ ~ (ELSE ~ #expr)? ~ END
        },
        |(_, operand, branches, else_result, _)| {
            let (conditions, results) = branches
                .into_iter()
                .map(|(_, cond, _, result)| (cond, result))
                .unzip();
            let else_result = else_result.map(|(_, result)| result);
            ExprElement::Case {
                operand,
                conditions,
                results,
                else_result,
            }
        },
    );
    let exists = map(
        rule! { EXISTS ~ "(" ~ #query ~ ")" },
        |(_, _, subquery, _)| ExprElement::Exists(subquery),
    );
    let subquery = map(rule! { "(" ~ #query ~ ")" }, |(_, subquery, _)| {
        ExprElement::Subquery(subquery)
    });
    let group = map(rule! { "(" ~ #expr ~ ")" }, |(_, expr, _)| {
        ExprElement::Group(expr)
    });
    let binary_op = map(binary_op, |op| ExprElement::BinaryOp { op });
    let unary_op = map(unary_op, |op| ExprElement::UnaryOp { op });
    let literal = map(literal, ExprElement::Literal);

    rule! (
        #column_ref
        | #is_null
        | #in_list
        | #in_subquery
        | #between
        | #binary_op
        | #unary_op
        | #cast
        | #count_all
        | #literal
        | #function_call_with_param
        | #function_call
        | #case
        | #exists
        | #subquery
        | #group
    )(i)
}

pub fn unary_op<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, UnaryOperator, Error>
where Error: ParseError<Input<'a>> {
    alt((
        value(UnaryOperator::Plus, rule! { Plus }),
        value(UnaryOperator::Minus, rule! { Minus }),
        value(UnaryOperator::Not, rule! { NOT }),
    ))(i)
}

pub fn binary_op<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, BinaryOperator, Error>
where Error: ParseError<Input<'a>> {
    alt((
        value(BinaryOperator::Plus, rule! { Plus }),
        value(BinaryOperator::Minus, rule! { Minus }),
        value(BinaryOperator::Multiply, rule! { Multiply }),
        value(BinaryOperator::Divide, rule! { Divide }),
        value(BinaryOperator::Div, rule! { DIV }),
        value(BinaryOperator::StringConcat, rule! { StringConcat }),
        value(BinaryOperator::Gt, rule! { Gt }),
        value(BinaryOperator::Lt, rule! { Lt }),
        value(BinaryOperator::Gte, rule! { Gte }),
        value(BinaryOperator::Lte, rule! { Lte }),
        value(BinaryOperator::Eq, rule! { Eq }),
        value(BinaryOperator::NotEq, rule! { NotEq }),
        value(BinaryOperator::And, rule! { AND }),
        value(BinaryOperator::Or, rule! { OR }),
        value(BinaryOperator::NotLike, rule! { NOT ~ LIKE }),
        value(BinaryOperator::Like, rule! { LIKE }),
        value(BinaryOperator::BitwiseOr, rule! { "|" }),
        value(BinaryOperator::BitwiseAnd, rule! { "&" }),
        value(BinaryOperator::BitwiseXor, rule! { "^" }),
    ))(i)
}

pub fn literal<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Literal, Error>
where Error: ParseError<Input<'a>> {
    let string = map(
        rule! {
            LiteralString
        },
        |quoted| Literal::String(quoted.text[1..quoted.text.len() - 1].to_string()),
    );
    // TODO (andylokandy): handle hex numbers in parser
    let number = map(
        rule! {
            LiteralHex | LiteralNumber
        },
        |number| Literal::Number(number.text.to_string()),
    );
    let boolean = alt((
        value(Literal::Boolean(true), rule! { TRUE }),
        value(Literal::Boolean(false), rule! { FALSE }),
    ));
    let null = value(Literal::Null, rule! { NULL });

    rule!(
        #string
        | #number
        | #boolean
        | #null
    )(i)
}

pub fn type_name<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, TypeName, Error>
where Error: ParseError<Input<'a>> {
    let ty_char = map(
        rule! { CHAR ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::Char(opt_args.map(|(_, length, _)| length)),
    );
    let ty_varchar = map(
        rule! { VARCHAR ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::Varchar(opt_args.map(|(_, length, _)| length)),
    );
    let ty_float = map(
        rule! { FLOAT ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::Float(opt_args.map(|(_, prec, _)| prec)),
    );
    let ty_int = map(
        rule! { INTEGER ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::Int(opt_args.map(|(_, display, _)| display)),
    );
    let ty_tiny_int = map(
        rule! { TINYINT ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::TinyInt(opt_args.map(|(_, display, _)| display)),
    );
    let ty_small_int = map(
        rule! { SMALLINT ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::SmallInt(opt_args.map(|(_, display, _)| display)),
    );
    let ty_big_int = map(
        rule! { BIGINT ~ ("(" ~ #literal_u64 ~ ")")? },
        |(_, opt_args)| TypeName::BigInt(opt_args.map(|(_, display, _)| display)),
    );
    let ty_real = value(TypeName::Real, rule! { REAL });
    let ty_double = value(TypeName::Double, rule! { DOUBLE });
    let ty_boolean = value(TypeName::Boolean, rule! { BOOLEAN });
    let ty_date = value(TypeName::Date, rule! { DATE });
    let ty_time = value(TypeName::Time, rule! { TIME });
    let ty_timestamp = value(TypeName::Timestamp, rule! { TIMESTAMP });
    let ty_text = value(TypeName::Text, rule! { TEXT });

    rule!(
        #ty_char
        | #ty_varchar
        | #ty_float
        | #ty_int
        | #ty_tiny_int
        | #ty_small_int
        | #ty_big_int
        | #ty_real
        | #ty_double
        | #ty_boolean
        | #ty_date
        | #ty_time
        | #ty_timestamp
        | #ty_text
    )(i)
}

// TODO(andylokandy): complete the keyword-function list, or remove the functions' name from keywords
pub fn function_name<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, String, Error>
where Error: ParseError<Input<'a>> {
    map(
        rule! {
            Ident
            | COUNT
            | SUM
            | AVG
            | MIN
            | MAX
            | STDDEV_POP
            | SQRT
        },
        |name| name.text.to_string(),
    )(i)
}
