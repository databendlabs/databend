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

use std::vec;

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::IntervalKind as ASTIntervalKind;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_ast::ast::SubqueryModifier;
use common_ast::ast::TrimWhere;
use common_ast::ast::UnaryOperator;
use common_ast::parser::token::Token;
use common_ast::DisplayError;
use common_datavalues::type_coercion::merge_types;
use common_datavalues::ArrayType;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::IntervalKind;
use common_datavalues::IntervalType;
use common_datavalues::NullType;
use common_datavalues::NullableType;
use common_datavalues::StringType;
use common_datavalues::StructType;
use common_datavalues::TimestampType;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::is_builtin_function;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionContext;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::TupleFunction;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use crate::binder::NameResolutionResult;
use crate::evaluator::Evaluator;
use crate::planner::metadata::optimize_remove_count_args;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::OrExpr;
use crate::plans::Scalar;
use crate::plans::SubqueryType;
use crate::validate_function_arg;
use crate::BindContext;
use crate::ScalarExpr;

// todo refine this
// `SyncTypeChecker` is the synchronous `TypeChecker`
pub struct SyncTypeChecker<'a> {
    bind_context: &'a BindContext,
    name_resolution_ctx: &'a NameResolutionContext,

    aliases: &'a [(String, Scalar)],

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,
}

impl<'a> SyncTypeChecker<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        name_resolution_ctx: &'a NameResolutionContext,
        aliases: &'a [(String, Scalar)],
    ) -> Self {
        Self {
            bind_context,
            name_resolution_ctx,
            aliases,
            in_aggregate_function: false,
        }
    }

    fn post_resolve(
        &mut self,
        scalar: &Scalar,
        data_type: &DataTypeImpl,
    ) -> Result<(Scalar, DataTypeImpl)> {
        // Try constant folding
        if let Ok((value, value_type)) = Evaluator::eval_scalar(scalar).and_then(|evaluator| {
            let func_ctx = FunctionContext::default();
            if scalar.is_deterministic() {
                evaluator.try_eval_const(&func_ctx)
            } else {
                Err(ErrorCode::Internal(
                    "Constant folding requires the function deterministic",
                ))
            }
        }) {
            Ok((
                ConstantExpr {
                    value,
                    data_type: Box::new(value_type),
                }
                .into(),
                data_type.clone(),
            ))
        } else {
            Ok((scalar.clone(), data_type.clone()))
        }
    }

    pub fn resolve(
        &mut self,
        expr: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let box (scalar, data_type): Box<(Scalar, DataTypeImpl)> = match expr {
            Expr::ColumnRef {
                database,
                table,
                column: ident,
                ..
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let table = table
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
                let column = normalize_identifier(ident, self.name_resolution_ctx).name;
                let result = self.bind_context.resolve_name(
                    database.as_deref(),
                    table.as_deref(),
                    column.as_str(),
                    &ident.span,
                    self.aliases,
                )?;
                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        let data_type = *column.data_type.clone();
                        (BoundColumnRef { column }.into(), data_type)
                    }
                    NameResolutionResult::Alias { scalar, .. } => {
                        (scalar.clone(), scalar.data_type())
                    }
                };

                Box::new((scalar, data_type))
            }

            Expr::IsNull {
                span, expr, not, ..
            } => {
                let args = &[expr.as_ref()];
                if *not {
                    self.resolve_function(span, "is_not_null", args, required_type)?
                } else {
                    self.resolve_function(
                        span,
                        "not",
                        &[&Expr::FunctionCall {
                            span,
                            distinct: false,
                            name: Identifier {
                                name: "is_not_null".to_string(),
                                quote: None,
                                span: span[0].clone(),
                            },
                            args: vec![(args[0]).clone()],
                            params: vec![],
                        }],
                        None,
                    )?
                }
            }

            Expr::IsDistinctFrom {
                span,
                left,
                right,
                not,
            } => {
                let left_null_expr = Box::new(Expr::IsNull {
                    span,
                    expr: left.clone(),
                    not: false,
                });
                let right_null_expr = Box::new(Expr::IsNull {
                    span,
                    expr: right.clone(),
                    not: false,
                });
                let op = if *not {
                    BinaryOperator::Eq
                } else {
                    BinaryOperator::NotEq
                };
                let box (scalar, data_type) = self.resolve_function(
                    span,
                    "multi_if",
                    &[
                        &Expr::BinaryOp {
                            span,
                            op: BinaryOperator::And,
                            left: left_null_expr.clone(),
                            right: right_null_expr.clone(),
                        },
                        &Expr::Literal {
                            span,
                            lit: Literal::Boolean(*not),
                        },
                        &Expr::BinaryOp {
                            span,
                            op: BinaryOperator::Or,
                            left: left_null_expr.clone(),
                            right: right_null_expr.clone(),
                        },
                        &Expr::Literal {
                            span,
                            lit: Literal::Boolean(!*not),
                        },
                        &Expr::BinaryOp {
                            span,
                            op,
                            left: left.clone(),
                            right: right.clone(),
                        },
                    ],
                    None,
                )?;
                self.resolve_scalar_function_call(
                    span,
                    "assume_not_null",
                    vec![scalar],
                    vec![data_type],
                    required_type,
                )?
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                if list.len() > 3
                    && list
                        .iter()
                        .all(|e| matches!(e, Expr::Literal { lit, .. } if lit != &Literal::Null))
                {
                    let tuple_expr = Expr::Tuple {
                        span,
                        exprs: list.clone(),
                    };
                    let args = vec![expr.as_ref(), &tuple_expr];
                    if *not {
                        self.resolve_function(
                            span,
                            "not",
                            &[&Expr::FunctionCall {
                                span,
                                distinct: false,
                                name: Identifier {
                                    name: "in".to_string(),
                                    quote: None,
                                    span: span[0].clone(),
                                },
                                args: args.iter().copied().cloned().collect(),
                                params: vec![],
                            }],
                            None,
                        )?
                    } else {
                        self.resolve_function(span, "in", &args, required_type)?
                    }
                } else {
                    let mut result = list
                        .iter()
                        .map(|e| Expr::BinaryOp {
                            span,
                            op: BinaryOperator::Eq,
                            left: expr.clone(),
                            right: Box::new(e.clone()),
                        })
                        .fold(None, |mut acc, e| {
                            match acc.as_mut() {
                                None => acc = Some(e),
                                Some(acc) => {
                                    *acc = Expr::BinaryOp {
                                        span,
                                        op: BinaryOperator::Or,
                                        left: Box::new(acc.clone()),
                                        right: Box::new(e.clone()),
                                    }
                                }
                            }
                            acc
                        })
                        .unwrap();

                    if *not {
                        result = Expr::UnaryOp {
                            span,
                            op: UnaryOperator::Not,
                            expr: Box::new(result),
                        };
                    }
                    self.resolve(&result, required_type)?
                }
            }

            Expr::Between {
                span,
                expr,
                low,
                high,
                not,
                ..
            } => {
                if !*not {
                    // Rewrite `expr BETWEEN low AND high`
                    // into `expr >= low AND expr <= high`
                    let box (ge_func, left_type) =
                        self.resolve_function(span, ">=", &[expr.as_ref(), low.as_ref()], None)?;
                    let box (le_func, right_type) =
                        self.resolve_function(span, "<=", &[expr.as_ref(), high.as_ref()], None)?;
                    let func =
                        FunctionFactory::instance().get("and", &[&left_type, &right_type])?;
                    Box::new((
                        AndExpr {
                            left: Box::new(ge_func),
                            right: Box::new(le_func),
                            return_type: Box::new(func.return_type()),
                        }
                        .into(),
                        func.return_type(),
                    ))
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let box (lt_func, left_type) =
                        self.resolve_function(span, "<", &[expr.as_ref(), low.as_ref()], None)?;
                    let box (gt_func, right_type) =
                        self.resolve_function(span, ">", &[expr.as_ref(), high.as_ref()], None)?;
                    let func = FunctionFactory::instance().get("or", &[&left_type, &right_type])?;
                    Box::new((
                        OrExpr {
                            left: Box::new(lt_func),
                            right: Box::new(gt_func),
                            return_type: Box::new(func.return_type()),
                        }
                        .into(),
                        func.return_type(),
                    ))
                }
            }

            Expr::BinaryOp {
                span,
                op,
                left,
                right,
                ..
            } => {
                if let Expr::Subquery {
                    subquery, modifier, ..
                } = &**right
                {
                    match modifier {
                        Some(SubqueryModifier::Any) | Some(SubqueryModifier::Some) => {
                            return Err(ErrorCode::SemanticError(
                                expr.span()
                                    .display_error("not support subquery".to_string()),
                            ));
                        }
                        Some(SubqueryModifier::All) => {
                            let contrary_op = op.to_contrary()?;
                            let rewritten_subquery = Expr::Subquery {
                                span: right.span(),
                                modifier: Some(SubqueryModifier::Any),
                                subquery: (*subquery).clone(),
                            };
                            return self.resolve_function(
                                span,
                                "not",
                                &[&Expr::BinaryOp {
                                    span,
                                    op: contrary_op,
                                    left: (*left).clone(),
                                    right: Box::new(rewritten_subquery),
                                }],
                                None,
                            );
                        }
                        None => {}
                    }
                }
                self.resolve_binary_op(span, op, left.as_ref(), right.as_ref(), required_type)?
            }

            Expr::UnaryOp { span, op, expr, .. } => {
                self.resolve_unary_op(span, op, expr.as_ref(), required_type)?
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type)?;
                let cast_func =
                    CastFunction::create("", target_type.to_string().as_str(), data_type.clone())?;
                Box::new((
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: Box::new(data_type),
                        target_type: Box::new(cast_func.return_type()),
                    }
                    .into(),
                    cast_func.return_type(),
                ))
            }

            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut arguments = Vec::with_capacity(conditions.len() * 2 + 1);
                for (c, r) in conditions.iter().zip(results.iter()) {
                    match operand {
                        Some(operand) => {
                            // compare case operand with each conditions until one of them is equal
                            let equal_expr = Expr::FunctionCall {
                                span,
                                distinct: false,
                                name: Identifier {
                                    name: "=".to_string(),
                                    quote: None,
                                    span: span[0].clone(),
                                },
                                args: vec![*operand.clone(), c.clone()],
                                params: vec![],
                            };
                            arguments.push(equal_expr)
                        }
                        None => arguments.push(c.clone()),
                    }
                    arguments.push(r.clone());
                }
                let null_arg = Expr::Literal {
                    span: &[],
                    lit: Literal::Null,
                };

                if let Some(expr) = else_result {
                    arguments.push(*expr.clone());
                } else {
                    arguments.push(null_arg)
                }
                let args_ref: Vec<&Expr> = arguments.iter().collect();
                self.resolve_function(span, "multi_if", &args_ref, required_type)?
            }

            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut arguments = vec![expr.as_ref(), substring_from.as_ref()];
                if let Some(substring_for) = substring_for {
                    arguments.push(substring_for.as_ref());
                }
                self.resolve_function(span, "substring", &arguments, required_type)?
            }

            Expr::Literal { lit, .. } => {
                let box (value, data_type) = self.resolve_literal(lit, required_type)?;
                Box::new((
                    ConstantExpr {
                        value,
                        data_type: Box::new(data_type.clone()),
                    }
                    .into(),
                    data_type,
                ))
            }

            Expr::FunctionCall {
                span,
                distinct,
                name,
                args,
                params,
                ..
            } => {
                let func_name = name.name.as_str();
                if !is_builtin_function(func_name)
                    && !Self::is_rewritable_scalar_function(func_name)
                {
                    return Err(ErrorCode::SemanticError(
                        expr.span()
                            .display_error("not support UDF functions".to_string()),
                    ));
                }

                let args: Vec<&Expr> = args.iter().collect();

                if AggregateFunctionFactory::instance().check(func_name) {
                    if self.in_aggregate_function {
                        // Reset the state
                        self.in_aggregate_function = false;
                        return Err(ErrorCode::SemanticError(expr.span().display_error(
                            "aggregate function calls cannot be nested".to_string(),
                        )));
                    }

                    // Check aggregate function
                    let params = params
                        .iter()
                        .map(|literal| {
                            self.resolve_literal(literal, None)
                                .map(|box (value, _)| value)
                        })
                        .collect::<Result<Vec<DataValue>>>()?;

                    self.in_aggregate_function = true;
                    let mut arguments = vec![];
                    for arg in args.iter() {
                        arguments.push(self.resolve(arg, None)?);
                    }
                    self.in_aggregate_function = false;

                    let data_fields = arguments
                        .iter()
                        .map(|box (_, data_type)| DataField::new("", data_type.clone()))
                        .collect();

                    // Rewrite `xxx(distinct)` to `xxx_distinct(...)`
                    let (func_name, distinct) =
                        if func_name.eq_ignore_ascii_case("count") && *distinct {
                            ("count_distinct", false)
                        } else {
                            (func_name, *distinct)
                        };

                    let func_name = if distinct {
                        format!("{}_distinct", func_name)
                    } else {
                        func_name.to_string()
                    };

                    let agg_func = AggregateFunctionFactory::instance()
                        .get(&func_name, params.clone(), data_fields)
                        .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;

                    let args = if optimize_remove_count_args(&func_name, distinct, args.as_slice())
                    {
                        vec![]
                    } else {
                        arguments.into_iter().map(|box (arg, _)| arg).collect()
                    };

                    Box::new((
                        AggregateFunction {
                            display_name: format!("{:#}", expr),
                            func_name,
                            distinct: false,
                            params,
                            args,
                            return_type: Box::new(agg_func.return_type()?),
                        }
                        .into(),
                        agg_func.return_type()?,
                    ))
                } else {
                    // Scalar function
                    self.resolve_function(span, func_name, &args, required_type)?
                }
            }

            Expr::CountAll { .. } => {
                let agg_func = AggregateFunctionFactory::instance().get("count", vec![], vec![])?;

                Box::new((
                    AggregateFunction {
                        display_name: format!("{:#}", expr),
                        func_name: "count".to_string(),
                        distinct: false,
                        params: vec![],
                        args: vec![],
                        return_type: Box::new(agg_func.return_type()?),
                    }
                    .into(),
                    agg_func.return_type()?,
                ))
            }

            Expr::Exists { .. } => {
                return Err(ErrorCode::SemanticError(
                    expr.span()
                        .display_error("not support subquery".to_string()),
                ));
            }

            Expr::Subquery { .. } => {
                return Err(ErrorCode::SemanticError(
                    expr.span()
                        .display_error("not support subquery".to_string()),
                ));
            }

            Expr::InSubquery { .. } => {
                return Err(ErrorCode::SemanticError(
                    expr.span()
                        .display_error("not support subquery".to_string()),
                ));
            }

            expr @ Expr::MapAccess {
                span,
                expr: inner_expr,
                accessor,
            } => {
                // If it's map accessors to a tuple column, pushdown the map accessors to storage.
                let mut accessors = Vec::new();
                let mut expr = expr;
                loop {
                    match expr {
                        Expr::MapAccess {
                            expr: inner_expr,
                            accessor:
                                accessor @ (MapAccessor::Period { .. }
                                | MapAccessor::PeriodNumber { .. }
                                | MapAccessor::Colon { .. }
                                | MapAccessor::Bracket {
                                    key:
                                        box Expr::Literal {
                                            lit: Literal::String(..),
                                            ..
                                        },
                                }),
                            ..
                        } => {
                            accessors.push(accessor.clone());
                            expr = &**inner_expr;
                        }
                        Expr::ColumnRef {
                            database,
                            table,
                            column,
                            ..
                        } => {
                            let (_, data_type) = *self.resolve(expr, None)?;
                            if data_type.data_type_id() != TypeID::Struct {
                                break;
                            }
                            return self.resolve_map_access_pushdown(
                                data_type,
                                accessors,
                                database.clone(),
                                table.clone(),
                                column.clone(),
                            );
                        }
                        _ => {
                            break;
                        }
                    }
                }

                // Otherwise, desugar it into a `get` function.
                let arg = match accessor {
                    MapAccessor::Bracket { key } => (**key).clone(),
                    MapAccessor::Period { key } | MapAccessor::Colon { key } => Expr::Literal {
                        span,
                        lit: Literal::String(key.name.clone()),
                    },
                    MapAccessor::PeriodNumber { .. } => unimplemented!(),
                };
                self.resolve_function(span, "get", &[inner_expr, &arg], None)?
            }

            Expr::TryCast {
                span,
                expr,
                target_type,
                ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type)?;
                let cast_func = CastFunction::create_try(
                    "",
                    target_type.to_string().as_str(),
                    data_type.clone(),
                )
                .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
                Box::new((
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: Box::new(data_type),
                        target_type: Box::new(cast_func.return_type()),
                    }
                    .into(),
                    cast_func.return_type(),
                ))
            }

            Expr::Extract {
                span, kind, expr, ..
            } => self.resolve_extract_expr(span, kind, expr, required_type)?,

            Expr::Interval {
                span, expr, unit, ..
            } => self.resolve_interval(span, expr, unit, required_type)?,

            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_add(span, unit, interval, date, required_type)?,
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_add(
                span,
                unit,
                &Expr::UnaryOp {
                    span,
                    op: UnaryOperator::Minus,
                    expr: interval.clone(),
                },
                date,
                required_type,
            )?,
            Expr::DateTrunc {
                span, unit, date, ..
            } => self.resolve_date_trunc(span, date, unit, required_type)?,
            Expr::Trim {
                span,
                expr,
                trim_where,
                ..
            } => self.resolve_trim_function(span, expr, trim_where)?,

            Expr::Array { span, exprs, .. } => self.resolve_array(span, exprs)?,

            Expr::Position {
                substr_expr,
                str_expr,
                span,
                ..
            } => self.resolve_function(
                span,
                "locate",
                &[substr_expr.as_ref(), str_expr.as_ref()],
                None,
            )?,

            Expr::Tuple { span, exprs, .. } => self.resolve_tuple(span, exprs)?,
        };

        Ok(Box::new(self.post_resolve(&scalar, &data_type)?))
    }

    /// Resolve function call.
    pub fn resolve_function(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: &[&Expr<'_>],
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let box (arg, mut arg_type) = self.resolve(argument, None)?;
            if let Scalar::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type().is_nullable() {
                    arg_type = NullableType::new_impl(arg_type);
                }
            }
            args.push(arg);
            arg_types.push(arg_type);
        }

        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();

        // Validate function arguments.
        // TODO(leiysky): should be done in `FunctionFactory::get`.
        let feature = FunctionFactory::instance().get_features(func_name)?;
        validate_function_arg(
            func_name,
            arguments.len(),
            feature.variadic_arguments,
            feature.num_arguments,
        )?;

        let func = FunctionFactory::instance()
            .get(func_name, &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok(Box::new((
            FunctionCall {
                arguments: args,
                func_name: func_name.to_string(),
                arg_types: arg_types.to_vec(),
                return_type: Box::new(func.return_type()),
            }
            .into(),
            func.return_type(),
        )))
    }

    pub fn resolve_scalar_function_call(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: Vec<Scalar>,
        arguments_types: Vec<DataTypeImpl>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let arg_types_ref: Vec<&DataTypeImpl> = arguments_types.iter().collect();
        let func = FunctionFactory::instance()
            .get(func_name, &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;

        Ok(Box::new((
            FunctionCall {
                arguments,
                func_name: func_name.to_string(),
                arg_types: arguments_types.to_vec(),
                return_type: Box::new(func.return_type()),
            }
            .into(),
            func.return_type(),
        )))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub fn resolve_binary_op(
        &mut self,
        span: &[Token<'_>],
        op: &BinaryOperator,
        left: &Expr<'_>,
        right: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Div
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::StringConcat
            | BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::Regexp
            | BinaryOperator::RLike
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike
            | BinaryOperator::BitwiseOr
            | BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseXor
            | BinaryOperator::Xor => {
                self.resolve_function(span, op.to_string().as_str(), &[left, right], required_type)
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let op = ComparisonOp::try_from(op)?;
                let box (left, _) = self.resolve(left, None)?;
                let box (right, _) = self.resolve(right, None)?;
                let func = FunctionFactory::instance()
                    .get(op.to_func_name(), &[&left.data_type(), &right.data_type()])?;
                Ok(Box::new((
                    ComparisonExpr {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(func.return_type()),
                    }
                    .into(),
                    func.return_type(),
                )))
            }
            BinaryOperator::And => {
                let box (left, _) = self.resolve(left, None)?;
                let box (right, _) = self.resolve(right, None)?;
                let func = FunctionFactory::instance()
                    .get("and", &[&left.data_type(), &right.data_type()])?;
                Ok(Box::new((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(func.return_type()),
                    }
                    .into(),
                    func.return_type(),
                )))
            }
            BinaryOperator::Or => {
                let box (left, _) = self.resolve(left, None)?;
                let box (right, _) = self.resolve(right, None)?;
                let func = FunctionFactory::instance()
                    .get("or", &[&left.data_type(), &right.data_type()])?;
                Ok(Box::new((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(func.return_type()),
                    }
                    .into(),
                    func.return_type(),
                )))
            }
        }
    }

    /// Resolve unary expressions.
    pub fn resolve_unary_op(
        &mut self,
        span: &[Token<'_>],
        op: &UnaryOperator,
        child: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match op {
            UnaryOperator::Plus => {
                // Omit unary + operator
                self.resolve(child, required_type)
            }

            UnaryOperator::Minus => self.resolve_function(span, "negate", &[child], required_type),

            UnaryOperator::Not => self.resolve_function(span, "not", &[child], required_type),
        }
    }

    pub fn resolve_extract_expr(
        &mut self,
        span: &[Token<'_>],
        interval_kind: &ASTIntervalKind,
        arg: &Expr<'_>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match interval_kind {
            ASTIntervalKind::Year => {
                self.resolve_function(span, "to_year", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(span, "to_quarter", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Month => {
                self.resolve_function(span, "to_month", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Day => self.resolve_function(
                span,
                "to_day_of_month",
                &[arg],
                Some(TimestampType::new_impl()),
            ),
            ASTIntervalKind::Hour => {
                self.resolve_function(span, "to_hour", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(span, "to_minute", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Second => {
                self.resolve_function(span, "to_second", &[arg], Some(TimestampType::new_impl()))
            }
            ASTIntervalKind::Doy => self.resolve_function(
                span,
                "to_day_of_year",
                &[arg],
                Some(TimestampType::new_impl()),
            ),
            ASTIntervalKind::Dow => self.resolve_function(
                span,
                "to_day_of_week",
                &[arg],
                Some(TimestampType::new_impl()),
            ),
        }
    }

    pub fn resolve_interval(
        &mut self,
        span: &[Token<'_>],
        arg: &Expr<'_>,
        interval_kind: &ASTIntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match interval_kind {
            ASTIntervalKind::Year => self.resolve_function(
                span,
                "to_interval_year",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Year)),
            ),
            ASTIntervalKind::Quarter => self.resolve_function(
                span,
                "to_interval_quarter",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Quarter)),
            ),
            ASTIntervalKind::Month => self.resolve_function(
                span,
                "to_interval_month",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Month)),
            ),
            ASTIntervalKind::Day => self.resolve_function(
                span,
                "to_interval_day",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Day)),
            ),
            ASTIntervalKind::Hour => self.resolve_function(
                span,
                "to_interval_hour",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Hour)),
            ),
            ASTIntervalKind::Minute => self.resolve_function(
                span,
                "to_interval_minute",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Minute)),
            ),
            ASTIntervalKind::Second => self.resolve_function(
                span,
                "to_interval_second",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Second)),
            ),
            ASTIntervalKind::Doy => self.resolve_function(
                span,
                "to_interval_doy",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Doy)),
            ),
            ASTIntervalKind::Dow => self.resolve_function(
                span,
                "to_interval_dow",
                &[arg],
                Some(IntervalType::new_impl(IntervalKind::Dow)),
            ),
        }
    }

    pub fn resolve_date_add(
        &mut self,
        span: &[Token<'_>],
        interval_kind: &ASTIntervalKind,
        interval: &Expr<'_>,
        date: &Expr<'_>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut args = vec![];
        let mut arg_types = vec![];

        let box (date, date_type) = self.resolve(date, None)?;
        args.push(date);
        arg_types.push(date_type);

        let box (interval, interval_type) =
            self.resolve_interval(span, interval, interval_kind, None)?;
        args.push(interval);
        arg_types.push(interval_type);

        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();

        let func = FunctionFactory::instance()
            .get("date_add", &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok(Box::new((
            FunctionCall {
                arguments: args,
                func_name: "date_add".to_string(),
                arg_types: arg_types.to_vec(),
                return_type: Box::new(func.return_type()),
            }
            .into(),
            func.return_type(),
        )))
    }

    pub fn resolve_date_trunc(
        &mut self,
        span: &[Token<'_>],
        date: &Expr<'_>,
        kind: &ASTIntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(
                    span,
                    "to_start_of_quarter",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            ASTIntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second",
                    &[date],
                    Some(TimestampType::new_impl()),
                )
            }
            _ => Err(ErrorCode::SemanticError(span.display_error("Only these interval types are currently supported: [year, month, day, hour, minute, second]".to_string()))),
        }
    }

    fn is_rewritable_scalar_function(func_name: &str) -> bool {
        matches!(
            func_name.to_lowercase().as_str(),
            "database"
                | "currentdatabase"
                | "current_database"
                | "version"
                | "user"
                | "currentuser"
                | "current_user"
                | "connection_id"
                | "timezone"
                | "nullif"
                | "ifnull"
                | "coalesce"
        )
    }

    fn resolve_trim_function(
        &mut self,
        span: &[Token<'_>],
        expr: &Expr<'_>,
        trim_where: &Option<(TrimWhere, Box<Expr<'_>>)>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let (func_name, trim_scalar, trim_type) = if let Some((trim_type, trim_expr)) = trim_where {
            let func_name = match trim_type {
                TrimWhere::Leading => "trim_leading",
                TrimWhere::Trailing => "trim_trailing",
                TrimWhere::Both => "trim_both",
            };

            let box (trim_scalar, trim_type) = self.resolve(trim_expr, None)?;
            (func_name, trim_scalar, trim_type)
        } else {
            let trim_scalar = ConstantExpr {
                value: DataValue::String(" ".as_bytes().to_vec()),
                data_type: Box::new(StringType::new_impl()),
            }
            .into();
            ("trim_both", trim_scalar, StringType::new_impl())
        };

        let box (trim_source, source_type) = self.resolve(expr, None)?;
        let args = vec![trim_source, trim_scalar];
        let func = FunctionFactory::instance()
            .get(func_name, &[&source_type, &trim_type])
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;

        Ok(Box::new((
            FunctionCall {
                arguments: args,
                func_name: func_name.to_string(),
                arg_types: vec![source_type, trim_type],
                return_type: Box::new(func.return_type()),
            }
            .into(),
            func.return_type(),
        )))
    }

    /// Resolve literal values.
    pub fn resolve_literal(
        &self,
        literal: &Literal,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(DataValue, DataTypeImpl)>> {
        // TODO(leiysky): try cast value to required type
        let value = match literal {
            Literal::Integer(uint) => DataValue::UInt64(*uint),
            Literal::Float(float) => DataValue::Float64(*float),
            Literal::String(string) => DataValue::String(string.as_bytes().to_vec()),
            Literal::Boolean(boolean) => DataValue::Boolean(*boolean),
            Literal::Null => DataValue::Null,
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported literal value: {literal}"
            )))?,
        };

        let data_type = value.data_type();

        Ok(Box::new((value, data_type)))
    }

    // TODO(leiysky): use an array builder function instead, since we should allow declaring
    // an array with variable as element.
    fn resolve_array(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        let mut types = Vec::with_capacity(exprs.len());
        for expr in exprs.iter() {
            let box (arg, data_type) = self.resolve(expr, None)?;
            types.push(data_type);
            if let Scalar::ConstantExpr(elem) = arg {
                elems.push(elem.value);
            } else {
                return Err(ErrorCode::SemanticError(
                    expr.span()
                        .display_error("Array element must be literal".to_string()),
                ));
            }
        }
        let element_type = if elems.is_empty() {
            NullType::new_impl()
        } else {
            types
                .iter()
                .fold(Ok(types[0].clone()), |acc, v| merge_types(&acc?, v))
                .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?
        };
        Ok(Box::new((
            ConstantExpr {
                value: DataValue::Array(elems),
                data_type: Box::new(ArrayType::new_impl(element_type.clone())),
            }
            .into(),
            ArrayType::new_impl(element_type),
        )))
    }

    fn resolve_tuple(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut args = Vec::with_capacity(exprs.len());
        let mut arg_types = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, data_type) = self.resolve(expr, None)?;
            args.push(arg);
            arg_types.push(data_type);
        }
        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();
        let tuple_func = TupleFunction::try_create_func("", &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok(Box::new((
            FunctionCall {
                arguments: args,
                func_name: "tuple".to_string(),
                arg_types,
                return_type: Box::new(tuple_func.return_type()),
            }
            .into(),
            tuple_func.return_type(),
        )))
    }

    fn resolve_map_access_pushdown(
        &mut self,
        data_type: DataTypeImpl,
        mut accessors: Vec<MapAccessor>,
        database: Option<Identifier>,
        table: Option<Identifier>,
        column: Identifier,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut names = Vec::new();
        let column_name = normalize_identifier(&column, self.name_resolution_ctx).name;
        names.push(column_name);
        let mut data_types = Vec::new();
        data_types.push(data_type);

        while !accessors.is_empty() {
            let data_type = data_types.pop().unwrap();
            let struct_type: StructType = data_type.try_into()?;
            let inner_types = struct_type.types();
            let inner_names = match struct_type.names() {
                Some(inner_names) => inner_names.clone(),
                None => (0..inner_types.len())
                    .map(|i| format!("{}", i + 1))
                    .collect::<Vec<_>>(),
            };

            let accessor = accessors.pop().unwrap();
            let accessor_lit = match accessor {
                MapAccessor::Bracket {
                    key:
                        box Expr::Literal {
                            lit: lit @ Literal::String(_),
                            ..
                        },
                } => lit,
                MapAccessor::Period { key } | MapAccessor::Colon { key } => {
                    Literal::String(key.name.clone())
                }
                MapAccessor::PeriodNumber { key } => Literal::Integer(key),
                _ => unreachable!(),
            };

            match accessor_lit {
                Literal::Integer(idx) => {
                    if idx == 0 {
                        return Err(ErrorCode::SemanticError(
                            "tuple index is starting from 1, but 0 is found".to_string(),
                        ));
                    }
                    if idx as usize > inner_types.len() {
                        return Err(ErrorCode::SemanticError(format!(
                            "tuple index {} is out of bounds for length {}",
                            idx,
                            inner_types.len()
                        )));
                    }
                    let inner_name = inner_names.get(idx as usize - 1).unwrap();
                    let inner_type = inner_types.get(idx as usize - 1).unwrap();
                    names.push(inner_name.clone());
                    data_types.push(inner_type.clone());
                }
                Literal::String(name) => match inner_names.iter().position(|k| k == &name) {
                    Some(idx) => {
                        let inner_name = inner_names.get(idx).unwrap();
                        let inner_type = inner_types.get(idx).unwrap();
                        names.push(inner_name.clone());
                        data_types.push(inner_type.clone());
                    }
                    None => {
                        return Err(ErrorCode::SemanticError(format!(
                            "tuple name `{}` does not exist, available names are: {:?}",
                            name, &inner_names
                        )));
                    }
                },
                _ => unreachable!(),
            }
        }

        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
        let table = table
            .as_ref()
            .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
        let inner_column_name = names.join(":");

        let result = self.bind_context.resolve_name(
            database.as_deref(),
            table.as_deref(),
            inner_column_name.as_str(),
            &column.span,
            self.aliases,
        )?;
        let (scalar, data_type) = match result {
            NameResolutionResult::Column(column) => {
                let data_type = *column.data_type.clone();
                (BoundColumnRef { column }.into(), data_type)
            }
            NameResolutionResult::Alias { scalar, .. } => (scalar.clone(), scalar.data_type()),
        };
        Ok(Box::new((scalar, data_type)))
    }
}
