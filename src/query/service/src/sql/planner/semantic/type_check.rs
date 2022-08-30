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

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_ast::ast::Query;
use common_ast::ast::SubqueryModifier;
use common_ast::ast::TrimWhere;
use common_ast::ast::UnaryOperator;
use common_ast::parser::parse_expr;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::DisplayError;
use common_datavalues::type_coercion::merge_types;
use common_datavalues::ArrayType;
use common_datavalues::BooleanType;
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
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::TupleFunction;
use common_planners::validate_function_arg;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use crate::evaluator::Evaluator;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::binder::wrap_cast_if_needed;
use crate::sql::binder::Binder;
use crate::sql::binder::NameResolutionResult;
use crate::sql::optimizer::RelExpr;
use crate::sql::planner::metadata::optimize_remove_count_args;
use crate::sql::planner::metadata::MetadataRef;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::SubqueryExpr;
use crate::sql::plans::SubqueryType;
use crate::sql::BindContext;
use crate::sql::ScalarExpr;

/// A helper for type checking.
///
/// `TypeChecker::resolve` will resolve types of `Expr` and transform `Expr` into
/// a typed expression `Scalar`. At the same time, name resolution will be performed,
/// which check validity of unbound `ColumnRef` and try to replace it with qualified
/// `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a BindContext,
    ctx: Arc<QueryContext>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,

    aliases: &'a [(String, Scalar)],

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,
}

impl<'a> TypeChecker<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        ctx: Arc<QueryContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, Scalar)],
    ) -> Self {
        Self {
            bind_context,
            ctx,
            name_resolution_ctx,
            metadata,
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
        if let Ok((value, value_type)) =
            Evaluator::eval_scalar::<String>(scalar).and_then(|evaluator| {
                let func_ctx = self.ctx.try_get_function_context()?;
                evaluator.try_eval_const(&func_ctx)
            })
        {
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

    /// Resolve types of `expr` with given `required_type`.
    /// If `required_type` is None, then there is no requirement of return type.
    ///
    /// TODO(leiysky): choose correct overloads of functions with given required_type and arguments
    #[async_recursion::async_recursion]
    pub async fn resolve(
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
                let func_name = if *not { "is_not_null" } else { "is_null" };

                self.resolve_function(span, func_name, &[expr.as_ref()], required_type)
                    .await?
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
                let box (scalar, data_type) = self
                    .resolve_function(
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
                    )
                    .await?;
                self.resolve_scalar_function_call(
                    span,
                    "assume_not_null",
                    vec![scalar],
                    vec![data_type],
                    required_type,
                )
                .await?
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                let func_name = if *not { "not_in" } else { "in" };
                let mut args = Vec::with_capacity(list.len() + 1);
                args.push(expr.as_ref());
                for expr in list.iter() {
                    args.push(expr);
                }
                self.resolve_function(span, func_name, &args, required_type)
                    .await?
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
                    let box (ge_func, left_type) = self
                        .resolve_function(span, ">=", &[expr.as_ref(), low.as_ref()], None)
                        .await?;
                    let box (le_func, right_type) = self
                        .resolve_function(span, "<=", &[expr.as_ref(), high.as_ref()], None)
                        .await?;
                    let func =
                        FunctionFactory::instance().get("and", &[&left_type, &right_type])?;
                    Box::new((
                        AndExpr {
                            left: Box::new(ge_func),
                            right: Box::new(le_func),
                            return_type: Box::new(func.return_type()),
                        }
                        .into(),
                        BooleanType::new_impl(),
                    ))
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let box (lt_func, left_type) = self
                        .resolve_function(span, "<", &[expr.as_ref(), low.as_ref()], None)
                        .await?;
                    let box (gt_func, right_type) = self
                        .resolve_function(span, ">", &[expr.as_ref(), high.as_ref()], None)
                        .await?;
                    let func = FunctionFactory::instance().get("or", &[&left_type, &right_type])?;
                    Box::new((
                        OrExpr {
                            left: Box::new(lt_func),
                            right: Box::new(gt_func),
                            return_type: Box::new(func.return_type()),
                        }
                        .into(),
                        BooleanType::new_impl(),
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
                    if let Some(subquery_modifier) = modifier {
                        match subquery_modifier {
                            SubqueryModifier::Any | SubqueryModifier::Some => {
                                let comparison_op = ComparisonOp::try_from(op)?;
                                self.resolve_subquery(
                                    SubqueryType::Any,
                                    subquery,
                                    true,
                                    Some(*left.clone()),
                                    Some(comparison_op),
                                    None,
                                )
                                .await?
                            }
                            SubqueryModifier::All => {
                                let contrary_op = op.to_contrary()?;
                                let rewritten_subquery = Expr::Subquery {
                                    span: right.span(),
                                    modifier: Some(SubqueryModifier::Any),
                                    subquery: (*subquery).clone(),
                                };
                                self.resolve_function(
                                    span,
                                    "not",
                                    &[&Expr::BinaryOp {
                                        span,
                                        op: contrary_op,
                                        left: (*left).clone(),
                                        right: Box::new(rewritten_subquery),
                                    }],
                                    None,
                                )
                                .await?
                            }
                        }
                    } else {
                        self.resolve_binary_op(
                            span,
                            op,
                            left.as_ref(),
                            right.as_ref(),
                            required_type,
                        )
                        .await?
                    }
                } else {
                    self.resolve_binary_op(span, op, left.as_ref(), right.as_ref(), required_type)
                        .await?
                }
            }

            Expr::UnaryOp { span, op, expr, .. } => {
                self.resolve_unary_op(span, op, expr.as_ref(), required_type)
                    .await?
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type).await?;
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
                self.resolve_function(span, "multi_if", &args_ref, required_type)
                    .await?
            }

            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut arguments = vec![expr.as_ref()];
                match (substring_from, substring_for) {
                    (Some(from_expr), None) => {
                        arguments.push(from_expr.as_ref());
                    }
                    (Some(from_expr), Some(for_expr)) => {
                        arguments.push(from_expr.as_ref());
                        arguments.push(for_expr.as_ref());
                    }
                    _ => return Err(ErrorCode::SemanticError("Invalid arguments of SUBSTRING")),
                }

                self.resolve_function(span, "substring", &arguments, required_type)
                    .await?
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
                    return self.resolve_udf(span, func_name, args).await;
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
                        arguments.push(self.resolve(arg, None).await?);
                    }
                    self.in_aggregate_function = false;

                    let data_fields = arguments
                        .iter()
                        .map(|box (_, data_type)| DataField::new("", data_type.clone()))
                        .collect();

                    // Rewrite `count(distinct)` to `uniq(...)`
                    let (func_name, distinct) =
                        if func_name.eq_ignore_ascii_case("count") && *distinct {
                            ("count_distinct", false)
                        } else {
                            (func_name, *distinct)
                        };

                    let agg_func = AggregateFunctionFactory::instance()
                        .get(func_name, params.clone(), data_fields)
                        .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;

                    Box::new((
                        AggregateFunction {
                            display_name: format!("{:#}", expr),
                            func_name: func_name.to_string(),
                            distinct,
                            params,
                            args: if optimize_remove_count_args(
                                func_name,
                                distinct,
                                args.as_slice(),
                            ) {
                                vec![]
                            } else {
                                arguments.into_iter().map(|box (arg, _)| arg).collect()
                            },
                            return_type: Box::new(agg_func.return_type()?),
                        }
                        .into(),
                        agg_func.return_type()?,
                    ))
                } else {
                    // Scalar function
                    self.resolve_function(span, func_name, &args, required_type)
                        .await?
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

            Expr::Exists {
                subquery,
                not,
                span,
                ..
            } => {
                if *not {
                    return self
                        .resolve_function(
                            span,
                            "not",
                            &[&Expr::Exists {
                                span: *span,
                                not: false,
                                subquery: subquery.clone(),
                            }],
                            None,
                        )
                        .await;
                }
                self.resolve_subquery(SubqueryType::Exists, subquery, true, None, None, None)
                    .await?
            }

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, false, None, None, None)
                    .await?
            }

            Expr::InSubquery {
                subquery,
                not,
                expr,
                span,
            } => {
                // Not in subquery will be transformed to not(Expr = Any(...))
                if *not {
                    return self
                        .resolve_function(
                            span,
                            "not",
                            &[&Expr::InSubquery {
                                subquery: subquery.clone(),
                                not: false,
                                expr: expr.clone(),
                                span: *span,
                            }],
                            required_type,
                        )
                        .await;
                }
                // InSubquery will be transformed to Expr = Any(...)
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    true,
                    Some(*expr.clone()),
                    Some(ComparisonOp::Equal),
                    None,
                )
                .await?
            }

            Expr::MapAccess {
                span,
                expr,
                accessor,
            } => {
                let mut exprs = Vec::new();
                let mut accessores = Vec::new();
                exprs.push(expr.clone());
                accessores.push(accessor.clone());
                while !exprs.is_empty() {
                    let expr = exprs.pop().unwrap();
                    match *expr {
                        Expr::MapAccess { expr, accessor, .. } => {
                            exprs.push(expr.clone());
                            accessores.push(accessor.clone());
                        }
                        Expr::ColumnRef {
                            ref database,
                            ref table,
                            ref column,
                            ..
                        } => {
                            let box (_, data_type) = self.resolve(&*expr, None).await?;
                            if data_type.data_type_id() != TypeID::Struct {
                                break;
                            }
                            // if the column is StructColumn, pushdown map access to storage
                            return self
                                .resolve_map_access_pushdown(
                                    data_type,
                                    accessores,
                                    database.clone(),
                                    table.clone(),
                                    column.clone(),
                                )
                                .await;
                        }
                        _ => break,
                    }
                }
                let arg = match accessor {
                    MapAccessor::Bracket { key } => Expr::Literal {
                        span,
                        lit: key.clone(),
                    },
                    MapAccessor::Period { key } | MapAccessor::Colon { key } => Expr::Literal {
                        span,
                        lit: Literal::String(key.name.clone()),
                    },
                };

                self.resolve_function(span, "get", &[expr.as_ref(), &arg], None)
                    .await?
            }

            Expr::TryCast {
                span,
                expr,
                target_type,
                ..
            } => {
                let box (scalar, data_type) = self.resolve(expr, required_type).await?;
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
            } => {
                self.resolve_extract_expr(span, kind, expr, required_type)
                    .await?
            }

            Expr::Interval {
                span, expr, unit, ..
            } => {
                self.resolve_interval(span, expr, unit, required_type)
                    .await?
            }

            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
                ..
            } => {
                self.resolve_date_add(span, unit, interval, date, required_type)
                    .await?
            }
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
                ..
            } => {
                self.resolve_date_add(
                    span,
                    unit,
                    &Expr::UnaryOp {
                        span,
                        op: UnaryOperator::Minus,
                        expr: interval.clone(),
                    },
                    date,
                    required_type,
                )
                .await?
            }
            Expr::DateTrunc {
                span, unit, date, ..
            } => {
                self.resolve_date_trunc(span, date, unit, required_type)
                    .await?
            }
            Expr::Trim {
                span,
                expr,
                trim_where,
                ..
            } => self.resolve_trim_function(span, expr, trim_where).await?,

            Expr::Array { span, exprs, .. } => self.resolve_array(span, exprs).await?,

            Expr::Position {
                substr_expr,
                str_expr,
                span,
                ..
            } => {
                self.resolve_function(
                    span,
                    "locate",
                    &[substr_expr.as_ref(), str_expr.as_ref()],
                    None,
                )
                .await?
            }

            Expr::Tuple { span, exprs, .. } => self.resolve_tuple(span, exprs).await?,
        };

        Ok(Box::new(self.post_resolve(&scalar, &data_type)?))
    }

    /// Resolve function call.
    #[async_recursion::async_recursion]
    pub async fn resolve_function(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: &[&Expr<'_>],
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        // Check if current function is a virtual function, e.g. `database`, `version`
        if let Some(rewriten_func_result) = self
            .try_rewrite_scalar_function(span, func_name, arguments)
            .await
        {
            return rewriten_func_result;
        }

        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let box (arg, mut arg_type) = self.resolve(argument, None).await?;
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

    #[async_recursion::async_recursion]
    pub async fn resolve_scalar_function_call(
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
    #[async_recursion::async_recursion]
    pub async fn resolve_binary_op(
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
                    .await
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let op = ComparisonOp::try_from(op)?;
                let box (left, _) = self.resolve(left, None).await?;
                let box (right, _) = self.resolve(right, None).await?;
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
                let box (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let box (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;
                let func = FunctionFactory::instance()
                    .get("and", &[&left.data_type(), &right.data_type()])?;
                Ok(Box::new((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(func.return_type()),
                    }
                    .into(),
                    BooleanType::new_impl(),
                )))
            }
            BinaryOperator::Or => {
                let box (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let box (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;
                let func = FunctionFactory::instance()
                    .get("or", &[&left.data_type(), &right.data_type()])?;
                Ok(Box::new((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                        return_type: Box::new(func.return_type()),
                    }
                    .into(),
                    BooleanType::new_impl(),
                )))
            }
        }
    }

    /// Resolve unary expressions.
    #[async_recursion::async_recursion]
    pub async fn resolve_unary_op(
        &mut self,
        span: &[Token<'_>],
        op: &UnaryOperator,
        child: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match op {
            UnaryOperator::Plus => {
                // Omit unary + operator
                self.resolve(child, required_type).await
            }

            UnaryOperator::Minus => {
                self.resolve_function(span, "negate", &[child], required_type)
                    .await
            }

            UnaryOperator::Not => {
                self.resolve_function(span, "not", &[child], required_type)
                    .await
            }
        }
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_extract_expr(
        &mut self,
        span: &[Token<'_>],
        interval_kind: &IntervalKind,
        arg: &Expr<'_>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match interval_kind {
            IntervalKind::Year => {
                self.resolve_function(span, "to_year", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Month => {
                self.resolve_function(span, "to_month", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_day_of_month",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
            IntervalKind::Hour => {
                self.resolve_function(span, "to_hour", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Minute => {
                self.resolve_function(span, "to_minute", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Second => {
                self.resolve_function(span, "to_second", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Doy => {
                self.resolve_function(
                    span,
                    "to_day_of_year",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
            IntervalKind::Dow => {
                self.resolve_function(
                    span,
                    "to_day_of_week",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
        }
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_interval(
        &mut self,
        span: &[Token<'_>],
        arg: &Expr<'_>,
        interval_kind: &IntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match interval_kind {
            IntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_interval_year",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Year)),
                )
                .await
            }
            IntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_interval_month",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Month)),
                )
                .await
            }
            IntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_interval_day",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Day)),
                )
                .await
            }
            IntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_interval_hour",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Hour)),
                )
                .await
            }
            IntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_interval_minute",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Minute)),
                )
                .await
            }
            IntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_interval_second",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Second)),
                )
                .await
            }
            IntervalKind::Doy => {
                self.resolve_function(
                    span,
                    "to_interval_doy",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Doy)),
                )
                .await
            }
            IntervalKind::Dow => {
                self.resolve_function(
                    span,
                    "to_interval_dow",
                    &[arg],
                    Some(IntervalType::new_impl(IntervalKind::Dow)),
                )
                .await
            }
        }
    }

    #[async_recursion::async_recursion]
    pub async fn resolve_date_add(
        &mut self,
        span: &[Token<'_>],
        interval_kind: &IntervalKind,
        interval: &Expr<'_>,
        date: &Expr<'_>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut args = vec![];
        let mut arg_types = vec![];

        let box (date, date_type) = self.resolve(date, None).await?;
        args.push(date);
        arg_types.push(date_type);

        let box (interval, interval_type) = self
            .resolve_interval(span, interval, interval_kind, None)
            .await?;
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

    #[async_recursion::async_recursion]
    pub async fn resolve_date_trunc(
        &mut self,
        span: &[Token<'_>],
        date: &Expr<'_>,
        kind: &IntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        match kind {
            IntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            IntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            IntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            IntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            IntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            IntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second",
                    &[date],
                    Some(TimestampType::new_impl(0)),
                )
                    .await
            }
            _ => Err(ErrorCode::SemanticError(span.display_error("Only these interval types are currently supported: [year, month, day, hour, minute, second]".to_string()))),
        }
    }

    pub async fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query<'_>,
        allow_multi_rows: bool,
        child_expr: Option<Expr<'_>>,
        compare_op: Option<ComparisonOp>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut binder = Binder::new(
            self.ctx.clone(),
            self.ctx.get_catalog_manager()?,
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let bind_context = BindContext::with_parent(Box::new(self.bind_context.clone()));
        let (s_expr, output_context) = binder.bind_query(&bind_context, subquery).await?;

        if (typ == SubqueryType::Scalar || typ == SubqueryType::Any)
            && output_context.columns.len() > 1
        {
            return Err(ErrorCode::SemanticError(format!(
                "Subquery must return only one column, but got {} columns",
                output_context.columns.len()
            )));
        }

        let mut data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        let mut child_scalar = None;
        if let Some(expr) = child_expr {
            assert_eq!(output_context.columns.len(), 1);
            let box (mut scalar, scalar_data_type) = self.resolve(&expr, None).await?;
            if scalar_data_type != *data_type {
                // Make comparison scalar type keep consistent
                let coercion_type = merge_types(&scalar_data_type, &data_type)?;
                scalar = wrap_cast_if_needed(scalar, &coercion_type);
                data_type = Box::new(coercion_type);
            }
            child_scalar = Some(Box::new(scalar));
        }

        let subquery_expr = SubqueryExpr {
            subquery: Box::new(s_expr),
            child_expr: child_scalar,
            compare_op,
            index: None,
            data_type: data_type.clone(),
            allow_multi_rows,
            typ,
            outer_columns: rel_prop.outer_columns,
        };

        Ok(Box::new((subquery_expr.into(), *data_type)))
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
                | "is_null"
                | "not_in"
                | "nullif"
                | "ifnull"
                | "coalesce"
        )
    }

    #[async_recursion::async_recursion]
    async fn try_rewrite_scalar_function(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        args: &[&Expr<'_>],
    ) -> Option<Result<Box<(Scalar, DataTypeImpl)>>> {
        match (func_name.to_lowercase().as_str(), &args) {
            ("database" | "currentdatabase" | "current_database", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_current_database()),
                    },
                    None,
                )
                .await,
            ),
            ("version", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_fuse_version()),
                    },
                    None,
                )
                .await,
            ),
            ("user" | "currentuser" | "current_user", &[]) => match self.ctx.get_current_user() {
                Ok(user) => Some(
                    self.resolve(
                        &Expr::Literal {
                            span,
                            lit: Literal::String(user.identity().to_string()),
                        },
                        None,
                    )
                    .await,
                ),
                Err(e) => Some(Err(e)),
            },
            ("connection_id", &[]) => Some(
                self.resolve(
                    &Expr::Literal {
                        span,
                        lit: Literal::String(self.ctx.get_connection_id()),
                    },
                    None,
                )
                .await,
            ),
            ("timezone", &[]) => {
                let tz = self.ctx.get_settings().get_timezone().unwrap();
                // No need to handle err, the tz in settings is valid.
                let tz = String::from_utf8(tz).unwrap();
                Some(
                    self.resolve(
                        &Expr::Literal {
                            span,
                            lit: Literal::String(tz),
                        },
                        None,
                    )
                    .await,
                )
            }
            ("is_null", &[arg0]) => {
                // Rewrite `is_null(arg0)` to `not(is_not_null(arg0))`
                Some(
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
                            args: vec![(*arg0).clone()],
                            params: vec![],
                        }],
                        None,
                    )
                    .await,
                )
            }
            ("not_in", args) => {
                // Rewrite `not_in(arg0)` to `not(in(arg0))`
                Some(
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
                    )
                    .await,
                )
            }
            ("nullif", &[arg0, arg1]) => {
                // Rewrite nullif(arg0, arg1) to if(arg0 = arg1, null, arg0)
                Some(
                    self.resolve_function(
                        span,
                        "if",
                        &[
                            &Expr::BinaryOp {
                                span,
                                op: BinaryOperator::Eq,
                                left: Box::new((*arg0).clone()),
                                right: Box::new((*arg1).clone()),
                            },
                            &Expr::Literal {
                                span,
                                lit: Literal::Null,
                            },
                            arg0,
                        ],
                        None,
                    )
                    .await,
                )
            }
            ("ifnull", &[arg0, arg1]) => {
                // Rewrite ifnull(arg0, arg1) to if(is_null(arg0), arg1, arg0)
                Some(
                    self.resolve_function(
                        span,
                        "if",
                        &[
                            &Expr::IsNull {
                                span,
                                expr: Box::new((*arg0).clone()),
                                not: false,
                            },
                            arg1,
                            arg0,
                        ],
                        None,
                    )
                    .await,
                )
            }
            ("coalesce", args) => {
                // coalesce(arg0, arg1, ..., argN) is essentially
                // multi_if(is_not_null(arg0), assume_not_null(arg0), is_not_null(arg1), assume_not_null(arg1), ..., argN)
                // with constant Literal::Null arguments removed.
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);

                for arg in args.iter() {
                    if let Expr::Literal {
                        span: _,
                        lit: Literal::Null,
                    } = arg
                    {
                        continue;
                    }

                    let is_not_null_expr = Expr::IsNull {
                        span,
                        expr: Box::new((*arg).clone()),
                        not: true,
                    };

                    let assume_not_null_expr = Expr::FunctionCall {
                        span,
                        distinct: false,
                        name: Identifier {
                            name: "assume_not_null".to_string(),
                            quote: None,
                            span: span[0].clone(),
                        },
                        args: vec![(*arg).clone()],
                        params: vec![],
                    };

                    new_args.push(is_not_null_expr);
                    new_args.push(assume_not_null_expr);
                }
                new_args.push(Expr::Literal {
                    span,
                    lit: Literal::Null,
                });
                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(
                    self.resolve_function(span, "multi_if", &args_ref, None)
                        .await,
                )
            }

            _ => None,
        }
    }

    #[async_recursion::async_recursion]
    async fn resolve_trim_function(
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

            let box (trim_scalar, trim_type) = self.resolve(trim_expr, None).await?;
            (func_name, trim_scalar, trim_type)
        } else {
            let trim_scalar = ConstantExpr {
                value: DataValue::String(" ".as_bytes().to_vec()),
                data_type: Box::new(StringType::new_impl()),
            }
            .into();
            ("trim_both", trim_scalar, StringType::new_impl())
        };

        let box (trim_source, source_type) = self.resolve(expr, None).await?;
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
    #[async_recursion::async_recursion]
    async fn resolve_array(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        let mut types = Vec::with_capacity(exprs.len());
        for expr in exprs.iter() {
            let box (arg, data_type) = self.resolve(expr, None).await?;
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

    #[async_recursion::async_recursion]
    async fn resolve_tuple(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut args = Vec::with_capacity(exprs.len());
        let mut arg_types = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, data_type) = self.resolve(expr, None).await?;
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

    #[async_recursion::async_recursion]
    async fn resolve_udf(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: &[Expr<'_>],
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let udf = self
            .ctx
            .get_user_manager()
            .get_udf(self.ctx.get_tenant().as_str(), func_name)
            .await;
        if let Ok(udf) = udf {
            let parameters = udf.parameters;
            if parameters.len() != arguments.len() {
                return Err(ErrorCode::SyntaxException(span.display_error(format!(
                    "Require {} parameters, but got: {}",
                    parameters.len(),
                    arguments.len()
                ))));
            }
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let backtrace = Backtrace::new();
            let sql_tokens = tokenize_sql(udf.definition.as_str())?;
            let expr = parse_expr(&sql_tokens, sql_dialect, &backtrace)?;
            let mut args_map = HashMap::new();
            arguments.iter().enumerate().for_each(|(idx, argument)| {
                if let Some(parameter) = parameters.get(idx) {
                    args_map.insert(parameter, (*argument).clone());
                }
            });
            let udf_expr = self
                .clone_expr_with_replacement(&expr, &|nest_expr| {
                    if let Expr::ColumnRef { column, .. } = nest_expr {
                        if let Some(arg) = args_map.get(&column.name) {
                            return Ok(Some(arg.clone()));
                        }
                    }
                    Ok(None)
                })
                .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
            self.resolve(&udf_expr, None).await
        } else {
            Err(ErrorCode::SemanticError(span.display_error(format!(
                "No function matches the given name: {func_name}"
            ))))
        }
    }

    #[async_recursion::async_recursion]
    async fn resolve_map_access_pushdown(
        &mut self,
        data_type: DataTypeImpl,
        mut accessores: Vec<MapAccessor<'async_recursion>>,
        database: Option<Identifier<'async_recursion>>,
        table: Option<Identifier<'async_recursion>>,
        column: Identifier<'async_recursion>,
    ) -> Result<Box<(Scalar, DataTypeImpl)>> {
        let mut names = Vec::new();
        let column_name = normalize_identifier(&column, self.name_resolution_ctx).name;
        names.push(column_name);
        let mut data_types = Vec::new();
        data_types.push(data_type.clone());

        while !accessores.is_empty() {
            let data_type = data_types.pop().unwrap();
            let struct_type: StructType = data_type.try_into()?;
            let inner_types = struct_type.types();
            let inner_names = match struct_type.names() {
                Some(inner_names) => inner_names.clone(),
                None => (0..inner_types.len())
                    .map(|i| format!("{}", i))
                    .collect::<Vec<_>>(),
            };

            let accessor = accessores.pop().unwrap();
            let accessor_lit = match accessor {
                MapAccessor::Bracket { key } => key.clone(),
                MapAccessor::Period { key } | MapAccessor::Colon { key } => {
                    Literal::String(key.name.clone())
                }
            };

            match accessor_lit {
                Literal::Integer(idx) => {
                    if idx as usize >= inner_types.len() {
                        return Err(ErrorCode::SemanticError(format!(
                            "tuple index {} is out of bounds for length {}",
                            idx,
                            inner_types.len()
                        )));
                    }
                    let inner_name = inner_names.get(idx as usize).unwrap();
                    let inner_type = inner_types.get(idx as usize).unwrap();
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
                            "tuple name `{}` is not exist",
                            name
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

    fn clone_expr_with_replacement<F>(
        &self,
        original_expr: &Expr<'a>,
        replacement_fn: &F,
    ) -> Result<Expr<'a>>
    where
        F: Fn(&Expr) -> Result<Option<Expr<'a>>>,
    {
        let replacement_opt = replacement_fn(original_expr)?;
        match replacement_opt {
            Some(replacement) => Ok(replacement),
            None => match original_expr {
                Expr::IsNull { span, expr, not } => Ok(Expr::IsNull {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    not: *not,
                }),
                Expr::InList {
                    span,
                    expr,
                    list,
                    not,
                } => Ok(Expr::InList {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    list: list
                        .iter()
                        .map(|item| self.clone_expr_with_replacement(item, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    not: *not,
                }),
                Expr::Between {
                    span,
                    expr,
                    low,
                    high,
                    not,
                } => Ok(Expr::Between {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    low: Box::new(self.clone_expr_with_replacement(low.as_ref(), replacement_fn)?),
                    high: Box::new(
                        self.clone_expr_with_replacement(high.as_ref(), replacement_fn)?,
                    ),
                    not: *not,
                }),
                Expr::BinaryOp {
                    span,
                    op,
                    left,
                    right,
                } => Ok(Expr::BinaryOp {
                    span,
                    op: op.clone(),
                    left: Box::new(
                        self.clone_expr_with_replacement(left.as_ref(), replacement_fn)?,
                    ),
                    right: Box::new(
                        self.clone_expr_with_replacement(right.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::UnaryOp { span, op, expr } => Ok(Expr::UnaryOp {
                    span,
                    op: op.clone(),
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Cast {
                    span,
                    expr,
                    target_type,
                    pg_style,
                } => Ok(Expr::Cast {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    target_type: target_type.clone(),
                    pg_style: *pg_style,
                }),
                Expr::TryCast {
                    span,
                    expr,
                    target_type,
                } => Ok(Expr::TryCast {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    target_type: target_type.clone(),
                }),
                Expr::Extract { span, kind, expr } => Ok(Expr::Extract {
                    span,
                    kind: *kind,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Position {
                    span,
                    substr_expr,
                    str_expr,
                } => Ok(Expr::Position {
                    span,
                    substr_expr: Box::new(
                        self.clone_expr_with_replacement(substr_expr.as_ref(), replacement_fn)?,
                    ),
                    str_expr: Box::new(
                        self.clone_expr_with_replacement(str_expr.as_ref(), replacement_fn)?,
                    ),
                }),
                Expr::Substring {
                    span,
                    expr,
                    substring_from,
                    substring_for,
                } => Ok(Expr::Substring {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    substring_from: if let Some(substring_from_expr) = substring_from {
                        Some(Box::new(self.clone_expr_with_replacement(
                            substring_from_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    substring_for: if let Some(substring_for_expr) = substring_for {
                        Some(Box::new(self.clone_expr_with_replacement(
                            substring_for_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                Expr::Trim {
                    span,
                    expr,
                    trim_where,
                } => {
                    Ok(Expr::Trim {
                        span,
                        expr: Box::new(
                            self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                        ),
                        trim_where: if let Some((trim, trim_expr)) = trim_where {
                            Some((
                                trim.clone(),
                                Box::new(self.clone_expr_with_replacement(
                                    trim_expr.as_ref(),
                                    replacement_fn,
                                )?),
                            ))
                        } else {
                            None
                        },
                    })
                }
                Expr::Tuple { span, exprs } => Ok(Expr::Tuple {
                    span,
                    exprs: exprs
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::FunctionCall {
                    span,
                    distinct,
                    name,
                    args,
                    params,
                } => Ok(Expr::FunctionCall {
                    span,
                    distinct: *distinct,
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|arg| self.clone_expr_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    params: params.clone(),
                }),
                Expr::Case {
                    span,
                    operand,
                    conditions,
                    results,
                    else_result,
                } => Ok(Expr::Case {
                    span,
                    operand: if let Some(operand_expr) = operand {
                        Some(Box::new(self.clone_expr_with_replacement(
                            operand_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    conditions: conditions
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    results: results
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    else_result: if let Some(else_result_expr) = else_result {
                        Some(Box::new(self.clone_expr_with_replacement(
                            else_result_expr.as_ref(),
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                }),
                Expr::MapAccess {
                    span,
                    expr,
                    accessor,
                } => Ok(Expr::MapAccess {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    accessor: accessor.clone(),
                }),
                Expr::Array { span, exprs } => Ok(Expr::Array {
                    span,
                    exprs: exprs
                        .iter()
                        .map(|expr| self.clone_expr_with_replacement(expr, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::Interval { span, expr, unit } => Ok(Expr::Interval {
                    span,
                    expr: Box::new(
                        self.clone_expr_with_replacement(expr.as_ref(), replacement_fn)?,
                    ),
                    unit: *unit,
                }),
                Expr::DateAdd {
                    span,
                    unit,
                    interval,
                    date,
                } => Ok(Expr::DateAdd {
                    span,
                    unit: *unit,
                    interval: Box::new(
                        self.clone_expr_with_replacement(interval.as_ref(), replacement_fn)?,
                    ),
                    date: Box::new(
                        self.clone_expr_with_replacement(date.as_ref(), replacement_fn)?,
                    ),
                }),
                _ => Ok(original_expr.clone()),
            },
        }
    }
}
