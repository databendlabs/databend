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

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_ast::ast::Query;
use common_ast::ast::TrimWhere;
use common_ast::ast::UnaryOperator;
use common_ast::parser::error::Backtrace;
use common_ast::parser::error::DisplayError;
use common_ast::parser::parse_expr;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_datavalues::type_coercion::merge_types;
use common_datavalues::ArrayType;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::IntervalKind;
use common_datavalues::IntervalType;
use common_datavalues::NullType;
use common_datavalues::StringType;
use common_datavalues::TimestampType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::is_builtin_function;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::TupleFunction;

use crate::common::ScalarEvaluator;
use crate::sessions::QueryContext;
use crate::sql::binder::Binder;
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
    metadata: MetadataRef,

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,
}

impl<'a> TypeChecker<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        ctx: Arc<QueryContext>,
        metadata: MetadataRef,
    ) -> Self {
        Self {
            bind_context,
            ctx,
            metadata,
            in_aggregate_function: false,
        }
    }

    fn post_resolve(
        &mut self,
        scalar: &Scalar,
        data_type: &DataTypeImpl,
    ) -> Result<(Scalar, DataTypeImpl)> {
        // Try constant folding
        if let Ok((value, value_type)) = ScalarEvaluator::try_create(scalar).and_then(|evaluator| {
            let func_ctx = self.ctx.try_get_function_context()?;
            evaluator.try_eval_const(&func_ctx)
        }) {
            Ok((
                ConstantExpr {
                    value,
                    data_type: value_type,
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
    ) -> Result<(Scalar, DataTypeImpl)> {
        let (scalar, data_type) = match expr {
            Expr::ColumnRef {
                database: _,
                table,
                column,
                ..
            } => {
                let column = self
                    .bind_context
                    .resolve_column(table.clone().map(|ident| ident.name), column)?;
                let data_type = column.data_type.clone();

                (BoundColumnRef { column }.into(), data_type)
            }

            Expr::IsNull {
                span, expr, not, ..
            } => {
                let func_name = if *not {
                    "is_not_null".to_string()
                } else {
                    "is_null".to_string()
                };

                self.resolve_function(span, func_name.as_str(), &[&**expr], required_type)
                    .await?
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                let func_name = if *not {
                    "not_in".to_string()
                } else {
                    "in".to_string()
                };
                let mut args = Vec::with_capacity(list.len() + 1);
                args.push(&**expr);
                for expr in list.iter() {
                    args.push(expr);
                }
                self.resolve_function(span, func_name.as_str(), &args, required_type)
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
                    let (ge_func, _) = self
                        .resolve_function(
                            span,
                            ">=",
                            &[&**expr, &**low],
                            Some(BooleanType::new_impl()),
                        )
                        .await?;
                    let (le_func, _) = self
                        .resolve_function(
                            span,
                            "<=",
                            &[&**expr, &**high],
                            Some(BooleanType::new_impl()),
                        )
                        .await?;
                    (
                        AndExpr {
                            left: Box::new(ge_func),
                            right: Box::new(le_func),
                        }
                        .into(),
                        BooleanType::new_impl(),
                    )
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let (lt_func, _) = self
                        .resolve_function(
                            span,
                            "<",
                            &[&**expr, &**low],
                            Some(BooleanType::new_impl()),
                        )
                        .await?;
                    let (gt_func, _) = self
                        .resolve_function(
                            span,
                            ">",
                            &[&**expr, &**high],
                            Some(BooleanType::new_impl()),
                        )
                        .await?;
                    (
                        OrExpr {
                            left: Box::new(lt_func),
                            right: Box::new(gt_func),
                        }
                        .into(),
                        BooleanType::new_impl(),
                    )
                }
            }

            Expr::BinaryOp {
                span,
                op,
                left,
                right,
                ..
            } => {
                self.resolve_binary_op(span, op, &**left, &**right, required_type)
                    .await?
            }

            Expr::UnaryOp { span, op, expr, .. } => {
                self.resolve_unary_op(span, op, &**expr, required_type)
                    .await?
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let (scalar, data_type) = self.resolve(expr, required_type).await?;
                let cast_func =
                    CastFunction::create("", target_type.to_string().as_str(), data_type.clone())?;
                (
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: data_type,
                        target_type: cast_func.return_type(),
                    }
                    .into(),
                    cast_func.return_type(),
                )
            }

            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut arguments = vec![&**expr];
                match (substring_from, substring_for) {
                    (Some(from_expr), None) => {
                        arguments.push(&**from_expr);
                    }
                    (Some(from_expr), Some(for_expr)) => {
                        arguments.push(&**from_expr);
                        arguments.push(&**for_expr);
                    }
                    _ => return Err(ErrorCode::SemanticError("Invalid arguments of SUBSTRING")),
                }

                self.resolve_function(span, "substring", &arguments, required_type)
                    .await?
            }

            Expr::Literal { lit, .. } => {
                let (value, data_type) = self.resolve_literal(lit, required_type)?;
                (
                    ConstantExpr {
                        value,
                        data_type: data_type.clone(),
                    }
                    .into(),
                    data_type,
                )
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
                if !is_builtin_function(func_name) {
                    return self.resolve_udf(span, func_name, args).await;
                }
                // Check if current function is a context function, e.g. `database`, `version`
                if let Some(ctx_func_result) =
                    self.try_resolve_context_function(span, func_name).await
                {
                    return ctx_func_result;
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
                        .map(|literal| self.resolve_literal(literal, None).map(|(value, _)| value))
                        .collect::<Result<Vec<DataValue>>>()?;

                    self.in_aggregate_function = true;
                    let mut arguments = vec![];
                    for arg in args.iter() {
                        arguments.push(self.resolve(arg, None).await?);
                    }
                    self.in_aggregate_function = false;

                    let data_fields = arguments
                        .iter()
                        .map(|(_, data_type)| DataField::new("", data_type.clone()))
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

                    (
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
                                arguments.into_iter().map(|arg| arg.0).collect()
                            },
                            return_type: agg_func.return_type()?,
                        }
                        .into(),
                        agg_func.return_type()?,
                    )
                } else {
                    // Scalar function
                    self.resolve_function(span, func_name, &args, required_type)
                        .await?
                }
            }

            Expr::CountAll { .. } => {
                let agg_func = AggregateFunctionFactory::instance().get("count", vec![], vec![])?;

                (
                    AggregateFunction {
                        display_name: format!("{:#}", expr),
                        func_name: "count".to_string(),
                        distinct: false,
                        params: vec![],
                        args: vec![],
                        return_type: agg_func.return_type()?,
                    }
                    .into(),
                    agg_func.return_type()?,
                )
            }

            Expr::Exists { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Exists, subquery, true, None)
                    .await?
            }

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, false, None)
                    .await?
            }

            Expr::MapAccess {
                span,
                expr,
                accessor,
            } => {
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

                self.resolve_function(span, "get", &[&**expr, &arg], None)
                    .await?
            }

            Expr::TryCast {
                span,
                expr,
                target_type,
                ..
            } => {
                let (scalar, data_type) = self.resolve(expr, required_type).await?;
                let cast_func = CastFunction::create_try(
                    "",
                    target_type.to_string().as_str(),
                    data_type.clone(),
                )
                .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
                (
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: data_type,
                        target_type: cast_func.return_type(),
                    }
                    .into(),
                    cast_func.return_type(),
                )
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
                date,
                interval,
                unit,
                ..
            } => {
                self.resolve_date_add(span, date, interval, unit, required_type)
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

            Expr::NullIf { span, expr1, expr2 } => {
                // Rewrite NULLIF(expr1, expr2) to IF(expr1 = expr2, NULL, expr1)
                self.resolve_function(
                    span,
                    "if",
                    &[
                        &Expr::BinaryOp {
                            span,
                            op: BinaryOperator::Eq,
                            left: expr1.clone(),
                            right: expr2.clone(),
                        },
                        &Expr::Literal {
                            span,
                            lit: Literal::Null,
                        },
                        expr1.as_ref(),
                    ],
                    None,
                )
                .await?
            }

            Expr::IfNull { span,expr1, expr2, .. } => {
                // Rewrite IFNULL(expr1, expr2) to IF(ISNULL(expr1), expr2, expr1)
                self.resolve_function(
                    "if",
                    &[
                        &Expr::IsNull {
                            span,
                            expr: expr1.clone(),
                            not: false,
                        },
                        expr2.as_ref(),
                        expr1.as_ref(),
                    ],
                    None,
                )
                .await
            }

            _ => Err(ErrorCode::UnImplement(format!(
                "Unsupported expr: {:?}",
                expr
            )))?,
        };

        self.post_resolve(&scalar, &data_type)
    }

    /// Resolve function call.
    pub async fn resolve_function(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: &[&Expr<'_>],
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let (arg, arg_type) = self.resolve(argument, None).await?;
            args.push(arg);
            arg_types.push(arg_type);
        }

        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();

        let func = FunctionFactory::instance()
            .get(func_name, &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok((
            FunctionCall {
                arguments: args,
                func_name: func_name.to_string(),
                arg_types: arg_types.to_vec(),
                return_type: func.return_type(),
            }
            .into(),
            func.return_type(),
        ))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub async fn resolve_binary_op(
        &mut self,
        span: &[Token<'_>],
        op: &BinaryOperator,
        left: &Expr<'_>,
        right: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
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
                let (left, _) = self.resolve(left, None).await?;
                let (right, _) = self.resolve(right, None).await?;

                Ok((
                    ComparisonExpr {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
            BinaryOperator::And => {
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;

                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
            BinaryOperator::Or => {
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;

                Ok((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
        }
    }

    /// Resolve unary expressions.
    pub async fn resolve_unary_op(
        &mut self,
        span: &[Token<'_>],
        op: &UnaryOperator,
        child: &Expr<'_>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
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

    pub async fn resolve_extract_expr(
        &mut self,
        span: &[Token<'_>],
        interval_kind: &IntervalKind,
        arg: &Expr<'_>,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        match interval_kind {
            IntervalKind::Year => {
                self.resolve_function(span, "toYear", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Month => {
                self.resolve_function(span, "toMonth", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Day => {
                self.resolve_function(
                    span,
                    "toDayOfMonth",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
            IntervalKind::Hour => {
                self.resolve_function(span, "toHour", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Minute => {
                self.resolve_function(span, "toMinute", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Second => {
                self.resolve_function(span, "toSecond", &[arg], Some(TimestampType::new_impl(0)))
                    .await
            }
            IntervalKind::Doy => {
                self.resolve_function(
                    span,
                    "toDayOfYear",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
            IntervalKind::Dow => {
                self.resolve_function(
                    span,
                    "toDayOfWeek",
                    &[arg],
                    Some(TimestampType::new_impl(0)),
                )
                .await
            }
        }
    }

    pub async fn resolve_interval(
        &mut self,
        span: &[Token<'_>],
        arg: &Expr<'_>,
        interval_kind: &IntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
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

    pub async fn resolve_date_add(
        &mut self,
        span: &[Token<'_>],
        date: &Expr<'_>,
        interval: &Expr<'_>,
        interval_kind: &IntervalKind,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut args = vec![];
        let mut arg_types = vec![];

        let (date, date_type) = self.resolve(date, None).await?;
        args.push(date);
        arg_types.push(date_type);

        let (interval, interval_type) = self
            .resolve_interval(span, interval, interval_kind, None)
            .await?;
        args.push(interval);
        arg_types.push(interval_type);

        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();

        let func = FunctionFactory::instance()
            .get("date_add", &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok((
            FunctionCall {
                arguments: args,
                func_name: "date_add".to_string(),
                arg_types: arg_types.to_vec(),
                return_type: func.return_type(),
            }
            .into(),
            func.return_type(),
        ))
    }

    pub async fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query<'_>,
        allow_multi_rows: bool,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut binder = Binder::new(
            self.ctx.clone(),
            self.ctx.get_catalogs(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let bind_context = BindContext::with_parent(Box::new(self.bind_context.clone()));
        let (s_expr, output_context) = binder.bind_query(&bind_context, subquery).await?;

        if typ == SubqueryType::Scalar && output_context.columns.len() > 1 {
            return Err(ErrorCode::SemanticError(
                "Scalar subquery must return only one column",
            ));
        }

        let data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        let subquery_expr = SubqueryExpr {
            subquery: s_expr,
            data_type: data_type.clone(),
            allow_multi_rows,
            typ,
            outer_columns: rel_prop.outer_columns,
        };

        Ok((subquery_expr.into(), data_type))
    }

    async fn try_resolve_context_function(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
    ) -> Option<Result<(Scalar, DataTypeImpl)>> {
        match func_name.to_lowercase().as_str() {
            "database" => {
                let arg = Expr::Literal {
                    span: &[],
                    lit: Literal::String(self.ctx.get_current_database()),
                };
                Some(self.resolve_function(span, "database", &[&arg], None).await)
            }
            "version" => {
                let arg = Expr::Literal {
                    span: &[],
                    lit: Literal::String(self.ctx.get_fuse_version()),
                };
                Some(self.resolve_function(span, "version", &[&arg], None).await)
            }
            "current_user" => match self.ctx.get_current_user() {
                Ok(user) => {
                    let arg = Expr::Literal {
                        span: &[],
                        lit: Literal::String(user.identity().to_string()),
                    };
                    Some(
                        self.resolve_function(span, "current_user", &[&arg], None)
                            .await,
                    )
                }
                Err(e) => Some(Err(e)),
            },
            "user" => match self.ctx.get_current_user() {
                Ok(user) => {
                    let arg = Expr::Literal {
                        span: &[],
                        lit: Literal::String(user.identity().to_string()),
                    };
                    Some(self.resolve_function(span, "user", &[&arg], None).await)
                }
                Err(e) => Some(Err(e)),
            },
            "connection_id" => {
                let arg = Expr::Literal {
                    span: &[],
                    lit: Literal::String(self.ctx.get_connection_id()),
                };
                Some(
                    self.resolve_function(span, "connection_id", &[&arg], None)
                        .await,
                )
            }
            "timezone" => {
                let tz = self.ctx.get_settings().get_timezone().unwrap();
                // No need to map err, the tz in settings is valid.
                let tz = String::from_utf8(tz).unwrap();
                let arg = Expr::Literal {
                    span: &[],
                    lit: Literal::String(tz),
                };
                Some(self.resolve_function(span, "timezone", &[&arg], None).await)
            }
            _ => None,
        }
    }

    async fn resolve_trim_function(
        &mut self,
        span: &[Token<'_>],
        expr: &Expr<'_>,
        trim_where: &Option<(TrimWhere, Box<Expr<'_>>)>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let (func_name, trim_scalar) = if let Some((trim_type, trim_expr)) = trim_where {
            let func_name = match trim_type {
                TrimWhere::Leading => "trim_leading",
                TrimWhere::Trailing => "trim_trailing",
                TrimWhere::Both => "trim_both",
            };

            let (trim_scalar, _) = self
                .resolve(trim_expr, Some(StringType::new_impl()))
                .await?;
            (func_name, trim_scalar)
        } else {
            let trim_scalar = ConstantExpr {
                value: DataValue::String(" ".as_bytes().to_vec()),
                data_type: StringType::new_impl(),
            }
            .into();
            ("trim_both", trim_scalar)
        };

        let (trim_source, _) = self.resolve(expr, Some(StringType::new_impl())).await?;
        let args = vec![trim_source, trim_scalar];
        let func = FunctionFactory::instance()
            .get(func_name, &[&StringType::new_impl(); 2])
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;

        Ok((
            FunctionCall {
                arguments: args,
                func_name: func_name.to_string(),
                arg_types: vec![StringType::new_impl(); 2],
                return_type: func.return_type(),
            }
            .into(),
            func.return_type(),
        ))
    }

    /// Resolve literal values.
    pub fn resolve_literal(
        &self,
        literal: &Literal,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(DataValue, DataTypeImpl)> {
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

        Ok((value, data_type))
    }

    // TODO(leiysky): use an array builder function instead, since we should allow declaring
    // an array with variable as element.
    async fn resolve_array(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut elems = Vec::with_capacity(exprs.len());
        let mut types = Vec::with_capacity(exprs.len());
        for expr in exprs.iter() {
            let (arg, data_type) = self.resolve(expr, None).await?;
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
        Ok((
            ConstantExpr {
                value: DataValue::Array(elems),
                data_type: ArrayType::new_impl(element_type.clone()),
            }
            .into(),
            ArrayType::new_impl(element_type),
        ))
    }

    async fn resolve_tuple(
        &mut self,
        span: &[Token<'_>],
        exprs: &[Expr<'_>],
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut args = Vec::with_capacity(exprs.len());
        let mut arg_types = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let (arg, data_type) = self.resolve(expr, None).await?;
            args.push(arg);
            arg_types.push(data_type);
        }
        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();
        let tuple_func = TupleFunction::try_create_func("", &arg_types_ref)
            .map_err(|e| ErrorCode::SemanticError(span.display_error(e.message())))?;
        Ok((
            FunctionCall {
                arguments: args,
                func_name: "tuple".to_string(),
                arg_types,
                return_type: tuple_func.return_type(),
            }
            .into(),
            tuple_func.return_type(),
        ))
    }

    async fn resolve_udf(
        &mut self,
        span: &[Token<'_>],
        func_name: &str,
        arguments: &[Expr<'_>],
    ) -> Result<(Scalar, DataTypeImpl)> {
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
            let backtrace = Backtrace::new();
            let sql_tokens = tokenize_sql(udf.definition.as_str())?;
            let expr = parse_expr(&sql_tokens, &backtrace)?;
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
            Err(ErrorCode::SemanticError(span.display_error(
                "No function matches the given name.".to_string(),
            )))
        }
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
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    not: *not,
                }),
                Expr::InList {
                    span,
                    expr,
                    list,
                    not,
                } => Ok(Expr::InList {
                    span,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
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
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    low: Box::new(self.clone_expr_with_replacement(&**low, replacement_fn)?),
                    high: Box::new(self.clone_expr_with_replacement(&**high, replacement_fn)?),
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
                    left: Box::new(self.clone_expr_with_replacement(&**left, replacement_fn)?),
                    right: Box::new(self.clone_expr_with_replacement(&**right, replacement_fn)?),
                }),
                Expr::UnaryOp { span, op, expr } => Ok(Expr::UnaryOp {
                    span,
                    op: op.clone(),
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                }),
                Expr::Cast {
                    span,
                    expr,
                    target_type,
                    pg_style,
                } => Ok(Expr::Cast {
                    span,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    target_type: target_type.clone(),
                    pg_style: *pg_style,
                }),
                Expr::TryCast {
                    span,
                    expr,
                    target_type,
                } => Ok(Expr::TryCast {
                    span,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    target_type: target_type.clone(),
                }),
                Expr::Extract { span, kind, expr } => Ok(Expr::Extract {
                    span,
                    kind: *kind,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                }),
                Expr::Position {
                    span,
                    substr_expr,
                    str_expr,
                } => Ok(Expr::Position {
                    span,
                    substr_expr: Box::new(
                        self.clone_expr_with_replacement(&**substr_expr, replacement_fn)?,
                    ),
                    str_expr: Box::new(
                        self.clone_expr_with_replacement(&**str_expr, replacement_fn)?,
                    ),
                }),
                Expr::Substring {
                    span,
                    expr,
                    substring_from,
                    substring_for,
                } => Ok(Expr::Substring {
                    span,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    substring_from: if let Some(substring_from_expr) = substring_from {
                        Some(Box::new(self.clone_expr_with_replacement(
                            &**substring_from_expr,
                            replacement_fn,
                        )?))
                    } else {
                        None
                    },
                    substring_for: if let Some(substring_for_expr) = substring_for {
                        Some(Box::new(self.clone_expr_with_replacement(
                            &**substring_for_expr,
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
                } => Ok(Expr::Trim {
                    span,
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    trim_where: if let Some((trim, trim_expr)) = trim_where {
                        Some((
                            trim.clone(),
                            Box::new(
                                self.clone_expr_with_replacement(&**trim_expr, replacement_fn)?,
                            ),
                        ))
                    } else {
                        None
                    },
                }),
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
                            &**operand_expr,
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
                            &**else_result_expr,
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
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
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
                    expr: Box::new(self.clone_expr_with_replacement(&**expr, replacement_fn)?),
                    unit: *unit,
                }),
                Expr::DateAdd {
                    span,
                    date,
                    interval,
                    unit,
                } => Ok(Expr::DateAdd {
                    span,
                    date: Box::new(self.clone_expr_with_replacement(&**date, replacement_fn)?),
                    interval: Box::new(
                        self.clone_expr_with_replacement(&**interval, replacement_fn)?,
                    ),
                    unit: *unit,
                }),
                _ => Ok(original_expr.clone()),
            },
        }
    }
}
