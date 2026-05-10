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

use std::collections::VecDeque;

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IntervalKind as ASTIntervalKind;
use databend_common_ast::ast::Lambda;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SubqueryModifier;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::ast::Window;
use databend_common_ast::ast::WindowDesc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use smallvec::SmallVec;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use super::async_functions::CoreAsyncFunction;
use super::date::AdjacentDayFunction;
use super::date::DateArithmeticFunction;
use super::literal::infer_literal_data_type;
use super::literal::literal_value;
use super::literal::minus_literal_scalar;
use super::rewrite_function::rewrite_function_name;
use super::search::CoreSearchFunction;
use super::set_returning::set_returning_function_name;
use super::special_function::CoreSpecialFunction;
use super::window::CoreWindow;
use super::window::CoreWindowDesc;
use crate::planner::semantic::normalize_identifier;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryType;

#[derive(Clone, Copy)]
pub struct CoreExprId {
    index: usize,
}

pub(super) type CoreExprArgs = SmallVec<[CoreExprId; 4]>;
pub(super) type CoreMapEntries = SmallVec<[(Literal, CoreExprId); 4]>;
pub(super) type CoreFunctionParams = SmallVec<[(String, CoreExprId); 4]>;
pub(super) type CoreOrderByExprs = SmallVec<[CoreOrderByExpr; 4]>;
pub(super) type CoreSearchFunctionArgs = SmallVec<[(String, CoreExprId); 4]>;
pub(super) type CoreUdfCallArgs = SmallVec<[(String, CoreExprId); 4]>;

pub struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
    week_start: u64,
    aggregate_function_factory: &'static AggregateFunctionFactory,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn new(week_start: u64) -> Self {
        Self {
            nodes: Vec::new(),
            week_start,
            aggregate_function_factory: AggregateFunctionFactory::instance(),
        }
    }

    pub(super) fn with_aggregate_function_factory(
        week_start: u64,
        aggregate_function_factory: &'static AggregateFunctionFactory,
    ) -> Self {
        Self {
            nodes: Vec::new(),
            week_start,
            aggregate_function_factory,
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &CoreExpr<'a>> {
        self.nodes.iter()
    }

    #[recursive::recursive]
    pub(super) fn lower_ast_expr(&mut self, expr: &'a Expr) -> Result<CoreExprId> {
        let id = match expr {
            Expr::ColumnRef { span, column } => self.column_ref(*span, column),
            Expr::Literal { span, value } => self.literal(*span, value.clone()),
            Expr::IsNull {
                span,
                expr: child,
                not,
            } => self.lower_is_null_expr(*span, child, *not)?,
            Expr::IsDistinctFrom {
                span,
                left,
                right,
                not,
            } => self.lower_is_distinct_from_expr(*span, left, right, *not)?,
            Expr::Between {
                span,
                expr,
                low,
                high,
                not,
            } => self.lower_between_expr(*span, expr, low, high, *not)?,
            Expr::BinaryOp {
                span,
                op,
                left,
                right,
            } if can_lower_binary_op(op, right) => {
                let left = self.lower_ast_expr(left)?;
                let right = self.lower_ast_expr(right)?;
                self.call(*span, binary_op_core_function(op).unwrap(), smallvec![
                    left, right
                ])
            }
            Expr::BinaryOp {
                span,
                op,
                left,
                right,
            } => self.lower_binary_op_expr(*span, op, left, right)?,
            Expr::JsonOp {
                span,
                op,
                left,
                right,
            } => {
                let left = self.lower_ast_expr(left)?;
                let right = self.lower_ast_expr(right)?;
                self.call(*span, json_op_core_function(op), smallvec![left, right])
            }
            Expr::UnaryOp {
                span,
                op,
                expr: child,
            } => self.lower_unary_op_expr(*span, op, child)?,
            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
            } => {
                self.lower_substring_expr(*span, expr, substring_from, substring_for.as_deref())?
            }
            Expr::Trim {
                span,
                expr,
                trim_where,
            } => self.lower_trim_expr(*span, expr, trim_where)?,
            Expr::Cast {
                expr, target_type, ..
            } => self.cast(expr.span(), expr, target_type.clone(), false)?,
            Expr::TryCast {
                expr, target_type, ..
            } => self.cast(expr.span(), expr, target_type.clone(), true)?,
            Expr::Position {
                span,
                substr_expr,
                str_expr,
            } => {
                self.lower_call_expr(*span, "locate", [substr_expr.as_ref(), str_expr.as_ref()])?
            }
            Expr::Extract { span, kind, expr } | Expr::DatePart { span, kind, expr } => {
                self.lower_extract_expr(*span, kind, expr)?
            }
            Expr::Interval { span, expr, unit } => self.lower_interval_expr(*span, expr, unit)?,
            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
            } => self.lower_date_arith_expr(
                *span,
                unit,
                interval,
                date,
                DateArithmeticFunction::Add,
            )?,
            Expr::DateDiff {
                span,
                unit,
                date_start: interval,
                date_end: date,
            } => self.lower_date_arith_expr(
                *span,
                unit,
                interval,
                date,
                DateArithmeticFunction::Diff,
            )?,
            Expr::DateBetween {
                span,
                unit,
                date_start: interval,
                date_end: date,
            } => self.lower_date_arith_expr(
                *span,
                unit,
                interval,
                date,
                DateArithmeticFunction::Between,
            )?,
            Expr::DateTrunc {
                span, unit, date, ..
            } => self.lower_date_trunc_expr(*span, unit, date, self.week_start)?,
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
            } => self.lower_date_sub_expr(*span, unit, interval, date)?,
            Expr::TimeSlice {
                span,
                unit,
                date,
                slice_length,
                start_or_end,
            } => {
                self.lower_time_slice_expr(*span, date, *slice_length, unit, start_or_end.clone())?
            }
            Expr::LastDay {
                span, unit, date, ..
            } => self.lower_last_day_expr(*span, date, unit)?,
            Expr::PreviousDay {
                span, unit, date, ..
            } => self.lower_previous_or_next_day_expr(
                *span,
                date,
                unit,
                AdjacentDayFunction::Previous,
            )?,
            Expr::NextDay {
                span, unit, date, ..
            } => {
                self.lower_previous_or_next_day_expr(*span, date, unit, AdjacentDayFunction::Next)?
            }
            Expr::MapAccess {
                span,
                expr,
                accessor,
            } => self.lower_map_access_expr(*span, expr, accessor)?,
            Expr::Array { span, exprs } => self.array(*span, exprs)?,
            Expr::Map { span, kvs } => self.map(*span, kvs)?,
            Expr::Tuple { span, exprs } => self.tuple(*span, exprs)?,
            Expr::InList {
                span,
                expr,
                list,
                not,
            } => self.in_list(*span, expr, list, *not)?,
            Expr::Exists {
                span,
                subquery,
                not,
            } => self.subquery(
                *span,
                subquery,
                if *not {
                    SubqueryType::NotExists
                } else {
                    SubqueryType::Exists
                },
                None,
                None,
            ),
            Expr::Subquery { span, subquery, .. } => {
                self.subquery(*span, subquery, SubqueryType::Scalar, None, None)
            }
            Expr::InSubquery {
                span,
                expr,
                subquery,
                not,
            } => {
                let child_expr = self.lower_ast_expr(expr)?;
                let subquery = self.subquery(
                    *span,
                    subquery,
                    SubqueryType::Any,
                    Some(child_expr),
                    Some(SubqueryComparisonOp::Equal),
                );
                if *not {
                    self.call(*span, "not", smallvec![subquery])
                } else {
                    subquery
                }
            }
            Expr::LikeSubquery {
                span,
                expr,
                subquery,
                modifier,
                escape,
            } => self.like_subquery(*span, expr, subquery, modifier, escape)?,
            Expr::LikeAnyWithEscape {
                span,
                left,
                right,
                escape,
            } => {
                let escape = Some(escape.clone());
                self.lower_like_escape_expr(*span, "like_any", left, right, &escape)?
            }
            Expr::LikeWithEscape {
                span,
                left,
                right,
                is_not,
                escape,
            } => {
                let escape = Some(escape.clone());
                self.lower_like_escape_expr(
                    *span,
                    if *is_not { "notlike" } else { "like" },
                    left,
                    right,
                    &escape,
                )?
            }
            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => self.lower_case_expr(
                *span,
                operand.as_deref(),
                conditions,
                results,
                else_result.as_deref(),
            )?,
            expr @ Expr::CountAll { span, window, .. } => {
                if let Some(window) = window.as_ref() {
                    self.count_all_window_function(format!("{:#}", expr), *span, window)?
                } else {
                    self.aggregate_function(
                        format!("{:#}", expr),
                        *span,
                        "count",
                        false,
                        SmallVec::new(),
                        SmallVec::new(),
                        true,
                        SmallVec::new(),
                    )
                }
            }
            expr @ Expr::FunctionCall { span, func } => {
                self.lower_function_call_expr(expr, *span, func)?
            }
            Expr::Hole { span, .. } | Expr::Placeholder { span } => {
                return Err(ErrorCode::SemanticError(
                    "Hole or Placeholder expression is impossible in trivial query".to_string(),
                )
                .set_span(*span));
            }
            Expr::StageLocation { span, location } => self.stage_location(*span, location),
        };
        Ok(id)
    }

    fn column_ref(&mut self, span: Span, column: &'a ColumnRef) -> CoreExprId {
        self.alloc(CoreExpr::ColumnRef { span, column })
    }

    pub(super) fn literal(&mut self, span: Span, value: Literal) -> CoreExprId {
        self.constant(span, literal_value(&value))
    }

    fn constant(&mut self, span: Span, value: Scalar) -> CoreExprId {
        self.alloc(CoreExpr::Literal { span, value })
    }

    #[allow(clippy::too_many_arguments)]
    fn aggregate_function(
        &mut self,
        display_name: String,
        span: Span,
        func_name: impl Into<String>,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::AggregateFunction {
            display_name,
            span,
            func_name: func_name.into(),
            distinct,
            params,
            args,
            remove_count_args,
            order_by,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn aggregate_window_function(
        &mut self,
        display_name: String,
        span: Span,
        func_name: impl Into<String>,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
        window: &'a WindowDesc,
    ) -> Result<CoreExprId> {
        let window = self.lower_window_desc(window)?;
        Ok(self.alloc(CoreExpr::AggregateWindowFunction {
            display_name,
            span,
            func_name: func_name.into(),
            distinct,
            params,
            args,
            remove_count_args,
            order_by,
            window,
        }))
    }

    fn count_all_window_function(
        &mut self,
        display_name: String,
        span: Span,
        window: &'a Window,
    ) -> Result<CoreExprId> {
        let window = self.lower_window(window)?;
        Ok(self.alloc(CoreExpr::CountAllWindowFunction {
            display_name,
            span,
            window,
        }))
    }

    fn general_window_function(
        &mut self,
        display_name: String,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        order_by: &'a [OrderByExpr],
        window: &'a WindowDesc,
    ) -> Result<CoreExprId> {
        let order_by = self.lower_order_by_exprs(order_by)?;
        let window = self.lower_window_desc(window)?;
        Ok(self.alloc(CoreExpr::GeneralWindowFunction {
            display_name,
            span,
            func_name,
            args,
            order_by,
            window,
        }))
    }

    fn array(&mut self, span: Span, exprs: &'a [Expr]) -> Result<CoreExprId> {
        let exprs = self.lower_expr_args(exprs)?;
        Ok(self.alloc(CoreExpr::Array { span, exprs }))
    }

    fn map(&mut self, span: Span, kvs: &'a [(Literal, Expr)]) -> Result<CoreExprId> {
        let kvs = kvs
            .iter()
            .map(|(key, value)| Ok((key.clone(), self.lower_ast_expr(value)?)))
            .collect::<Result<_>>()?;
        Ok(self.alloc(CoreExpr::Map { span, kvs }))
    }

    fn tuple(&mut self, span: Span, exprs: &'a [Expr]) -> Result<CoreExprId> {
        let exprs = self.lower_expr_args(exprs)?;
        Ok(self.alloc(CoreExpr::Tuple { span, exprs }))
    }

    pub(super) fn lower_expr_args(&mut self, exprs: &'a [Expr]) -> Result<CoreExprArgs> {
        exprs.iter().map(|expr| self.lower_ast_expr(expr)).collect()
    }

    fn lower_function_params(&mut self, params: &'a [Expr]) -> Result<CoreFunctionParams> {
        params
            .iter()
            .map(|param| Ok((format!("{:#}", param), self.lower_ast_expr(param)?)))
            .collect()
    }

    pub(super) fn lower_display_expr_args(
        &mut self,
        args: &'a [Expr],
    ) -> Result<CoreSearchFunctionArgs> {
        args.iter()
            .map(|arg| Ok((format!("{:#}", arg), self.lower_ast_expr(arg)?)))
            .collect()
    }

    fn lower_runtime_call_args(&mut self, args: &'a [Expr]) -> Result<CoreUdfCallArgs> {
        args.iter()
            .map(|arg| Ok((format!("{}", arg), self.lower_ast_expr(arg)?)))
            .collect()
    }

    pub(super) fn lower_order_by_exprs(
        &mut self,
        order_by: &'a [OrderByExpr],
    ) -> Result<CoreOrderByExprs> {
        order_by
            .iter()
            .map(
                |OrderByExpr {
                     expr,
                     asc,
                     nulls_first,
                 }| {
                    Ok(CoreOrderByExpr {
                        expr: self.lower_ast_expr(expr)?,
                        asc: *asc,
                        nulls_first: *nulls_first,
                    })
                },
            )
            .collect()
    }

    fn lower_map_access_expr(
        &mut self,
        root_span: Span,
        root_expr: &'a Expr,
        root_accessor: &MapAccessor,
    ) -> Result<CoreExprId> {
        let mut current_span = root_span;
        let mut expr = root_expr;
        let mut accessor = root_accessor;
        let mut paths = VecDeque::new();
        loop {
            let path = match accessor {
                MapAccessor::Bracket {
                    key: box Expr::Literal { value, .. },
                } => {
                    if !matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                        return Err(ErrorCode::SemanticError(format!(
                            "Unsupported accessor: {:?}",
                            value
                        ))
                        .set_span(current_span));
                    }
                    value.clone()
                }
                MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                MapAccessor::DotNumber { key } => Literal::UInt64(*key),
                _ => {
                    return Err(ErrorCode::SemanticError(format!(
                        "Unsupported accessor: {:?}",
                        accessor
                    ))
                    .set_span(current_span));
                }
            };
            paths.push_front((current_span, path));

            let Expr::MapAccess {
                span,
                expr: inner_expr,
                accessor: inner_accessor,
                ..
            } = expr
            else {
                break;
            };
            current_span = *span;
            expr = inner_expr;
            accessor = inner_accessor;
        }
        let expr_span = expr.span();
        let expr = self.lower_ast_expr(expr)?;
        Ok(self.alloc(CoreExpr::MapAccess {
            span: root_span,
            expr_span,
            expr,
            paths,
        }))
    }

    fn lower_function_call_expr(
        &mut self,
        original_expr: &'a Expr,
        span: Span,
        func: &'a ASTFunctionCall,
    ) -> Result<CoreExprId> {
        let ASTFunctionCall {
            distinct,
            name,
            args,
            params,
            order_by,
            window,
            lambda,
        } = func;
        let func_name = normalized_func_name(&name.name);

        if !is_builtin_function(&func_name)
            && !TypeChecker::<super::FullTypeCheckAdapter>::all_special_functions()
                .contains(&Ascii::new(func_name.as_str()))
            && rewrite_function_name(&func_name).is_none()
        {
            return self.runtime_call(span, name, args);
        }

        if lambda.is_none()
            && let Some(func_name) = general_window_function_name(&func_name)
        {
            return match window.as_ref() {
                Some(window) => {
                    let args = self.lower_expr_args(args)?;
                    self.general_window_function(
                        format!("{:#}", original_expr),
                        span,
                        func_name,
                        args,
                        order_by,
                        window,
                    )
                }
                None => Err(ErrorCode::SemanticError(format!(
                    "window function {func_name} can only be used in window clause"
                ))
                .set_span(span)),
            };
        }

        if lambda.is_none() && self.aggregate_function_factory.contains(&func_name) {
            let remove_count_args = can_remove_count_args(&func_name, *distinct, args);
            let params = self.lower_function_params(params)?;
            let args = self.lower_expr_args(args)?;
            let order_by = self.lower_order_by_exprs(order_by)?;
            return Ok(if let Some(window) = window.as_ref() {
                self.aggregate_window_function(
                    format!("{:#}", original_expr),
                    span,
                    func_name,
                    *distinct,
                    params,
                    args,
                    remove_count_args,
                    order_by,
                    window,
                )?
            } else {
                self.aggregate_function(
                    format!("{:#}", original_expr),
                    span,
                    func_name,
                    *distinct,
                    params,
                    args,
                    remove_count_args,
                    order_by,
                )
            });
        }

        let uni_case_func_name = Ascii::new(func_name.as_str());
        if !order_by.is_empty() && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&uni_case_func_name) {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(span));
        }
        if window.is_some()
            && !self.aggregate_function_factory.contains(&func_name)
            && !GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name)
        {
            return Err(ErrorCode::SemanticError(
                "only window and aggregate functions allowed in window syntax",
            )
            .set_span(span));
        }
        if lambda.is_some() && !GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
            return Err(
                ErrorCode::SemanticError("only lambda functions allowed in lambda syntax")
                    .set_span(span),
            );
        }

        if let Some(func_name) = general_lambda_function_name(&func_name) {
            return match lambda.as_ref() {
                Some(lambda) => self.lambda_function(span, func_name, args, lambda),
                None => Err(ErrorCode::SemanticError(format!(
                    "function {func_name} must have a lambda expression",
                ))
                .set_span(span)),
            };
        }

        if let Some(expr) = self.search_function(span, &func_name, args)? {
            return Ok(expr);
        }

        if let Some(expr) = self.async_function(span, &func_name, args)? {
            return Ok(expr);
        }

        if let Some(func_name) = set_returning_function_name(&func_name) {
            let args = self.lower_expr_args(args)?;
            return Ok(self.set_returning_function(span, func_name, args));
        }

        if !*distinct
            && params.is_empty()
            && order_by.is_empty()
            && window.is_none()
            && lambda.is_none()
        {
            if let Some(func_name) = rewrite_function_name(&func_name) {
                return self.lower_rewrite_function(span, func_name, args);
            }

            if let Some(func_name) = special_function_name(&func_name) {
                return self.special_function(span, func_name, args);
            }

            if let Some(func_name) = builtin_scalar_function_name(&func_name) {
                let args = self.lower_expr_args(args)?;
                return Ok(self.call(span, func_name, args));
            }
        }

        let Some(func_name) = builtin_scalar_function_name(&func_name) else {
            return Err(ErrorCode::Internal(format!(
                "function {func_name} should have been classified before scalar lowering",
            )));
        };
        let params = self.lower_function_params(params)?;
        let args = self.lower_expr_args(args)?;
        Ok(self.scalar_function(span, func_name, params, args))
    }

    pub(super) fn lower_unary_op_expr(
        &mut self,
        span: Span,
        op: &'a UnaryOperator,
        child: &'a Expr,
    ) -> Result<CoreExprId> {
        if matches!(op, UnaryOperator::Plus) {
            return self.lower_ast_expr(child);
        }

        if let (UnaryOperator::Minus, Expr::Literal { value, .. }) = (op, child) {
            let value = minus_literal_scalar(span, value)?;
            return Ok(self.constant(span, value));
        }

        let func_name = unary_op_core_function(op).expect("unary plus should have returned");
        let child = self.lower_ast_expr(child)?;
        Ok(self.call(span, func_name, smallvec![child]))
    }

    pub(super) fn lower_call_expr(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: impl IntoIterator<Item = &'a Expr>,
    ) -> Result<CoreExprId> {
        let args = args
            .into_iter()
            .map(|arg| self.lower_ast_expr(arg))
            .collect::<Result<_>>()?;
        Ok(self.call(span, func_name, args))
    }

    pub(super) fn lower_is_null_expr(
        &mut self,
        span: Span,
        child: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let child = self.lower_ast_expr(child)?;
        let is_not_null = self.call(span, "is_not_null", smallvec![child]);
        Ok(if not {
            is_not_null
        } else {
            self.call(span, "not", smallvec![is_not_null])
        })
    }

    fn lower_is_distinct_from_expr(
        &mut self,
        span: Span,
        left: &'a Expr,
        right: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let left_null = self.lower_is_null_expr(span, left, false)?;
        let right_null = self.lower_is_null_expr(span, right, false)?;
        let both_null = self.call(span, "and", smallvec![left_null, right_null]);

        let left_null = self.lower_is_null_expr(span, left, false)?;
        let right_null = self.lower_is_null_expr(span, right, false)?;
        let either_null = self.call(span, "or", smallvec![left_null, right_null]);

        let op = if not { "eq" } else { "noteq" };
        let left = self.lower_ast_expr(left)?;
        let right = self.lower_ast_expr(right)?;
        let compare = self.call(span, op, smallvec![left, right]);

        let both_null_result = self.literal(span, Literal::Boolean(not));
        let either_null_result = self.literal(span, Literal::Boolean(!not));
        let result = self.call(span, "if", smallvec![
            both_null,
            both_null_result,
            either_null,
            either_null_result,
            compare,
        ]);
        Ok(self.call(span, "assume_not_null", smallvec![result]))
    }

    fn lower_between_expr(
        &mut self,
        span: Span,
        expr: &'a Expr,
        low: &'a Expr,
        high: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let expr_low = self.lower_ast_expr(expr)?;
        let low = self.lower_ast_expr(low)?;
        let low_compare = self.call(span, if not { "lt" } else { "gte" }, smallvec![
            expr_low, low
        ]);

        let expr_high = self.lower_ast_expr(expr)?;
        let high = self.lower_ast_expr(high)?;
        let high_compare = self.call(span, if not { "gt" } else { "lte" }, smallvec![
            expr_high, high
        ]);

        Ok(self.call(span, if not { "or" } else { "and" }, smallvec![
            low_compare,
            high_compare
        ]))
    }

    fn lower_case_expr(
        &mut self,
        span: Span,
        operand: Option<&'a Expr>,
        conditions: &'a [Expr],
        results: &'a [Expr],
        else_result: Option<&'a Expr>,
    ) -> Result<CoreExprId> {
        let mut args = CoreExprArgs::with_capacity(conditions.len() * 2 + 1);
        for (condition, result) in conditions.iter().zip(results.iter()) {
            let condition = if let Some(operand) = operand {
                let operand = self.lower_ast_expr(operand)?;
                let condition = self.lower_ast_expr(condition)?;
                self.call(span, "eq", smallvec![operand, condition])
            } else {
                self.lower_ast_expr(condition)?
            };
            args.push(condition);
            args.push(self.lower_ast_expr(result)?);
        }
        args.push(match else_result {
            Some(else_result) => self.lower_ast_expr(else_result)?,
            None => self.literal(None, Literal::Null),
        });
        Ok(self.call(span, "if", args))
    }

    fn lower_interval_expr(
        &mut self,
        span: Span,
        expr: &'a Expr,
        unit: &ASTIntervalKind,
    ) -> Result<CoreExprId> {
        let expr = self.cast(expr.span(), expr, TypeName::String, false)?;
        let unit = self.literal(span, Literal::String(format!(" {}", unit)));
        let concat = self.call(span, "concat", smallvec![expr, unit]);
        Ok(self.call(span, "to_interval", smallvec![concat]))
    }

    pub(super) fn cast(
        &mut self,
        span: Span,
        expr: &'a Expr,
        target_type: TypeName,
        is_try: bool,
    ) -> Result<CoreExprId> {
        let expr = self.lower_ast_expr(expr)?;
        Ok(self.alloc(CoreExpr::Cast {
            span,
            is_try,
            expr,
            target_type,
        }))
    }

    pub(super) fn call(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::Call {
            span,
            func_name,
            args,
        })
    }

    pub(super) fn runtime_call(
        &mut self,
        span: Span,
        name: &'a Identifier,
        args: &'a [Expr],
    ) -> Result<CoreExprId> {
        let args = self.lower_runtime_call_args(args)?;
        Ok(self.alloc(CoreExpr::UdfCall { span, name, args }))
    }

    pub(super) fn lower_binary_op_expr(
        &mut self,
        span: Span,
        op: &'a BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<CoreExprId> {
        let Expr::Subquery {
            subquery,
            modifier: Some(modifier),
            ..
        } = right
        else {
            return self.lower_special_binary_op_expr(span, op, left, right);
        };

        let child_expr = self.lower_ast_expr(left)?;
        match modifier {
            SubqueryModifier::Any | SubqueryModifier::Some => {
                let compare_op = SubqueryComparisonOp::try_from(op)?;
                Ok(self.subquery(
                    span,
                    subquery,
                    SubqueryType::Any,
                    Some(child_expr),
                    Some(compare_op),
                ))
            }
            SubqueryModifier::All => {
                let contrary_op = op.to_contrary()?;
                let compare_op = SubqueryComparisonOp::try_from(&contrary_op)?;
                let subquery = self.subquery(
                    span,
                    subquery,
                    SubqueryType::Any,
                    Some(child_expr),
                    Some(compare_op),
                );
                Ok(self.call(span, "not", smallvec![subquery]))
            }
        }
    }

    fn in_list(
        &mut self,
        span: Span,
        expr: &'a Expr,
        list: &'a [Expr],
        not: bool,
    ) -> Result<CoreExprId> {
        let expr = self.lower_ast_expr(expr)?;
        let list = self.lower_expr_args(list)?;
        Ok(self.alloc(CoreExpr::InList {
            span,
            expr,
            list,
            not,
        }))
    }

    fn subquery(
        &mut self,
        span: Span,
        subquery: &'a Query,
        typ: SubqueryType,
        child_expr: Option<CoreExprId>,
        compare_op: Option<SubqueryComparisonOp>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::Subquery {
            span,
            subquery,
            typ,
            child_expr,
            compare_op,
        })
    }

    fn like_subquery(
        &mut self,
        span: Span,
        expr: &'a Expr,
        subquery: &'a Query,
        modifier: &'a SubqueryModifier,
        escape: &'a Option<String>,
    ) -> Result<CoreExprId> {
        let child_expr = self.lower_ast_expr(expr)?;
        match modifier {
            SubqueryModifier::Any | SubqueryModifier::Some => Ok(self.subquery(
                span,
                subquery,
                SubqueryType::Any,
                Some(child_expr),
                Some(SubqueryComparisonOp::Like(escape.clone())),
            )),
            SubqueryModifier::All => {
                let op = BinaryOperator::Like(escape.clone());
                let contrary_op = op.to_contrary()?;
                let compare_op = SubqueryComparisonOp::try_from(&contrary_op)?;
                let subquery = self.subquery(
                    span,
                    subquery,
                    SubqueryType::Any,
                    Some(child_expr),
                    Some(compare_op),
                );
                Ok(self.call(span, "not", smallvec![subquery]))
            }
        }
    }

    fn stage_location(&mut self, span: Span, location: &'a str) -> CoreExprId {
        self.alloc(CoreExpr::StageLocation { span, location })
    }

    fn lambda_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
        lambda: &'a Lambda,
    ) -> Result<CoreExprId> {
        let args = self.lower_expr_args(args)?;
        let lambda_expr = self.lower_ast_expr(&lambda.expr)?;
        Ok(self.alloc(CoreExpr::LambdaFunction {
            span,
            func_name,
            args,
            lambda_params: &lambda.params,
            lambda_expr,
        }))
    }

    fn scalar_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        params: CoreFunctionParams,
        args: CoreExprArgs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::ScalarFunction {
            span,
            func_name,
            params,
            args,
        })
    }

    pub(super) fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId {
        let index = self.nodes.len();
        self.nodes.push(expr);
        CoreExprId { index }
    }

    pub(super) fn get(&self, id: CoreExprId) -> &CoreExpr<'a> {
        &self.nodes[id.index]
    }
}

pub(super) enum CoreExpr<'a> {
    ColumnRef {
        span: Span,
        column: &'a ColumnRef,
    },
    Literal {
        span: Span,
        value: Scalar,
    },
    Array {
        span: Span,
        exprs: CoreExprArgs,
    },
    Map {
        span: Span,
        kvs: CoreMapEntries,
    },
    Tuple {
        span: Span,
        exprs: CoreExprArgs,
    },
    MapAccess {
        span: Span,
        expr_span: Span,
        expr: CoreExprId,
        paths: VecDeque<(Span, Literal)>,
    },
    Call {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    },
    UdfCall {
        span: Span,
        name: &'a Identifier,
        args: CoreUdfCallArgs,
    },
    LambdaFunction {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        lambda_params: &'a [Identifier],
        lambda_expr: CoreExprId,
    },
    SearchFunction {
        span: Span,
        function: CoreSearchFunction,
    },
    AsyncFunction {
        span: Span,
        function: CoreAsyncFunction<'a>,
    },
    SetReturningFunction {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    },
    ScalarFunction {
        span: Span,
        func_name: &'static str,
        params: CoreFunctionParams,
        args: CoreExprArgs,
    },
    InList {
        span: Span,
        expr: CoreExprId,
        list: CoreExprArgs,
        not: bool,
    },
    Subquery {
        span: Span,
        subquery: &'a Query,
        typ: SubqueryType,
        child_expr: Option<CoreExprId>,
        compare_op: Option<SubqueryComparisonOp>,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: CoreExprId,
        target_type: TypeName,
    },
    SpecialFunction {
        span: Span,
        function: CoreSpecialFunction,
    },
    AggregateFunction {
        display_name: String,
        span: Span,
        func_name: String,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
    },
    AggregateWindowFunction {
        display_name: String,
        span: Span,
        func_name: String,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
        window: CoreWindowDesc<'a>,
    },
    CountAllWindowFunction {
        display_name: String,
        span: Span,
        window: CoreWindow<'a>,
    },
    GeneralWindowFunction {
        display_name: String,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        order_by: CoreOrderByExprs,
        window: CoreWindowDesc<'a>,
    },
    StageLocation {
        span: Span,
        location: &'a str,
    },
}

pub(super) struct CoreOrderByExpr {
    pub(super) expr: CoreExprId,
    pub(super) asc: Option<bool>,
    pub(super) nulls_first: Option<bool>,
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    #[recursive::recursive]
    pub(super) fn resolve_core(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match arena.get(id) {
            CoreExpr::Literal { span, value } => {
                let (value, data_type) = infer_literal_data_type(value.clone());
                Ok(Box::new((
                    crate::plans::ConstantExpr { span: *span, value }.into(),
                    data_type,
                )))
            }
            CoreExpr::Array { span, exprs } => self.resolve_core_array(arena, *span, exprs),
            CoreExpr::Map { span, kvs } => self.resolve_core_map(arena, *span, kvs),
            CoreExpr::Tuple { span, exprs } => self.resolve_core_tuple(arena, *span, exprs),
            CoreExpr::MapAccess {
                span,
                expr_span,
                expr,
                paths,
            } => {
                let box (scalar, data_type) = self.resolve_core(arena, *expr)?;
                self.resolve_map_access_from_scalar(
                    *span,
                    *expr_span,
                    scalar,
                    data_type,
                    paths.clone(),
                )
            }
            CoreExpr::Call {
                span,
                func_name,
                args,
            } => self.resolve_core_call(arena, *span, func_name, args),
            CoreExpr::SetReturningFunction {
                span,
                func_name,
                args,
            } => self.resolve_core_set_returning_function(arena, *span, func_name, args),
            CoreExpr::ScalarFunction {
                span,
                func_name,
                params,
                args,
            } => self.resolve_core_scalar_function(arena, *span, func_name, params, args),
            CoreExpr::Cast {
                span,
                is_try,
                expr,
                target_type,
            } => {
                let box (scalar, data_type) = self.resolve_core(arena, *expr)?;
                self.resolve_cast_expr(*span, scalar, data_type, target_type, *is_try)
            }
            CoreExpr::AggregateFunction {
                display_name,
                span,
                func_name,
                distinct,
                params,
                args,
                remove_count_args,
                order_by,
            } => self.resolve_core_aggregate_function(
                arena,
                display_name,
                *span,
                func_name,
                *distinct,
                params,
                args,
                *remove_count_args,
                order_by,
            ),
            CoreExpr::ColumnRef { span, column } => self.resolve_column_ref(*span, column),
            CoreExpr::SpecialFunction { span, function } => function.resolve(self, arena, *span),
            CoreExpr::UdfCall { span, name, args } => {
                self.resolve_core_udf_call(arena, *span, name, args)
            }
            CoreExpr::LambdaFunction {
                span,
                func_name,
                args,
                lambda_params,
                lambda_expr,
            } => self.resolve_core_lambda_function(
                arena,
                *span,
                func_name,
                args,
                lambda_params,
                *lambda_expr,
            ),
            CoreExpr::SearchFunction { span, function } => {
                self.resolve_core_search_function(arena, *span, function)
            }
            CoreExpr::AsyncFunction { span, function } => {
                self.resolve_async_function(arena, *span, function)
            }
            CoreExpr::InList {
                span,
                expr,
                list,
                not,
            } => self.resolve_core_in_list(arena, *span, *expr, list, *not),
            CoreExpr::Subquery {
                span,
                subquery,
                typ,
                child_expr,
                compare_op,
            } => {
                let child_expr = child_expr
                    .map(|child_expr| self.resolve_core(arena, child_expr))
                    .transpose()?
                    .map(|resolved| *resolved);
                self.resolve_subquery(*span, *typ, subquery, child_expr, compare_op.clone())
            }
            CoreExpr::AggregateWindowFunction {
                display_name,
                span,
                func_name,
                distinct,
                params,
                args,
                remove_count_args,
                order_by,
                window,
            } => self.resolve_core_aggregate_window_function(
                arena,
                display_name,
                *span,
                func_name,
                *distinct,
                params,
                args,
                *remove_count_args,
                order_by,
                window,
            ),
            CoreExpr::CountAllWindowFunction {
                display_name,
                span,
                window,
            } => self.resolve_core_count_all_window_function(arena, *span, display_name, window),
            CoreExpr::GeneralWindowFunction {
                display_name,
                span,
                func_name,
                args,
                order_by,
                window,
            } => self.resolve_core_general_window_function(
                arena,
                *span,
                display_name,
                func_name,
                args,
                order_by,
                window,
            ),
            CoreExpr::StageLocation { span, location } => {
                self.resolve_stage_location(*span, location)
            }
        }
    }

    fn resolve_core_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if TypeChecker::<super::FullTypeCheckAdapter>::all_special_functions()
            .contains(&Ascii::new(func_name))
            || rewrite_function_name(func_name).is_some()
        {
            return Err(ErrorCode::Internal(format!(
                "special function {} should not be represented as core call",
                func_name
            )));
        }

        let mut scalars = SmallVec::<[ScalarExpr; 4]>::with_capacity(args.len());
        for arg in args {
            let box (scalar, _) = self.resolve_core(arena, *arg)?;
            scalars.push(scalar);
        }

        if self.should_try_rewrite_variant_function(func_name) {
            let mut arg_types = SmallVec::<[DataType; 4]>::with_capacity(scalars.len());
            for scalar in &scalars {
                let mut data_type = scalar.data_type()?;
                if let ScalarExpr::SubqueryExpr(subquery) = scalar
                    && subquery.typ == SubqueryType::Scalar
                    && !data_type.is_nullable()
                {
                    data_type = data_type.wrap_nullable();
                }
                arg_types.push(data_type);
            }
            if let Some(rewritten_variant_expr) =
                self.try_rewrite_variant_function(span, func_name, &scalars, &arg_types)
            {
                return rewritten_variant_expr;
            }
        }
        if Self::is_vector_function(func_name)
            && let Some(rewritten_vector_expr) =
                self.try_rewrite_vector_function(span, func_name, &scalars)
        {
            return rewritten_vector_expr;
        }
        let box (scalar, data_type) =
            self.resolve_scalar_function_call(span, func_name, vec![], scalars.into_vec())?;
        if func_name == "eq" || func_name == "noteq" {
            self.rewrite_variant_compare_constant(scalar, data_type)
        } else {
            Ok(Box::new((scalar, data_type)))
        }
    }

    pub(super) fn resolve_core_udf_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        name: &Identifier,
        args: &CoreUdfCallArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let udf_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
        if let Some(udf) = self.resolve_udf(arena, span, &udf_name, args)? {
            return Ok(udf);
        }
        Err(self.unknown_function_error(span, &udf_name))
    }

    fn resolve_core_scalar_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        params: &CoreFunctionParams,
        args: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let params = self.resolve_core_function_params(arena, span, params, "scalar")?;
        let (scalars, _) = self.resolve_core_expr_args(arena, args)?;

        if self.should_try_rewrite_variant_function(func_name) {
            let mut arg_types = Vec::with_capacity(scalars.len());
            for scalar in &scalars {
                let mut data_type = scalar.data_type()?;
                if let ScalarExpr::SubqueryExpr(subquery) = scalar
                    && subquery.typ == SubqueryType::Scalar
                    && !data_type.is_nullable()
                {
                    data_type = data_type.wrap_nullable();
                }
                arg_types.push(data_type);
            }
            if let Some(rewritten_variant_expr) =
                self.try_rewrite_variant_function(span, func_name, &scalars, &arg_types)
            {
                return rewritten_variant_expr;
            }
        }
        if Self::is_vector_function(func_name)
            && let Some(rewritten_vector_expr) =
                self.try_rewrite_vector_function(span, func_name, &scalars)
        {
            return rewritten_vector_expr;
        }

        self.resolve_scalar_function_call(span, func_name, params, scalars)
    }

    pub(super) fn resolve_core_expr_args(
        &mut self,
        arena: &CoreExprArena<'_>,
        args: &CoreExprArgs,
    ) -> Result<(Vec<ScalarExpr>, Vec<DataType>)> {
        let mut scalars = Vec::with_capacity(args.len());
        let mut data_types = Vec::with_capacity(args.len());
        for arg in args {
            let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
            scalars.push(scalar);
            data_types.push(data_type);
        }
        Ok((scalars, data_types))
    }

    fn resolve_core_in_list(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        expr: CoreExprId,
        list: &CoreExprArgs,
        not: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (expr_scalar, _) = self.resolve_core(arena, expr)?;
        let max_inlist_to_or = self.adapter.settings().get_max_inlist_to_or()? as usize;

        if list.len() > max_inlist_to_or
            && list
                .iter()
                .all(|item| satisfy_core_contain_func(arena, *item))
        {
            let (list_scalars, _) = self.resolve_core_expr_args(arena, list)?;
            let box (array, _) =
                self.resolve_scalar_function_call(span, "array", vec![], list_scalars)?;
            let box (array, _) =
                self.resolve_scalar_function_call(span, "array_distinct", vec![], vec![array])?;
            let box (contains, data_type) =
                self.resolve_scalar_function_call(span, "contains", vec![], vec![
                    array,
                    expr_scalar,
                ])?;
            return if not {
                self.resolve_scalar_function_call(span, "not", vec![], vec![contains])
            } else {
                Ok(Box::new((contains, data_type)))
            };
        }

        let mut result = None;
        for item in list {
            let box (item, _) = self.resolve_core(arena, *item)?;
            let box (predicate, _) =
                self.resolve_scalar_function_call(span, "eq", vec![], vec![
                    expr_scalar.clone(),
                    item,
                ])?;
            result = Some(match result {
                None => predicate,
                Some(acc) => {
                    let box (or_predicate, _) =
                        self.resolve_scalar_function_call(span, "or", vec![], vec![
                            acc, predicate,
                        ])?;
                    or_predicate
                }
            });
        }

        let result = result.expect("IN list should not be empty");
        let data_type = result.data_type()?;
        if not {
            self.resolve_scalar_function_call(span, "not", vec![], vec![result])
        } else {
            Ok(Box::new((result, data_type)))
        }
    }

    pub(super) fn resolve_core_function_params(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        params: &CoreFunctionParams,
        kind: &str,
    ) -> Result<Vec<Scalar>> {
        let mut new_params = Vec::with_capacity(params.len());
        for (display_name, param) in params {
            let box (scalar, _) = self.resolve_core(arena, *param)?;
            let expr = scalar.as_expr()?;
            let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let constant = expr
                .into_constant()
                .map_err(|_| {
                    ErrorCode::SemanticError(format!(
                        "invalid parameter {display_name} for {kind} function, expected constant",
                    ))
                    .set_span(span)
                })?
                .scalar;
            new_params.push(constant);
        }
        Ok(new_params)
    }
}

fn normalized_func_name(func_name: &str) -> String {
    if func_name.chars().any(char::is_uppercase) {
        func_name.to_lowercase()
    } else {
        func_name.to_string()
    }
}

fn general_window_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    GENERAL_WINDOW_FUNCTIONS
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

fn general_lambda_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    GENERAL_LAMBDA_FUNCTIONS
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

fn special_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    TypeChecker::<super::FullTypeCheckAdapter>::all_special_functions()
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

fn builtin_scalar_function_name(func_name: &str) -> Option<&'static str> {
    if !TypeChecker::<super::FullTypeCheckAdapter>::can_lower_core_scalar_function(func_name) {
        return None;
    }

    let functions: &'static databend_common_expression::FunctionRegistry = &BUILTIN_FUNCTIONS;
    if let Some((name, _)) = functions.funcs.get_key_value(func_name) {
        return Some(name.as_str());
    }
    if let Some((name, _)) = functions.factories.get_key_value(func_name) {
        return Some(name.as_str());
    }
    if let Some((alias, _)) = functions.aliases.get_key_value(func_name) {
        return Some(alias.as_str());
    }
    None
}

fn satisfy_core_contain_func(arena: &CoreExprArena<'_>, expr: CoreExprId) -> bool {
    match arena.get(expr) {
        CoreExpr::Literal { value, .. } => !matches!(value, Scalar::Null),
        CoreExpr::Tuple { exprs, .. } | CoreExpr::Array { exprs, .. } => exprs
            .iter()
            .all(|expr| satisfy_core_contain_func(arena, *expr)),
        _ => false,
    }
}

fn can_remove_count_args(func_name: &str, distinct: bool, args: &[Expr]) -> bool {
    func_name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal { value, .. } if *value != Literal::Null))
}

fn can_lower_binary_op(op: &BinaryOperator, right: &Expr) -> bool {
    if matches!(right, Expr::Subquery {
        modifier: Some(_),
        ..
    }) {
        return false;
    }

    if matches!(
        op,
        BinaryOperator::Like(_)
            | BinaryOperator::NotLike(_)
            | BinaryOperator::LikeAny(_)
            | BinaryOperator::Regexp
            | BinaryOperator::RLike
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike
            | BinaryOperator::SoundsLike
    ) {
        return false;
    }

    binary_op_core_function(op)
        .map(TypeChecker::<super::FullTypeCheckAdapter>::can_lower_core_scalar_function)
        .unwrap_or(false)
}

pub(super) fn binary_op_core_function(op: &BinaryOperator) -> Option<&'static str> {
    let func_name = match op {
        BinaryOperator::Plus => "plus",
        BinaryOperator::Minus => "minus",
        BinaryOperator::Multiply => "multiply",
        BinaryOperator::Div => "div",
        BinaryOperator::Divide => "divide",
        BinaryOperator::IntDiv => "intdiv",
        BinaryOperator::Modulo => "modulo",
        BinaryOperator::StringConcat => "concat",
        BinaryOperator::Gt => "gt",
        BinaryOperator::Lt => "lt",
        BinaryOperator::Gte => "gte",
        BinaryOperator::Lte => "lte",
        BinaryOperator::Eq => "eq",
        BinaryOperator::NotEq => "noteq",
        BinaryOperator::Caret => "pow",
        BinaryOperator::And => "and",
        BinaryOperator::Or => "or",
        BinaryOperator::Xor => "xor",
        BinaryOperator::LikeAny(_) => "like_any",
        BinaryOperator::Like(_) => "like",
        BinaryOperator::Regexp => "regexp",
        BinaryOperator::RLike => "rlike",
        BinaryOperator::BitwiseOr => "bit_or",
        BinaryOperator::BitwiseAnd => "bit_and",
        BinaryOperator::BitwiseXor => "bit_xor",
        BinaryOperator::BitwiseShiftLeft => "bit_shift_left",
        BinaryOperator::BitwiseShiftRight => "bit_shift_right",
        BinaryOperator::CosineDistance => "cosine_distance",
        BinaryOperator::L1Distance => "l1_distance",
        BinaryOperator::L2Distance => "l2_distance",
        BinaryOperator::NotLike(_)
        | BinaryOperator::NotRegexp
        | BinaryOperator::NotRLike
        | BinaryOperator::SoundsLike => return None,
    };
    Some(func_name)
}

pub(super) fn like_op_core_function(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Like(_) => Some("like"),
        BinaryOperator::NotLike(_) => Some("notlike"),
        BinaryOperator::LikeAny(_) => Some("like_any"),
        BinaryOperator::Regexp => Some("regexp"),
        BinaryOperator::RLike => Some("rlike"),
        BinaryOperator::NotRegexp => Some("notregexp"),
        BinaryOperator::NotRLike => Some("notrlike"),
        _ => None,
    }
}

fn json_op_core_function(op: &databend_common_ast::ast::JsonOperator) -> &'static str {
    match op {
        databend_common_ast::ast::JsonOperator::Arrow => "get",
        databend_common_ast::ast::JsonOperator::LongArrow => "get_string",
        databend_common_ast::ast::JsonOperator::HashArrow => "get_by_keypath",
        databend_common_ast::ast::JsonOperator::HashLongArrow => "get_by_keypath_string",
        databend_common_ast::ast::JsonOperator::Question => "json_exists_key",
        databend_common_ast::ast::JsonOperator::QuestionOr => "json_exists_any_keys",
        databend_common_ast::ast::JsonOperator::QuestionAnd => "json_exists_all_keys",
        databend_common_ast::ast::JsonOperator::AtArrow => "json_contains_in_left",
        databend_common_ast::ast::JsonOperator::ArrowAt => "json_contains_in_right",
        databend_common_ast::ast::JsonOperator::AtQuestion => "json_path_exists",
        databend_common_ast::ast::JsonOperator::AtAt => "json_path_match",
        databend_common_ast::ast::JsonOperator::HashMinus => "delete_by_keypath",
    }
}

pub(super) fn unary_op_core_function(op: &UnaryOperator) -> Option<&'static str> {
    let func_name = match op {
        UnaryOperator::Plus => return None,
        UnaryOperator::Minus => "minus",
        UnaryOperator::Not => "not",
        UnaryOperator::Factorial => "factorial",
        UnaryOperator::SquareRoot => "sqrt",
        UnaryOperator::CubeRoot => "cbrt",
        UnaryOperator::Abs => "abs",
        UnaryOperator::BitwiseNot => "bit_not",
    };
    Some(func_name)
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::Identifier;
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    fn assert_sql_lowers_to(sql: &str, check: impl FnOnce(&CoreExprArena<'_>, CoreExprId)) {
        let tokens = tokenize_sql(sql).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut arena = CoreExprArena::new(0);
        let root = arena.lower_ast_expr(&expr).unwrap();
        check(&arena, root);
    }

    fn assert_expr_lowers_to(expr: &Expr, check: impl FnOnce(&CoreExprArena<'_>, CoreExprId)) {
        let mut arena = CoreExprArena::new(0);
        let root = arena.lower_ast_expr(expr).unwrap();
        check(&arena, root);
    }

    fn assert_sql_lower_error_contains(sql: &str, expected: &str) {
        let tokens = tokenize_sql(sql).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut arena = CoreExprArena::new(0);
        let err = match arena.lower_ast_expr(&expr) {
            Ok(_) => panic!("expected lower to fail for `{sql}`"),
            Err(err) => err,
        };
        assert!(
            err.message().contains(expected),
            "expected error to contain `{expected}`, got `{}`",
            err.message()
        );
    }

    #[test]
    fn lowers_collection_exprs_without_legacy_ast() {
        assert_sql_lowers_to("[1, 2, 3]", |arena, root| {
            let CoreExpr::Array { exprs, .. } = arena.get(root) else {
                panic!("array should lower to CoreExpr::Array");
            };
            assert!(
                exprs
                    .iter()
                    .all(|expr| matches!(arena.get(*expr), CoreExpr::Literal { .. }))
            );
        });

        assert_sql_lowers_to("{'k1': 1, 'k2': 2}", |arena, root| {
            let CoreExpr::Map { kvs, .. } = arena.get(root) else {
                panic!("map should lower to CoreExpr::Map");
            };
            assert!(
                kvs.iter()
                    .all(|(_key, value)| matches!(arena.get(*value), CoreExpr::Literal { .. }))
            );
        });

        assert_sql_lowers_to("(1, 'a', true)", |arena, root| {
            let CoreExpr::Tuple { exprs, .. } = arena.get(root) else {
                panic!("tuple should lower to CoreExpr::Tuple");
            };
            assert!(
                exprs
                    .iter()
                    .all(|expr| matches!(arena.get(*expr), CoreExpr::Literal { .. }))
            );
        });
    }

    #[test]
    fn lowers_map_access_without_legacy_ast() {
        let expr = Expr::MapAccess {
            span: None,
            expr: Box::new(Expr::Literal {
                span: None,
                value: Literal::String("{\"k1\":1}".to_string()),
            }),
            accessor: MapAccessor::Colon {
                key: Identifier::from_name(None, "k1"),
            },
        };
        assert_expr_lowers_to(&expr, |arena, root| {
            let CoreExpr::MapAccess { expr, .. } = arena.get(root) else {
                panic!("map access should lower to CoreExpr::MapAccess");
            };
            assert!(matches!(arena.get(*expr), CoreExpr::Literal { .. }));
        });
    }

    #[test]
    fn lowers_dependency_boundaries_without_legacy_ast() {
        assert_sql_lowers_to("a", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::ColumnRef { .. }));
        });

        assert_sql_lowers_to("a IN (1, 2)", |arena, root| {
            let CoreExpr::InList { expr, list, .. } = arena.get(root) else {
                panic!("IN list should lower to CoreExpr::InList");
            };
            assert!(matches!(arena.get(*expr), CoreExpr::ColumnRef { .. }));
            assert!(
                list.iter()
                    .all(|item| matches!(arena.get(*item), CoreExpr::Literal { .. }))
            );
        });

        assert_sql_lowers_to("EXISTS (SELECT 1)", |arena, root| {
            let CoreExpr::Subquery { typ, .. } = arena.get(root) else {
                panic!("EXISTS should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Exists);
        });

        assert_sql_lowers_to("(SELECT 1)", |arena, root| {
            let CoreExpr::Subquery { typ, .. } = arena.get(root) else {
                panic!("scalar subquery should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Scalar);
        });

        assert_sql_lowers_to("a IN (SELECT 1)", |arena, root| {
            let CoreExpr::Subquery {
                typ,
                child_expr,
                compare_op,
                ..
            } = arena.get(root)
            else {
                panic!("IN subquery should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Any);
            assert!(child_expr.is_some());
            assert_eq!(compare_op, &Some(SubqueryComparisonOp::Equal));
        });

        assert_sql_lowers_to("a NOT IN (SELECT 1)", |arena, root| {
            let CoreExpr::Call {
                func_name: "not",
                args,
                ..
            } = arena.get(root)
            else {
                panic!("NOT IN subquery should lower to a not call");
            };
            let CoreExpr::Subquery {
                typ,
                child_expr,
                compare_op,
                ..
            } = arena.get(args[0])
            else {
                panic!("NOT IN child should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Any);
            assert!(child_expr.is_some());
            assert_eq!(compare_op, &Some(SubqueryComparisonOp::Equal));
        });

        assert_sql_lowers_to("a = ANY (SELECT 1)", |arena, root| {
            let CoreExpr::Subquery {
                typ,
                child_expr,
                compare_op,
                ..
            } = arena.get(root)
            else {
                panic!("= ANY should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Any);
            assert!(child_expr.is_some());
            assert_eq!(compare_op, &Some(SubqueryComparisonOp::Equal));
        });

        assert_sql_lowers_to("a = ALL (SELECT 1)", |arena, root| {
            let CoreExpr::Call {
                func_name: "not",
                args,
                ..
            } = arena.get(root)
            else {
                panic!("= ALL should lower to a not call");
            };
            let CoreExpr::Subquery {
                typ,
                child_expr,
                compare_op,
                ..
            } = arena.get(args[0])
            else {
                panic!("= ALL child should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Any);
            assert!(child_expr.is_some());
            assert_eq!(compare_op, &Some(SubqueryComparisonOp::NotEqual));
        });

        assert_sql_lowers_to("a LIKE ANY (SELECT 'x') ESCAPE '!'", |arena, root| {
            let CoreExpr::Subquery {
                typ,
                child_expr,
                compare_op,
                ..
            } = arena.get(root)
            else {
                panic!("LIKE ANY should lower to CoreExpr::Subquery");
            };
            assert_eq!(typ, &SubqueryType::Any);
            assert!(child_expr.is_some());
            assert_eq!(
                compare_op,
                &Some(SubqueryComparisonOp::Like(Some("!".to_string())))
            );
        });

        assert_sql_lowers_to("a LIKE 'x' ESCAPE '!'", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::Call {
                func_name: "like",
                ..
            }));
        });

        assert_sql_lowers_to("array_filter([1], x -> x > 0)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::LambdaFunction { .. }));
        });

        assert_sql_lowers_to("score()", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::SearchFunction { .. }));
        });

        assert_sql_lowers_to("nextval(seq)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::AsyncFunction { .. }));
        });

        assert_sql_lowers_to("unnest([1, 2])", |arena, root| {
            assert!(matches!(
                arena.get(root),
                CoreExpr::SetReturningFunction { .. }
            ));
        });

        assert_sql_lowers_to("abs(DISTINCT 1)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::ScalarFunction { .. }));
        });
    }

    #[test]
    fn splits_static_and_runtime_function_names() {
        assert_sql_lowers_to("1 + 2", |arena, root| {
            let CoreExpr::Call { func_name, .. } = arena.get(root) else {
                panic!("operator lower should use static CoreExpr::Call");
            };
            assert_eq!(*func_name, "plus");
        });

        assert_sql_lowers_to("ABS(1)", |arena, root| {
            let CoreExpr::Call { func_name, .. } = arena.get(root) else {
                panic!("builtin scalar function should lower to static CoreExpr::Call");
            };
            assert_eq!(*func_name, "abs");
        });

        assert_sql_lowers_to("potential_udf(1)", |arena, root| {
            let CoreExpr::UdfCall { name, .. } = arena.get(root) else {
                panic!("only potential UDF calls should keep the runtime function path");
            };
            assert_eq!(name.name, "potential_udf");
        });

        assert_sql_lowers_to("ROW_NUMBER() OVER ()", |arena, root| {
            let CoreExpr::GeneralWindowFunction { func_name, .. } = arena.get(root) else {
                panic!("general window function should lower to CoreExpr::GeneralWindowFunction");
            };
            assert_eq!(*func_name, "row_number");
        });

        assert_sql_lowers_to("date_add(day, 1, date)", |arena, root| {
            let CoreExpr::Call { func_name, .. } = arena.get(root) else {
                panic!("date lower should use static CoreExpr::Call");
            };
            assert_eq!(*func_name, "add_days");
        });
    }

    #[test]
    fn rejects_unsupported_date_interval_before_function_resolution() {
        assert_sql_lower_error_contains(
            "date_diff(isoweek, date, date)",
            "Unsupported interval type ISOWEEK for date_diff",
        );
        assert_sql_lower_error_contains(
            "date_add(microsecond, 1, date)",
            "Unsupported interval type MICROSECOND for date_add/date_sub",
        );
    }
}
