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
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::IntervalKind as ASTIntervalKind;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::OrderByExpr;
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
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::RANK_WINDOW_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use smallvec::SmallVec;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use super::date::AdjacentDayFunction;
use super::date::DateArithmeticFunction;
use super::literal::infer_literal_data_type;
use super::literal::literal_value;
use super::literal::minus_literal_scalar;
use crate::binder::ExprContext;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryType;
use crate::plans::WindowFuncType;

#[derive(Clone, Copy)]
pub(super) struct CoreExprId {
    index: usize,
}

pub(super) type CoreExprArgs = SmallVec<[CoreExprId; 4]>;
pub(super) type CoreMapEntries = SmallVec<[(Literal, CoreExprId); 4]>;
pub(super) type CoreFunctionParams = SmallVec<[(String, CoreExprId); 4]>;
pub(super) type CoreOrderByExprs = SmallVec<[CoreOrderByExpr; 4]>;
pub(super) type AstExprArgs<'a> = SmallVec<[&'a Expr; 4]>;
pub(super) type SugarFunctionArgs<'a> = AstExprArgs<'a>;

pub(super) struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
    week_start: u64,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn new(week_start: u64) -> Self {
        Self {
            nodes: Vec::new(),
            week_start,
        }
    }

    pub(super) fn legacy_ast(&mut self, expr: &'a Expr) -> CoreExprId {
        self.alloc(CoreExpr::LegacyAst(expr))
    }

    #[recursive::recursive]
    pub(super) fn lower_ast_expr(&mut self, expr: &'a Expr) -> Result<CoreExprId> {
        Ok(match expr {
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
                self.call(*span, op.to_func_name(), smallvec![left, right])
            }
            Expr::JsonOp {
                span,
                op,
                left,
                right,
            } => {
                let left = self.lower_ast_expr(left)?;
                let right = self.lower_ast_expr(right)?;
                self.call(*span, op.to_func_name(), smallvec![left, right])
            }
            expr @ Expr::UnaryOp {
                span,
                op,
                expr: child,
            } => self.lower_unary_op_expr(expr, *span, op, child)?,
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
                ..
            } => self.lower_trim_expr(*span, expr, trim_where)?,
            Expr::Cast {
                expr, target_type, ..
            } => self.cast(expr.span(), expr, target_type.clone(), false)?,
            Expr::TryCast {
                expr, target_type, ..
            } => self.cast(expr.span(), expr, target_type.clone(), true)?,
            Expr::Position {
                substr_expr,
                str_expr,
                span,
            } => {
                self.lower_call_expr(*span, "locate", [substr_expr.as_ref(), str_expr.as_ref()])?
            }
            Expr::Extract { span, kind, expr } | Expr::DatePart { span, kind, expr }
                if can_lower_extract(kind) =>
            {
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
            } if can_lower_date_trunc(unit) => {
                self.lower_date_trunc_expr(*span, unit, date, self.week_start)?
            }
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
            } if can_lower_time_slice(*slice_length, unit, start_or_end) => {
                self.lower_time_slice_expr(*span, date, *slice_length, unit, start_or_end.clone())?
            }
            Expr::LastDay {
                span, unit, date, ..
            } if can_lower_last_day(unit) => self.lower_last_day_expr(*span, date, unit)?,
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
            expr @ Expr::MapAccess { span, .. } => self.lower_map_access_expr(expr, *span)?,
            Expr::Array { span, exprs } => self.array(*span, exprs)?,
            Expr::Map { span, kvs } => self.map(*span, kvs)?,
            Expr::Tuple { span, exprs } => self.tuple(*span, exprs)?,
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
                let call = self.aggregate_call(
                    format!("{:#}", expr),
                    *span,
                    "count",
                    false,
                    SmallVec::new(),
                    SmallVec::new(),
                    true,
                    SmallVec::new(),
                );
                if let Some(window) = window.as_ref() {
                    self.window_function(CoreWindowFunction::Aggregate {
                        call: Box::new(call),
                        window: CoreAggregateWindow::CountAll(window),
                    })
                } else {
                    self.aggregate_function(call)
                }
            }
            expr @ Expr::FunctionCall { span, func } => {
                self.lower_function_call_expr(expr, *span, func)?
            }
            _ => self.legacy_ast(expr),
        })
    }

    pub(super) fn literal(&mut self, span: Span, value: Literal) -> CoreExprId {
        self.constant(span, literal_value(&value))
    }

    fn constant(&mut self, span: Span, value: Scalar) -> CoreExprId {
        self.alloc(CoreExpr::Literal { span, value })
    }

    pub(super) fn sugar_function(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: AstExprArgs<'a>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::SugarFunction {
            span,
            func_name: func_name.into(),
            args,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn aggregate_call(
        &mut self,
        display_name: String,
        span: Span,
        func_name: impl Into<String>,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
    ) -> CoreAggregateCall {
        CoreAggregateCall {
            display_name,
            span,
            func_name: func_name.into(),
            distinct,
            params,
            args,
            remove_count_args,
            order_by,
        }
    }

    fn aggregate_function(&mut self, call: CoreAggregateCall) -> CoreExprId {
        self.alloc(CoreExpr::AggregateFunction { call })
    }

    fn window_function(&mut self, function: CoreWindowFunction<'a>) -> CoreExprId {
        self.alloc(CoreExpr::WindowFunction { function })
    }

    fn missing_window_function(&mut self, span: Span, func_name: impl Into<String>) -> CoreExprId {
        self.alloc(CoreExpr::MissingWindowFunction {
            span,
            func_name: func_name.into(),
        })
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

    fn lower_expr_args(&mut self, exprs: &'a [Expr]) -> Result<CoreExprArgs> {
        exprs.iter().map(|expr| self.lower_ast_expr(expr)).collect()
    }

    fn lower_function_params(&mut self, params: &'a [Expr]) -> Result<CoreFunctionParams> {
        params
            .iter()
            .map(|param| Ok((format!("{:#}", param), self.lower_ast_expr(param)?)))
            .collect()
    }

    fn lower_order_by_exprs(&mut self, order_by: &'a [OrderByExpr]) -> Result<CoreOrderByExprs> {
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

    fn lower_map_access_expr(&mut self, expr: &'a Expr, root_span: Span) -> Result<CoreExprId> {
        let mut expr = expr;
        let mut paths = VecDeque::new();
        while let Expr::MapAccess {
            span,
            expr: inner_expr,
            accessor,
        } = expr
        {
            expr = &**inner_expr;
            let path = match accessor {
                MapAccessor::Bracket {
                    key: box Expr::Literal { value, .. },
                } => {
                    if !matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                        return Err(ErrorCode::SemanticError(format!(
                            "Unsupported accessor: {:?}",
                            value
                        ))
                        .set_span(*span));
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
                    .set_span(*span));
                }
            };
            paths.push_front((*span, path));
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

        if lambda.is_none() && GENERAL_WINDOW_FUNCTIONS.contains(&Ascii::new(func_name.as_str())) {
            return Ok(match window.as_ref() {
                Some(window) => self.window_function(CoreWindowFunction::General {
                    display_name: format!("{:#}", original_expr),
                    span,
                    func_name,
                    args: args.iter().collect(),
                    order_by,
                    window,
                }),
                None => self.missing_window_function(span, func_name),
            });
        }

        if lambda.is_none() && AggregateFunctionFactory::instance().contains(&func_name) {
            let remove_count_args = can_remove_count_args(&func_name, *distinct, args);
            let params = self.lower_function_params(params)?;
            let args = self.lower_expr_args(args)?;
            let order_by = self.lower_order_by_exprs(order_by)?;
            let call = self.aggregate_call(
                format!("{:#}", original_expr),
                span,
                func_name,
                *distinct,
                params,
                args,
                remove_count_args,
                order_by,
            );
            return Ok(if let Some(window) = window.as_ref() {
                self.window_function(CoreWindowFunction::Aggregate {
                    call: Box::new(call),
                    window: CoreAggregateWindow::Function(window),
                })
            } else {
                self.aggregate_function(call)
            });
        }

        if !*distinct
            && params.is_empty()
            && order_by.is_empty()
            && window.is_none()
            && lambda.is_none()
        {
            if TypeChecker::all_sugar_functions().contains(&Ascii::new(func_name.as_str())) {
                return if TypeChecker::can_lower_core_sugar_function(&func_name) {
                    self.lower_sugar_function(span, &func_name, args.iter().collect())
                } else {
                    Ok(self.sugar_function(span, func_name, args.iter().collect()))
                };
            }

            if TypeChecker::can_lower_core_scalar_function(&func_name) {
                let args = args
                    .iter()
                    .map(|arg| self.lower_ast_expr(arg))
                    .collect::<Result<_>>()?;
                return Ok(self.call(span, func_name, args));
            }
        }

        Ok(self.legacy_ast(original_expr))
    }

    fn lower_unary_op_expr(
        &mut self,
        original_expr: &'a Expr,
        span: Span,
        op: &UnaryOperator,
        child: &'a Expr,
    ) -> Result<CoreExprId> {
        if matches!(op, UnaryOperator::Plus) {
            return self.lower_ast_expr(child);
        }

        if let (UnaryOperator::Minus, Expr::Literal { value, .. }) = (op, child) {
            let value = minus_literal_scalar(span, value)?;
            return Ok(self.constant(span, value));
        }

        let func_name = op.to_func_name();
        if TypeChecker::can_lower_core_scalar_function(&func_name) {
            let child = self.lower_ast_expr(child)?;
            return Ok(self.call(span, func_name, smallvec![child]));
        }

        Ok(self.legacy_ast(original_expr))
    }

    pub(super) fn lower_call_expr(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
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
        func_name: impl Into<String>,
        args: CoreExprArgs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::Call {
            span,
            func_name: func_name.into(),
            args,
        })
    }

    fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId {
        let index = self.nodes.len();
        self.nodes.push(expr);
        CoreExprId { index }
    }

    fn get(&self, id: CoreExprId) -> &CoreExpr<'a> {
        &self.nodes[id.index]
    }
}

pub(super) enum CoreExpr<'a> {
    LegacyAst(&'a Expr),
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
        func_name: String,
        args: CoreExprArgs,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: CoreExprId,
        target_type: TypeName,
    },
    SugarFunction {
        span: Span,
        func_name: String,
        args: AstExprArgs<'a>,
    },
    AggregateFunction {
        call: CoreAggregateCall,
    },
    WindowFunction {
        function: CoreWindowFunction<'a>,
    },
    MissingWindowFunction {
        span: Span,
        func_name: String,
    },
}

pub(super) struct CoreAggregateCall {
    display_name: String,
    span: Span,
    func_name: String,
    distinct: bool,
    params: CoreFunctionParams,
    args: CoreExprArgs,
    remove_count_args: bool,
    order_by: CoreOrderByExprs,
}

pub(super) struct CoreOrderByExpr {
    expr: CoreExprId,
    asc: Option<bool>,
    nulls_first: Option<bool>,
}

#[derive(Clone, Copy)]
pub(super) enum CoreAggregateWindow<'a> {
    CountAll(&'a Window),
    Function(&'a WindowDesc),
}

pub(super) enum CoreWindowFunction<'a> {
    Aggregate {
        call: Box<CoreAggregateCall>,
        window: CoreAggregateWindow<'a>,
    },
    General {
        display_name: String,
        span: Span,
        func_name: String,
        args: AstExprArgs<'a>,
        order_by: &'a [OrderByExpr],
        window: &'a WindowDesc,
    },
}

impl<'a> TypeChecker<'a> {
    #[recursive::recursive]
    pub(super) fn resolve_core(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match arena.get(id) {
            CoreExpr::LegacyAst(expr) => self.resolve_legacy_ast(expr),
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
            CoreExpr::SugarFunction {
                span,
                func_name,
                args,
            } => {
                if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
                    self.try_rewrite_sugar_function(*span, func_name, args),
                ) {
                    return rewritten_func_result;
                }
                self.resolve_function(*span, func_name, vec![], args)
            }
            CoreExpr::Call {
                span,
                func_name,
                args,
            } => {
                if Self::all_sugar_functions().contains(&Ascii::new(func_name.as_str())) {
                    return Err(ErrorCode::Internal(format!(
                        "sugar function {} should not be represented as core call",
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
                        self.try_rewrite_variant_function(*span, func_name, &scalars, &arg_types)
                    {
                        return rewritten_variant_expr;
                    }
                }
                if Self::is_vector_function(func_name) {
                    if let Some(rewritten_vector_expr) =
                        self.try_rewrite_vector_function(*span, func_name, &scalars)
                    {
                        return rewritten_vector_expr;
                    }
                }
                let box (scalar, data_type) = self.resolve_scalar_function_call(
                    *span,
                    func_name,
                    vec![],
                    scalars.into_vec(),
                )?;
                if func_name == "eq" || func_name == "noteq" {
                    self.rewrite_variant_compare_constant(scalar, data_type)
                } else {
                    Ok(Box::new((scalar, data_type)))
                }
            }
            CoreExpr::Cast {
                span,
                is_try,
                expr,
                target_type,
            } => {
                let box (scalar, data_type) = self.resolve_core(arena, *expr)?;
                self.resolve_cast_expr(*span, scalar, data_type, target_type, *is_try)
            }
            CoreExpr::AggregateFunction { call } => {
                self.resolve_core_aggregate_function(arena, call)
            }
            CoreExpr::WindowFunction { function } => {
                self.resolve_core_window_function(arena, function)
            }
            CoreExpr::MissingWindowFunction { span, func_name } => Err(ErrorCode::SemanticError(
                format!("window function {func_name} can only be used in window clause"),
            )
            .set_span(*span)),
        }
    }

    fn resolve_core_aggregate_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        call: &CoreAggregateCall,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (new_agg_func, data_type) = self.resolve_core_aggregate_call(arena, call, false)?;
        Ok(Box::new((new_agg_func.into(), data_type)))
    }

    fn resolve_core_window_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        function: &CoreWindowFunction<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match function {
            CoreWindowFunction::Aggregate { call, window } => {
                let (new_agg_func, _data_type) =
                    self.resolve_core_aggregate_call(arena, call, true)?;
                let window = match window {
                    CoreAggregateWindow::CountAll(window) => *window,
                    CoreAggregateWindow::Function(window_desc) => {
                        if window_desc.ignore_nulls.is_some() {
                            return Err(ErrorCode::SemanticError(format!(
                                "window function {} not support IGNORE/RESPECT NULLS option",
                                call.func_name
                            ))
                            .set_span(call.span));
                        }
                        &window_desc.window
                    }
                };
                let func = WindowFuncType::Aggregate(new_agg_func);
                self.resolve_window(call.span, call.display_name.clone(), window, func)
            }
            CoreWindowFunction::General {
                display_name,
                span,
                func_name,
                args,
                order_by,
                window,
            } => {
                if !order_by.is_empty() {
                    return Err(ErrorCode::SemanticError(
                        "only aggregate functions allowed in within group syntax",
                    )
                    .set_span(*span));
                }
                if !RANK_WINDOW_FUNCTIONS.contains(&func_name.as_str())
                    && window.ignore_nulls.is_some()
                {
                    return Err(ErrorCode::SemanticError(format!(
                        "window function {} not support IGNORE/RESPECT NULLS option",
                        func_name
                    ))
                    .set_span(*span));
                }
                let func = self.resolve_general_window_function(
                    *span,
                    func_name,
                    args,
                    &window.ignore_nulls,
                )?;
                self.resolve_window(*span, display_name.clone(), &window.window, func)
            }
        }
    }

    fn resolve_core_aggregate_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        call: &CoreAggregateCall,
        in_window_call: bool,
    ) -> Result<(crate::plans::AggregateFunction, DataType)> {
        if !call.order_by.is_empty()
            && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&Ascii::new(call.func_name.as_str()))
        {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(call.span));
        }
        let new_params =
            self.resolve_core_function_params(arena, call.span, &call.params, "aggregate")?;
        let in_window = self.in_window_function;
        self.in_window_function = self.in_window_function || in_window_call;
        let in_aggregate_function = self.in_aggregate_function;
        let result = self.resolve_core_aggregate_call_inner(arena, call, new_params);
        self.in_window_function = in_window;
        self.in_aggregate_function = in_aggregate_function;
        result
    }

    fn resolve_core_aggregate_call_inner(
        &mut self,
        arena: &CoreExprArena<'_>,
        call: &CoreAggregateCall,
        new_params: Vec<Scalar>,
    ) -> Result<(crate::plans::AggregateFunction, DataType)> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "aggregate functions can not be used in lambda function".to_string(),
            )
            .set_span(call.span));
        }

        if self.in_aggregate_function {
            if self.in_window_function {
                // An aggregate may appear as the argument of a window aggregate,
                // but grouped aggregates cannot be nested.
                self.in_window_function = false;
            } else {
                self.in_aggregate_function = false;
                return Err(ErrorCode::SemanticError(
                    "aggregate function calls cannot be nested".to_string(),
                )
                .set_span(call.span));
            }
        }

        // Only force aggregate arguments to skip alias resolution in contexts
        // that would otherwise prefer aliases over input columns, such as
        // HAVING or ORDER BY. In the SELECT list we still want the existing
        // column-first fallback so `sum(c1)` can bind a same-select alias when
        // there is no real `c1` column.
        self.in_aggregate_function = true;
        let original_context = self.bind_context.expr_context;
        let disallow_alias_resolution = original_context.prefer_resolve_alias();
        if disallow_alias_resolution {
            self.bind_context.expr_context = ExprContext::InAggregateFunction;
        }
        let arguments_result = self.resolve_core_expr_args(arena, &call.args);
        if disallow_alias_resolution {
            self.bind_context.expr_context = original_context;
        }
        self.in_aggregate_function = false;
        let (arguments, arg_types) = arguments_result?;

        let sort_descs = call
            .order_by
            .iter()
            .map(|order_by| {
                if disallow_alias_resolution {
                    self.bind_context.expr_context = ExprContext::InAggregateFunction;
                }
                let result = self.resolve_core(arena, order_by.expr);
                if disallow_alias_resolution {
                    self.bind_context.expr_context = original_context;
                }
                let box (scalar_expr, _) = result?;

                Ok(AggregateFunctionScalarSortDesc {
                    expr: scalar_expr,
                    is_reuse_index: false,
                    nulls_first: order_by.nulls_first.unwrap_or(false),
                    asc: order_by.asc.unwrap_or(true),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.resolve_aggregate_function(
            call.span,
            &call.func_name,
            call.display_name.clone(),
            call.distinct,
            new_params,
            arguments,
            arg_types,
            sort_descs,
            call.remove_count_args,
        )
    }

    fn resolve_core_expr_args(
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

    fn resolve_core_function_params(
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

    TypeChecker::can_lower_core_scalar_function(&op.to_func_name())
}

fn can_lower_date_trunc(kind: &ASTIntervalKind) -> bool {
    matches!(
        kind,
        ASTIntervalKind::Year
            | ASTIntervalKind::ISOYear
            | ASTIntervalKind::Quarter
            | ASTIntervalKind::Month
            | ASTIntervalKind::Week
            | ASTIntervalKind::ISOWeek
            | ASTIntervalKind::Day
            | ASTIntervalKind::Hour
            | ASTIntervalKind::Minute
            | ASTIntervalKind::Second
    )
}

fn can_lower_extract(kind: &ASTIntervalKind) -> bool {
    matches!(
        kind,
        ASTIntervalKind::ISOYear
            | ASTIntervalKind::Year
            | ASTIntervalKind::Quarter
            | ASTIntervalKind::Month
            | ASTIntervalKind::Day
            | ASTIntervalKind::Hour
            | ASTIntervalKind::Minute
            | ASTIntervalKind::Second
            | ASTIntervalKind::Doy
            | ASTIntervalKind::Dow
            | ASTIntervalKind::Week
            | ASTIntervalKind::Epoch
            | ASTIntervalKind::MicroSecond
            | ASTIntervalKind::ISODow
            | ASTIntervalKind::YearWeek
            | ASTIntervalKind::Millennium
    )
}

fn can_lower_time_slice(slice_length: u64, kind: &ASTIntervalKind, start_or_end: &str) -> bool {
    slice_length >= 1
        && (start_or_end.eq_ignore_ascii_case("start") || start_or_end.eq_ignore_ascii_case("end"))
        && matches!(
            kind,
            ASTIntervalKind::Year
                | ASTIntervalKind::Quarter
                | ASTIntervalKind::Month
                | ASTIntervalKind::Week
                | ASTIntervalKind::ISOWeek
                | ASTIntervalKind::Day
                | ASTIntervalKind::Hour
                | ASTIntervalKind::Minute
                | ASTIntervalKind::Second
        )
}

fn can_lower_last_day(kind: &ASTIntervalKind) -> bool {
    matches!(
        kind,
        ASTIntervalKind::Year
            | ASTIntervalKind::Quarter
            | ASTIntervalKind::Month
            | ASTIntervalKind::Week
    )
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
}
