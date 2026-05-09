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
use databend_common_expression::FunctionKind;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_SEARCH_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use smallvec::SmallVec;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use super::date::AdjacentDayFunction;
use super::date::DateArithmeticFunction;
use super::literal::infer_literal_data_type;
use super::literal::literal_value;
use super::literal::minus_literal_scalar;
use crate::planner::semantic::normalize_identifier;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryType;

#[derive(Clone, Copy)]
pub struct CoreExprId {
    index: usize,
}

pub(super) type CoreExprArgs = SmallVec<[CoreExprId; 4]>;
pub(super) type CoreMapEntries = SmallVec<[(Literal, CoreExprId); 4]>;
pub(super) type CoreFunctionParams = SmallVec<[(String, CoreExprId); 4]>;
pub(super) type CoreOrderByExprs = SmallVec<[CoreOrderByExpr; 4]>;
pub(super) type AstExprArgs<'a> = SmallVec<[&'a Expr; 4]>;
pub(super) type SugarFunctionArgs<'a> = AstExprArgs<'a>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoreExprContextDependencies {
    /// Pure expression evaluation plus builtin scalar resolution.
    ///
    /// Typical callers: default/constraint expressions, statement settings,
    /// expression parser helpers, and scalar binder paths that do not need
    /// catalog, subquery, async, or UDF behavior.
    pub scalar_evaluation: bool,

    /// Name resolution against the current bind context, including virtual
    /// columns and masking-policy fallback.
    pub column_resolution: bool,

    /// Session/catalog/user/version/variable sugar functions.
    pub session_function: bool,

    /// Subquery and IN-list-to-subquery lowering paths that need binder-style
    /// planning support.
    pub subquery: bool,

    /// Builtin async functions such as sequence, dictionary, and read_file.
    pub async_function: bool,

    /// Potential UDF resolution and UDF execution metadata.
    pub udf: bool,
}

impl CoreExprContextDependencies {
    pub fn all() -> Self {
        Self {
            scalar_evaluation: true,
            column_resolution: true,
            session_function: true,
            subquery: true,
            async_function: true,
            udf: true,
        }
    }

    fn require_udf(&mut self) {
        self.udf = true;
    }

    fn require_async_function(&mut self) {
        self.async_function = true;
    }

    fn require_sugar_function(&mut self, func_name: &str) {
        match func_name.to_ascii_lowercase().as_str() {
            "current_catalog"
            | "database"
            | "currentdatabase"
            | "current_database"
            | "version"
            | "user"
            | "currentuser"
            | "current_user"
            | "current_role"
            | "current_secondary_roles"
            | "current_available_roles"
            | "connection_id"
            | "client_session_id"
            | "timezone"
            | "last_query_id"
            | "array_sort"
            | "getvariable" => self.session_function = true,
            _ => {}
        }
    }

    fn contains(self, required: Self) -> bool {
        (!required.scalar_evaluation || self.scalar_evaluation)
            && (!required.column_resolution || self.column_resolution)
            && (!required.session_function || self.session_function)
            && (!required.subquery || self.subquery)
            && (!required.async_function || self.async_function)
            && (!required.udf || self.udf)
    }

    fn missing_from(self, allowed: Self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.scalar_evaluation && !allowed.scalar_evaluation {
            missing.push("scalar_evaluation");
        }
        if self.column_resolution && !allowed.column_resolution {
            missing.push("column_resolution");
        }
        if self.session_function && !allowed.session_function {
            missing.push("session_function");
        }
        if self.subquery && !allowed.subquery {
            missing.push("subquery");
        }
        if self.async_function && !allowed.async_function {
            missing.push("async_function");
        }
        if self.udf && !allowed.udf {
            missing.push("udf");
        }
        missing
    }
}

pub trait CoreExprContextPolicy {
    fn allowed_core_expr_context_dependencies(&self) -> CoreExprContextDependencies;
}

pub struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
    week_start: u64,
    allowed_context_dependencies: CoreExprContextDependencies,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn new(week_start: u64) -> Self {
        Self {
            nodes: Vec::new(),
            week_start,
            allowed_context_dependencies: CoreExprContextDependencies::all(),
        }
    }

    pub(super) fn with_context_policy<P>(week_start: u64, context_policy: &P) -> Self
    where P: CoreExprContextPolicy + ?Sized {
        Self {
            nodes: Vec::new(),
            week_start,
            allowed_context_dependencies: context_policy.allowed_core_expr_context_dependencies(),
        }
    }

    pub(super) fn context_dependencies(&self) -> CoreExprContextDependencies {
        let mut dependencies = CoreExprContextDependencies::default();
        for node in &self.nodes {
            node.add_context_dependencies(&mut dependencies);
        }
        dependencies
    }

    pub(super) fn check_context_policy(&self) -> Result<()> {
        if self.allowed_context_dependencies == CoreExprContextDependencies::all() {
            return Ok(());
        }

        let required = self.context_dependencies();
        if self.allowed_context_dependencies.contains(required) {
            return Ok(());
        }

        let missing = required.missing_from(self.allowed_context_dependencies);
        Err(ErrorCode::SemanticError(format!(
            "type check context does not allow required capabilities: {}",
            missing.join(", ")
        )))
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
            } => self.binary_op(*span, op, left, right),
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
            expr @ Expr::MapAccess { span, .. } => self.lower_map_access_expr(expr, *span)?,
            Expr::Array { span, exprs } => self.array(*span, exprs)?,
            Expr::Map { span, kvs } => self.map(*span, kvs)?,
            Expr::Tuple { span, exprs } => self.tuple(*span, exprs)?,
            Expr::InList {
                span,
                expr,
                list,
                not,
            } => self.in_list(*span, expr, list, *not),
            Expr::Exists { subquery, not, .. } => self.exists(subquery, *not),
            Expr::Subquery { subquery, .. } => self.scalar_subquery(subquery),
            Expr::InSubquery {
                span,
                expr,
                subquery,
                not,
            } => self.in_subquery(*span, expr, subquery, *not),
            Expr::LikeSubquery {
                span,
                expr,
                subquery,
                modifier,
                escape,
            } => self.like_subquery(*span, expr, subquery, modifier, escape),
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
                    self.count_all_window_function(format!("{:#}", expr), *span, window)
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
        self.check_context_policy()?;
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
    ) -> CoreExprId {
        self.alloc(CoreExpr::AggregateWindowFunction {
            display_name,
            span,
            func_name: func_name.into(),
            distinct,
            params,
            args,
            remove_count_args,
            order_by,
            window,
        })
    }

    fn count_all_window_function(
        &mut self,
        display_name: String,
        span: Span,
        window: &'a Window,
    ) -> CoreExprId {
        self.alloc(CoreExpr::CountAllWindowFunction {
            display_name,
            span,
            window,
        })
    }

    fn general_window_function(
        &mut self,
        display_name: String,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        order_by: &'a [OrderByExpr],
        window: &'a WindowDesc,
    ) -> CoreExprId {
        self.alloc(CoreExpr::GeneralWindowFunction {
            display_name,
            span,
            func_name,
            args,
            order_by,
            window,
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

        if !is_builtin_function(&func_name)
            && !TypeChecker::<super::FullTypeCheckPolicy>::all_sugar_functions()
                .contains(&Ascii::new(func_name.as_str()))
        {
            return Ok(self.runtime_call(span, name, args));
        }

        if lambda.is_none()
            && let Some(func_name) = general_window_function_name(&func_name)
        {
            return match window.as_ref() {
                Some(window) => {
                    let args = self.lower_expr_args(args)?;
                    Ok(self.general_window_function(
                        format!("{:#}", original_expr),
                        span,
                        func_name,
                        args,
                        order_by,
                        window,
                    ))
                }
                None => Err(ErrorCode::SemanticError(format!(
                    "window function {func_name} can only be used in window clause"
                ))
                .set_span(span)),
            };
        }

        if lambda.is_none() && AggregateFunctionFactory::instance().contains(&func_name) {
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
                )
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
            && !AggregateFunctionFactory::instance().contains(&func_name)
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
                Some(lambda) => {
                    Ok(self.lambda_function(span, func_name, args.iter().collect(), lambda))
                }
                None => Err(ErrorCode::SemanticError(format!(
                    "function {func_name} must have a lambda expression",
                ))
                .set_span(span)),
            };
        }

        if let Some(func_name) = general_search_function_name(&func_name) {
            return Ok(self.search_function(span, func_name, args.iter().collect()));
        }

        if let Some(func_name) = async_function_name(&func_name) {
            return Ok(self.async_function(span, func_name, args.iter().collect()));
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
            if TypeChecker::<super::FullTypeCheckPolicy>::all_sugar_functions()
                .contains(&Ascii::new(func_name.as_str()))
            {
                return if TypeChecker::<super::FullTypeCheckPolicy>::can_lower_core_sugar_function(
                    &func_name,
                ) {
                    self.lower_sugar_function(span, &func_name, args.iter().collect())
                } else {
                    Ok(self.sugar_function(span, func_name, args.iter().collect()))
                };
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

    fn lower_unary_op_expr(
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
    ) -> CoreExprId {
        self.alloc(CoreExpr::RuntimeCall { span, name, args })
    }

    fn binary_op(
        &mut self,
        span: Span,
        op: &'a BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    ) -> CoreExprId {
        self.alloc(CoreExpr::BinaryOp {
            span,
            op,
            left,
            right,
        })
    }

    fn in_list(&mut self, span: Span, expr: &'a Expr, list: &'a [Expr], not: bool) -> CoreExprId {
        self.alloc(CoreExpr::InList {
            span,
            expr,
            list,
            not,
        })
    }

    fn exists(&mut self, subquery: &'a Query, not: bool) -> CoreExprId {
        self.alloc(CoreExpr::Exists { subquery, not })
    }

    fn scalar_subquery(&mut self, subquery: &'a Query) -> CoreExprId {
        self.alloc(CoreExpr::ScalarSubquery { subquery })
    }

    fn in_subquery(
        &mut self,
        span: Span,
        expr: &'a Expr,
        subquery: &'a Query,
        not: bool,
    ) -> CoreExprId {
        self.alloc(CoreExpr::InSubquery {
            span,
            expr,
            subquery,
            not,
        })
    }

    fn like_subquery(
        &mut self,
        span: Span,
        expr: &'a Expr,
        subquery: &'a Query,
        modifier: &'a SubqueryModifier,
        escape: &'a Option<String>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::LikeSubquery {
            span,
            expr,
            subquery,
            modifier,
            escape,
        })
    }

    fn stage_location(&mut self, span: Span, location: &'a str) -> CoreExprId {
        self.alloc(CoreExpr::StageLocation { span, location })
    }

    fn lambda_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
        lambda: &'a Lambda,
    ) -> CoreExprId {
        self.alloc(CoreExpr::LambdaFunction {
            span,
            func_name,
            args,
            lambda,
        })
    }

    fn search_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::SearchFunction {
            span,
            func_name,
            args,
        })
    }

    fn async_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::AsyncFunction {
            span,
            func_name,
            args,
        })
    }

    fn set_returning_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::SetReturningFunction {
            span,
            func_name,
            args,
        })
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

    fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId {
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
    RuntimeCall {
        span: Span,
        name: &'a Identifier,
        args: &'a [Expr],
    },
    LambdaFunction {
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
        lambda: &'a Lambda,
    },
    SearchFunction {
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
    },
    AsyncFunction {
        span: Span,
        func_name: &'static str,
        args: AstExprArgs<'a>,
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
    BinaryOp {
        span: Span,
        op: &'a BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    },
    InList {
        span: Span,
        expr: &'a Expr,
        list: &'a [Expr],
        not: bool,
    },
    Exists {
        subquery: &'a Query,
        not: bool,
    },
    ScalarSubquery {
        subquery: &'a Query,
    },
    InSubquery {
        span: Span,
        expr: &'a Expr,
        subquery: &'a Query,
        not: bool,
    },
    LikeSubquery {
        span: Span,
        expr: &'a Expr,
        subquery: &'a Query,
        modifier: &'a SubqueryModifier,
        escape: &'a Option<String>,
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
        window: &'a WindowDesc,
    },
    CountAllWindowFunction {
        display_name: String,
        span: Span,
        window: &'a Window,
    },
    GeneralWindowFunction {
        display_name: String,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        order_by: &'a [OrderByExpr],
        window: &'a WindowDesc,
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

impl<'a> CoreExpr<'a> {
    fn add_context_dependencies(&self, dependencies: &mut CoreExprContextDependencies) {
        match self {
            CoreExpr::ColumnRef { .. } => {
                dependencies.column_resolution = true;
            }
            CoreExpr::Call { .. }
            | CoreExpr::ScalarFunction { .. }
            | CoreExpr::AggregateFunction { .. }
            | CoreExpr::Array { .. }
            | CoreExpr::Map { .. }
            | CoreExpr::Tuple { .. }
            | CoreExpr::MapAccess { .. }
            | CoreExpr::Cast { .. } => {
                dependencies.scalar_evaluation = true;
            }
            CoreExpr::RuntimeCall { .. } => {
                dependencies.require_udf();
            }
            CoreExpr::LambdaFunction { .. } => {
                dependencies.column_resolution = true;
            }
            CoreExpr::SearchFunction { .. } => {}
            CoreExpr::AsyncFunction { .. } => {
                dependencies.require_async_function();
            }
            CoreExpr::SetReturningFunction { .. } => {}
            CoreExpr::BinaryOp { .. }
            | CoreExpr::InList { .. }
            | CoreExpr::InSubquery { .. }
            | CoreExpr::LikeSubquery { .. } => {
                dependencies.column_resolution = true;
                dependencies.subquery = true;
            }
            CoreExpr::Exists { .. } | CoreExpr::ScalarSubquery { .. } => {
                dependencies.subquery = true;
            }
            CoreExpr::SugarFunction { func_name, .. } => {
                dependencies.require_sugar_function(func_name);
            }
            CoreExpr::AggregateWindowFunction { .. } | CoreExpr::GeneralWindowFunction { .. } => {}
            CoreExpr::CountAllWindowFunction { .. } => {}
            CoreExpr::StageLocation { .. } => {}
            CoreExpr::Literal { .. } => {}
        }
    }
}

impl<'a, P> TypeChecker<'a, P>
where P: super::TypeCheckPolicy
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
            CoreExpr::SugarFunction {
                span,
                func_name,
                args,
            } => {
                if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
                    self.try_rewrite_sugar_function(*span, func_name.as_str(), args.as_slice()),
                ) {
                    return rewritten_func_result;
                }
                self.resolve_function(*span, func_name.as_str(), vec![], args.as_slice())
            }
            CoreExpr::RuntimeCall { span, name, args } => {
                self.resolve_core_runtime_call(*span, name, args)
            }
            CoreExpr::LambdaFunction {
                span,
                func_name,
                args,
                lambda,
            } => self.resolve_lambda_function(*span, func_name, args.as_slice(), lambda),
            CoreExpr::SearchFunction {
                span,
                func_name,
                args,
            } => match *func_name {
                "score" => self.resolve_score_search_function(*span, func_name, args.as_slice()),
                "match" => self.resolve_match_search_function(*span, func_name, args.as_slice()),
                "query" => self.resolve_query_search_function(*span, func_name, args.as_slice()),
                _ => Err(ErrorCode::SemanticError(format!(
                    "cannot find search function {}",
                    func_name
                ))
                .set_span(*span)),
            },
            CoreExpr::AsyncFunction {
                span,
                func_name,
                args,
            } => self.resolve_async_function(*span, func_name, args.as_slice()),
            CoreExpr::BinaryOp {
                span,
                op,
                left,
                right,
            } => self.resolve_binary_op_or_subquery(span, op, left, right),
            CoreExpr::InList {
                span,
                expr,
                list,
                not,
            } => self.resolve_in_list(*span, expr, list, *not),
            CoreExpr::Exists { subquery, not } => self.resolve_subquery(
                if !not {
                    SubqueryType::Exists
                } else {
                    SubqueryType::NotExists
                },
                subquery,
                None,
                None,
            ),
            CoreExpr::ScalarSubquery { subquery } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, None, None)
            }
            CoreExpr::InSubquery {
                span,
                expr,
                subquery,
                not,
            } => self.resolve_in_subquery(*span, expr, subquery, *not),
            CoreExpr::LikeSubquery {
                span,
                expr,
                subquery,
                modifier,
                escape,
            } => self.resolve_scalar_subquery(
                subquery,
                expr,
                span,
                span,
                modifier,
                &BinaryOperator::Like((*escape).clone()),
            ),
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
            } => self.resolve_core_count_all_window_function(*span, display_name, window),
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
        if Self::all_sugar_functions().contains(&Ascii::new(func_name)) {
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

    pub(super) fn resolve_core_runtime_call(
        &mut self,
        span: Span,
        name: &Identifier,
        args: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let udf_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
        if let Some(udf) = self.resolve_udf(span, &udf_name, args)? {
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

fn general_search_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    GENERAL_SEARCH_FUNCTIONS
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

fn async_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    ASYNC_FUNCTIONS
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

fn builtin_scalar_function_name(func_name: &str) -> Option<&'static str> {
    if !TypeChecker::<super::FullTypeCheckPolicy>::can_lower_core_scalar_function(func_name) {
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

fn set_returning_function_name(func_name: &str) -> Option<&'static str> {
    if !BUILTIN_FUNCTIONS
        .get_property(func_name)
        .map(|property| property.kind == FunctionKind::SRF)
        .unwrap_or(false)
    {
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
        .map(TypeChecker::<super::FullTypeCheckPolicy>::can_lower_core_scalar_function)
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

    #[derive(Clone, Copy)]
    struct StaticCoreExprContextPolicy {
        allowed: CoreExprContextDependencies,
    }

    impl CoreExprContextPolicy for StaticCoreExprContextPolicy {
        fn allowed_core_expr_context_dependencies(&self) -> CoreExprContextDependencies {
            self.allowed
        }
    }

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

    fn assert_sql_policy_error_contains(
        sql: &str,
        policy: &impl CoreExprContextPolicy,
        expected: &str,
    ) {
        let tokens = tokenize_sql(sql).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut arena = CoreExprArena::with_context_policy(0, policy);
        let err = match arena.lower_ast_expr(&expr) {
            Ok(_) => panic!("expected context policy violation"),
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
            assert!(matches!(arena.get(root), CoreExpr::InList { .. }));
        });

        assert_sql_lowers_to("EXISTS (SELECT 1)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::Exists { .. }));
        });

        assert_sql_lowers_to("(SELECT 1)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::ScalarSubquery { .. }));
        });

        assert_sql_lowers_to("a IN (SELECT 1)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::InSubquery { .. }));
        });

        assert_sql_lowers_to("a = ANY (SELECT 1)", |arena, root| {
            assert!(matches!(arena.get(root), CoreExpr::BinaryOp { .. }));
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
    fn collects_context_dependencies_from_flat_nodes() {
        assert_sql_lowers_to("1", |arena, _root| {
            assert_eq!(
                arena.context_dependencies(),
                CoreExprContextDependencies::default()
            );
        });

        assert_sql_lowers_to("a", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.column_resolution);
            assert!(!dependencies.scalar_evaluation);
        });

        assert_sql_lowers_to("1 + 2", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.scalar_evaluation);
            assert!(!dependencies.column_resolution);
        });

        assert_sql_lowers_to("a IN (1, 2)", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.column_resolution);
            assert!(dependencies.subquery);
        });

        assert_sql_lowers_to("current_database()", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.session_function);
            assert!(!dependencies.subquery);
        });

        assert_sql_lowers_to("getvariable('x')", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.session_function);
        });

        assert_sql_lowers_to("nextval(seq)", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.async_function);
            assert!(!dependencies.udf);
        });

        assert_sql_lowers_to("potential_udf(1)", |arena, _root| {
            let dependencies = arena.context_dependencies();
            assert!(dependencies.udf);
            assert!(!dependencies.async_function);
        });
    }

    #[test]
    fn rejects_context_policy_violations_after_lower() {
        let scalar_only = StaticCoreExprContextPolicy {
            allowed: CoreExprContextDependencies {
                scalar_evaluation: true,
                ..Default::default()
            },
        };

        assert_sql_lowers_to("1 + 2", |arena, _root| {
            assert!(scalar_only.allowed.contains(arena.context_dependencies()));
        });

        assert_sql_policy_error_contains("a", &scalar_only, "column_resolution");
        assert_sql_policy_error_contains("a IN (1, 2)", &scalar_only, "subquery");
        assert_sql_policy_error_contains("current_database()", &scalar_only, "session_function");
        assert_sql_policy_error_contains("nextval(seq)", &scalar_only, "async_function");
        assert_sql_policy_error_contains("potential_udf(1)", &scalar_only, "udf");
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
            let CoreExpr::RuntimeCall { name, .. } = arena.get(root) else {
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
