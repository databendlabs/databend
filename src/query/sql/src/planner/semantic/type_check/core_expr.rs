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

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::OrderByExpr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use smallvec::smallvec;

use super::CoreDisplayExprArg;
use super::CoreDisplayExprArgs;
use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::CoreFunctionParams;
use super::CoreOrderByExpr;
use super::CoreOrderByExprs;
use super::TypeCheckAdapter;
use super::TypeChecker;
use super::date::AdjacentDayFunction;
use super::date::DateArithmeticFunction;
use super::literal::infer_literal_data_type;
use super::scalar_rewrite::binary_op_core_function;
use super::scalar_rewrite::can_lower_binary_op;
use super::variant::json_op_core_function;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryType;

impl<'a> CoreExprArena<'a> {
    pub(super) fn new(week_start: u64) -> Self {
        Self {
            nodes: Vec::new(),
            week_start,
            aggregate_function_factory: AggregateFunctionFactory::instance(),
            in_lambda_function: false,
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
            in_lambda_function: false,
        }
    }

    #[recursive::recursive]
    pub(super) fn lower_ast_expr(&mut self, expr: &'a Expr) -> Result<CoreExprId> {
        let id = match expr {
            Expr::ColumnRef { span, column } => self.alloc(CoreExpr::ColumnRef {
                span: *span,
                column,
            }),
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
            Expr::Array { span, exprs } => {
                let exprs = self.lower_expr_args(exprs)?;
                self.alloc(CoreExpr::Array { span: *span, exprs })
            }
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
            } => self.lower_like_escape_expr(*span, "like_any", left, right, Some(escape))?,
            Expr::LikeWithEscape {
                span,
                left,
                right,
                is_not,
                escape,
            } => {
                let like = self.lower_like_escape_expr(*span, "like", left, right, Some(escape))?;
                if *is_not {
                    self.call(*span, "not", smallvec![like])
                } else {
                    like
                }
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
            expr @ Expr::CountAll {
                span,
                filter,
                window,
                ..
            } => self.lower_count_all_expr(
                format!("{expr:#}"),
                *span,
                filter.as_deref(),
                window.as_ref(),
            )?,
            expr @ Expr::FunctionCall { span, func } => {
                self.lower_function_call_expr(expr, *span, func)?
            }
            Expr::Hole { span, .. } | Expr::Placeholder { span } => {
                return Err(ErrorCode::SemanticError(
                    "Hole or Placeholder expression is impossible in trivial query".to_string(),
                )
                .set_span(*span));
            }
            Expr::StageLocation { span, location } => self.alloc(CoreExpr::StageLocation {
                span: *span,
                location,
            }),
        };
        Ok(id)
    }

    pub(super) fn lower_expr_args(&mut self, exprs: &'a [Expr]) -> Result<CoreExprArgs> {
        exprs.iter().map(|expr| self.lower_ast_expr(expr)).collect()
    }

    fn lower_function_call_expr(
        &mut self,
        original_expr: &'a Expr,
        span: Span,
        func: &'a ASTFunctionCall,
    ) -> Result<CoreExprId> {
        let func_name = func.name.name.to_ascii_lowercase();
        self.ensure_window_not_in_lambda(span, func.window.is_some())?;
        if let Some(expr) = self.try_lower_udf_call(span, &func_name, func)? {
            return Ok(expr);
        }

        if func.filter.is_some() && !self.aggregate_function_factory.contains(&func_name) {
            return Err(ErrorCode::SemanticError(
                "FILTER clause is only supported for aggregate functions",
            )
            .set_span(span));
        }

        if func.distinct
            && !func.order_by.is_empty()
            && self.aggregate_function_factory.contains(&func_name)
        {
            return Err(
                ErrorCode::SyntaxException("DISTINCT aggregate ORDER BY is not supported")
                    .set_span(span),
            );
        }

        self.ensure_within_group_function_call(span, &func_name, !func.order_by.is_empty())?;
        self.ensure_window_function_call(
            span,
            &func_name,
            func.window.is_some(),
            self.aggregate_function_factory.contains(&func_name),
        )?;

        if let Some(expr) =
            self.try_lower_general_window_function(original_expr, span, &func_name, func)?
        {
            return Ok(expr);
        }

        if let Some(expr) =
            self.try_lower_aggregate_function(original_expr, span, &func_name, func)?
        {
            return Ok(expr);
        }

        if let Some(expr) = self.try_lower_lambda(span, &func_name, func)? {
            return Ok(expr);
        }

        if let Some(expr) = self.try_lower_search(span, &func_name, func)? {
            return Ok(expr);
        }

        if let Some(expr) = self.try_lower_async_function(span, &func_name, func)? {
            return Ok(expr);
        }

        if let Some(expr) = self.try_lower_set_returning_function(span, &func_name, func)? {
            return Ok(expr);
        }

        self.lower_scalar_function(span, &func_name, func)
    }

    pub(super) fn lower_function_params(
        &mut self,
        params: &'a [Expr],
    ) -> Result<CoreFunctionParams> {
        params
            .iter()
            .map(|param| Ok((format!("{param:#}"), self.lower_ast_expr(param)?)))
            .collect()
    }

    pub(super) fn lower_display_expr_args(
        &mut self,
        args: &'a [Expr],
    ) -> Result<CoreDisplayExprArgs> {
        args.iter()
            .map(|arg| self.lower_display_expr_arg(arg))
            .collect()
    }

    pub(super) fn lower_display_expr_arg(&mut self, arg: &'a Expr) -> Result<CoreDisplayExprArg> {
        Ok((format!("{arg:#}"), self.lower_ast_expr(arg)?))
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

    pub(super) fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId {
        let index = self.nodes.len();
        self.nodes.push(expr);
        CoreExprId { index }
    }

    pub(super) fn get(&self, id: CoreExprId) -> &CoreExpr<'a> {
        &self.nodes[id.index]
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
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
            CoreExpr::Array { span, exprs } => self.resolve_array(arena, *span, exprs),
            CoreExpr::Map { span, kvs } => self.resolve_map(arena, *span, kvs),
            CoreExpr::Tuple { span, exprs } => self.resolve_tuple(arena, *span, exprs),
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
            } => self.resolve_call(arena, *span, func_name, args),
            CoreExpr::SetReturningFunction {
                span,
                func_name,
                args,
            } => self.resolve_set_returning_function(arena, *span, func_name, args),
            CoreExpr::ScalarFunction {
                span,
                func_name,
                params,
                args,
            } => self.resolve_scalar_function(arena, *span, func_name, params, args),
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
            } => {
                let (agg_func, data_type) = self.resolve_aggregate_call(
                    arena,
                    display_name,
                    *span,
                    func_name,
                    *distinct,
                    params,
                    args,
                    *remove_count_args,
                    order_by,
                    false,
                )?;
                Ok(Box::new((agg_func.into(), data_type)))
            }
            CoreExpr::ColumnRef { span, column } => self.resolve_column_ref(*span, column),
            CoreExpr::SpecialFunction { span, function } => function.resolve(self, arena, *span),
            CoreExpr::UdfCall {
                span,
                name,
                args,
                filter,
            } => self.resolve_udf_call(arena, *span, name, args, *filter),
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
            } => self.resolve_in_list(arena, *span, *expr, list, *not),
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
            CoreExpr::StageLocation { span, location } => Ok(Box::new((
                crate::plans::ConstantExpr {
                    span: *span,
                    value: Scalar::String(location.to_string()),
                }
                .into(),
                DataType::String,
            ))),
        }
    }

    pub(super) fn resolve_expr_args(
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

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::Identifier;
    use databend_common_ast::ast::Literal;
    use databend_common_ast::ast::MapAccessor;
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

        assert_sql_lowers_to("a NOT LIKE 'x' ESCAPE '!'", |arena, root| {
            let CoreExpr::Call {
                func_name: "not",
                args,
                ..
            } = arena.get(root)
            else {
                panic!("NOT LIKE ESCAPE should lower to a not call");
            };
            assert!(matches!(arena.get(args[0]), CoreExpr::Call {
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
