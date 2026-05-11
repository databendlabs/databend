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
use databend_common_ast::ast::Literal;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use unicase::Ascii;

use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreExprId;
use super::core_expr::CoreFunctionParams;
use super::core_expr::CoreOrderByExprs;
use crate::binder::ExprContext;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a> CoreExprArena<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn aggregate_function(
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
}

pub(super) fn can_remove_count_args(func_name: &str, distinct: bool, args: &[Expr]) -> bool {
    func_name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal { value, .. } if *value != Literal::Null))
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn resolve_aggregate_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        display_name: &str,
        span: Span,
        func_name: &str,
        distinct: bool,
        params: &CoreFunctionParams,
        args: &CoreExprArgs,
        remove_count_args: bool,
        order_by: &CoreOrderByExprs,
        in_window_call: bool,
    ) -> Result<(AggregateFunction, DataType)> {
        if !order_by.is_empty() && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&Ascii::new(func_name))
        {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(span));
        }
        let new_params = self.resolve_core_function_params(arena, span, params, "aggregate")?;
        let in_window = self.in_window_function;
        self.in_window_function = self.in_window_function || in_window_call;
        let in_aggregate_function = self.in_aggregate_function;
        let result = self.resolve_core_aggregate_call_inner(
            arena,
            display_name,
            span,
            func_name,
            distinct,
            args,
            remove_count_args,
            order_by,
            new_params,
        );
        self.in_window_function = in_window;
        self.in_aggregate_function = in_aggregate_function;
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_core_aggregate_call_inner(
        &mut self,
        arena: &CoreExprArena<'_>,
        display_name: &str,
        span: Span,
        func_name: &str,
        distinct: bool,
        args: &CoreExprArgs,
        remove_count_args: bool,
        order_by: &CoreOrderByExprs,
        new_params: Vec<Scalar>,
    ) -> Result<(AggregateFunction, DataType)> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "aggregate functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
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
                .set_span(span));
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
        let arguments_result = self.resolve_expr_args(arena, args);
        if disallow_alias_resolution {
            self.bind_context.expr_context = original_context;
        }
        self.in_aggregate_function = false;
        let (arguments, arg_types) = arguments_result?;

        let sort_descs = order_by
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
            span,
            func_name,
            display_name.to_string(),
            distinct,
            new_params,
            arguments,
            arg_types,
            sort_descs,
            remove_count_args,
        )
    }

    /// Resolve aggregation function call.
    pub(super) fn resolve_aggregate_function(
        &mut self,
        span: Span,
        func_name: &str,
        display_name: String,
        distinct: bool,
        params: Vec<Scalar>,
        mut arguments: Vec<ScalarExpr>,
        mut arg_types: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionScalarSortDesc>,
        remove_count_args: bool,
    ) -> Result<(AggregateFunction, DataType)> {
        // Convert the delimiter of string_agg to params
        let params = if (func_name.eq_ignore_ascii_case("string_agg")
            || func_name.eq_ignore_ascii_case("listagg")
            || func_name.eq_ignore_ascii_case("group_concat"))
            && arguments.len() == 2
            && params.is_empty()
        {
            let delimiter_value = ConstantExpr::try_from(arguments[1].clone());
            if arg_types[1] != DataType::String || delimiter_value.is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "The delimiter of `{func_name}` must be a constant string"
                )));
            }
            let _ = arguments.pop();
            let _ = arg_types.pop();
            let delimiter = delimiter_value.unwrap();
            vec![delimiter.value]
        } else {
            params
        };

        // Convert the num_buckets of histogram to params
        let params = if func_name.eq_ignore_ascii_case("histogram")
            && arguments.len() == 2
            && params.is_empty()
        {
            let max_num_buckets: u64 = check_number(
                None,
                &FunctionContext::default(),
                &arguments[1].as_expr()?,
                &BUILTIN_FUNCTIONS,
            )?;

            vec![Scalar::Number(NumberScalar::UInt64(max_num_buckets))]
        } else {
            params
        };

        // Rewrite `xxx(distinct)` to `xxx_distinct(...)`
        let (func_name, distinct) = if func_name.eq_ignore_ascii_case("count") && distinct {
            ("count_distinct", false)
        } else {
            (func_name, distinct)
        };

        let func_name = if distinct {
            format!("{}_distinct", func_name)
        } else {
            func_name.to_string()
        };

        let agg_func = self
            .adapter
            .aggregate_function_factory()
            .get(&func_name, params.clone(), arg_types, vec![])
            .map_err(|e| e.set_span(span))?;

        let args = if remove_count_args { vec![] } else { arguments };

        let new_agg_func = AggregateFunction {
            span,
            display_name,
            func_name,
            distinct: false,
            params,
            args,
            return_type: Box::new(agg_func.return_type()?),
            sort_descs,
        };

        let data_type = agg_func.return_type()?;

        Ok((new_agg_func, data_type))
    }
}
