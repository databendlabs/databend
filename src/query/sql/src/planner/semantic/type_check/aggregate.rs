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
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Window;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use smallvec::SmallVec;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::CoreFunctionParams;
use super::CoreOrderByExprs;
use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::binder::ExprContext;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

fn aggregate_base_name(func_name: &str) -> &str {
    let if_suffix_len = "_if".len();
    if let Some(suffix) = func_name.get(func_name.len().saturating_sub(if_suffix_len)..)
        && suffix.eq_ignore_ascii_case("_if")
    {
        return &func_name[..func_name.len() - if_suffix_len];
    }
    func_name
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn try_lower_aggregate_function(
        &mut self,
        original_expr: &'a Expr,
        span: Span,
        func_name: &str,
        func: &'a ASTFunctionCall,
    ) -> Result<Option<CoreExprId>> {
        if func.lambda.is_some() || !self.aggregate_function_factory.contains(func_name) {
            return Ok(None);
        }

        let ASTFunctionCall {
            distinct,
            args,
            params,
            order_by,
            filter,
            window,
            ..
        } = func;
        let display_name = format!("{original_expr:#}");
        let mut func_name = func_name.to_string();

        if *distinct && !order_by.is_empty() {
            return Err(
                ErrorCode::SyntaxException("DISTINCT aggregate ORDER BY is not supported")
                    .set_span(span),
            );
        }

        let remove_count_args = filter.is_none()
            && func_name.eq_ignore_ascii_case("count")
            && !*distinct
            && args
                .iter()
                .all(|expr| matches!(expr, Expr::Literal { value, .. } if *value != Literal::Null));
        let params = self.lower_function_params(params)?;
        let mut args = self.lower_expr_args(args)?;
        let order_by = self.lower_order_by_exprs(order_by)?;

        if let Some(filter) = filter {
            if *distinct {
                return Err(
                    ErrorCode::SemanticError("DISTINCT aggregate FILTER is not supported")
                        .set_span(span),
                );
            }
            // FILTER appends `_if`, but the factory only resolves one combinator
            // suffix over a base aggregate. On a combinator call like `sum_if`
            // this would yield `sum_if_if`, so reject it instead.
            if !self.aggregate_function_factory.contains_base(&func_name) {
                return Err(ErrorCode::SemanticError(format!(
                    "FILTER clause is not supported for aggregate combinator `{func_name}`"
                ))
                .set_span(span));
            }
            args.push(self.lower_ast_expr(filter)?);
            func_name = format!("{func_name}_if");
        }

        Ok(Some(if let Some(window) = window {
            let window = super::window::CoreWindowDesc {
                ignore_nulls: window.ignore_nulls,
                window: self.lower_window(&window.window)?,
            };
            self.alloc(CoreExpr::AggregateWindowFunction {
                display_name,
                span,
                func_name,
                distinct: *distinct,
                params,
                args,
                remove_count_args,
                order_by,
                window,
            })
        } else {
            self.alloc(CoreExpr::AggregateFunction {
                display_name,
                span,
                func_name,
                distinct: *distinct,
                params,
                args,
                remove_count_args,
                order_by,
            })
        }))
    }

    pub(super) fn ensure_within_group_function_call(
        &self,
        span: Span,
        func_name: &str,
        has_order_by: bool,
    ) -> Result<()> {
        let base_func_name = aggregate_base_name(func_name);
        if has_order_by && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&Ascii::new(base_func_name)) {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(span));
        }
        Ok(())
    }

    pub(super) fn lower_count_all_expr(
        &mut self,
        display_name: String,
        span: Span,
        filter: Option<&'a Expr>,
        window: Option<&'a Window>,
    ) -> Result<CoreExprId> {
        if let Some(filter) = filter {
            let mut args = SmallVec::new();
            let one = self.alloc(CoreExpr::Literal {
                span,
                value: Scalar::Number(NumberScalar::UInt64(1)),
            });
            args.push(one);
            args.push(self.lower_ast_expr(filter)?);

            return Ok(if let Some(window) = window {
                let window = super::window::CoreWindowDesc {
                    ignore_nulls: None,
                    window: self.lower_window(window)?,
                };
                self.alloc(CoreExpr::AggregateWindowFunction {
                    display_name,
                    span,
                    func_name: "count_if".to_string(),
                    distinct: false,
                    params: SmallVec::new(),
                    args,
                    remove_count_args: false,
                    order_by: SmallVec::new(),
                    window,
                })
            } else {
                self.alloc(CoreExpr::AggregateFunction {
                    display_name,
                    span,
                    func_name: "count_if".to_string(),
                    distinct: false,
                    params: SmallVec::new(),
                    args,
                    remove_count_args: false,
                    order_by: SmallVec::new(),
                })
            });
        }

        if let Some(window) = window {
            let window = self.lower_window(window)?;
            Ok(self.alloc(CoreExpr::CountAllWindowFunction {
                display_name,
                span,
                window,
            }))
        } else {
            Ok(self.alloc(CoreExpr::AggregateFunction {
                display_name,
                span,
                func_name: "count".to_string(),
                distinct: false,
                params: SmallVec::new(),
                args: SmallVec::new(),
                remove_count_args: true,
                order_by: SmallVec::new(),
            }))
        }
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
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
        let base_func_name = aggregate_base_name(func_name);
        if !order_by.is_empty()
            && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&Ascii::new(base_func_name))
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
        let (mut arguments, mut arg_types) = arguments_result?;

        self.try_widen_sum_decimal_argument(func_name, &mut arguments, &mut arg_types)?;

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
    fn resolve_aggregate_function(
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
        let base_func_name = aggregate_base_name(func_name);
        let expected_string_agg_args = if base_func_name == func_name { 2 } else { 3 };
        let params = if (base_func_name.eq_ignore_ascii_case("string_agg")
            || base_func_name.eq_ignore_ascii_case("listagg")
            || base_func_name.eq_ignore_ascii_case("group_concat"))
            && arguments.len() == expected_string_agg_args
            && params.is_empty()
        {
            let delimiter_value = ConstantExpr::try_from(arguments[1].clone());
            if arg_types[1] != DataType::String || delimiter_value.is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "The delimiter of `{base_func_name}` must be a constant string"
                )));
            }
            arguments.remove(1);
            arg_types.remove(1);
            let delimiter = delimiter_value.unwrap();
            vec![delimiter.value]
        } else {
            params
        };

        // Convert the num_buckets of histogram to params
        let expected_histogram_args = if base_func_name == func_name { 2 } else { 3 };
        let params = if base_func_name.eq_ignore_ascii_case("histogram")
            && arguments.len() == expected_histogram_args
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
            format!("{func_name}_distinct")
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

    fn try_widen_sum_decimal_argument(
        &self,
        func_name: &str,
        arguments: &mut [ScalarExpr],
        arg_types: &mut [DataType],
    ) -> Result<()> {
        let base_func_name = aggregate_base_name(func_name);
        let expected_args_len = if base_func_name.len() == func_name.len() {
            1
        } else {
            2
        };
        if !base_func_name.eq_ignore_ascii_case("sum")
            || arguments.len() != expected_args_len
            || !self.adapter.settings().get_enable_decimal_sum_widening()?
        {
            return Ok(());
        }

        let input_is_nullable = arg_types[0].is_nullable();
        let DataType::Decimal(size) = arg_types[0].remove_nullable() else {
            return Ok(());
        };

        if !size.can_carried_by_128() || size.precision() <= i64::MAX_PRECISION {
            return Ok(());
        }

        let mut target_type = DataType::Decimal(DecimalSize::new_unchecked(
            i256::MAX_PRECISION,
            size.scale(),
        ));
        if input_is_nullable {
            target_type = target_type.wrap_nullable();
        }

        arguments[0] = ScalarExpr::CastExpr(CastExpr {
            span: arguments[0].span(),
            is_try: false,
            argument: Box::new(arguments[0].clone()),
            target_type: Box::new(target_type.clone()),
        });
        arg_types[0] = target_type;

        Ok(())
    }
}
