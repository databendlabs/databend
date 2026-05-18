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
use databend_common_ast::ast::OrderByExpr;
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
use databend_common_functions::aggregates::AggregateFunctionFactory;

use super::TypeChecker;
use crate::binder::ExprContext;
use crate::planner::metadata::optimize_remove_count_args;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
    /// Resolve aggregation function call.
    pub(super) fn resolve_aggregate_function(
        &mut self,
        span: Span,
        func_name: &str,
        expr: &Expr,
        distinct: bool,
        params: Vec<Scalar>,
        args: &[&Expr],
        order_by: &[OrderByExpr],
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
                // The aggregate function can be in window function call,
                // but it cannot be nested.
                // E.g. `select sum(sum(x)) over (partition by y) from t group by y;` is allowed.
                // But `select sum(sum(sum(x))) from t;` is not allowed.
                self.in_window_function = false;
            } else {
                // Reset the state
                self.in_aggregate_function = false;
                return Err(ErrorCode::SemanticError(
                    "aggregate function calls cannot be nested".to_string(),
                )
                .set_span(expr.span()));
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
        let arguments_result = (|| {
            let mut arguments = vec![];
            let mut arg_types = vec![];
            for arg in args {
                let box (argument, arg_type) = self.resolve(arg)?;
                arguments.push(argument);
                arg_types.push(arg_type);
            }
            Ok::<_, ErrorCode>((arguments, arg_types))
        })();
        if disallow_alias_resolution {
            self.bind_context.expr_context = original_context;
        }
        self.in_aggregate_function = false;
        let (mut arguments, mut arg_types) = arguments_result?;

        self.try_widen_sum_decimal_argument(func_name, &mut arguments, &mut arg_types)?;

        let sort_descs = order_by
            .iter()
            .map(
                |OrderByExpr {
                     expr,
                     asc,
                     nulls_first,
                 }| {
                    if disallow_alias_resolution {
                        self.bind_context.expr_context = ExprContext::InAggregateFunction;
                    }
                    let result = self.resolve(expr);
                    if disallow_alias_resolution {
                        self.bind_context.expr_context = original_context;
                    }
                    let box (scalar_expr, _) = result?;

                    Ok(AggregateFunctionScalarSortDesc {
                        expr: scalar_expr,
                        is_reuse_index: false,
                        nulls_first: nulls_first.unwrap_or(false),
                        asc: asc.unwrap_or(true),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

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

        let agg_func = AggregateFunctionFactory::instance()
            .get(&func_name, params.clone(), arg_types, vec![])
            .map_err(|e| e.set_span(span))?;

        let args = if optimize_remove_count_args(&func_name, distinct, args) {
            vec![]
        } else {
            arguments
        };

        let display_name = format!("{:#}", expr);
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
        if !func_name.eq_ignore_ascii_case("sum")
            || arguments.len() != 1
            || !self.ctx.get_settings().get_enable_decimal_sum_widening()?
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
