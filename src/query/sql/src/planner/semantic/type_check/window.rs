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
use databend_common_ast::ast::WindowFrameUnits;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::RANK_WINDOW_FUNCTIONS;
use smallvec::SmallVec;

use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreFunctionParams;
use super::core_expr::CoreOrderByExprs;
use super::core_expr::CoreWindow;
use super::core_expr::CoreWindowDesc;
use super::core_expr::CoreWindowFrame;
use super::core_expr::CoreWindowFrameBound;
use super::core_expr::CoreWindowSpec;
use crate::plans::CastExpr;
use crate::plans::LagLeadFunction;
use crate::plans::NthValueFunction;
use crate::plans::NtileFunction;
use crate::plans::ScalarExpr;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;

impl<'a, P> TypeChecker<'a, P>
where P: super::TypeCheckPolicy
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn resolve_core_aggregate_window_function(
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
        window: &CoreWindowDesc<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (new_agg_func, _data_type) = self.resolve_core_aggregate_call(
            arena,
            display_name,
            span,
            func_name,
            distinct,
            params,
            args,
            remove_count_args,
            order_by,
            true,
        )?;
        if window.ignore_nulls.is_some() {
            return Err(ErrorCode::SemanticError(format!(
                "window function {} not support IGNORE/RESPECT NULLS option",
                func_name
            ))
            .set_span(span));
        }
        let func = WindowFuncType::Aggregate(new_agg_func);
        self.resolve_window(arena, span, display_name.to_string(), &window.window, func)
    }

    pub(super) fn resolve_core_count_all_window_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        display_name: &str,
        window: &CoreWindow<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let aggregate_arena = CoreExprArena::new(self.func_ctx.week_start as u64);
        let params = SmallVec::new();
        let args = SmallVec::new();
        let order_by = SmallVec::new();
        let (new_agg_func, _data_type) = self.resolve_core_aggregate_call(
            &aggregate_arena,
            display_name,
            span,
            "count",
            false,
            &params,
            &args,
            true,
            &order_by,
            true,
        )?;
        let func = WindowFuncType::Aggregate(new_agg_func);
        self.resolve_window(arena, span, display_name.to_string(), window, func)
    }

    pub(super) fn resolve_core_general_window_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        display_name: &str,
        func_name: &str,
        args: &CoreExprArgs,
        order_by: &CoreOrderByExprs,
        window: &CoreWindowDesc<'_>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !order_by.is_empty() {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(span));
        }
        if !RANK_WINDOW_FUNCTIONS.contains(&func_name) && window.ignore_nulls.is_some() {
            return Err(ErrorCode::SemanticError(format!(
                "window function {} not support IGNORE/RESPECT NULLS option",
                func_name
            ))
            .set_span(span));
        }
        if let Ok(window_func) = WindowFuncType::from_name(func_name) {
            return self.resolve_window(
                arena,
                span,
                display_name.to_string(),
                &window.window,
                window_func,
            );
        }

        if self.in_window_function {
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        self.in_window_function = true;
        let arguments_result = self.resolve_core_expr_args(arena, args);
        self.in_window_function = false;
        let (arguments, arg_types) = arguments_result?;

        let ignore_null = window.ignore_nulls.unwrap_or(false);

        let func = match func_name {
            "lag" | "lead" => {
                self.resolve_lag_lead_window_function(func_name, &arguments, &arg_types)?
            }
            "first_value" | "first" | "last_value" | "last" | "nth_value" => self
                .resolve_nth_value_window_function(
                    func_name,
                    &arguments,
                    &arg_types,
                    ignore_null,
                )?,
            "ntile" => self.resolve_ntile_window_function(&arguments)?,
            _ => {
                return Err(ErrorCode::UnknownFunction(format!(
                    "Unknown window function: {func_name}"
                )));
            }
        };
        self.resolve_window(arena, span, display_name.to_string(), &window.window, func)
    }

    pub(super) fn resolve_window(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        display_name: String,
        window: &CoreWindow<'_>,
        func: WindowFuncType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if self.in_aggregate_function {
            // Reset the state
            self.in_aggregate_function = false;
            return Err(ErrorCode::SemanticError(
                "aggregate function calls cannot contain window function calls".to_string(),
            )
            .set_span(span));
        }
        if self.in_window_function {
            // Reset the state
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        let spec = match window {
            CoreWindow::WindowSpec(spec) => spec,
            CoreWindow::WindowReference(window_name) => {
                let spec = self
                    .bind_context
                    .window_definitions
                    .get(&window_name.name)
                    .ok_or_else(|| {
                        ErrorCode::SyntaxException(format!(
                            "Window definition {} not found",
                            window_name.name
                        ))
                    })?
                    .value()
                    .clone();
                let mut named_arena = self.core_expr_arena();
                let named_spec = named_arena.lower_window_spec(&spec)?;
                return self.resolve_window_spec(
                    &named_arena,
                    span,
                    display_name,
                    &named_spec,
                    func,
                );
            }
        };
        self.resolve_window_spec(arena, span, display_name, spec, func)
    }

    fn resolve_window_spec(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        display_name: String,
        spec: &CoreWindowSpec<'_>,
        func: WindowFuncType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if spec.existing_window_name.is_some() {
            return Err(ErrorCode::Unimplemented(
                "existing window name in window specification is not supported in type check core expression",
            )
            .set_span(span));
        }

        self.in_window_function = true;
        let mut partitions = Vec::with_capacity(spec.partition_by.len());
        for p in &spec.partition_by {
            let box (part, _part_type) = self.resolve_core(arena, *p)?;
            partitions.push(part);
        }

        let mut order_by = Vec::with_capacity(spec.order_by.len());
        for o in &spec.order_by {
            let box (order, _) = self.resolve_core(arena, o.expr)?;

            if matches!(order, ScalarExpr::ConstantExpr(_)) {
                continue;
            }

            order_by.push(WindowOrderBy {
                expr: order,
                asc: o.asc,
                nulls_first: o.nulls_first,
            })
        }
        self.in_window_function = false;

        let frame =
            self.resolve_window_frame(arena, span, &func, &mut order_by, &spec.window_frame)?;

        if matches!(&frame.start_bound, WindowFuncFrameBound::Following(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame start cannot be UNBOUNDED FOLLOWING".to_string(),
            )
            .set_span(span));
        }

        if matches!(&frame.end_bound, WindowFuncFrameBound::Preceding(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame end cannot be UNBOUNDED PRECEDING".to_string(),
            )
            .set_span(span));
        }

        let data_type = func.return_type();
        let window_func = WindowFunc {
            span,
            display_name,
            func,
            partition_by: partitions,
            order_by,
            frame,
        };
        Ok(Box::new((window_func.into(), data_type)))
    }

    fn resolve_rows_offset(
        &self,
        arena: &CoreExprArena<'_>,
        span: Span,
        expr: super::core_expr::CoreExprId,
    ) -> Result<Scalar> {
        if let CoreExpr::Literal { value, .. } = arena.get(expr) {
            match value {
                Scalar::Number(NumberScalar::UInt8(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(*v as u64)));
                }
                Scalar::Number(NumberScalar::UInt16(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(*v as u64)));
                }
                Scalar::Number(NumberScalar::UInt32(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(*v as u64)));
                }
                Scalar::Number(NumberScalar::UInt64(_)) => return Ok(value.clone()),
                _ => {}
            }
        }

        Err(ErrorCode::SemanticError(
            "Only unsigned numbers are allowed in ROWS offset".to_string(),
        )
        .set_span(span))
    }

    fn resolve_window_rows_frame(
        &self,
        arena: &CoreExprArena<'_>,
        span: Span,
        frame: &CoreWindowFrame,
    ) -> Result<WindowFuncFrame> {
        let units = match &frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match &frame.start_bound {
            CoreWindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            CoreWindowFrameBound::Preceding(expr) => WindowFuncFrameBound::Preceding(
                expr.map(|expr| self.resolve_rows_offset(arena, span, expr))
                    .transpose()?,
            ),
            CoreWindowFrameBound::Following(expr) => WindowFuncFrameBound::Following(
                expr.map(|expr| self.resolve_rows_offset(arena, span, expr))
                    .transpose()?,
            ),
        };
        let end = match &frame.end_bound {
            CoreWindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            CoreWindowFrameBound::Preceding(expr) => WindowFuncFrameBound::Preceding(
                expr.map(|expr| self.resolve_rows_offset(arena, span, expr))
                    .transpose()?,
            ),
            CoreWindowFrameBound::Following(expr) => WindowFuncFrameBound::Following(
                expr.map(|expr| self.resolve_rows_offset(arena, span, expr))
                    .transpose()?,
            ),
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_range_offset(
        &mut self,
        arena: &CoreExprArena<'_>,
        bound: &CoreWindowFrameBound,
    ) -> Result<Option<Scalar>> {
        match bound {
            CoreWindowFrameBound::Following(Some(expr))
            | CoreWindowFrameBound::Preceding(Some(expr)) => {
                let box (expr, _) = self.resolve_core(arena, *expr)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr.as_expr()?, &self.func_ctx, &BUILTIN_FUNCTIONS);
                match expr.into_constant() {
                    Ok(expr::Constant { scalar, .. }) => Ok(Some(scalar)),
                    Err(expr) => Err(ErrorCode::SemanticError(
                        "Only constant is allowed in RANGE offset".to_string(),
                    )
                    .set_span(expr.span())),
                }
            }
            _ => Ok(None),
        }
    }

    fn resolve_window_range_frame(
        &mut self,
        arena: &CoreExprArena<'_>,
        frame: &CoreWindowFrame,
    ) -> Result<WindowFuncFrame> {
        let start_offset = self.resolve_range_offset(arena, &frame.start_bound)?;
        let end_offset = self.resolve_range_offset(arena, &frame.end_bound)?;

        let units = match &frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match &frame.start_bound {
            CoreWindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            CoreWindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(start_offset),
            CoreWindowFrameBound::Following(_) => WindowFuncFrameBound::Following(start_offset),
        };
        let end = match &frame.end_bound {
            CoreWindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            CoreWindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(end_offset),
            CoreWindowFrameBound::Following(_) => WindowFuncFrameBound::Following(end_offset),
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_window_frame(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func: &WindowFuncType,
        order_by: &mut [WindowOrderBy],
        window_frame: &Option<CoreWindowFrame>,
    ) -> Result<WindowFuncFrame> {
        match func {
            WindowFuncType::PercentRank => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::LagLead(lag_lead) if lag_lead.is_lag => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::LagLead(lag_lead) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::Ntile(_) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::CumeDist => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Range,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            _ => {}
        }
        if let Some(frame) = window_frame {
            if frame.units.is_range() {
                if order_by.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "The RANGE OFFSET window frame requires exactly one ORDER BY column, {} given.",
                        order_by.len()
                    )).set_span(span));
                }
                self.resolve_window_range_frame(arena, frame)
            } else {
                self.resolve_window_rows_frame(arena, span, frame)
            }
        } else if order_by.is_empty() {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::Following(None),
            })
        } else {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::CurrentRow,
            })
        }
    }

    pub(super) fn resolve_lag_lead_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
    ) -> Result<WindowFuncType> {
        if args.is_empty() || args.len() > 3 {
            return Err(ErrorCode::InvalidArgument(format!(
                "Function {:?} only support 1 to 3 arguments",
                func_name
            )));
        }

        let offset = if args.len() >= 2 {
            let off = args[1].as_expr()?;
            match off {
                EExpr::Constant(_) => Some(check_number::<i64, _>(
                    off.span(),
                    &self.func_ctx,
                    &off,
                    &BUILTIN_FUNCTIONS,
                )?),
                _ => {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The second argument to the function {:?} must be a constant",
                        func_name
                    )));
                }
            }
        } else {
            None
        };

        let offset = offset.unwrap_or(1);

        let is_lag = match func_name {
            "lag" if offset < 0 => false,
            "lead" if offset < 0 => true,
            "lag" => true,
            "lead" => false,
            _ => unreachable!(),
        };

        let (default, return_type) = if args.len() == 3 {
            (Some(args[2].clone()), arg_types[0].clone())
        } else {
            (None, arg_types[0].wrap_nullable())
        };

        let cast_default = default.map(|d| {
            Box::new(ScalarExpr::CastExpr(CastExpr {
                span: d.span(),
                is_try: false,
                argument: Box::new(d),
                target_type: Box::new(return_type.clone()),
            }))
        });

        Ok(WindowFuncType::LagLead(LagLeadFunction {
            is_lag,
            arg: Box::new(args[0].clone()),
            offset: offset.unsigned_abs(),
            default: cast_default,
            return_type: Box::new(return_type),
        }))
    }

    pub(super) fn resolve_nth_value_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
        ignore_null: bool,
    ) -> Result<WindowFuncType> {
        Ok(match func_name {
            "first_value" | "first" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(1),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            "last_value" | "last" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: None,
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            _ => {
                // nth_value
                if args.len() != 2 {
                    return Err(ErrorCode::InvalidArgument(
                        "The function nth_value must take two arguments".to_string(),
                    ));
                }
                let return_type = arg_types[0].wrap_nullable();
                let n_expr = args[1].as_expr()?;
                let n = match n_expr {
                    EExpr::Constant(_) => check_number::<u64, _>(
                        n_expr.span(),
                        &self.func_ctx,
                        &n_expr,
                        &BUILTIN_FUNCTIONS,
                    )?,
                    _ => {
                        return Err(ErrorCode::InvalidArgument(
                            "The count of `nth_value` must be constant positive integer",
                        ));
                    }
                };
                if n == 0 {
                    return Err(ErrorCode::InvalidArgument(
                        "nth_value should count from 1".to_string(),
                    ));
                }

                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(n),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
        })
    }

    pub(super) fn resolve_ntile_window_function(
        &mut self,
        args: &[ScalarExpr],
    ) -> Result<WindowFuncType> {
        if args.len() != 1 {
            return Err(ErrorCode::InvalidArgument(
                "Function ntile can only take one argument".to_string(),
            ));
        }
        let n_expr = args[0].as_expr()?;
        let return_type = DataType::Number(NumberDataType::UInt64);
        let n = match n_expr {
            EExpr::Constant(_) => {
                check_number::<u64, _>(n_expr.span(), &self.func_ctx, &n_expr, &BUILTIN_FUNCTIONS)?
            }
            _ => {
                return Err(ErrorCode::InvalidArgument(
                    "The argument of `ntile` must be constant".to_string(),
                ));
            }
        };
        if n == 0 {
            return Err(ErrorCode::InvalidArgument(
                "ntile buckets must be greater than 0".to_string(),
            ));
        }

        Ok(WindowFuncType::Ntile(NtileFunction {
            n,
            return_type: Box::new(return_type),
        }))
    }
}
