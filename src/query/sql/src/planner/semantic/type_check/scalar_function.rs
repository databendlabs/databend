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
use databend_common_ast::ast::TypeName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr;
use databend_common_expression::type_check;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_SEARCH_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use simsearch::SimSearch;
use smallvec::SmallVec;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::CoreFunctionParams;
use super::DEFAULT_DECIMAL_PRECISION;
use super::DEFAULT_DECIMAL_SCALE;
use super::TypeCheckAdapter;
use super::TypeChecker;
use super::rewrite_function;
use super::rewrite_function::rewrite_function_name;
use crate::planner::semantic::resolve_type_name;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryType;

impl<'a> CoreExprArena<'a> {
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

    pub(super) fn lower_scalar_function(
        &mut self,
        span: Span,
        func_name: &str,
        func: &'a ASTFunctionCall,
    ) -> Result<CoreExprId> {
        let ASTFunctionCall {
            distinct,
            args,
            params,
            order_by,
            filter,
            window,
            lambda,
            ..
        } = func;

        if filter.is_some() {
            return Err(ErrorCode::SemanticError(
                "FILTER clause is only supported for aggregate functions",
            )
            .set_span(span));
        }

        if !*distinct
            && params.is_empty()
            && order_by.is_empty()
            && window.is_none()
            && lambda.is_none()
        {
            if let Some(expr) = self.lower_rewrite_function(span, func_name, args)? {
                return Ok(expr);
            }

            if let Some(expr) = self.special_function(span, func_name, args)? {
                return Ok(expr);
            }

            if let Some(func_name) = builtin_scalar_function_name(func_name) {
                let args = self.lower_expr_args(args)?;
                return Ok(self.call(span, func_name, args));
            }
        }

        let Some(func_name) = builtin_scalar_function_name(func_name) else {
            return Err(ErrorCode::Internal(format!(
                "function {func_name} should have been classified before scalar lowering",
            )));
        };
        let params = self.lower_function_params(params)?;
        let args = self.lower_expr_args(args)?;
        Ok(self.alloc(CoreExpr::ScalarFunction {
            span,
            func_name,
            params,
            args,
        }))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    // TODO: remove this function
    fn rewrite_substring(args: &mut [ScalarExpr]) {
        if let ScalarExpr::ConstantExpr(expr) = &args[1] {
            if let Scalar::Number(NumberScalar::UInt8(0)) = expr.value {
                args[1] = ConstantExpr {
                    span: expr.span,
                    value: Scalar::Number(1i64.into()),
                }
                .into();
            }
        }
    }

    pub(super) fn unknown_function_error(&self, span: Span, func_name: &str) -> ErrorCode {
        // Function not found, try to find and suggest similar function name.
        let all_funcs = BUILTIN_FUNCTIONS
            .all_function_names()
            .into_iter()
            .chain(self.adapter.aggregate_function_factory().registered_names())
            .chain(
                GENERAL_WINDOW_FUNCTIONS
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            )
            .chain(
                GENERAL_LAMBDA_FUNCTIONS
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            )
            .chain(
                GENERAL_SEARCH_FUNCTIONS
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            )
            .chain(
                ASYNC_FUNCTIONS
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            )
            .chain(
                TypeChecker::<()>::all_special_functions()
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            )
            .chain(
                rewrite_function::all_rewrite_functions()
                    .iter()
                    .cloned()
                    .map(|ascii| ascii.into_inner().to_string()),
            );
        let mut engine: SimSearch<String> = SimSearch::new();
        for func_name in all_funcs {
            engine.insert(func_name.clone(), &func_name);
        }
        let possible_funcs = engine
            .search(func_name)
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>();
        if possible_funcs.is_empty() {
            ErrorCode::UnknownFunction(format!("no function matches the given name: {func_name}"))
                .set_span(span)
        } else {
            ErrorCode::UnknownFunction(format!(
                "no function matches the given name: '{func_name}', do you mean {}?",
                possible_funcs.join(", ")
            ))
            .set_span(span)
        }
    }

    pub(super) fn resolve_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if TypeChecker::<()>::all_special_functions().contains(&Ascii::new(func_name))
            || rewrite_function_name(func_name).is_some()
        {
            return Err(ErrorCode::Internal(format!(
                "special function {} should not be represented as core call",
                func_name
            )));
        }

        if let Some(rewritten_get_expr) =
            self.try_resolve_get_function_chain(arena, span, func_name, args)
        {
            return rewritten_get_expr;
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

    pub(super) fn resolve_scalar_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        params: &CoreFunctionParams,
        args: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let params = self.resolve_core_function_params(arena, span, params, "scalar")?;
        let (scalars, _) = self.resolve_expr_args(arena, args)?;

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

    pub(super) fn resolve_cast_expr(
        &mut self,
        span: Span,
        scalar: ScalarExpr,
        data_type: DataType,
        target_type: &TypeName,
        is_try: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if target_type == &TypeName::Variant {
            if let Some(result) = self.resolve_cast_to_variant(span, &data_type, &scalar, is_try) {
                return result;
            }
        }

        let raw_expr = RawExpr::Cast {
            span,
            is_try,
            expr: Box::new(scalar.as_raw_expr()),
            dest_type: DataType::from(&resolve_type_name(target_type, true)?),
        };
        let checked_expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;

        if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
            return Ok(constant);
        }

        // cast variant to other type should nest wrap nullable,
        // as we cast JSON null to SQL NULL.
        let target_type = if data_type.remove_nullable() == DataType::Variant {
            checked_expr.data_type().nest_wrap_nullable()
        // if the source type is nullable, cast target type should also be nullable.
        } else if !is_try && data_type.is_nullable_or_null() {
            checked_expr.data_type().wrap_nullable()
        } else {
            checked_expr.data_type().clone()
        };

        Ok(Box::new((
            CastExpr {
                span,
                is_try,
                argument: Box::new(scalar),
                target_type: Box::new(target_type.clone()),
            }
            .into(),
            target_type,
        )))
    }

    pub(super) fn rewrite_variant_compare_constant(
        &self,
        scalar: ScalarExpr,
        data_type: DataType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let ScalarExpr::FunctionCall(ref func) = scalar else {
            return Ok(Box::new((scalar, data_type)));
        };
        if func.arguments.len() != 2 {
            return Ok(Box::new((scalar, data_type)));
        }
        let arg0 = &func.arguments[0];
        let arg1 = &func.arguments[1];
        let (constant_arg_index, constant_arg) = match (arg0, arg1) {
            (ScalarExpr::ConstantExpr(_), _)
                if arg1.data_type()?.remove_nullable() == DataType::Variant
                    && !arg1.used_columns().is_empty()
                    && arg0.data_type()? == DataType::String =>
            {
                (0, arg0)
            }
            (_, ScalarExpr::ConstantExpr(_))
                if arg0.data_type()?.remove_nullable() == DataType::Variant
                    && !arg0.used_columns().is_empty()
                    && arg1.data_type()? == DataType::String =>
            {
                (1, arg1)
            }
            _ => {
                return Ok(Box::new((scalar, data_type)));
            }
        };

        let wrap_new_arg = ScalarExpr::FunctionCall(FunctionCall {
            span: func.span,
            func_name: "to_variant".to_string(),
            params: vec![],
            arguments: vec![constant_arg.clone()],
        });
        let mut new_arguments = func.arguments.clone();
        new_arguments[constant_arg_index] = wrap_new_arg;

        let new_func = ScalarExpr::FunctionCall(FunctionCall {
            span: func.span,
            func_name: func.func_name.clone(),
            params: func.params.clone(),
            arguments: new_arguments,
        });

        Ok(Box::new((new_func, data_type)))
    }

    pub fn resolve_scalar_function_call(
        &self,
        span: Span,
        func_name: &str,
        mut params: Vec<Scalar>,
        mut args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // rewrite substr('xx', 0, xx) -> substr('xx', 1, xx)
        if (func_name == "substr" || func_name == "substring")
            && self
                .adapter
                .settings()
                .get_sql_dialect()
                .unwrap()
                .substr_index_zero_literal_as_one()
        {
            Self::rewrite_substring(&mut args);
        }

        self.adjust_date_interval_function_args(func_name, &mut args)?;

        // Type check
        let mut arguments = args.iter().map(|v| v.as_raw_expr()).collect::<Vec<_>>();
        // inject the params
        if ["round", "truncate"].contains(&func_name)
            && !args.is_empty()
            && params.is_empty()
            && args[0].data_type()?.remove_nullable().is_decimal()
        {
            let scale = if args.len() == 2 {
                let scalar_expr = &arguments[1];
                let expr = type_check::check(scalar_expr, &BUILTIN_FUNCTIONS)?;

                let scale: i64 = check_number(
                    expr.span(),
                    &FunctionContext::default(),
                    &expr,
                    &BUILTIN_FUNCTIONS,
                )?;
                scale.clamp(-76, 76)
            } else {
                0
            };
            params.push(Scalar::Number(NumberScalar::Int64(scale)));
        } else if func_name.eq_ignore_ascii_case("as_decimal") {
            // Convert the precision and scale argument of `as_decimal` to params
            if !params.is_empty() {
                if params.len() > 2 || arguments.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                        params.len(),
                        arguments.len()
                    )));
                }
            } else {
                if arguments.is_empty() || arguments.len() > 3 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}` require 1, 2 or 3 arguments, but got {} arguments",
                        arguments.len()
                    )));
                }
                let param_args = arguments.split_off(1);
                for arg in param_args.into_iter() {
                    let expr = type_check::check(&arg, &BUILTIN_FUNCTIONS)?;
                    let param: u8 = check_number(
                        expr.span(),
                        &FunctionContext::default(),
                        &expr,
                        &BUILTIN_FUNCTIONS,
                    )?;
                    params.push(Scalar::Number(NumberScalar::UInt8(param)));
                }
            }
            if !params.is_empty() {
                let Some(precision) = params[0].get_i64() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{}` for `{func_name}` precision parameter",
                        params[0]
                    )));
                };
                if precision < 0 || precision > i256::MAX_PRECISION as i64 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{precision}` for `{func_name}` precision parameter"
                    )));
                }
                if params.len() == 2 {
                    let Some(scale) = params[1].get_i64() else {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{}` for `{func_name}` scale parameter",
                            params[1]
                        )));
                    };
                    if scale < 0 || scale > precision {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{scale}` for `{func_name}` scale parameter"
                        )));
                    }
                }
            }
        } else if (func_name.eq_ignore_ascii_case("to_number")
            || func_name.eq_ignore_ascii_case("to_numeric")
            || func_name.eq_ignore_ascii_case("to_decimal")
            || func_name.eq_ignore_ascii_case("try_to_number")
            || func_name.eq_ignore_ascii_case("try_to_numeric")
            || func_name.eq_ignore_ascii_case("try_to_decimal"))
            && params.is_empty()
        {
            if args.is_empty() || args.len() > 4 {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                    params.len(),
                    arguments.len()
                )));
            }
            let func_ctx = self.adapter.function_context()?;
            let arg_fn = |args: &[ScalarExpr],
                          index: usize,
                          arg_name: &str,
                          default: i64|
             -> Result<i64> {
                Ok(args.get(index).map(|arg| {
                    match ConstantFolder::fold(&arg.as_expr()?, &func_ctx, &BUILTIN_FUNCTIONS).0 {
                        EExpr::Constant(Constant {
                            scalar,
                            ..
                        }) => Ok(scalar.get_i64()),
                        _ => Err(ErrorCode::SemanticError(format!("Invalid arguments for `{func_name}`, {arg_name} is only allowed to be a constant"))),
                    }
                }).transpose()?.flatten().unwrap_or(default))
            };

            let (precision_index, scale_index) =
                if args.len() > 1 && args[1].data_type()?.remove_nullable().is_string() {
                    (2, 3)
                } else {
                    (1, 2)
                };
            let precision = arg_fn(
                &args,
                precision_index,
                "precision",
                DEFAULT_DECIMAL_PRECISION,
            )?;
            let scale = arg_fn(&args, scale_index, "scale", DEFAULT_DECIMAL_SCALE)?;

            if let Err(err) = DecimalSize::new(precision as u8, scale as u8) {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, {}",
                    err,
                )));
            }

            params.push(Scalar::Number(NumberScalar::Int64(precision as _)));
            params.push(Scalar::Number(NumberScalar::Int64(scale as _)));
        }

        let raw_expr = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.clone(),
            args: arguments,
        };

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        let expr = type_check::rewrite_function_to_cast(expr);

        // Run constant folding for arguments of the scalar function.
        // This will be helpful to simplify some constant expressions, especially
        // the implicitly casted literal values, e.g. `timestamp > '2001-01-01'`
        // will be folded from `timestamp > to_timestamp('2001-01-01')` to `timestamp > 978307200000000`
        // Note: check function may reorder the args

        let mut folded_args = match &expr {
            expr::Expr::FunctionCall(expr::FunctionCall {
                function,
                args: checked_args,
                ..
            }) => checked_args
                .iter()
                .zip(
                    function
                        .signature
                        .args_type
                        .iter()
                        .map(DataType::is_generic),
                )
                .zip(args)
                .map(|((checked_arg, is_generic), arg)| {
                    if !arg.evaluable() {
                        return arg;
                    }
                    match self.try_fold_constant(checked_arg, !is_generic) {
                        Some(box (constant, _)) => constant,
                        _ => arg,
                    }
                })
                .collect(),
            _ => args,
        };

        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            self.adapter.set_result_cache_uncacheable();
        }

        if let Some(constant) = self.try_fold_constant(&expr, true) {
            return Ok(constant);
        }

        if let expr::Expr::Cast(expr::Cast {
            span,
            is_try,
            dest_type,
            ..
        }) = expr
        {
            assert_eq!(folded_args.len(), 1);
            return Ok(Box::new((
                CastExpr {
                    span,
                    is_try,
                    argument: Box::new(folded_args.pop().unwrap()),
                    target_type: Box::new(dest_type.clone()),
                }
                .into(),
                dest_type,
            )));
        }

        // reorder
        if func_name == "eq"
            && folded_args.len() == 2
            && matches!(folded_args[0], ScalarExpr::ConstantExpr(_))
            && !matches!(folded_args[1], ScalarExpr::ConstantExpr(_))
        {
            folded_args.swap(0, 1);
        }

        Ok(Box::new((
            FunctionCall {
                span,
                params,
                arguments: folded_args,
                func_name: func_name.to_string(),
            }
            .into(),
            expr.data_type().clone(),
        )))
    }
}

pub(super) fn builtin_scalar_function_name(func_name: &str) -> Option<&'static str> {
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
