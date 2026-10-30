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
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::TypeName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FoldRules;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::resolve_type_name;
use databend_common_expression::type_check;
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
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::DEFAULT_DECIMAL_PRECISION;
use super::DEFAULT_DECIMAL_SCALE;
use super::TypeCheckAdapter;
use super::TypeChecker;
use super::rewrite_function;
use super::rewrite_function::rewrite_function_name;
use super::scalar_fold::BoundScalar;
use super::scalar_fold::ScalarFacts;
use crate::binder::AliasLookup;
use crate::binder::NameResolutionResult;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

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
            && !func.has_explicit_lambda()
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
            .chain(
                self.adapter
                    .aggregate_function_registry()
                    .registered_names(),
            )
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

    /// GROUPING arguments must match grouping-sets items exactly (see
    /// `AggregateInfo::replace_grouping`), but alias-preferring clauses such as
    /// HAVING and ORDER BY can resolve a bare name to a same-name SELECT alias
    /// like `if(grouping(x) = 1, 0, x) AS x`, shadowing the group column `x`.
    /// Retarget such an argument to the input column when that column is a
    /// grouping-sets item. This only fires on arguments that would otherwise
    /// be rejected, so it never changes a previously working query.
    pub(super) fn grouping_argument_group_item(
        &self,
        arena: &CoreExprArena<'_>,
        arg: CoreExprId,
        resolved: &ScalarExpr,
    ) -> Option<ScalarExpr> {
        let aggregate_info = &self.bind_context.aggregate_info;
        if aggregate_info.grouping_sets().is_none()
            || aggregate_info.is_grouping_sets_item(resolved)
        {
            return None;
        }

        // Only bare unqualified names can be shadowed by SELECT aliases.
        let CoreExpr::ColumnRef {
            span,
            column:
                ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(ident),
                },
        } = arena.get(arg)
        else {
            return None;
        };

        // Unknown or ambiguous names keep the original resolution.
        let column = match self.bind_context.resolve_name(
            None,
            None,
            ident,
            AliasLookup::all(&[]),
            self.name_resolution_ctx,
        ) {
            Ok(NameResolutionResult::Column(column)) => column,
            _ => return None,
        };

        let candidate: ScalarExpr = BoundColumnRef {
            span: *span,
            column,
        }
        .into();
        aggregate_info
            .is_grouping_sets_item(&candidate)
            .then_some(candidate)
    }

    /// Rewrites that must run before generic child traversal. A recognized
    /// variant access owns its fallback too, so its base is never bound twice.
    pub(super) fn try_resolve_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        if TypeChecker::<()>::all_special_functions().contains(&Ascii::new(func_name))
            || rewrite_function_name(func_name).is_some()
        {
            return Some(Err(ErrorCode::Internal(format!(
                "special function {} should not be represented as core call",
                func_name
            ))));
        }

        // Variant access must be rewritten before resolving arguments. Otherwise a nested
        // `get(get(v, ...), ...)` chain binds intermediate virtual columns before the complete
        // path gets a chance to bind as one virtual column.
        self.try_resolve_variant_function(arena, span, func_name, args)
    }

    pub(super) fn resolve_call(
        &mut self,
        span: Span,
        func_name: &str,
        args: Vec<BoundScalar>,
    ) -> Result<BoundScalar> {
        if Self::is_vector_function(func_name) {
            let scalars = args
                .iter()
                .map(|arg| arg.scalar.clone())
                .collect::<Vec<_>>();
            if let Some(rewritten_vector_expr) =
                self.try_rewrite_vector_function(span, func_name, &scalars)
            {
                return rewritten_vector_expr.map(|pair| self.bind_derived_pair(pair));
            }
        }

        let result = self.resolve_bound_function_call(span, func_name, vec![], args)?;
        if (func_name == "eq" || func_name == "noteq")
            && Self::compares_variant_with_constant(&result.scalar)
        {
            let pair = self.rewrite_variant_compare_constant(result.scalar, result.data_type)?;
            Ok(self.bind_derived_pair(pair))
        } else {
            Ok(result)
        }
    }

    /// Whether `rewrite_variant_compare_constant` may rewrite the call.
    fn compares_variant_with_constant(scalar: &ScalarExpr) -> bool {
        let ScalarExpr::FunctionCall(func) = scalar else {
            return false;
        };
        func.arguments.len() == 2
            && func
                .arguments
                .iter()
                .any(|arg| matches!(arg, ScalarExpr::ConstantExpr(_)))
            && func
                .arguments
                .iter()
                .any(|arg| arg.data_type().remove_nullable() == DataType::Variant)
    }

    pub(super) fn resolve_cast_expr(
        &mut self,
        span: Span,
        scalar: ScalarExpr,
        data_type: DataType,
        target_type: &TypeName,
        is_try: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let child = self.bind_derived(scalar, data_type);
        self.resolve_bound_cast(span, child, target_type, is_try)
            .map(BoundScalar::into_pair)
    }

    pub(super) fn resolve_bound_cast(
        &mut self,
        span: Span,
        child: BoundScalar,
        target_type: &TypeName,
        is_try: bool,
    ) -> Result<BoundScalar> {
        if target_type == &TypeName::Variant {
            if let Some(result) =
                self.resolve_cast_to_variant(span, &child.data_type, &child.scalar, is_try)
            {
                return result.map(|pair| self.bind_derived_pair(pair));
            }
        }

        let raw_expr = RawExpr::Cast {
            span,
            is_try,
            expr: Box::new(Self::placeholder(0, &child.scalar, &child.facts)),
            dest_type: DataType::from(&resolve_type_name(target_type, true)?),
        };
        let checked_expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        let data_type = &child.data_type;

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

        let facts = self.cast_facts(span, is_try, &child.scalar, child.facts, &target_type);
        // Fold here, so that the parent is checked on the folded argument. The
        // cast is evaluated to `target_type`, which may be more nullable than
        // the checked cast, so that the value has the type it is given.
        if facts.column_free
            && facts.evaluable
            && facts.deterministic
            && let Some(value) = self.eval_cast(span, is_try, &child.scalar, &facts, &target_type)
        {
            return Ok(Self::folded_constant_of_type(span, value, target_type));
        }
        Ok(BoundScalar {
            scalar: CastExpr {
                span,
                is_try,
                argument: Box::new(child.scalar),
                target_type: Box::new(target_type.clone()),
            }
            .into(),
            data_type: target_type,
            facts,
        })
    }

    /// The value of `CAST(child AS target_type)` for a column-free child.
    fn eval_cast(
        &self,
        span: Span,
        is_try: bool,
        child: &ScalarExpr,
        facts: &ScalarFacts,
        target_type: &DataType,
    ) -> Option<Scalar> {
        let raw_expr = RawExpr::Cast {
            span,
            is_try,
            expr: Box::new(Self::placeholder(0, child, facts)),
            dest_type: target_type.clone(),
        };
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).ok()?;
        FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, &expr)
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
                if arg1.data_type().remove_nullable() == DataType::Variant
                    && !arg1.used_columns().is_empty()
                    && arg0.data_type().as_ref() == &DataType::String =>
            {
                (0, arg0)
            }
            (_, ScalarExpr::ConstantExpr(_))
                if arg0.data_type().remove_nullable() == DataType::Variant
                    && !arg0.used_columns().is_empty()
                    && arg1.data_type().as_ref() == &DataType::String =>
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
            return_type: Box::new(DataType::Variant),
        });
        let mut new_arguments = func.arguments.clone();
        new_arguments[constant_arg_index] = wrap_new_arg;

        let new_func = ScalarExpr::FunctionCall(FunctionCall {
            span: func.span,
            func_name: func.func_name.clone(),
            params: func.params.clone(),
            arguments: new_arguments,
            return_type: Box::new(data_type.clone()),
        });

        Ok(Box::new((new_func, data_type)))
    }

    pub fn resolve_scalar_function_call(
        &self,
        span: Span,
        func_name: &str,
        params: Vec<Scalar>,
        args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let args = args
            .into_iter()
            .map(|scalar| {
                let data_type = scalar.data_type().into_owned();
                self.bind_derived(scalar, data_type)
            })
            .collect();
        self.resolve_bound_function_call(span, func_name, params, args)
            .map(BoundScalar::into_pair)
    }

    /// Binds a scalar function call over bound arguments and folds it with
    /// the arguments' facts. Only this call is type checked; arguments are
    /// represented by placeholders of their types.
    pub(super) fn resolve_bound_function_call(
        &self,
        span: Span,
        func_name: &str,
        mut params: Vec<Scalar>,
        mut args: Vec<BoundScalar>,
    ) -> Result<BoundScalar> {
        // rewrite substr('xx', 0, xx) -> substr('xx', 1, xx)
        if (func_name == "substr" || func_name == "substring")
            && self
                .adapter
                .sql_dialect()
                .unwrap()
                .substr_index_zero_literal_as_one()
        {
            let mut scalars = args
                .iter()
                .map(|arg| arg.scalar.clone())
                .collect::<Vec<_>>();
            Self::rewrite_substring(&mut scalars);
            if scalars[1] != args[1].scalar {
                let scalar = scalars.swap_remove(1);
                let data_type = scalar.data_type().into_owned();
                args[1] = self.bind_derived(scalar, data_type);
            }
        }

        // inject the params
        if ["round", "truncate"].contains(&func_name)
            && !args.is_empty()
            && params.is_empty()
            && args[0].scalar.data_type().remove_nullable().is_decimal()
        {
            let scale = if args.len() == 2 {
                let scale: i64 = self.constant_number(args[1].scalar.span(), &args[1].scalar)?;
                scale.clamp(-76, 76)
            } else {
                0
            };
            params.push(Scalar::Number(NumberScalar::Int64(scale)));
        } else if func_name.eq_ignore_ascii_case("as_decimal") {
            // Convert the precision and scale argument of `as_decimal` to params
            if !params.is_empty() {
                if params.len() > 2 || args.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                        params.len(),
                        args.len()
                    )));
                }
            } else {
                if args.is_empty() || args.len() > 3 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}` require 1, 2 or 3 arguments, but got {} arguments",
                        args.len()
                    )));
                }
                let param_args = args.split_off(1);
                for arg in param_args.into_iter() {
                    let param: u8 = self.constant_number(arg.scalar.span(), &arg.scalar)?;
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
                    args.len()
                )));
            }
            let arg_fn = |args: &[BoundScalar],
                          index: usize,
                          arg_name: &str,
                          default: i64|
             -> Result<i64> {
                Ok(args
                    .get(index)
                    .map(|arg| match self.bound_constant_value(&arg.scalar, &arg.facts) {
                        Some((value, _)) => Ok(value.get_i64()),
                        None => Err(ErrorCode::SemanticError(format!(
                            "Invalid arguments for `{func_name}`, {arg_name} is only allowed to be a constant"
                        ))),
                    })
                    .transpose()?
                    .flatten()
                    .unwrap_or(default))
            };

            let (precision_index, scale_index) =
                if args.len() > 1 && args[1].scalar.data_type().remove_nullable().is_string() {
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

        let placeholders = args
            .iter()
            .enumerate()
            .map(|(index, arg)| Self::placeholder(index, &arg.scalar, &arg.facts))
            .collect::<Vec<_>>();
        let identity = match placeholders.as_slice() {
            [placeholder] => Some(type_check::check(placeholder, &BUILTIN_FUNCTIONS)?),
            _ => None,
        };
        let raw_expr = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.clone(),
            args: placeholders,
        };
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        if let Some(identity) = identity
            && expr == identity
        {
            let argument = args.pop().unwrap();
            if !argument.facts.deterministic {
                self.adapter.set_result_cache_uncacheable();
            }
            return Ok(BoundScalar {
                data_type: expr.data_type().clone(),
                ..argument
            });
        }

        match type_check::rewrite_function_to_cast(expr, &BUILTIN_FUNCTIONS) {
            EExpr::FunctionCall(call) => self.fold_bound_call(span, func_name, params, call, args),
            EExpr::Cast(cast) => {
                let child = args.pop().expect("a cast call has one argument");
                Ok(self.fold_bound_cast(cast, child))
            }
            expr => {
                let return_type = expr.into_data_type();
                let scalar = FunctionCall {
                    span,
                    params,
                    arguments: args.into_iter().map(|arg| arg.scalar).collect(),
                    func_name: func_name.to_string(),
                    return_type: Box::new(return_type.clone()),
                }
                .into();
                Ok(self.bind_derived(scalar, return_type))
            }
        }
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
