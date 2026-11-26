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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Write;

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use smallvec::SmallVec;

use crate::AutoCastRules;
use crate::ColumnIndex;
use crate::DynamicCastRules;
use crate::ExprVisitor;
use crate::FoldRules;
use crate::FunctionContext;
use crate::Scalar;
use crate::auto_cast_rules::decimal_auto_cast;
use crate::auto_cast_rules::is_auto_try_cast;
use crate::cast_plan::CastStep;
use crate::cast_plan::RegisteredCast;
use crate::cast_plan::resolve_cast_plan;
use crate::cast_scalar;
use crate::expr::*;
use crate::expression::Expr;
use crate::expression::RawExpr;
use crate::function::FunctionID;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::function::NullableInput;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::Number;
use crate::types::decimal::DecimalSize;
use crate::types::i256;
use crate::visit_expr;

const MAX_FUNCTION_MISMATCH_HINT_CANDIDATES: usize = 10;

#[recursive::recursive]
pub fn check<Index: ColumnIndex>(
    expr: &RawExpr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    match expr {
        RawExpr::Constant {
            span,
            scalar,
            data_type,
        } => Ok(Constant {
            span: *span,
            scalar: scalar.clone(),
            data_type: data_type
                .as_ref()
                .cloned()
                .unwrap_or_else(|| scalar.as_ref().infer_data_type()),
        }
        .into()),
        RawExpr::ColumnRef {
            span,
            id,
            data_type,
            display_name,
        } => Ok(ColumnRef {
            span: *span,
            id: id.clone(),
            data_type: data_type.clone(),
            display_name: display_name.clone(),
        }
        .into()),
        RawExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => {
            let expr = check(expr, fn_registry)?;
            check_cast(*span, *is_try, expr, dest_type, fn_registry)
        }
        RawExpr::FunctionCall {
            span,
            name,
            args,
            params,
        } => {
            let mut args_expr: Vec<_> = args
                .iter()
                .map(|arg| check(arg, fn_registry))
                .try_collect()?;

            // https://github.com/datafuselabs/databend/issues/11541
            // c:int16 = 12456 will be resolve as `to_int32(c) == to_int32(12456)`
            // This may hurt the bloom filter, we should try cast to literal as the datatype of column
            if name == "eq" && args_expr.len() == 2 {
                match args_expr.as_mut_slice() {
                    [
                        e,
                        Expr::Constant(Constant {
                            span,
                            scalar,
                            data_type,
                        }),
                    ]
                    | [
                        Expr::Constant(Constant {
                            span,
                            scalar,
                            data_type,
                        }),
                        e,
                    ] => {
                        let src_ty = data_type.remove_nullable();
                        let dest_ty = e.data_type().remove_nullable();

                        if dest_ty.is_integer() && src_ty.is_integer() {
                            if let Ok(casted_scalar) =
                                cast_scalar(*span, scalar.clone(), &dest_ty, fn_registry)
                            {
                                *scalar = casted_scalar;
                                *data_type = scalar.as_ref().infer_data_type();
                            }
                        }
                    }
                    _ => {}
                }
            }

            check_function(*span, name, params, &args_expr, fn_registry)
        }
        RawExpr::LambdaFunctionCall {
            span,
            name,
            args,
            lambda_expr,
            lambda_display,
            return_type,
        } => {
            let args: Vec<_> = args
                .iter()
                .map(|arg| check(arg, fn_registry))
                .try_collect()?;

            Ok(LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args,
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            }
            .into())
        }
    }
}

pub fn check_cast<Index: ColumnIndex>(
    span: Span,
    is_try: bool,
    expr: Expr<Index>,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    let wrapped_dest_type = if is_try {
        wrap_nullable_for_try_cast(span, dest_type)?
    } else {
        dest_type.clone()
    };

    if expr.data_type() == &DataType::String
        && dest_type.remove_nullable() == DataType::StageLocation
    {
        return Ok(expr);
    }
    let source_aggregate_state = match expr.data_type().remove_nullable() {
        DataType::AggregateState(state) => Some(state),
        _ => None,
    };
    let destination_aggregate_state = match wrapped_dest_type.remove_nullable() {
        DataType::AggregateState(state) => Some(state),
        _ => None,
    };
    if let (Some(source), Some(destination)) = (source_aggregate_state, destination_aggregate_state)
        && source != destination
    {
        return Err(ErrorCode::BadArguments(format!(
            "aggregate state metadata does not match: source {} version {}, destination {} version {}",
            source.function_name,
            source.state_version,
            destination.function_name,
            destination.state_version,
        ))
        .set_span(span));
    }
    // A cast type checks when it has a plan, and needs no node when its value already has the
    // layout of the destination.
    let Some(plan) = resolve_cast_plan(expr.data_type(), &wrapped_dest_type, fn_registry) else {
        return Err(ErrorCode::BadArguments(format!(
            "unable to cast type `{}` to type `{}`",
            expr.data_type(),
            dest_type,
        ))
        .set_span(span));
    };
    if matches!(plan.step, CastStep::Identity) {
        return Ok(expr);
    }
    Ok(Expr::Cast(Cast {
        span,
        is_try,
        expr: Box::new(expr),
        dest_type: wrapped_dest_type,
    }))
}

/// The destination type of `TRY_CAST` to `ty`: nullable, with nullable array elements and tuple
/// fields, so that a failed element or field becomes NULL on its own.
///
/// Known issue: the values of a map are not made nullable, so a value that fails makes the whole
/// map NULL, e.g. `TRY_CAST({'a': 1000} AS MAP(STRING, INT8))`. The base type checked the same
/// destination, and failed at runtime instead; the type is kept so that the result types of
/// existing queries do not change.
#[recursive::recursive]
pub fn wrap_nullable_for_try_cast(span: Span, ty: &DataType) -> Result<DataType> {
    match ty {
        DataType::Null => Err(ErrorCode::from_string_no_backtrace(
            "TRY_CAST() to NULL is not supported".to_string(),
        )
        .set_span(span)),
        DataType::Nullable(ty) => wrap_nullable_for_try_cast(span, ty),
        DataType::Array(inner_ty) => Ok(DataType::Nullable(Box::new(DataType::Array(Box::new(
            wrap_nullable_for_try_cast(span, inner_ty)?,
        ))))),
        DataType::Tuple(fields_ty) => Ok(DataType::Nullable(Box::new(DataType::Tuple(
            fields_ty
                .iter()
                .map(|ty| wrap_nullable_for_try_cast(span, ty))
                .collect::<Result<Vec<_>>>()?,
        )))),
        _ => Ok(DataType::Nullable(Box::new(ty.clone()))),
    }
}

pub fn check_string<Index: ColumnIndex>(
    span: Span,
    func_ctx: &FunctionContext,
    expr: Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<String> {
    let expr = if expr.data_type() != &DataType::String {
        Expr::Cast(Cast {
            span,
            is_try: false,
            expr: Box::new(expr),
            dest_type: DataType::String,
        })
    } else {
        expr
    };

    match FoldRules::eval_constant(func_ctx, fn_registry, &expr) {
        Some(Scalar::String(string)) => Ok(string),
        _ => Err(
            ErrorCode::from_string_no_backtrace("expected string literal".to_string())
                .set_span(span),
        ),
    }
}

pub fn check_number<T: Number, Index: ColumnIndex>(
    span: Span,
    func_ctx: &FunctionContext,
    expr: Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<T> {
    let origin_ty = expr.data_type().clone();
    let expr = if origin_ty != DataType::Number(T::data_type()) {
        Expr::Cast(Cast {
            span,
            is_try: false,
            expr: Box::new(expr),
            dest_type: DataType::Number(T::data_type()),
        })
    } else {
        expr
    };

    match FoldRules::eval_constant(func_ctx, fn_registry, &expr) {
        Some(Scalar::Number(num)) => T::try_downcast_scalar(&num).ok_or_else(|| {
            ErrorCode::InvalidArgument(format!("Expect {}, but got {}", T::data_type(), origin_ty))
                .set_span(span)
        }),
        _ => Err(ErrorCode::InvalidArgument(format!(
            "Need constant number, but got {}",
            expr.sql_display()
        ))
        .set_span(span)),
    }
}

#[recursive::recursive]
pub fn check_function<Index: ColumnIndex>(
    span: Span,
    name: &str,
    params: &[Scalar],
    args: &[Expr<Index>],
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    if let Some(original_fn_name) = fn_registry.aliases.get(name) {
        return check_function(span, original_fn_name, params, args, fn_registry);
    }

    let candidates = fn_registry.search_candidates(name, params, args);

    if candidates.is_empty() && !fn_registry.contains(name) {
        return Err(
            ErrorCode::UnknownFunction(format!("function `{name}` does not exist")).set_span(span),
        );
    }

    // Do not check grouping
    if name == "grouping" {
        debug_assert!(!candidates.is_empty());
        let (id, function) = candidates.into_iter().next().unwrap();
        let return_type = function.signature.return_type.clone();
        return Ok(Expr::FunctionCall(FunctionCall {
            span,
            id: Box::new(id),
            function,
            generics: vec![],
            args: args.to_vec(),
            return_type,
        }));
    }

    let auto_cast_rules = fn_registry.get_auto_cast_rules(name);
    let dynamic_cast_rules = fn_registry.get_dynamic_cast_rules(name);

    let mut fail_reasons = SmallVec::<[_; 1]>::with_capacity(candidates.len());
    let mut checked_candidates = vec![];
    let need_sort = candidates.len() > 1 && args.iter().any(|arg| !arg.contains_column_ref());
    let args_with_column = need_sort.then(|| {
        args.iter()
            .map(Expr::contains_column_ref)
            .collect::<Vec<_>>()
    });
    for (seq, (id, func)) in candidates.iter().enumerate() {
        match try_check_function(
            args,
            &func.signature,
            auto_cast_rules,
            &dynamic_cast_rules,
            fn_registry,
        ) {
            Ok((args, return_type, generics)) => {
                let score = if need_sort {
                    args.iter()
                        .zip(args_with_column.as_deref().unwrap().iter().copied())
                        .map(|(expr, contains_column)| {
                            // smaller score win
                            if contains_column && expr.is_cast() {
                                1
                            } else {
                                0
                            }
                        })
                        .sum::<usize>()
                } else {
                    0
                };
                let expr = Expr::FunctionCall(FunctionCall {
                    span,
                    id: Box::new(id.clone()),
                    function: func.clone(),
                    generics,
                    args,
                    return_type,
                });
                if !need_sort {
                    return Ok(expr);
                }
                checked_candidates.push((expr, score, seq));
            }
            Err(err) => fail_reasons.push(err),
        }
    }

    if !checked_candidates.is_empty() {
        checked_candidates.sort_by_key(|(_, score, seq)| std::cmp::Reverse((*score, *seq)));
        return Ok(checked_candidates.pop().unwrap().0);
    }

    let mut msg = format_no_matching_function_signature(name, params, args);

    if !candidates.is_empty() {
        let candidates_sig: Vec<_> = candidates
            .iter()
            .map(|(_, func)| func.signature.to_string())
            .collect();

        let max_len = candidates_sig.iter().map(|s| s.len()).max().unwrap_or(0);

        let candidates_len = candidates_sig.len();
        let take_len = candidates_len.min(3);
        let candidates_fail_reason = candidates_sig
            .into_iter()
            .take(3)
            .zip(fail_reasons)
            .map(|(sig, err)| format!("  {sig:<max_len$}  : {}", err.message()))
            .join("\n");

        write!(
            &mut msg,
            "\n\ncandidate functions:\n{candidates_fail_reason}",
        )
        .unwrap();
        if candidates_len > take_len {
            write!(&mut msg, "\n... and {} more", candidates_len - take_len).unwrap();
        }
    };

    Err(ErrorCode::SemanticError(msg).set_span(span))
}

/// Check `call` again for `args`, which may be nullable where its arguments
/// were not, without searching other overloads. A builtin overload takes them
/// as its signature allows, and a factory creates its function for them again,
/// as candidate search would. Otherwise the overloads that its registration
/// names for nullable arguments take them, see `NullableInput`.
pub fn check_nullable_arguments<Index: ColumnIndex>(
    call: &FunctionCall<Index>,
    args: Vec<Expr<Index>>,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    let name = call.id.name().into_owned();
    let params = call.id.params();
    // `check_function` does not check grouping.
    if name == "grouping" {
        return Ok(FunctionCall {
            span: call.span,
            id: call.id.clone(),
            function: call.function.clone(),
            generics: vec![],
            args,
            return_type: call.return_type.clone(),
        }
        .into());
    }

    let args_type = args
        .iter()
        .map(|arg| arg.data_type().clone())
        .collect::<Vec<_>>();
    let own = match call.id.as_ref() {
        FunctionID::Builtin { .. } => Some((call.id.as_ref().clone(), call.function.clone())),
        FunctionID::Factory { id, .. } => {
            fn_registry.create_factory_function(&name, *id, params, &args_type)
        }
    };
    let variants = match fn_registry.nullable_input(&call.id) {
        Some(
            NullableInput::PassthroughVariant { variant }
            | NullableInput::CombineVariant { variant }
            | NullableInput::NullAwareVariant { variant },
        ) => {
            let id = FunctionID::Builtin {
                name: name.clone(),
                id: *variant,
            };
            fn_registry
                .get(&id)
                .map(|function| (id, function))
                .into_iter()
                .collect()
        }
        Some(NullableInput::PassthroughFactory { factory }) => fn_registry
            .create_factory_function(&name, *factory, params, &args_type)
            .into_iter()
            .collect(),
        Some(NullableInput::NullAwareFactories { factories }) => factories
            .iter()
            .filter_map(|id| fn_registry.create_factory_function(&name, *id, params, &args_type))
            .collect(),
        None => vec![],
    };

    let auto_cast_rules = fn_registry.get_auto_cast_rules(&name);
    let dynamic_cast_rules = fn_registry.get_dynamic_cast_rules(&name);
    let mut reason = None;
    for (id, function) in own.into_iter().chain(variants) {
        match try_check_function(
            &args,
            &function.signature,
            auto_cast_rules,
            &dynamic_cast_rules,
            fn_registry,
        ) {
            Ok((args, return_type, generics)) => {
                return Ok(FunctionCall {
                    span: call.span,
                    id: Box::new(id),
                    function,
                    generics,
                    args,
                    return_type,
                }
                .into());
            }
            Err(err) => {
                reason.get_or_insert(err);
            }
        }
    }
    Err(ErrorCode::SemanticError(format!(
        "{} does not take {}{}",
        call.function.signature,
        format_function_signature_from_arg_types(
            &name,
            params,
            &args_type
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        ),
        reason.map_or(String::new(), |err| format!(": {}", err.message())),
    ))
    .set_span(call.span))
}

pub fn infer_function_return_type<T>(
    span: Span,
    name: &str,
    params: &[Scalar],
    argument_types: impl Iterator<Item = T>,
    fn_registry: &FunctionRegistry,
) -> Result<DataType>
where
    T: Borrow<DataType>,
{
    let arguments = argument_types
        .enumerate()
        .map(|(id, data_type)| {
            Expr::ColumnRef(ColumnRef {
                span,
                id,
                data_type: data_type.borrow().clone(),
                display_name: id.to_string(),
            })
        })
        .collect::<Vec<_>>();
    Ok(check_function(span, name, params, &arguments, fn_registry)?.into_data_type())
}

fn format_function_signature<Index: ColumnIndex>(
    name: &str,
    params: &[Scalar],
    args: &[Expr<Index>],
) -> String {
    let args = args
        .iter()
        .map(|arg| arg.data_type().to_string())
        .collect::<Vec<_>>();
    format_function_signature_from_arg_types(name, params, &args)
}

fn format_function_signature_from_arg_types(
    name: &str,
    params: &[Scalar],
    args: &[String],
) -> String {
    let args = args.iter().join(", ");
    if params.is_empty() {
        format!("{name}({args})")
    } else {
        format!("{name}({})({args})", params.iter().join(", "))
    }
}

fn format_no_matching_function_signature<Index: ColumnIndex>(
    name: &str,
    params: &[Scalar],
    args: &[Expr<Index>],
) -> String {
    format!(
        "no function matches signature `{}`, you might need to add explicit type casts.",
        format_function_signature(name, params, args)
    )
}

pub fn format_function_argument_mismatch_hint<Index: ColumnIndex>(
    name: &str,
    params: &[Scalar],
    args: &[Expr<Index>],
    fn_registry: &FunctionRegistry,
) -> Option<String> {
    let name = fn_registry
        .aliases
        .get(name)
        .map(|name| name.as_str())
        .unwrap_or(name);

    let candidates = fn_registry.search_candidates(name, params, args);
    if candidates.is_empty() {
        return None;
    }

    let auto_cast_rules = fn_registry.get_auto_cast_rules(name);
    let dynamic_cast_rules = fn_registry.get_dynamic_cast_rules(name);
    let mut concrete_candidates_sig = Vec::new();
    for (_, func) in &candidates {
        let Ok((checked_args, return_type, _)) = try_check_function(
            args,
            &func.signature,
            auto_cast_rules,
            &dynamic_cast_rules,
            fn_registry,
        ) else {
            continue;
        };

        if checked_args == args {
            return None;
        }

        concrete_candidates_sig.push(format!(
            "{} :: {}",
            format_function_signature(name, params, &checked_args),
            return_type
        ));
    }

    let mut msg = format_no_matching_function_signature(name, params, args);

    let candidates_sig = if concrete_candidates_sig.is_empty() {
        candidates
            .iter()
            .map(|(_, func)| func.signature.to_string())
            .collect()
    } else {
        concrete_candidates_sig
    };
    let (mut candidates_sig, nullable_candidates_sig): (Vec<_>, Vec<_>) = candidates_sig
        .into_iter()
        .unique()
        .partition(|sig| !sig.contains("NULL"));
    candidates_sig.extend(nullable_candidates_sig);

    let candidates_len = candidates_sig.len();
    let take_len = candidates_len.min(MAX_FUNCTION_MISMATCH_HINT_CANDIDATES);
    let candidates_sig = candidates_sig
        .into_iter()
        .take(take_len)
        .map(|sig| format!("  {sig}"))
        .join("\n");
    write!(&mut msg, "\n\ncandidate functions:\n{candidates_sig}").unwrap();
    if candidates_len > take_len {
        write!(&mut msg, "\n... and {} more", candidates_len - take_len).unwrap();
    }

    Some(msg)
}

#[derive(Debug)]
pub struct Substitution(pub HashMap<usize, DataType>);

impl Substitution {
    pub fn empty() -> Self {
        Substitution(HashMap::new())
    }

    pub fn equation(idx: usize, ty: DataType) -> Self {
        let mut subst = Self::empty();
        subst.0.insert(idx, ty);
        subst
    }

    pub fn merge(mut self, other: Self, auto_cast_rules: AutoCastRules) -> Result<Self> {
        for (idx, ty2) in other.0 {
            if let Some(ty1) = self.0.remove(&idx) {
                let common_ty = common_super_type(ty2.clone(), ty1.clone(), auto_cast_rules)
                    .ok_or_else(|| {
                        ErrorCode::from_string_no_backtrace(format!(
                            "unable to find a common super type for `{ty1}` and `{ty2}`"
                        ))
                    })?;
                self.0.insert(idx, common_ty);
            } else {
                self.0.insert(idx, ty2);
            }
        }

        Ok(self)
    }

    pub fn apply(&self, ty: &DataType) -> Result<DataType> {
        match ty {
            DataType::Generic(idx) => self.0.get(idx).cloned().ok_or_else(|| {
                ErrorCode::from_string_no_backtrace(format!("unbound generic type `T{idx}`"))
            }),
            DataType::Nullable(deref!(ty)) => {
                let inner_ty = self.apply(ty)?;
                Ok(inner_ty.wrap_nullable())
            }
            DataType::Array(deref!(ty)) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            DataType::Map(deref!(ty)) => {
                let inner_ty = self.apply(ty)?;
                Ok(DataType::Map(Box::new(inner_ty)))
            }
            DataType::Tuple(fields_ty) => {
                let fields_ty = fields_ty
                    .iter()
                    .map(|field_ty| self.apply(field_ty))
                    .collect::<Result<_>>()?;
                Ok(DataType::Tuple(fields_ty))
            }
            ty => Ok(ty.clone()),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function<Index: ColumnIndex>(
    args: &[Expr<Index>],
    sig: &FunctionSignature,
    auto_cast_rules: AutoCastRules,
    dynamic_cast_rules: &DynamicCastRules,
    fn_registry: &FunctionRegistry,
) -> Result<(Vec<Expr<Index>>, DataType, Vec<DataType>)> {
    let subst = try_unify_signature(
        args.iter().map(Expr::data_type),
        sig.args_type.iter(),
        auto_cast_rules,
        dynamic_cast_rules,
    )?;

    let checked_args = args
        .iter()
        .zip(&sig.args_type)
        .map(|(arg, sig_type)| {
            let sig_type = subst.apply(sig_type)?;
            let is_try = is_auto_try_cast(arg.data_type(), &sig_type);
            check_cast(arg.span(), is_try, arg.clone(), &sig_type, fn_registry)
        })
        .collect::<Result<Vec<_>>>()?;
    let return_type = subst.apply(&sig.return_type)?;
    assert!(!return_type.has_nested_nullable());

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| {
                    subst.0.get(&idx).cloned().ok_or_else(|| {
                        ErrorCode::from_string_no_backtrace(format!(
                            "unable to resolve generic T{idx}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok((checked_args, return_type, generics))
}

pub fn try_unify_signature(
    src_tys: impl IntoIterator<Item = &DataType> + ExactSizeIterator,
    dest_tys: impl IntoIterator<Item = &DataType> + ExactSizeIterator,
    auto_cast_rules: AutoCastRules,
    dynamic_cast_rules: &DynamicCastRules,
) -> Result<Substitution> {
    if src_tys.len() != dest_tys.len() {
        return Err(ErrorCode::from_string_no_backtrace(format!(
            "expected {} arguments, got {}",
            dest_tys.len(),
            src_tys.len()
        )));
    }

    let substs = src_tys
        .into_iter()
        .zip(dest_tys)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules))
        .collect::<Result<Vec<_>>>()?;

    Ok(substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2, auto_cast_rules))?
        .unwrap_or_else(Substitution::empty))
}

pub fn unify(
    src_ty: &DataType,
    dest_ty: &DataType,
    auto_cast_rules: AutoCastRules,
    dynamic_cast_rules: &DynamicCastRules,
) -> Result<Substitution> {
    match (src_ty, dest_ty) {
        (ty, _) if ty.has_generic() => Err(ErrorCode::from_string_no_backtrace(
            "source type {src_ty} must not contain generic type".to_string(),
        )),
        (ty, DataType::Generic(idx)) => Ok(Substitution::equation(*idx, ty.clone())),
        (src_ty, dest_ty)
            if can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules) =>
        {
            Ok(Substitution::empty())
        }
        (DataType::Null, DataType::Nullable(_)) => Ok(Substitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Ok(Substitution::empty()),
        (DataType::EmptyMap, DataType::Map(_)) => Ok(Substitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (src_ty, DataType::Nullable(dest_ty)) => {
            unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => {
            unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (DataType::Map(deref!(src_ty)), DataType::Map(deref!(dest_ty))) => {
            match (src_ty, dest_ty) {
                (DataType::Tuple(_), DataType::Tuple(_)) => {
                    unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
                }
                (_, _) => unreachable!(),
            }
        }
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| {
                    unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
                })
                .collect::<Result<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2, auto_cast_rules))?
                .unwrap_or_else(Substitution::empty);
            Ok(subst)
        }
        (DataType::AggregateState(state), dest_ty) => unify(
            state.physical_type(),
            dest_ty,
            auto_cast_rules,
            dynamic_cast_rules,
        ),
        _ => Err(ErrorCode::from_string_no_backtrace(format!(
            "unable to unify `{}` with `{}`",
            src_ty, dest_ty
        ))),
    }
}

pub fn can_auto_cast_to(
    src_ty: &DataType,
    dest_ty: &DataType,
    auto_cast_rules: AutoCastRules,
    dynamic_cast_rules: &DynamicCastRules,
) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,
        (src_ty, dest_ty)
            if dynamic_cast_rules.iter().any(|r| r(src_ty, dest_ty))
                || auto_cast_rules
                    .iter()
                    .any(|(src, dest)| src == src_ty && dest == dest_ty) =>
        {
            true
        }
        (DataType::Null, DataType::Nullable(_)) => true,
        (DataType::EmptyArray, DataType::Array(_)) => true,
        (DataType::EmptyMap, DataType::Map(_)) => true,
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (src_ty, DataType::Nullable(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
        }
        (DataType::Map(deref!(src_ty)), DataType::Map(deref!(dest_ty))) => {
            match (src_ty, dest_ty) {
                (DataType::Tuple(_), DataType::Tuple(_)) => {
                    can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
                }
                (_, _) => unreachable!(),
            }
        }
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys)) => {
            src_tys.len() == dest_tys.len()
                && src_tys.iter().zip(dest_tys).all(|(src_ty, dest_ty)| {
                    can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
                })
        }
        (src_ty, dest_ty) => decimal_auto_cast(src_ty, dest_ty),
    }
}

pub fn common_super_type(
    ty1: DataType,
    ty2: DataType,
    auto_cast_rules: AutoCastRules,
) -> Option<DataType> {
    let dynamic_cast_rules = &vec![];
    match (ty1, ty2) {
        (ty1, ty2) if can_auto_cast_to(&ty1, &ty2, auto_cast_rules, dynamic_cast_rules) => {
            Some(ty2)
        }
        (ty1, ty2) if can_auto_cast_to(&ty2, &ty1, auto_cast_rules, dynamic_cast_rules) => {
            Some(ty1)
        }
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),
        (DataType::Nullable(deref!(ty1)), DataType::Nullable(deref!(ty2)))
        | (DataType::Nullable(deref!(ty1)), ty2)
        | (ty1, DataType::Nullable(deref!(ty2))) => Some(DataType::Nullable(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(deref!(ty1)), DataType::Array(deref!(ty2))) => Some(DataType::Array(
            Box::new(common_super_type(ty1, ty2, auto_cast_rules)?),
        )),
        (DataType::EmptyMap, ty @ DataType::Map(_))
        | (ty @ DataType::Map(_), DataType::EmptyMap) => Some(ty),
        (DataType::Map(deref!(ty1)), DataType::Map(deref!(ty2))) => Some(DataType::Map(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::Tuple(tys1), DataType::Tuple(tys2)) if tys1.len() == tys2.len() => {
            let tys = tys1
                .into_iter()
                .zip(tys2)
                .map(|(ty1, ty2)| common_super_type(ty1, ty2, auto_cast_rules))
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Tuple(tys))
        }
        (DataType::String, DataType::Number(num_ty))
        | (DataType::Number(num_ty), DataType::String) => {
            if num_ty.is_float() {
                Some(DataType::Number(num_ty))
            } else {
                let precision = i128::MAX_PRECISION;
                let scale = 5;
                Some(DataType::Decimal(DecimalSize::new(precision, scale).ok()?))
            }
        }
        (DataType::String, decimal_ty @ DataType::Decimal(_))
        | (decimal_ty @ DataType::Decimal(_), DataType::String) => Some(decimal_ty),
        (DataType::Decimal(a), DataType::Decimal(b)) => {
            let scale = a.scale().max(b.scale());
            let precision = scale + a.leading_digits().max(b.leading_digits());
            let precision =
                if a.precision() <= i128::MAX_PRECISION && b.precision() <= i128::MAX_PRECISION {
                    precision.min(i128::MAX_PRECISION)
                } else {
                    precision.min(i256::MAX_PRECISION)
                };

            Some(DataType::Decimal(DecimalSize::new(precision, scale).ok()?))
        }
        (DataType::Number(num_ty), DataType::Decimal(a))
        | (DataType::Decimal(a), DataType::Number(num_ty))
            if !num_ty.is_float() =>
        {
            let b = num_ty.get_decimal_properties().unwrap();

            let scale = a.scale().max(b.scale());
            let precision = a.leading_digits().max(b.leading_digits()) + scale;

            Some(DataType::Decimal(
                DecimalSize::new(precision.min(i256::MAX_PRECISION), scale).ok()?,
            ))
        }
        (DataType::Number(num_ty), DataType::Decimal(_))
        | (DataType::Decimal(_), DataType::Number(num_ty))
            if num_ty.is_float() =>
        {
            Some(DataType::Number(num_ty))
        }
        (ty1, ty2) => {
            let ty1_can_cast_to = auto_cast_rules
                .iter()
                .filter(|(src, _)| *src == ty1)
                .map(|(_, dest)| dest)
                .collect::<Vec<_>>();
            let ty2_can_cast_to = auto_cast_rules
                .iter()
                .filter(|(src, _)| *src == ty2)
                .map(|(_, dest)| dest)
                .collect::<Vec<_>>();
            ty1_can_cast_to
                .into_iter()
                .find(|ty| ty2_can_cast_to.contains(ty))
                .cloned()
        }
    }
}

/// Return a cast only when evaluating it invokes this exact checked function.
///
/// `to_x(a)` is `CAST(a AS X)` when the plan of the cast is a single step that runs the same
/// function overload, `function_id`, with the same return type. Composite container casts and
/// casts through other steps are not rewritten.
pub fn function_call_to_cast(
    function_id: &FunctionID,
    argument_type: &DataType,
    return_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Option<DataType> {
    // Filter unrelated unary functions before resolving the plan of the cast.
    let Some(RegisteredCast::Function(function)) = fn_registry.get_cast(argument_type, return_type)
    else {
        return None;
    };
    if function.name != function_id.name() {
        return None;
    }

    let plan = resolve_cast_plan(argument_type, return_type, fn_registry)?;
    let CastStep::Function {
        call: cast_call, ..
    } = &plan.step
    else {
        return None;
    };
    let same_function = *function_id == cast_call.id
        && !plan.rejects_null()
        && return_type == &cast_call.return_type;
    same_function.then(|| return_type.clone())
}

pub fn rewrite_function_to_cast<Index: ColumnIndex>(
    expr: Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Expr<Index> {
    match visit_expr(&expr, &mut RewriteCast { fn_registry }).unwrap() {
        None => expr,
        Some(expr) => expr,
    }
}

struct RewriteCast<'a> {
    fn_registry: &'a FunctionRegistry,
}

impl RewriteCast<'_> {
    fn classify<Index: ColumnIndex>(&self, call: &FunctionCall<Index>) -> Option<DataType> {
        let [argument] = call.args.as_slice() else {
            return None;
        };
        function_call_to_cast(
            &call.id,
            argument.data_type(),
            &call.return_type,
            self.fn_registry,
        )
    }

    fn make_cast<Index: ColumnIndex>(
        span: Span,
        argument: Expr<Index>,
        dest_type: DataType,
    ) -> Expr<Index> {
        Expr::Cast(Cast {
            span,
            is_try: false,
            expr: Box::new(argument),
            dest_type,
        })
    }
}

impl<Index: ColumnIndex> ExprVisitor<Index> for RewriteCast<'_> {
    type Error = !;

    fn enter_function_call(
        &mut self,
        call: &FunctionCall<Index>,
    ) -> std::result::Result<Option<Expr<Index>>, Self::Error> {
        match Self::visit_function_call(call, self)? {
            Some(expr) => {
                let mut call = expr.into_function_call().unwrap();
                if call.args.len() != 1 {
                    return Ok(Some(Expr::FunctionCall(call)));
                }
                let Some(result) = self.classify(&call) else {
                    return Ok(Some(Expr::FunctionCall(call)));
                };
                let argument = call.args.pop().unwrap();
                Ok(Some(Self::make_cast(call.span, argument, result)))
            }
            None => {
                let [argument] = call.args.as_slice() else {
                    return Ok(None);
                };
                Ok(self
                    .classify(call)
                    .map(|result| Self::make_cast(call.span, argument.clone(), result)))
            }
        }
    }
}

pub fn convert_escape_pattern(pattern: &str, escape_char: char) -> String {
    let mut result = String::with_capacity(pattern.len() + 2);
    let mut chars = pattern.chars();

    while let Some(c) = chars.next() {
        if c == escape_char {
            match chars.next() {
                Some(next_char @ ('%' | '_' | '\\')) => {
                    result.push('\\');
                    result.push(next_char);
                }
                Some(next_char) => result.push(next_char),
                None => {
                    if escape_char == '\\' {
                        result.push_str("\\\\");
                    } else {
                        result.push(escape_char);
                    }
                }
            }
        } else if c == '\\' {
            result.push_str("\\\\");
        } else {
            result.push(c);
        }
    }

    result
}
