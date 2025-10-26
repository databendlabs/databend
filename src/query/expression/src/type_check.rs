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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write;

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::cast_scalar;
use crate::expr::*;
use crate::expression::Expr;
use crate::expression::RawExpr;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::decimal::DecimalSize;
use crate::types::i256;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::Number;
use crate::visit_expr;
use crate::AutoCastRules;
use crate::ColumnIndex;
use crate::ConstantFolder;
use crate::DynamicCastRules;
use crate::ExprVisitor;
use crate::FunctionContext;
use crate::Scalar;

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
                    [e, Expr::Constant(Constant {
                        span,
                        scalar,
                        data_type,
                    })]
                    | [Expr::Constant(Constant {
                        span,
                        scalar,
                        data_type,
                    }), e] => {
                        let src_ty = data_type.remove_nullable();
                        let dest_ty = e.data_type().remove_nullable();

                        if dest_ty.is_integer() && src_ty.is_integer() {
                            if let Ok(casted_scalar) =
                                cast_scalar(*span, scalar.clone(), dest_ty, fn_registry)
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
    _: &FunctionRegistry,
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
    if expr.data_type() == &wrapped_dest_type {
        Ok(expr)
    } else if expr.data_type().wrap_nullable() == wrapped_dest_type {
        Ok(Expr::Cast(Cast {
            span,
            is_try,
            expr: Box::new(expr),
            dest_type: wrapped_dest_type,
        }))
    } else {
        if !can_cast_to(expr.data_type(), dest_type) {
            return Err(ErrorCode::BadArguments(format!(
                "unable to cast type `{}` to type `{}`",
                expr.data_type(),
                dest_type,
            ))
            .set_span(span));
        }

        Ok(Expr::Cast(Cast {
            span,
            is_try,
            expr: Box::new(expr),
            dest_type: wrapped_dest_type,
        }))
    }
}

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
    expr: &Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<String> {
    let origin_ty = expr.data_type();
    let (expr, _) = if origin_ty != &DataType::String {
        ConstantFolder::fold(
            &Expr::Cast(Cast {
                span,
                is_try: false,
                expr: Box::new(expr.clone()),
                dest_type: DataType::String,
            }),
            func_ctx,
            fn_registry,
        )
    } else {
        ConstantFolder::fold(expr, func_ctx, fn_registry)
    };

    match expr {
        Expr::Constant(Constant {
            scalar: Scalar::String(string),
            ..
        }) => Ok(string.clone()),
        _ => Err(
            ErrorCode::from_string_no_backtrace("expected string literal".to_string())
                .set_span(span),
        ),
    }
}

pub fn check_number<T: Number, Index: ColumnIndex>(
    span: Span,
    func_ctx: &FunctionContext,
    expr: &Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<T> {
    let origin_ty = expr.data_type();
    let (expr, _) = if origin_ty != &DataType::Number(T::data_type()) {
        ConstantFolder::fold(
            &Expr::Cast(Cast {
                span,
                is_try: false,
                expr: Box::new(expr.clone()),
                dest_type: DataType::Number(T::data_type()),
            }),
            func_ctx,
            fn_registry,
        )
    } else {
        ConstantFolder::fold(expr, func_ctx, fn_registry)
    };

    match expr {
        Expr::Constant(Constant {
            scalar: Scalar::Number(num),
            ..
        }) => T::try_downcast_scalar(&num).ok_or_else(|| {
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

    // to_string('a')
    if params.is_empty() && name.starts_with("to_") && args.len() == 1 {
        let type_name = args[0].data_type().remove_nullable();
        match get_simple_cast_function(false, &type_name, &type_name) {
            Some(n) if name.eq_ignore_ascii_case(&n) => return Ok(args[0].clone()),
            _ => {}
        }
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

    let mut fail_reasons = Vec::with_capacity(candidates.len());
    let mut checked_candidates = vec![];
    let args_not_const = args
        .iter()
        .map(Expr::contains_column_ref)
        .collect::<Vec<_>>();
    let need_sort = candidates.len() > 1 && args_not_const.iter().any(|contain| !*contain);
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
                        .zip(args_not_const.iter().copied())
                        .map(|(expr, not_const)| {
                            // smaller score win
                            if not_const && expr.is_cast() {
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

    let mut msg = if params.is_empty() {
        format!(
            "no function matches signature `{name}({})`, you might need to add explicit type casts.",
            args.iter()
                .map(|arg| arg.data_type().to_string())
                .join(", ")
        )
    } else {
        format!(
            "no function matches signature `{name}({})({})`, you might need to add explicit type casts.",
            params.iter().join(", "),
            args.iter()
                .map(|arg| arg.data_type().to_string())
                .join(", ")
        )
    };

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

        let shorten_msg = if candidates_len > take_len {
            format!("\n... and {} more", candidates_len - take_len)
        } else {
            "".to_string()
        };
        write!(
            &mut msg,
            "\n\ncandidate functions:\n{candidates_fail_reason}{shorten_msg}",
        )
        .unwrap();
    };

    Err(ErrorCode::SemanticError(msg).set_span(span))
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
            DataType::Nullable(box ty) => {
                let inner_ty = self.apply(ty)?;
                Ok(inner_ty.wrap_nullable())
            }
            DataType::Array(box ty) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            DataType::Map(box ty) => {
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
            let is_try = fn_registry.is_auto_try_cast_rule(arg.data_type(), &sig_type);
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
        (DataType::Map(box src_ty), DataType::Map(box dest_ty)) => match (src_ty, dest_ty) {
            (DataType::Tuple(_), DataType::Tuple(_)) => {
                unify(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
            }
            (_, _) => unreachable!(),
        },
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
        _ => Err(ErrorCode::from_string_no_backtrace(format!(
            "unable to unify `{}` with `{}`",
            src_ty, dest_ty
        ))),
    }
}

fn can_cast_to(src_ty: &DataType, dest_ty: &DataType) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,

        (DataType::Null, _)
        | (DataType::EmptyArray, DataType::Array(_))
        | (DataType::EmptyMap, DataType::Map(_))
        | (DataType::Variant, DataType::Array(_))
        | (DataType::Variant, DataType::Map(_)) => true,

        (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
            if fields_src_ty.len() == fields_dest_ty.len() =>
        {
            true
        }
        (DataType::Array(fields_src_ty), DataType::Vector(_))
            if matches!(
                fields_src_ty.remove_nullable(),
                DataType::Number(_) | DataType::Decimal(_)
            ) =>
        {
            true
        }
        (DataType::Nullable(box inner_src_ty), DataType::Nullable(box inner_dest_ty))
        | (DataType::Nullable(box inner_src_ty), inner_dest_ty)
        | (inner_src_ty, DataType::Nullable(box inner_dest_ty))
        | (DataType::Array(box inner_src_ty), DataType::Array(box inner_dest_ty))
        | (DataType::Map(box inner_src_ty), DataType::Map(box inner_dest_ty)) => {
            can_cast_to(inner_src_ty, inner_dest_ty)
        }

        (src_ty, dest_ty) => get_simple_cast_function(false, src_ty, dest_ty).is_some(),
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
        (DataType::Map(box src_ty), DataType::Map(box dest_ty)) => match (src_ty, dest_ty) {
            (DataType::Tuple(_), DataType::Tuple(_)) => {
                can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
            }
            (_, _) => unreachable!(),
        },
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys)) => {
            src_tys.len() == dest_tys.len()
                && src_tys.iter().zip(dest_tys).all(|(src_ty, dest_ty)| {
                    can_auto_cast_to(src_ty, dest_ty, auto_cast_rules, dynamic_cast_rules)
                })
        }
        (DataType::String, DataType::Decimal(_)) => true,
        (DataType::Decimal(x), DataType::Decimal(y)) => {
            x.scale() <= y.scale()
                && (x.leading_digits() <= y.leading_digits()
                    || y.precision() == i256::MAX_PRECISION)
        }
        (DataType::Number(n), DataType::Decimal(d)) if !n.is_float() => {
            let properties = n.get_decimal_properties().unwrap();
            properties.scale() <= d.scale()
                && properties.precision() - properties.scale() <= d.leading_digits()
        }
        // Only available for decimal --> f64, otherwise `sqrt(1234.56789)` will have signature: `sqrt(1234.56789::Float32)`
        (DataType::Decimal(_), DataType::Number(n)) if n.is_float64() => true,
        _ => false,
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
        (DataType::Nullable(box ty1), DataType::Nullable(box ty2))
        | (DataType::Nullable(box ty1), ty2)
        | (ty1, DataType::Nullable(box ty2)) => Some(DataType::Nullable(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(box ty1), DataType::Array(box ty2)) => Some(DataType::Array(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::EmptyMap, ty @ DataType::Map(_))
        | (ty @ DataType::Map(_), DataType::EmptyMap) => Some(ty),
        (DataType::Map(box ty1), DataType::Map(box ty2)) => Some(DataType::Map(Box::new(
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

pub fn get_simple_cast_function(
    is_try: bool,
    src_type: &DataType,
    dest_type: &DataType,
) -> Option<String> {
    let function_name = if dest_type.is_decimal() {
        "to_decimal".to_owned()
    } else if src_type.remove_nullable() == DataType::String
        && dest_type.remove_nullable() == DataType::Variant
    {
        // parse JSON string to variant instead of cast
        "parse_json".to_owned()
    } else if dest_type.is_timestamp_timezone() {
        "to_timestamp_timezone".to_owned()
    } else {
        format!("to_{}", dest_type.to_string().to_lowercase())
    };

    if is_simple_cast_function(&function_name) {
        let prefix: &str = if is_try { "try_" } else { "" };
        Some(format!("{prefix}{function_name}"))
    } else {
        None
    }
}

pub const ALL_SIMPLE_CAST_FUNCTIONS: &[&str] = &[
    "to_binary",
    "to_string",
    "to_uint8",
    "to_uint16",
    "to_uint32",
    "to_uint64",
    "to_int8",
    "to_int16",
    "to_int32",
    "to_int64",
    "to_float32",
    "to_float64",
    "to_timestamp",
    "to_timestamp_timezone",
    "to_interval",
    "to_date",
    "to_variant",
    "to_boolean",
    "to_decimal",
    "to_bitmap",
    "to_geometry",
    "parse_json",
];

fn is_simple_cast_function(name: &str) -> bool {
    ALL_SIMPLE_CAST_FUNCTIONS.contains(&name)
}

pub fn rewrite_function_to_cast<Index: ColumnIndex>(expr: Expr<Index>) -> Expr<Index> {
    match visit_expr(&expr, &mut RewriteCast).unwrap() {
        None => expr,
        Some(expr) => expr,
    }
}

struct RewriteCast;

impl<Index: ColumnIndex> ExprVisitor<Index> for RewriteCast {
    type Error = !;

    fn enter_function_call(
        &mut self,
        call: &FunctionCall<Index>,
    ) -> std::result::Result<Option<Expr<Index>>, Self::Error> {
        let expr = match Self::visit_function_call(call, self)? {
            Some(expr) => Cow::Owned(expr.into_function_call().unwrap()),
            None => Cow::Borrowed(call),
        };
        let FunctionCall {
            span,
            function,
            generics,
            args,
            return_type,
            ..
        } = expr.as_ref();
        if !generics.is_empty() || args.len() != 1 {
            return match expr {
                Cow::Borrowed(_) => Ok(None),
                Cow::Owned(call) => Ok(Some(call.into())),
            };
        }
        if function.signature.name == "parse_json"
            || function.signature.name == "to_timestamp_timezone"
        {
            return Ok(Some(Expr::Cast(Cast {
                span: *span,
                is_try: false,
                expr: Box::new(args.first().unwrap().clone()),
                dest_type: return_type.clone(),
            })));
        }
        let func_name = format!(
            "to_{}",
            return_type.remove_nullable().to_string().to_lowercase()
        );
        if function.signature.name == func_name {
            return Ok(Some(Expr::Cast(Cast {
                span: *span,
                is_try: false,
                expr: Box::new(args.first().unwrap().clone()),
                dest_type: return_type.clone(),
            })));
        };
        if function.signature.name == format!("try_{func_name}") {
            return Ok(Some(Expr::Cast(Cast {
                span: *span,
                is_try: true,
                expr: Box::new(args.first().unwrap().clone()),
                dest_type: return_type.clone(),
            })));
        }
        match expr {
            Cow::Borrowed(_) => Ok(None),
            Cow::Owned(call) => Ok(Some(call.into())),
        }
    }
}

pub fn convert_escape_pattern(pattern: &str, escape_char: char) -> String {
    let mut result = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        if c == escape_char {
            if let Some(next_char) = chars.next() {
                result.push('\\');
                result.push(next_char);
            } else {
                result.push(escape_char);
            }
        } else {
            result.push(c);
        }
    }

    result
}
