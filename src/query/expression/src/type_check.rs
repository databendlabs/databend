// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::fmt::Write;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use itertools::Itertools;

use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::number::NumberDataType;
use crate::types::number::NumberScalar;
use crate::types::DataType;
use crate::AutoCastSignature;
use crate::ColumnIndex;
use crate::Scalar;

pub fn check<Index: ColumnIndex>(
    ast: &RawExpr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    match ast {
        RawExpr::Literal { span, lit } => {
            let (scalar, data_type) = check_literal(lit);
            Ok(Expr::Constant {
                span: *span,
                scalar,
                data_type,
            })
        }
        RawExpr::ColumnRef {
            span,
            id,
            data_type,
            display_name,
        } => Ok(Expr::ColumnRef {
            span: *span,
            id: id.clone(),
            data_type: data_type.clone(),
            display_name: display_name.clone(),
        }),
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
            let args_expr: Vec<_> = args
                .iter()
                .map(|arg| check(arg, fn_registry))
                .try_collect()?;
            check_function(*span, name, params, &args_expr, fn_registry)
        }
    }
}

pub fn check_literal(literal: &Literal) -> (Scalar, DataType) {
    match literal {
        Literal::Null => (Scalar::Null, DataType::Null),
        Literal::UInt8(v) => (
            Scalar::Number(NumberScalar::UInt8(*v)),
            DataType::Number(NumberDataType::UInt8),
        ),
        Literal::UInt16(v) => (
            Scalar::Number(NumberScalar::UInt16(*v)),
            DataType::Number(NumberDataType::UInt16),
        ),
        Literal::UInt32(v) => (
            Scalar::Number(NumberScalar::UInt32(*v)),
            DataType::Number(NumberDataType::UInt32),
        ),
        Literal::UInt64(v) => (
            Scalar::Number(NumberScalar::UInt64(*v)),
            DataType::Number(NumberDataType::UInt64),
        ),
        Literal::Int8(v) => (
            Scalar::Number(NumberScalar::Int8(*v)),
            DataType::Number(NumberDataType::Int8),
        ),
        Literal::Int16(v) => (
            Scalar::Number(NumberScalar::Int16(*v)),
            DataType::Number(NumberDataType::Int16),
        ),
        Literal::Int32(v) => (
            Scalar::Number(NumberScalar::Int32(*v)),
            DataType::Number(NumberDataType::Int32),
        ),
        Literal::Int64(v) => (
            Scalar::Number(NumberScalar::Int64(*v)),
            DataType::Number(NumberDataType::Int64),
        ),
        Literal::Float32(v) => (
            Scalar::Number(NumberScalar::Float32(*v)),
            DataType::Number(NumberDataType::Float32),
        ),
        Literal::Float64(v) => (
            Scalar::Number(NumberScalar::Float64(*v)),
            DataType::Number(NumberDataType::Float64),
        ),
        Literal::Boolean(v) => (Scalar::Boolean(*v), DataType::Boolean),
        Literal::String(v) => (Scalar::String(v.clone()), DataType::String),
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

    if expr.data_type() == &wrapped_dest_type {
        Ok(expr)
    } else {
        // fast path to eval function for cast
        if let Some(cast_fn) = get_simple_cast_function(is_try, dest_type) {
            let params = if let DataType::Decimal(ty) = dest_type {
                vec![ty.precision() as usize, ty.scale() as usize]
            } else {
                vec![]
            };

            if let Ok(cast_expr) =
                check_function(span, &cast_fn, &params, &[expr.clone()], fn_registry)
            {
                if cast_expr.data_type() == &wrapped_dest_type {
                    return Ok(cast_expr);
                }
            }
        }

        Ok(Expr::Cast {
            span,
            is_try,
            expr: Box::new(expr),
            dest_type: wrapped_dest_type,
        })
    }
}

fn wrap_nullable_for_try_cast(span: Span, ty: &DataType) -> Result<DataType> {
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

pub fn check_function<Index: ColumnIndex>(
    span: Span,
    name: &str,
    params: &[usize],
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

    let additional_rules = fn_registry
        .get_casting_rules(name)
        .cloned()
        .unwrap_or_default();

    let mut fail_resaons = Vec::with_capacity(candidates.len());
    for (id, func) in &candidates {
        match try_check_function(args, &func.signature, &additional_rules, fn_registry) {
            Ok((checked_args, return_type, generics)) => {
                return Ok(Expr::FunctionCall {
                    span,
                    id: id.clone(),
                    function: func.clone(),
                    generics,
                    args: checked_args,
                    return_type,
                });
            }
            Err(err) => fail_resaons.push(err),
        }
    }

    let mut msg = if params.is_empty() {
        format!(
            "no overload satisfies `{name}({})`",
            args.iter()
                .map(|arg| arg.data_type().to_string())
                .join(", ")
        )
    } else {
        format!(
            "no overload satisfies `{name}({})({})`",
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

        let candidates_fail_reason = candidates_sig
            .into_iter()
            .zip(fail_resaons)
            .map(|(sig, err)| format!("  {sig:<max_len$}  : {}", err.message()))
            .join("\n");

        write!(
            &mut msg,
            "\n\nhas tried possible overloads:\n{}",
            candidates_fail_reason
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

    pub fn merge(mut self, other: Self) -> Result<Self> {
        for (idx, ty2) in other.0 {
            if let Some(ty1) = self.0.remove(&idx) {
                let common_ty = common_super_type(ty2.clone(), ty1.clone()).ok_or_else(|| {
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

    pub fn apply(&self, ty: DataType) -> Result<DataType> {
        match ty {
            DataType::Generic(idx) => self.0.get(&idx).cloned().ok_or_else(|| {
                ErrorCode::from_string_no_backtrace(format!("unbound generic type `T{idx}`"))
            }),
            DataType::Nullable(box ty) => Ok(DataType::Nullable(Box::new(self.apply(ty)?))),
            DataType::Array(box ty) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            DataType::Tuple(fields_ty) => {
                let fields_ty = fields_ty
                    .into_iter()
                    .map(|field_ty| self.apply(field_ty))
                    .collect::<Result<_>>()?;
                Ok(DataType::Tuple(fields_ty))
            }
            ty => Ok(ty),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function<Index: ColumnIndex>(
    args: &[Expr<Index>],
    sig: &FunctionSignature,
    additional_rules: &AutoCastSignature,
    fn_registry: &FunctionRegistry,
) -> Result<(Vec<Expr<Index>>, DataType, Vec<DataType>)> {
    assert_eq!(args.len(), sig.args_type.len());

    let substs = args
        .iter()
        .map(Expr::data_type)
        .zip(&sig.args_type)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty, additional_rules))
        .collect::<Result<Vec<_>>>()?;

    let subst = substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2))?
        .unwrap_or_else(Substitution::empty);

    let checked_args = args
        .iter()
        .zip(&sig.args_type)
        .map(|(arg, sig_type)| {
            let sig_type = subst.apply(sig_type.clone())?;
            check_cast(None, false, arg.clone(), &sig_type, fn_registry)
        })
        .collect::<Result<Vec<_>>>()?;

    let return_type = subst.apply(sig.return_type.clone())?;

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| match subst.0.get(&idx) {
                    Some(ty) => Ok(ty.clone()),
                    None => Err(ErrorCode::from_string_no_backtrace(format!(
                        "unable to resolve generic T{idx}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok((checked_args, return_type, generics))
}

pub fn unify(
    src_ty: &DataType,
    dest_ty: &DataType,
    additional_rules: &AutoCastSignature,
) -> Result<Substitution> {
    match (src_ty, dest_ty) {
        (DataType::Generic(_), _) => unreachable!("source type must not contain generic type"),
        (ty, DataType::Generic(idx)) => Ok(Substitution::equation(*idx, ty.clone())),
        (DataType::Null, DataType::Nullable(_)) => Ok(Substitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Ok(Substitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            unify(src_ty, dest_ty, additional_rules)
        }
        (src_ty, DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty, additional_rules),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => {
            unify(src_ty, dest_ty, additional_rules)
        }
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty, additional_rules))
                .collect::<Result<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2))?
                .unwrap_or_else(Substitution::empty);
            Ok(subst)
        }
        (src_ty, dest_ty) if can_auto_cast_to(src_ty, dest_ty) => Ok(Substitution::empty()),
        (src_ty, dest_ty)
            if additional_rules
                .iter()
                .any(|(src, dest)| src == src_ty && dest == dest_ty) =>
        {
            Ok(Substitution::empty())
        }
        _ => Err(ErrorCode::from_string_no_backtrace(format!(
            "unable to unify `{}` with `{}`",
            src_ty, dest_ty
        ))),
    }
}

pub fn can_auto_cast_to(src_ty: &DataType, dest_ty: &DataType) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,
        (DataType::Null, DataType::Nullable(_)) => true,
        (DataType::EmptyArray, DataType::Array(_)) => true,
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty)
        }
        (src_ty, DataType::Nullable(dest_ty)) => can_auto_cast_to(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => can_auto_cast_to(src_ty, dest_ty),
        (DataType::Number(src_num_ty), DataType::Number(dest_num_ty)) => {
            // all integer types can cast to int64
            (*dest_num_ty == NumberDataType::Int64 && !src_num_ty.is_float())
            // all numeric types can cast to float64
            || *dest_num_ty == NumberDataType::Float64
            || src_num_ty.can_lossless_cast_to(*dest_num_ty)
        }

        (DataType::Number(_) | DataType::Decimal(_), DataType::Decimal(_)) => true,

        // Note: comment these because : select 'str' -1 will auto transform into: `minus(CAST('str' AS Date), CAST(1 AS Int64))`
        // (DataType::String, DataType::Date) => true,
        // (DataType::String, DataType::Timestamp) => true,

        // Note: integer can't auto cast to boolean, because 1 = 2 will auto transform into: `true = true` if the register order is not correct
        // (DataType::Number(_), DataType::Boolean) => true,

        // Note: Variant is not super type any more, because '1' > 3 will auto transform into: `gt(CAST("1" AS Variant), CAST(3 AS Variant))`
        // (_, DataType::Variant) => true,
        _ => false,
    }
}

pub fn common_super_type(ty1: DataType, ty2: DataType) -> Option<DataType> {
    match (ty1, ty2) {
        (ty1, ty2) if ty1 == ty2 => Some(ty1),
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),
        (DataType::Nullable(box ty1), DataType::Nullable(box ty2))
        | (DataType::Nullable(box ty1), ty2)
        | (ty1, DataType::Nullable(box ty2)) => {
            Some(DataType::Nullable(Box::new(common_super_type(ty1, ty2)?)))
        }
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(box ty1), DataType::Array(box ty2)) => {
            Some(DataType::Array(Box::new(common_super_type(ty1, ty2)?)))
        }
        (DataType::Number(num1), DataType::Number(num2)) => {
            Some(DataType::Number(num1.super_type(num2)))
        }

        (DataType::String, DataType::Timestamp) | (DataType::Timestamp, DataType::String) => {
            Some(DataType::Timestamp)
        }
        (DataType::String, DataType::Date) | (DataType::Date, DataType::String) => {
            Some(DataType::Date)
        }
        (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date) => {
            Some(DataType::Timestamp)
        }
        _ => Some(DataType::Variant),
    }
}

pub fn get_simple_cast_function(is_try: bool, dest_type: &DataType) -> Option<String> {
    let mut function_name = format!("to_{}", dest_type.to_string().to_lowercase());
    if dest_type.is_decimal() {
        function_name = "to_decimal".to_owned();
    }

    if is_simple_cast_function(&function_name) {
        let prefix = if is_try { "try_" } else { "" };
        Some(format!("{prefix}{function_name}"))
    } else {
        None
    }
}

pub fn is_simple_cast_function(name: &str) -> bool {
    const SIMPLE_CAST_FUNCTIONS: &[&str; 16] = &[
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
        "to_date",
        "to_variant",
        "to_boolean",
        "to_decimal",
    ];
    SIMPLE_CAST_FUNCTIONS.contains(&name)
}
