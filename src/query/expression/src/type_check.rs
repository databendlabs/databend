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

use itertools::Itertools;

use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::expression::Span;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::number::NumberDataType;
use crate::types::number::NumberScalar;
use crate::types::DataType;
use crate::Result;
use crate::Scalar;

pub fn check(ast: &RawExpr, fn_registry: &FunctionRegistry) -> Result<Expr> {
    match ast {
        RawExpr::Literal { span, lit } => {
            let (scalar, data_type) = check_literal(lit);
            Ok(Expr::Constant {
                span: span.clone(),
                scalar,
                data_type,
            })
        }
        RawExpr::ColumnRef {
            span,
            id,
            data_type,
        } => Ok(Expr::ColumnRef {
            span: span.clone(),
            id: *id,
            data_type: data_type.clone(),
        }),
        RawExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => {
            let wrapped_dest_type = if *is_try {
                wrap_nullable_for_try_cast(span.clone(), dest_type)?
            } else {
                dest_type.clone()
            };
            let expr = check(expr, fn_registry)?;
            if expr.data_type() == &wrapped_dest_type {
                Ok(expr)
            } else {
                // faster path to eval function for cast
                if let Some(cast_fn) = check_simple_cast(*is_try, dest_type) {
                    return check_function(span.clone(), &cast_fn, &[], &[expr], fn_registry);
                }
                Ok(Expr::Cast {
                    span: span.clone(),
                    is_try: *is_try,
                    expr: Box::new(expr),
                    dest_type: wrapped_dest_type,
                })
            }
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
            check_function(span.clone(), name, params, &args_expr, fn_registry)
        }
    }
}

fn wrap_nullable_for_try_cast(span: Span, ty: &DataType) -> Result<DataType> {
    match ty {
        DataType::Null => Err((span, "TRY_CAST() to NULL is not supported".to_string())),
        DataType::Nullable(_) => Ok(ty.clone()),
        DataType::Array(inner_ty) => Ok(DataType::Nullable(Box::new(DataType::Array(Box::new(
            wrap_nullable_for_try_cast(span, inner_ty)?,
        ))))),
        DataType::Tuple(fields_ty) => Ok(DataType::Nullable(Box::new(DataType::Tuple(
            fields_ty
                .iter()
                .map(|ty| wrap_nullable_for_try_cast(span.clone(), ty))
                .collect::<Result<Vec<_>>>()?,
        )))),
        _ => Ok(DataType::Nullable(Box::new(ty.clone()))),
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
            Scalar::Number(NumberScalar::Float32((*v).into())),
            DataType::Number(NumberDataType::Float32),
        ),
        Literal::Float64(v) => (
            Scalar::Number(NumberScalar::Float64((*v).into())),
            DataType::Number(NumberDataType::Float64),
        ),
        Literal::Boolean(v) => (Scalar::Boolean(*v), DataType::Boolean),
        Literal::String(v) => (Scalar::String(v.clone()), DataType::String),
    }
}

pub fn check_function(
    span: Span,
    name: &str,
    params: &[usize],
    args: &[Expr],
    fn_registry: &FunctionRegistry,
) -> Result<Expr> {
    let candidates = fn_registry.search_candidates(name, params, args);

    let mut fail_resaons = Vec::with_capacity(candidates.len());
    for (id, func) in &candidates {
        match try_check_function(span.clone(), args, &func.signature) {
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
            .map(|(sig, (_, reason))| format!("  {sig:<max_len$}  : {reason}"))
            .join("\n");

        write!(
            &mut msg,
            "\n\nhas tried possible overloads:\n{}",
            candidates_fail_reason
        )
        .unwrap();
    };

    Err((span, msg))
}

#[derive(Debug)]
pub struct Subsitution(pub HashMap<usize, DataType>);

impl Subsitution {
    pub fn empty() -> Self {
        Subsitution(HashMap::new())
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
                    (
                        None,
                        (format!("unable to find a common super type for `{ty1}` and `{ty2}`")),
                    )
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
            DataType::Generic(idx) => self
                .0
                .get(&idx)
                .cloned()
                .ok_or_else(|| (None, (format!("unbound generic type `T{idx}`")))),
            DataType::Nullable(box ty) => Ok(DataType::Nullable(Box::new(self.apply(ty)?))),
            DataType::Array(box ty) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            ty => Ok(ty),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function(
    span: Span,
    args: &[Expr],
    sig: &FunctionSignature,
) -> Result<(Vec<Expr>, DataType, Vec<DataType>)> {
    assert_eq!(args.len(), sig.args_type.len());

    let substs = args
        .iter()
        .map(Expr::data_type)
        .zip(&sig.args_type)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty).map_err(|(_, err)| (span.clone(), err)))
        .collect::<Result<Vec<_>>>()?;
    let subst = substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2).map_err(|(_, err)| (span.clone(), err)))?
        .unwrap_or_else(Subsitution::empty);

    let checked_args = args
        .iter()
        .zip(&sig.args_type)
        .map(|(arg, sig_type)| {
            let sig_type = subst.apply(sig_type.clone())?;
            Ok(if arg.data_type() == &sig_type {
                arg.clone()
            } else {
                Expr::Cast {
                    span: span.clone(),
                    is_try: false,
                    expr: Box::new(arg.clone()),
                    dest_type: sig_type,
                }
            })
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
                    None => Err((span.clone(), format!("unable to resolve generic T{idx}"))),
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok((checked_args, return_type, generics))
}

pub fn unify(src_ty: &DataType, dest_ty: &DataType) -> Result<Subsitution> {
    match (src_ty, dest_ty) {
        (DataType::Generic(_), _) => unreachable!("source type must not contain generic type"),
        (ty, DataType::Generic(idx)) => Ok(Subsitution::equation(*idx, ty.clone())),
        (DataType::Null, DataType::Nullable(_)) => Ok(Subsitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Ok(Subsitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (src_ty, DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty))
                .collect::<Result<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2))?
                .unwrap_or_else(Subsitution::empty);
            Ok(subst)
        }
        (src_ty, dest_ty) if can_auto_cast_to(src_ty, dest_ty) => Ok(Subsitution::empty()),
        _ => Err((
            None,
            (format!("unable to unify `{}` with `{}`", src_ty, dest_ty)),
        )),
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
        (_, DataType::Variant) => true,
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
            Some(DataType::Number(num1.lossless_super_type(num2)?))
        }
        _ => Some(DataType::Variant),
    }
}

pub fn check_simple_cast(is_try: bool, dest_type: &DataType) -> Option<String> {
    let prefix = if is_try { "try_" } else { "" };
    let cast_function_name = match dest_type {
        DataType::String => Some("to_string"),
        DataType::Number(NumberDataType::UInt8) => Some("to_uint8"),
        DataType::Number(NumberDataType::UInt16) => Some("to_uint16"),
        DataType::Number(NumberDataType::UInt32) => Some("to_uint32"),
        DataType::Number(NumberDataType::UInt64) => Some("to_uint64"),
        DataType::Number(NumberDataType::Int8) => Some("to_int8"),
        DataType::Number(NumberDataType::Int16) => Some("to_int16"),
        DataType::Number(NumberDataType::Int32) => Some("to_int32"),
        DataType::Number(NumberDataType::Int64) => Some("to_int64"),
        DataType::Number(NumberDataType::Float32) => Some("to_float32"),
        DataType::Number(NumberDataType::Float64) => Some("to_float64"),
        DataType::Timestamp => Some("to_timestamp"),
        DataType::Date => Some("to_date"),
        DataType::Variant => Some("to_variant"),
        _ => None,
    };
    cast_function_name.map(|name| format!("{prefix}{name}"))
}
